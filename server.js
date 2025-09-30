// server.js
import express from 'express';
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

// ============ ENV ============
const PORT              = Number(process.env.PORT || 8080);
const WEBHOOK_SECRET    = process.env.WEBHOOK_SECRET || '';
const TASK_KEY          = process.env.TASK_KEY || '';
const DEFAULT_REPLY     = process.env.DEFAULT_REPLY || 'Здравствуйте!';
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';
const CLAIM_SCAN_LIMIT  = Number(process.env.CLAIM_SCAN_LIMIT || 50);
const LOG_DIR           = process.env.LOG_DIR || path.join('/mnt/data', 'logs');
const TASK_DIR          = process.env.TASK_DIR || path.join('/mnt/data', 'tasks');
const REDIS_URL         = process.env.REDIS_URL || '';

// ============ Redis (опционально) ============
let redis = null;
if (REDIS_URL) {
  const { createClient } = await import('redis');
  redis = createClient({ url: REDIS_URL });
  redis.on('error', (e) => console.error('[Redis] error', e));
  await redis.connect().catch(e => console.error('[Redis] connect failed', e));
}

// ============ FS helpers ============
async function ensureDir(dir) {
  await fsp.mkdir(dir, { recursive: true }).catch(() => {});
}
await ensureDir(LOG_DIR);
await ensureDir(TASK_DIR);

function nowIso() { return new Date().toISOString(); }

async function appendLog(text, fileBase = null) {
  const name = fileBase || `logs.${new Date().toISOString().slice(0,10).replaceAll('-','')}.log`;
  const full = path.join(LOG_DIR, name);
  await ensureDir(path.dirname(full));
  await fsp.appendFile(full, String(text) + '\n').catch(()=>{});
}

// ============ Chat history in Redis ============
function histKey(account, chatId) {
  // ключ: chat:hr-gpt:u2i-ABC
  return `chat:${account}:${chatId}`;
}
async function saveToHistory({ account, value }) {
  if (!redis) return;
  if (!value || !value.chat_id) return;

  // Фильтр системных сообщений: author_id === 0 ИЛИ текст начинается с "[Системное сообщение]"
  const text = String(value?.content?.text || '');
  const isSystem = (Number(value?.author_id) === 0) || text.startsWith('[Системное сообщение]');
  if (isSystem) return; // не сохраняем

  const role = (Number(value?.author_id) === 0) ? 'system' : 'user';
  const item = {
    ts: Date.now(),
    role,
    text,
    author_id: value.author_id,
    chat_id: value.chat_id,
    item_id: value.item_id || null,
  };

  const key = histKey(account, value.chat_id);
  // Храним последние 200 сообщений
  await redis.lPush(key, JSON.stringify(item));
  await redis.lTrim(key, 0, 199);
  // TTL 3 дня
  await redis.expire(key, 3 * 24 * 60 * 60);
}

// ============ Tasks ============
function genId(n = 16) {
  return crypto.randomBytes(n).toString('hex');
}
async function createTask({ account, chat_id, reply_text }) {
  const payload = {
    account,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: '',
    created_at: Date.now()
  };
  const fname = `${account}__${genId(16)}.json`;
  const full = path.join(TASK_DIR, fname);
  await fsp.writeFile(full, JSON.stringify(payload, null, 2));
  return fname;
}

// ============ HMAC check ============
function hmacOk(secret, rawBody, signatureHex) {
  try {
    const mac = crypto.createHmac('sha256', secret).update(rawBody).digest('hex');
    return crypto.timingSafeEqual(Buffer.from(mac, 'hex'), Buffer.from(signatureHex, 'hex'));
  } catch {
    return false;
  }
}

// ============ App ============
const app = express();

// Общий JSON для всех обычных маршрутов
app.use(express.json({ limit: '1mb' }));

// Health
app.get('/', (req, res) => {
  res.json({
    ok: true,
    time: nowIso(),
    redis: Boolean(redis),
    LOG_DIR,
    TASK_DIR,
  });
});

// Заглушка второго пути (чтобы не видеть лишние 403 в Railway HTTP Logs)
app.post('/webhook/personalpro', (req, res) => res.status(200).json({ ok: true }));

// Вебхук Авито: нужен "сырой" body для HMAC — используем raw только тут
app.post('/webhook/:account', express.raw({ type: '*/*', limit: '1mb' }), async (req, res) => {
  const account = req.params.account;

  // 1) Авторизация
  const providedSecret = req.headers['x-avito-secret'];
  const providedSign   = req.headers['x-avito-messenger-signature'];

  let authorized = false;
  if (WEBHOOK_SECRET) {
    if (providedSign) {
      // HMAC-подпись по «сырому» телу
      authorized = hmacOk(WEBHOOK_SECRET, req.body, String(providedSign));
    } else if (providedSecret) {
      // Простой заголовок
      authorized = (String(providedSecret) === WEBHOOK_SECRET);
    } else {
      authorized = false;
    }
  } else {
    // если секрет не задан — пропускаем (но логируем предупреждение)
    console.warn('[WEBHOOK] WARNING: WEBHOOK_SECRET is empty; skipping auth check');
    authorized = true;
  }

  if (!authorized) {
    await appendLog(`[WEBHOOK] Forbidden: секрет не совпадает {
  providedLen: ${String(providedSecret || providedSign || '').length},
  expectedLen: ${String(WEBHOOK_SECRET).length},
  headerKeys: ${JSON.stringify(Object.keys(req.headers).filter(k=>k.startsWith('x-avito')))}
}`);
    return res.status(403).json({ ok: false, error: 'forbidden' });
  }

  // 2) Разбор JSON (req.body сейчас — Buffer из raw())
  let json = {};
  try {
    json = req.body?.length ? JSON.parse(req.body.toString('utf8')) : {};
  } catch (e) {
    await appendLog(`[WEBHOOK] JSON parse error: ${e?.message}`);
    return res.status(400).json({ ok: false, error: 'bad json' });
  }

  // 3) Лог «как есть»
  await appendLog(`=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n${JSON.stringify(json, null, 2)}\n=========================\n`);

  // 4) Бизнес-логика
  const val = json?.payload?.value;
  if (val) {
    // a) сохраняем историю (НЕ сохраняем системные)
    await saveToHistory({ account, value: val });

    // b) создаём задачу при системном "Кандидат откликнулся..."
    const text = String(val?.content?.text || '');
    const isSystem = Number(val?.author_id) === 0;
    if (isSystem && /Кандидат откликнулся/iu.test(text)) {
      if (!ONLY_FIRST_SYSTEM || ONLY_FIRST_SYSTEM) {
        await createTask({ account, chat_id: val.chat_id, reply_text: DEFAULT_REPLY });
      }
    }
  }

  res.json({ ok: true });
});

// ===== Debug: логи =====
app.get('/logs', async (req, res) => {
  try {
    const files = (await fsp.readdir(LOG_DIR))
      .filter(n => n.startsWith('logs.') && n.endsWith('.log'))
      .map(n => ({ name: n, mtime: fs.statSync(path.join(LOG_DIR, n)).mtimeMs }));
    res.json({ ok: true, files });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/logs/read', async (req, res) => {
  try {
    const file = String(req.query.file || '');
    const p = path.join(LOG_DIR, file);
    const tail = Number(req.query.tail || 20000);
    const data = await fsp.readFile(p, 'utf8');
    res.type('text/plain').send(data.slice(-tail));
  } catch (e) {
    res.status(404).send('not found');
  }
});

// ===== Debug: задачи =====
app.get('/tasks/debug', async (_req, res) => {
  try {
    const files = (await fsp.readdir(TASK_DIR))
      .filter(n => n.endsWith('.json') || n.endsWith('.json.taking'))
      .sort();
    res.json({ ok: true, files });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// CLAIM: берёт первую свободную задачу
app.get('/tasks/claim', async (req, res) => {
  const key = String(req.query.key || '');
  const account = String(req.query.account || '');
  if (key !== TASK_KEY) return res.status(401).json({ ok: false, error: 'bad key' });

  try {
    const files = (await fsp.readdir(TASK_DIR))
      .filter(n => n.endsWith('.json') && !n.endsWith('.json.taking'))
      .filter(n => !account || n.startsWith(account + '__'))
      .slice(0, CLAIM_SCAN_LIMIT)
      .sort();

    if (!files.length) return res.json({ ok: true, has: false });

    const pick = files[0];
    const from = path.join(TASK_DIR, pick);
    const lock = `${pick}.taking`;
    const to = path.join(TASK_DIR, lock);

    await fsp.rename(from, to);

    const payload = JSON.parse(await fsp.readFile(to, 'utf8'));
    res.json({
      ok: true,
      has: true,
      lockId: lock,
      ChatId: payload.chat_id,
      ReplyText: payload.reply_text,
      MessageId: payload.message_id || '',
      Account: payload.account || ''
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// DONE: закрывает лок
app.post('/tasks/done', async (req, res) => {
  const key  = String(req.query.key || '');
  const lock = String(req.query.lock || '');
  if (key !== TASK_KEY) return res.status(401).json({ ok: false, error: 'bad key' });
  try {
    const p = path.join(TASK_DIR, lock);
    await fsp.unlink(p);
    res.json({ ok: true });
  } catch {
    res.status(404).json({ ok: false, error: 'not found' });
  }
});

// ===== Start
app.listen(PORT, () => {
  console.log(`Server on port ${PORT}`);
  console.log(`LOG_DIR=${LOG_DIR}, TASK_DIR=${TASK_DIR}, SECRET=${WEBHOOK_SECRET ? '(set)' : '(empty)'}`);
});
