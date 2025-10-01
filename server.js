// server.js
import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import { createClient as createRedisClient } from 'redis';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const app  = express();

// ---------- ENV ----------
const PORT             = Number(process.env.PORT || 8080);
const LOG_DIR          = process.env.LOG_DIR || path.join(__dirname, 'data', 'logs');
const TASK_DIR         = process.env.TASK_DIR || path.join(__dirname, 'data', 'tasks');
const TASK_KEY         = process.env.TASK_KEY || 'kK9f4JQ7uX2pL0aN';             // ключ для claim/done
const WEBHOOK_SECRET   = process.env.WEBHOOK_SECRET || '';                          // если пусто — проверку выключаем
const CLAIM_SCAN_LIMIT = Number(process.env.CLAIM_SCAN_LIMIT || 50);                // сколько файлов смотреть в claim
const DEFAULT_REPLY    = process.env.DEFAULT_REPLY || 'Здравствуйте!';

const REDIS_URL        = process.env.REDIS_URL || '';
const HISTORY_LIMIT    = Number(process.env.HISTORY_LIMIT || 100);
const HISTORY_TTL_SEC  = Number(process.env.HISTORY_TTL_SEC || 3 * 24 * 3600);     // 3 дня

// ---------- DIRS ----------
function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}
ensureDir(LOG_DIR);
ensureDir(TASK_DIR);

// ---------- REDIS (опц.) ----------
let redis = null;
if (REDIS_URL) {
  redis = createRedisClient({ url: REDIS_URL });
  redis.on('error', (e) => console.error('[REDIS] error', e.message));
  redis.connect().then(() => console.log('[REDIS] connected')).catch(() => {});
}
const histKey = (account, chatId) => `hist:${account}:${chatId}`;

// сохраняем в историю (кроме системных)
async function saveToHistory({ account, value }) {
  if (!redis || !value) return;

  const isSystem = String(value.type || '') === 'system'
                || /\[Системное сообщение\]/i.test(value?.content?.text || '');

  if (isSystem) return;

  const record = {
    ts: (value.created && Number(value.created) * 1000) || Date.now(),
    author_id: value.author_id || null,
    type: value.type || 'text',
    text: value?.content?.text || '',
    item_id: value.item_id || value.itemId || null,
  };

  const k = histKey(account, value.chat_id);
  try {
    await redis.rPush(k, JSON.stringify(record));
    await redis.lTrim(k, -HISTORY_LIMIT, -1);
    if (HISTORY_TTL_SEC > 0) await redis.expire(k, HISTORY_TTL_SEC);
  } catch (e) {
    console.error('[REDIS] save error', e.message);
  }
}

// ---------- LOG ----------
function nowIso() {
  return new Date().toISOString();
}
function curLogFile() {
  const d = new Date();
  const name = `logs.${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}${String(d.getDate()).padStart(2,'0')}.log`;
  return path.join(LOG_DIR, name);
}
async function appendLog(text) {
  await fs.promises.appendFile(curLogFile(), text + '\n', 'utf8');
}

// ---------- TASKS ----------
function genId() {
  return crypto.randomBytes(16).toString('hex');
}
function taskFileName(account, id) {
  return path.join(TASK_DIR, `${account}__${id}.json`);
}
async function createTask({ account, chat_id, reply_text = DEFAULT_REPLY, message_id = '', item_id = '' }) {
  const id = genId();
  const file = taskFileName(account, id);
  const payload = {
    id,
    account,
    chat_id,
    reply_text,
    message_id,
    item_id,
    created_at: nowIso(),
  };
  await fs.promises.writeFile(file, JSON.stringify(payload, null, 2), 'utf8');
  return file;
}

// ---------- MIDDLEWARE ----------
app.use(express.json({ limit: '1mb' }));

// ---------- HEALTH ----------
app.get('/', (req, res) => {
  res.json({
    ok: true,
    redis: Boolean(redis),
    log_dir: LOG_DIR,
    task_dir: TASK_DIR,
  });
});

// ---------- WEBHOOK ----------
app.post('/webhook/:account', async (req, res) => {
  const account = req.params.account;

  // --- авторизация: принимаем X-Avito-Secret ИЛИ X-Avito-Messenger-Signature
  if (WEBHOOK_SECRET) {
    const hdrSecret = req.get('x-avito-secret');
    const hdrSig    = req.get('x-avito-messenger-signature');
    const badSecret = hdrSecret && String(hdrSecret) !== String(WEBHOOK_SECRET);
    const noAuthHdr = !hdrSecret && !hdrSig;

    if (badSecret || noAuthHdr) {
      await appendLog(`[WEBHOOK] Forbidden: secret mismatch { providedLen: ${String(hdrSecret||'').length}, expectedLen: ${WEBHOOK_SECRET.length}, headerKeys: ${JSON.stringify(Object.keys(req.headers).filter(k=>k.startsWith('x-')))} }`);
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }
    // если есть x-avito-messenger-signature — пропускаем без HMAC-проверки (упрощение)
  }

  const pretty = JSON.stringify(req.body || {}, null, 2);
  await appendLog(`=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n${pretty}\n=========================`);

  try {
    const val = req.body?.payload?.value;
    if (val) {
      // сохраняем историю (кроме системных)
      await saveToHistory({ account, value: val });

      // Решение по созданию задачи
      const text = String(val?.content?.text || '');
      const hasSystem = /\[Системное сообщение\]/i.test(text);
      const hasApply  = /Кандидат\s+откликнулся/i.test(text);

      const shouldCreate =
        (hasSystem && hasApply) || !hasSystem;

      if (shouldCreate) {
        await createTask({
          account,
          chat_id: val.chat_id,
          reply_text: DEFAULT_REPLY,
          message_id: val.id || '',
          item_id: val.item_id || '',
        });
        await appendLog(`[TASK] created for ${account} chat=${val.chat_id}`);
      }
    }
  } catch (e) {
    await appendLog(`[WEBHOOK] error: ${e.message}`);
  }

  res.json({ ok: true });
});

// ---------- TASKS API ----------
app.get('/tasks/debug', async (req, res) => {
  try {
    const files = await fs.promises.readdir(TASK_DIR);
    const list = await Promise.all(files.map(async (name) => {
      const full = path.join(TASK_DIR, name);
      const st = await fs.promises.stat(full);
      return { name, mtime: st.mtimeMs };
    }));
    list.sort((a, b) => b.mtime - a.mtime);
    res.json({ ok: true, files: list.map(f => f.name) });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/tasks/read', async (req, res) => {
  const file = req.query.file;
  if (!file) return res.status(400).json({ ok: false, error: 'file required' });
  const full = path.join(TASK_DIR, path.basename(file));
  try {
    const txt = await fs.promises.readFile(full, 'utf8');
    res.type('application/json').send(txt);
  } catch {
    res.status(404).json({ ok: false, error: 'not found' });
  }
});

app.get('/tasks/claim', async (req, res) => {
  const key = req.query.key || '';
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: 'bad key' });

  const account = (req.query.account || '').trim();

  try {
    const files = (await fs.promises.readdir(TASK_DIR))
      .filter(n => n.endsWith('.json'))                      // только свободные
      .filter(n => !account || n.startsWith(`${account}__`))
      .sort((a, b) => {
        // по времени (новые сначала)
        const sa = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
        const sb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
        return sb - sa;
      })
      .slice(0, CLAIM_SCAN_LIMIT);

    if (!files.length) return res.json({ ok: true, has: false });

    const picked = files[0];
    const from = path.join(TASK_DIR, picked);
    const to   = from + '.taking';

    // создаём lock
    await fs.promises.rename(from, to);

    const json = JSON.parse(await fs.promises.readFile(to, 'utf8'));
    return res.json({
      ok: true,
      has: true,
      lockId: path.basename(to),
      ChatId: json.chat_id || '',
      ReplyText: json.reply_text || DEFAULT_REPLY,
      MessageId: json.message_id || '',
      Account: json.account || ''
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/tasks/done', async (req, res) => {
  const key  = req.query.key || '';
  const lock = req.query.lock || '';
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: 'bad key' });
  if (!lock) return res.status(400).json({ ok: false, error: 'lock required' });

  const full = path.join(TASK_DIR, path.basename(lock));
  try {
    await fs.promises.rm(full, { force: true });
    // на всякий случай удалим исходный .json (если позвали ошибочно)
    if (full.endsWith('.json.taking')) {
      const orig = full.slice(0, -('.taking'.length));
      await fs.promises.rm(orig, { force: true });
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/tasks/requeue', async (req, res) => {
  const key  = req.query.key || '';
  const lock = req.query.lock || '';
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: 'bad key' });
  if (!lock) return res.status(400).json({ ok: false, error: 'lock required' });

  const full = path.join(TASK_DIR, path.basename(lock));
  if (!full.endsWith('.json.taking'))
    return res.status(400).json({ ok: false, error: 'not a lock file' });

  try {
    await fs.promises.rename(full, full.replace(/\.taking$/, ''));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// doneSafe: 204 если в текущем логе есть запись с chat_id и author_id; иначе 428
app.post('/tasks/doneSafe', async (req, res) => {
  const key    = req.query.key    || '';
  const lock   = req.query.lock   || '';
  const chatId = req.query.chat   || '';
  const author = req.query.author || '';
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: 'bad key' });
  if (!lock) return res.status(400).json({ ok: false, error: 'lock required' });

  try {
    const lf = curLogFile();
    const txt = await fs.promises.readFile(lf, 'utf8').catch(() => '');
    const hasChat   = chatId && txt.includes(`"chat_id": "${chatId}"`);
    const hasAuthor = author && txt.includes(`"author_id": ${author}`);
    if (hasChat && hasAuthor) {
      // подтверждено — удаляем lock
      const full = path.join(TASK_DIR, path.basename(lock));
      await fs.promises.rm(full, { force: true });
      if (full.endsWith('.json.taking')) {
        const orig = full.slice(0, -('.taking'.length));
        await fs.promises.rm(orig, { force: true });
      }
      return res.status(204).end(); // No Content
    }
    return res.status(428).json({ ok: false, error: 'no confirmation yet' });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ---------- LOGS API ----------
app.get('/logs', async (req, res) => {
  try {
    const files = await fs.promises.readdir(LOG_DIR);
    const list = await Promise.all(files.map(async (name) => {
      const full = path.join(LOG_DIR, name);
      const st = await fs.promises.stat(full);
      return { name, mtime: st.mtimeMs };
    }));
    list.sort((a, b) => b.mtime - a.mtime);
    res.json({ ok: true, files: list });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/logs/read', async (req, res) => {
  const file = req.query.file;
  const tail = Number(req.query.tail || 0);
  if (!file) return res.status(400).json({ ok: false, error: 'file required' });

  const full = path.join(LOG_DIR, path.basename(file));
  try {
    let txt = await fs.promises.readFile(full, 'utf8');
    if (tail > 0 && txt.length > tail) {
      txt = txt.slice(-tail);
    }
    res.type('text/plain').send(txt);
  } catch (e) {
    res.status(404).json({ ok: false, error: 'not found' });
  }
});

// ---------- HISTORY API ----------
app.get('/history/:account/:chat', async (req, res) => {
  if (!redis) return res.json({ ok: true, count: 0, history: [] });
  const account = req.params.account;
  const chat    = req.params.chat;
  const k = histKey(account, chat);
  try {
    const arr = await redis.lRange(k, -HISTORY_LIMIT, -1);
    const history = arr.map(s => {
      try { return JSON.parse(s); } catch { return null; }
    }).filter(Boolean);
    res.json({ ok: true, count: history.length, history });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ---------- START ----------
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`LOG_DIR=${LOG_DIR}, TASK_DIR=${TASK_DIR}, SECRET=${WEBHOOK_SECRET ? '(set)' : '(empty)'}`);
});
