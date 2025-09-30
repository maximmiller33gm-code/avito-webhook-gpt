// server.js
import express from 'express';
import { promises as fs } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// ---- Paths & env
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const PORT            = Number(process.env.PORT || 8080);
const LOG_DIR         = process.env.LOG_DIR  || '/mnt/data/logs';
const TASK_DIR        = process.env.TASK_DIR || '/mnt/data/tasks'; // на будущее
const WEBHOOK_SECRET  = process.env.WEBHOOK_SECRET || '';          // если пусто — не проверяем
const REDIS_URL       = process.env.REDIS_URL || '';               // если пусто — без Redis
const HISTORY_TTL_SEC = Number(process.env.HISTORY_TTL_SEC || 3 * 24 * 3600);
const HISTORY_LIMIT   = Number(process.env.HISTORY_LIMIT || 100);

// ---- Optional Redis
let redis = null;
if (REDIS_URL) {
  try {
    const { createClient } = await import('redis');
    redis = createClient({ url: REDIS_URL });
    redis.on('error', (e) => console.error('[REDIS] error', e));
    await redis.connect();
    console.log('✅ Redis connected');
  } catch (e) {
    console.error('❌ Redis init failed:', e.message);
    redis = null;
  }
}

// ---- Helpers
const ensureDir = async (dir) => { await fs.mkdir(dir, { recursive: true }); };

const nowIso = () => new Date().toISOString();

const dayLogPath = () => {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return path.join(LOG_DIR, `logs.${y}${m}${day}.log`);
};

const appendLog = async (line) => {
  try {
    await ensureDir(LOG_DIR);
    await fs.appendFile(dayLogPath(), line + '\n', 'utf8');
  } catch (e) {
    console.error('[LOG] append failed:', e.message);
  }
};

const histKey = (account, chatId) => `hist:${account}:${chatId}`;

const saveToHistory = async (account, valueObj) => {
  if (!redis) return;
  try {
    const chatId = valueObj?.chat_id;
    if (!chatId) return;

    const text = String(valueObj?.content?.text || '');
    const isSystem = valueObj?.author_id === 0 || /Системное сообщение/i.test(text);
    if (isSystem) return; // не сохраняем системные сообщения

    const key = histKey(account, chatId);
    const entry = {
      ts: Date.now(),
      author_id: valueObj.author_id,
      text,
      type: valueObj.type,
      item_id: valueObj.item_id || null
    };

    // Ограничим длину и выставим TTL
    await redis.multi()
      .rPush(key, JSON.stringify(entry))
      .lTrim(key, -HISTORY_LIMIT, -1)
      .expire(key, HISTORY_TTL_SEC)
      .exec();

  } catch (e) {
    console.error('[REDIS] save history failed:', e.message);
  }
};

// ---- App
const app = express();
app.use(express.json({ limit: '1mb' }));

app.get('/', (req, res) => res.json({ ok: true, redis: Boolean(redis) }));

// ---- Основной вебхук
app.post('/webhook/:account', async (req, res) => {
  const account = req.params.account;

  // 1) Игнорируем всё, что идёт на hr-gpt
  if (account === 'hr-gpt') {
    await appendLog(`[WEBHOOK] ${account} ignored @ ${nowIso()}`);
    return res.status(200).json({ ok: true, ignored: true, account });
  }

  // 2) Если задан секрет — проверим только для реальных аккаунтов (например, personalpro)
  if (WEBHOOK_SECRET) {
    const provided = req.headers['x-avito-secret'];
    if (!provided || String(provided) !== WEBHOOK_SECRET) {
      await appendLog(`[WEBHOOK] Forbidden (bad secret) for ${account} @ ${nowIso()} { providedLen: ${String(provided || '').length} }`);
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }
  }

  // 3) Логируем входящий JSON (красиво)
  const pretty = JSON.stringify(req.body || {}, null, 2);
  await appendLog(`=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n${pretty}\n=========================`);

  // 4) Спец-обработка для personalpro (можно расширять)
  if (account === 'personalpro') {
    const val = req.body?.payload?.value;
    if (val) {
      // Сохраняем историю, пропуская системные сообщения
      await saveToHistory(account, val);

      // Пример: Если текст = "Кандидат откликнулся" — тут можно создать задачу и т.п.
      // (каркас оставлен на будущее)
      const txt = String(val?.content?.text || '');
      if (/Кандидат откликнулся/i.test(txt)) {
        // TODO: create task в файловой очереди или в Redis, если нужно
        // await createTask({ account, chat_id: val.chat_id, reply_text: 'Здравствуйте!' });
      }
    }
  }

  return res.json({ ok: true, account });
});

// ---- Логи (debug)
app.get('/logs', async (_req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const list = await fs.readdir(LOG_DIR, { withFileTypes: true });
    const files = list
      .filter(d => d.isFile() && d.name.startsWith('logs.') && d.name.endsWith('.log'))
      .map(d => d.name)
      .sort();
    res.json({ ok: true, files });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/logs/read', async (req, res) => {
  const { file, tail } = req.query;
  if (!file) return res.status(400).send('missing ?file=');
  try {
    const fp = path.join(LOG_DIR, String(file));
    const data = await fs.readFile(fp, 'utf8');
    if (tail) {
      const n = Number(tail) || 20000;
      return res.type('text/plain').send(data.slice(-n));
    }
    res.type('text/plain').send(data);
  } catch (e) {
    res.status(404).send('not found');
  }
});

// ---- Запуск
app.listen(PORT, () => {
  console.log(`✅ Server running on port ${PORT}`);
  console.log(`LOG_DIR=${LOG_DIR}, TASK_DIR=${TASK_DIR}, SECRET=${WEBHOOK_SECRET ? '(set)' : '(not set)'}${redis ? ', Redis ON' : ''}`);
});
