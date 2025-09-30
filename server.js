// server.js (ESM)
// package.json должен содержать: { "type": "module", "engines": { "node": ">=18" } }

import express from 'express';
import crypto from 'crypto';
import fs from 'fs';
import { promises as fsp } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// ====== ESM __dirname / __filename ======
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ====== ENV ======
const PORT = Number(process.env.PORT || 3000);
const TASK_KEY = (process.env.TASK_KEY || '').trim();
const LOG_DIR = process.env.LOG_DIR || '/mnt/data/logs';
const TASK_DIR = process.env.TASK_DIR || '/mnt/data/tasks';
const DEFAULT_REPLY = process.env.DEFAULT_REPLY || 'Здравствуйте!';
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';
const CLAIM_SCAN_LIMIT = Math.max(1, Number(process.env.CLAIM_SCAN_LIMIT || 50));

// ====== helpers ======
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
function nowIso() { return new Date().toISOString(); }
function genId() { return crypto.randomBytes(16).toString('hex'); }

function todayLogName() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `logs.${y}${m}${dd}.log`;
}

async function appendLog(text) {
  console.log(text);
  await ensureDir(LOG_DIR);
  const file = path.join(LOG_DIR, todayLogName());
  await fsp.appendFile(file, text + '\n', 'utf8');
  return file;
}

function ok(res, extra = {}) { return res.status(200).json({ ok: true, ...extra }); }
function bad(res, code, msg, extra = {}) { return res.status(code).json({ ok: false, error: msg, ...extra }); }

// ====== TASKS (file-queue) ======
// файл задачи: { id, account, chat_id, reply_text, message_text, message_type, message_id, item_id, created_at }

async function createTask({ account, chat_id, reply_text, message_text, message_type, message_id, item_id }) {
  await ensureDir(TASK_DIR);
  const id = genId();
  const acc = (account || 'hr-main').replace(/[^a-zA-Z0-9_-]/g, '_');

  const task = {
    id,
    account: acc,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_text: message_text || '',
    message_type: message_type || '',
    message_id: message_id || '',
    item_id: item_id || null,
    created_at: nowIso()
  };

  const file = path.join(TASK_DIR, `${acc}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(task, null, 2), 'utf8');
  return task;
}

// Claim: берём не более CLAIM_SCAN_LIMIT последних файлов (по mtime), опционально фильтруем по account префиксу.
// Файлы .json переименовываем в .json.taking (атомарно) и возвращаем их содержимое.
async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json'));

  // сортировка по mtime desc
  files.sort((a, b) => {
    const ta = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
    const tb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
    return tb - ta;
  });

  if (account) {
    const pref = `${account}__`;
    files = files.filter(f => f.startsWith(pref));
  }

  files = files.slice(0, CLAIM_SCAN_LIMIT);

  for (const f of files) {
    const full = path.join(TASK_DIR, f);
    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // захват
      const raw = JSON.parse(await fsp.readFile(taking, 'utf8'));
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // кто-то забрал параллельно — пробуем следующий
    }
  }
  return null;
}

async function doneTask(lockId) {
  const file = path.join(TASK_DIR, lockId);
  try { await fsp.unlink(file); } catch {}
  return true;
}

async function requeueTask(lockId) {
  const from = path.join(TASK_DIR, lockId);
  const to = from.replace(/\.json\.taking$/, '.json');
  try { await fsp.rename(from, to); } catch {}
  return true;
}

// ====== SIGNATURE CHECK (Avito) ======
// ВАЖНО: сохраняем "raw body" до парсинга JSON
const app = express();
app.use(express.json({
  verify: (req, _res, buf) => { req.rawBody = Buffer.from(buf); }
}));

function timingSafeEqualHex(hexA, hexB) {
  try {
    const a = Buffer.from(String(hexA || '').trim().toLowerCase(), 'hex');
    const b = Buffer.from(String(hexB || '').trim().toLowerCase(), 'hex');
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch { return false; }
}

function verifyAvitoSignature(req) {
  const secret = WEBHOOK_SECRET;
  if (!secret) return false;
  const given = (req.headers['x-avito-messenger-signature'] || '').toString().trim().toLowerCase();
  if (!given || !req.rawBody) return false;
  const calc = crypto.createHmac('sha256', secret).update(req.rawBody).digest('hex').toLowerCase();
  return timingSafeEqualHex(calc, given);
}

function verifyAvitoSecretHeader(req) {
  // fallback для ручных тестов
  const header = (req.headers['x-avito-secret'] || '').toString();
  return !!WEBHOOK_SECRET && header === WEBHOOK_SECRET;
}

// ====== ROUTES ======

// health
app.get('/', (req, res) => ok(res, { up: true }));

// debug: список файлов очереди
app.get('/tasks/debug', async (req, res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR)).sort();
    return ok(res, { files });
  } catch (e) {
    return bad(res, 500, String(e));
  }
});

// прочитать произвольный файл из очереди (для диагностики)
app.get('/tasks/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(TASK_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');
    const buf = await fsp.readFile(full, 'utf8');
    res.type('application/json').send(buf);
  } catch (e) {
    return bad(res, 500, String(e));
  }
});

// список логов
app.get('/logs', async (req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = (await fsp.readdir(LOG_DIR))
      .filter(f => f.endsWith('.log'))
      .map(f => ({ name: f, mtime: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);
    return ok(res, { files });
  } catch (e) {
    return bad(res, 500, String(e));
  }
});

// прочитать лог (целиком или хвост)
app.get('/logs/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(LOG_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');
    const tail = Math.max(1, Number(req.query.tail || 300000)); // 300KB по умолчанию
    let buf = await fsp.readFile(full, 'utf8');
    if (buf.length > tail) buf = buf.slice(buf.length - tail);
    res.type('text/plain').send(buf);
  } catch (e) {
    return bad(res, 500, String(e));
  }
});

// ====== /webhook/:account ======
app.post('/webhook/:account', async (req, res) => {
  const account = (req.params.account || 'hr-main').trim();

  // сигнатура (HMAC) ИЛИ тестовый секрет
  const okSig = verifyAvitoSignature(req);
  const okHdr = verifyAvitoSecretHeader(req);
  if (!okSig && !okHdr) {
    return bad(res, 403, 'forbidden');
  }

  // логируем заголовки и тело
  const headersSafe = JSON.stringify(req.headers, null, 2);
  const bodySafe = JSON.stringify(req.body || {}, null, 2);
  await appendLog(`=== INCOMING WEBHOOK (${account}) @ ${nowIso()} ===\n-- HEADERS --\n${headersSafe}\n-- BODY --\n${bodySafe}\n=========================`);

  // парсим полезное
  try {
    const payload = req.body?.payload || {};
    const val = payload?.value || {};
    const msgType = String(val?.type || '').toLowerCase(); // 'system' | 'text'
    const chatId = val?.chat_id;
    const msgId = val?.id || '';
    const itemId = val?.item_id || null;
    const text = String(val?.content?.text || '');

    if (!chatId) return ok(res); // ничего не ставим, но webhook успешен

    // фильтр: только "Кандидат откликнулся…" и/или любое живое текстовое (на твой выбор)
    const isSystem = msgType === 'system';
    const looksLikeApply = /кандидат\s+откликнулся/i.test(text);
    const isUserText = msgType === 'text';

    // защита "только первое системное по чату" в рамках запущенного процесса
    let allowed = true;
    const keySeen = `${account}:${chatId}`;
    app._seen = app._seen || new Set();
    if (ONLY_FIRST_SYSTEM && isSystem) {
      if (app._seen.has(keySeen)) allowed = false;
      else app._seen.add(keySeen);
    }

    if (allowed && (looksLikeApply || isUserText)) {
      await createTask({
        account,
        chat_id: chatId,
        reply_text: DEFAULT_REPLY,
        message_text: text,
        message_type: msgType,
        message_id: msgId,
        item_id: itemId
      });
    }
  } catch {
    // не валим webhook — отвечаем ok, чтобы Avito не ретраил
  }

  return ok(res);
});

// ====== tasks API ======
function checkKey(req, res) {
  const key = String(req.query.key || req.body?.key || '').trim();
  if (!TASK_KEY || key !== TASK_KEY) { bad(res, 403, 'bad key'); return false; }
  return true;
}

app.all('/tasks/claim', async (req, res) => {
  if (!checkKey(req, res)) return;
  const account = String(req.query.account || req.body?.account || '').trim();
  const got = await claimTask(account);
  if (!got) return ok(res, { has: false });

  const { task, lockId } = got;
  return ok(res, {
    has: true,
    lockId,
    Account: task.account || '',
    ChatId: task.chat_id || '',
    ReplyText: task.reply_text || DEFAULT_REPLY,
    MessageId: task.message_id || '',
    MessageText: task.message_text || '',
    MessageType: task.message_type || '',
    ItemId: task.item_id || null,
    CreatedAt: task.created_at || ''
  });
});

app.post('/tasks/done', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await doneTask(lock);
  return ok(res);
});

app.post('/tasks/requeue', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await requeueTask(lock);
  return ok(res);
});

// ====== START ======
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  console.log(`App root: ${path.resolve(__dirname)}`);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`ONLY_FIRST_SYSTEM=${ONLY_FIRST_SYSTEM}`);
  console.log(`CLAIM_SCAN_LIMIT=${CLAIM_SCAN_LIMIT}`);
  app.listen(PORT, () => console.log(`Server on :${PORT}`));
})();
