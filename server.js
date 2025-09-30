// server.js — простая очередь задач под вебхуки Авито (CJS)

const express = require('express');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const crypto = require('crypto');

// ===== CONFIG =====
const PORT = Number(process.env.PORT || 8080);

// Куда складывать логи и задачи
const LOG_DIR  = process.env.LOG_DIR  || '/mnt/data/logs';
const TASK_DIR = process.env.TASK_DIR || '/mnt/data/tasks';

// Ключ для claim/done/requeue
const TASK_KEY = process.env.TASK_KEY || 'kK9f4JQ7uX2pL0aN';

// Текст автоответа по умолчанию
const DEFAULT_REPLY = process.env.DEFAULT_REPLY || 'Здравствуйте!';

// Брать только первое системное сообщение по чату (антидубль за сессию)
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';

// Сколько последних файлов смотреть при claim (увеличено, чтобы ничего не «терялось»)
const CLAIM_WINDOW = Number(process.env.CLAIM_WINDOW || 50);

// ===== HELPERS =====
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
function nowIso() { return new Date().toISOString(); }
function genId() { return crypto.randomBytes(16).toString('hex'); }

function todayLogName() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth()+1).padStart(2, '0');
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

// ===== TASK QUEUE (file-based) =====
// Формат файла задачи: { id, account, chat_id, reply_text, message_id, created_at }

async function createTask({ account, chat_id, reply_text, message_id }) {
  await ensureDir(TASK_DIR);
  const id  = genId();
  const acc = (account || 'default').replace(/[^a-zA-Z0-9_-]/g, '_');

  const task = {
    id,
    account: acc,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: message_id || null,
    created_at: nowIso(),
  };

  const file = path.join(TASK_DIR, `${acc}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(task, null, 2), 'utf8');
  return { task, file };
}

async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json'));

  // фильтр по аккаунту
  if (account) {
    const pref = `${account}__`;
    files = files.filter(f => f.startsWith(pref));
  }

  // сортировка новые → старые по mtime
  files.sort((a, b) => {
    const ta = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
    const tb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
    return tb - ta;
  });

  // окно просмотра
  files = files.slice(0, CLAIM_WINDOW);

  for (const f of files) {
    const full   = path.join(TASK_DIR, f);
    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // атомарная «блокировка»
      const raw = JSON.parse(await fsp.readFile(taking, 'utf8'));
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // параллельный захват — пробуем следующий
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
  const to   = from.replace(/\.json\.taking$/, '.json');
  try { await fsp.rename(from, to); } catch {}
  return true;
}

// ===== APP =====
const app = express();
app.use(express.json({ limit: '1mb' }));

function ok(res, extra = {}) { return res.json({ ok: true, ...extra }); }
function bad(res, code, msg) { return res.status(code).json({ ok: false, error: msg }); }

// health
app.get('/', (_req, res) => ok(res, { up: true }));

// ===== LOGS API (диагностика) =====
app.get('/logs', async (_req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = (await fsp.readdir(LOG_DIR))
      .filter(f => f.endsWith('.log'))
      .map(f => ({ name: f, mtime: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);
    ok(res, { files });
  } catch (e) {
    bad(res, 500, String(e));
  }
});

app.get('/logs/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(LOG_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');
    const tail = Number(req.query.tail || 300000);
    let buf = await fsp.readFile(full, 'utf8');
    if (buf.length > tail) buf = buf.slice(buf.length - tail);
    res.type('text/plain').send(buf);
  } catch (e) {
    bad(res, 500, String(e));
  }
});

// ===== TASKS DEBUG =====
app.get('/tasks/debug', async (_req, res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR))
      .filter(f => f.endsWith('.json') || f.endsWith('.json.taking'))
      .sort();
    ok(res, { files });
  } catch (e) {
    bad(res, 500, String(e));
  }
});

app.get('/tasks/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(TASK_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');
    const raw = await fsp.readFile(full, 'utf8');
    res.type('application/json').send(raw);
  } catch (e) {
    bad(res, 500, String(e));
  }
});

// ===== AUTH FOR TASKS =====
function checkKey(req, res) {
  const key = String(req.query.key || req.body?.key || '').trim();
  if (!TASK_KEY || key !== TASK_KEY) { bad(res, 403, 'bad key'); return false; }
  return true;
}

// ===== CLAIM / DONE / REQUEUE =====
app.all('/tasks/claim', async (req, res) => {
  if (!checkKey(req, res)) return;
  const account = String(req.query.account || req.body?.account || '').trim();

  const got = await claimTask(account);
  if (!got) return ok(res, { has: false });

  const { task, lockId } = got;
  ok(res, {
    has: true,
    lockId,
    ChatId: task.chat_id,
    ReplyText: task.reply_text,
    MessageId: task.message_id || '',
    Account: task.account || ''
  });
});

app.post('/tasks/done', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await doneTask(lock);
  ok(res);
});

app.post('/tasks/requeue', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await requeueTask(lock);
  ok(res);
});

// ===== WEBHOOK (ловим любые имена аккаунтов) =====
const seenSystemToday = new Set(); // антидубль по (account:chat_id) для системного "Откликнулся"

app.post('/webhook/:account', async (req, res) => {
  const account = String(req.params.account || 'default');

  // ЛОГИРУЕМ ВСЁ: заголовки + тело
  try {
    const headTxt = JSON.stringify(req.headers || {}, null, 2);
    const bodyTxt = JSON.stringify(req.body || {},   null, 2);
    const head = `=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n-- HEADERS --\n${headTxt}\n-- BODY --\n${bodyTxt}\n=========================\n`;
    await appendLog(head);
  } catch {}

  try {
    const payload = req.body?.payload || {};
    const val     = payload?.value || {};
    const chatId  = val?.chat_id;
    const msgId   = val?.id || null;

    // Берём "сырую" строку текста как есть (на одной строке)
    const textRaw = String(val?.content?.text || '').replace(/\r?\n/g, ' ');
    // Признаки
    const hasSystemTag   = textRaw.includes('[Системное сообщение]');
    const hasCandidateKW = /кандидат\s+откликнулся/i.test(textRaw);

    // Условия создания задач:
    // 1) [Системное сообщение] И содержит "Кандидат откликнулся" → создаём (с антидублем)
    // 2) НЕ содержит [Системное сообщение] → создаём (обычный текст от пользователя)
    const isSystemCandidate = hasSystemTag && hasCandidateKW;
    const isUserText        = !hasSystemTag; // любое не-системное — это сообщ. пользователя

    let shouldCreate = false;
    if (isSystemCandidate) {
      // антидубль на сессию
      const key = `${account}:${chatId}`;
      if (!seenSystemToday.has(key)) {
        seenSystemToday.add(key);
        shouldCreate = true;
      }
    } else if (isUserText) {
      shouldCreate = true;
    }

    if (shouldCreate && chatId) {
      await createTask({
        account,
        chat_id: chatId,
        reply_text: DEFAULT_REPLY,
        message_id: msgId
      });
    }

  } catch (e) {
    await appendLog(`[WEBHOOK ${account}] handler error: ${String(e)}`);
  }

  // Авито всегда ждёт 200
  res.json({ ok: true });
});

// ===== START =====
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`TASK_KEY set: ${TASK_KEY ? 'yes' : 'no'}`);
  console.log(`ONLY_FIRST_SYSTEM=${ONLY_FIRST_SYSTEM}`);
  console.log(`CLAIM_WINDOW=${CLAIM_WINDOW}`);
  app.listen(PORT, () => console.log(`Server on :${PORT}`));
})();
