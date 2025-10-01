// server.js  (ESM, package.json должен содержать: { "type": "module" })
import express from 'express';
import fs from 'fs';
import { promises as fsp } from 'fs';
import path from 'path';
import crypto from 'crypto';
import process from 'process';
import { fileURLToPath } from 'url';

// ===== ESM dirname =====
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ===== ENV =====
const PORT              = Number(process.env.PORT || 8080);
const TASK_KEY          = process.env.TASK_KEY || 'kK9f4JQ7uX2pL0aN';
const LOG_DIR           = process.env.LOG_DIR  || path.join(__dirname, 'logs');
const TASK_DIR          = process.env.TASK_DIR || path.join(__dirname, 'tasks');
const DEFAULT_REPLY     = process.env.DEFAULT_REPLY || 'Здравствуйте!';
const WEBHOOK_SECRET    = process.env.WEBHOOK_SECRET || '';  // если пусто — не проверяем
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';
const MAX_CLAIM_SCAN    = Number(process.env.MAX_CLAIM_SCAN || 50); // сколько файлов смотреть в claim

// ===== helpers =====
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

function ok(res, extra={}) { return res.send({ ok: true, ...extra }); }
function bad(res, code, msg) { return res.status(code).send({ ok: false, error: msg }); }

// ===== FILE QUEUE (файлы-задачи) =====
// структура задачи: { id, account, chat_id, reply_text, message_id, created_at }

async function createTask({ account, chat_id, reply_text, message_id }) {
  await ensureDir(TASK_DIR);
  const id  = genId();
  const acc = (account || 'hr-main').replace(/[^a-zA-Z0-9_-]/g, '_');

  const task = {
    id,
    account: acc,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: message_id || '',
    created_at: nowIso(),
  };

  const file = path.join(TASK_DIR, `${acc}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(task, null, 2), 'utf8');
  return task;
}

// Claim: смотрим по mtime последних N файлов (по умолчанию 50), с опц. фильтром по account
async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json'));

  // фильтр по account (префикс "<acc>__")
  if (account) {
    const pref = `${account}__`;
    files = files.filter(f => f.startsWith(pref));
  }

  // сортировка по дате изменения: новые вперёд
  files.sort((a, b) => {
    const ta = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
    const tb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
    return tb - ta;
  });

  // берём верхние MAX_CLAIM_SCAN
  files = files.slice(0, Math.max(1, MAX_CLAIM_SCAN));

  for (const f of files) {
    const full   = path.join(TASK_DIR, f);
    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // атомарная блокировка
      const raw = JSON.parse(await fsp.readFile(taking, 'utf8'));
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // кто-то успел раньше — пробуем следующий
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

// ===== лог-поиск подтверждения (для doneSafe) =====
async function hasConfirmationInTwoLogs({ chat, author }) {
  await ensureDir(LOG_DIR);
  const entries = (await fsp.readdir(LOG_DIR))
    .filter(f => f.endsWith('.log'))
    .map(f => ({ f, t: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
    .sort((a, b) => b.t - a.t)
    .slice(0, 2);

  if (entries.length === 0) return false;

  const wantChat   = `"chat_id": "${chat}"`;
  const wantAuthor = `"author_id": ${author}`;
  const wantType   = `"type": "text"`;

  const MAX = 500 * 1024; // читаем «хвост» до 500 КБ
  for (const e of entries) {
    const full = path.join(LOG_DIR, e.f);
    let buf = '';
    try { buf = await fsp.readFile(full, 'utf8'); } catch { continue; }
    if (buf.length > MAX) buf = buf.slice(buf.length - MAX);
    if (buf.includes(wantChat) && buf.includes(wantAuthor) && buf.includes(wantType)) {
      return true;
    }
  }
  return false;
}

// ===== APP =====
const app = express();
app.use(express.json({ limit: '1mb' }));

// health
app.get('/', (req, res) => ok(res, { up: true }));

// ===== ЛОГИ: список/чтение =====
app.get('/logs', async (req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = (await fsp.readdir(LOG_DIR))
      .filter(f => f.endsWith('.log'))
      .map(f => ({ name: f, mtime: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);
    res.send({ ok: true, files });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
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
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// Быстрый чек: есть ли в логах chat+author
app.get('/logs/has', async (req, res) => {
  const chat   = String(req.query.chat   || '').trim();
  const author = String(req.query.author || '').trim();
  if (!chat || !author) return bad(res, 400, 'chat & author required');

  const ok2 = await hasConfirmationInTwoLogs({ chat, author });
  return ok(res, { exists: ok2 });
});

// ===== DEBUG задач =====
app.get('/tasks/debug', async (req, res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR)).sort();
    res.send({ ok: true, files });
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// Прочитать конкретный файл задачи (в т.ч. .taking)
app.get('/tasks/read', async (req, res) => {
  try {
    const file = String(req.query.file || '').trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(TASK_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');
    const raw = JSON.parse(await fsp.readFile(full, 'utf8'));
    res.send(raw);
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e) });
  }
});

// ===== CLAIM / DONE / REQUEUE / DONESAFE =====
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
  return ok(res);
});

app.post('/tasks/requeue', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');
  await requeueTask(lock);
  return ok(res);
});

// doneSafe: подтверждаем исходящее наличие по логам
app.post('/tasks/doneSafe', async (req, res) => {
  if (!checkKey(req, res)) return;
  const lock = String(req.query.lock || req.body?.lock || '').trim();
  if (!lock || !lock.endsWith('.json.taking')) return bad(res, 400, 'lock invalid');

  // пытаемся получить chat/author либо из query, либо из самой задачи
  let chat = String(req.query.chat || '').trim();
  let author = String(req.query.author || '').trim();

  try {
    const full = path.join(TASK_DIR, lock);
    if (fs.existsSync(full)) {
      const raw = JSON.parse(await fsp.readFile(full, 'utf8'));
      if (!chat && raw.chat_id) chat = String(raw.chat_id);
      // author можно так и оставить обязательным в query — у разных аккаунтов свой id
    }
  } catch {}

  if (!chat || !author) return bad(res, 428, 'need chat & author to confirm');

  const ok2 = await hasConfirmationInTwoLogs({ chat, author });
  if (!ok2) return bad(res, 428, 'not confirmed in logs');

  await doneTask(lock);
  return res.status(204).send(); // как договорились: 204 = закрыто
});

// ===== WEBHOOK =====
// Принимаем на любом имени аккаунта: /webhook/:account
// Создаём задачу если:
//  1) system + текст содержит "Кандидат откликнулся"
//  2) type === "text" (обычное пользовательское сообщение)
// Проверка секрета: если WEBHOOK_SECRET задан, ждём X-Avito-Secret == WEBHOOK_SECRET или body.secret == WEBHOOK_SECRET
const seenSystemPerChat = new Set(); // защита от дублей в течение аптайма

app.post('/webhook/:account', async (req, res) => {
  const account = (req.params.account || 'hr-main').trim();

  // секрет
  if (WEBHOOK_SECRET) {
    const headerSecret = req.headers['x-avito-secret'];
    const bodySecret = req.body && req.body.secret;
    if (String(headerSecret || bodySecret || '') !== String(WEBHOOK_SECRET)) {
      return bad(res, 403, 'forbidden');
    }
  }

  // лог: заголовки + тело
  try {
    const header = `=== INCOMING WEBHOOK (${account}) @ ${nowIso()} ===\n-- HEADERS --\n${JSON.stringify(req.headers, null, 2)}\n-- BODY --\n${JSON.stringify(req.body || {}, null, 2)}\n=========================\n`;
    await appendLog(header);
  } catch {}

  // извлекаем полезное
  try {
    const payload = req.body?.payload || {};
    const val = payload?.value || {};
    const type = String(val?.type || '');        // "system" | "text"
    const text = String(val?.content?.text || '');
    const chatId = val?.chat_id;
    const msgId = val?.id || '';
    const itemId = val?.item_id || '';

    const isSystem = type === 'system';
    const isText   = type === 'text';

    // правило создания задачи
    let shouldCreate = false;

    if (isSystem) {
      // ровно системка "Кандидат откликнулся"
      if (/\[Системное сообщение\].*Кандидат\s+откликнулся/i.test(text)) {
        shouldCreate = true;
        if (ONLY_FIRST_SYSTEM) {
          const mark = `${account}:${chatId}:apply`;
          if (seenSystemPerChat.has(mark)) shouldCreate = false;
          else seenSystemPerChat.add(mark);
        }
      }
    } else if (isText) {
      // обычное текстовое сообщение пользователя
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
  } catch {
    // не мешаем вебхуку — всегда 200
  }

  return ok(res);
});

// ===== START =====
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);

  console.log(`App root: ${path.resolve(__dirname)}`);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`TASK_KEY set=${!!TASK_KEY}`);
  console.log(`WEBHOOK_SECRET set=${!!WEBHOOK_SECRET}`);
  console.log(`MAX_CLAIM_SCAN=${MAX_CLAIM_SCAN}`);

  app.listen(PORT, () => console.log(`Server on :${PORT}`));
})();
