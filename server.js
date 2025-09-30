// server.js — CommonJS/ESM-совместимый (если у тебя "type":"module", оставь как есть)
// Требует: express, crypto, fs, path

import express from 'express';
import fs from 'fs';
import { promises as fsp } from 'fs';
import path from 'path';
import crypto from 'crypto';
import process from 'process';
import { fileURLToPath } from 'url';

// __dirname для ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

/* ==================== ENV ==================== */
const PORT               = Number(process.env.PORT || 8080);
const TASK_KEY           = (process.env.TASK_KEY || 'kK9f4JQ7uX2pL0aN').trim();
const LOG_DIR            = (process.env.LOG_DIR  || '/mnt/data/logs').trim();
const TASK_DIR           = (process.env.TASK_DIR || '/mnt/data/tasks').trim();
const DEFAULT_REPLY      = (process.env.DEFAULT_REPLY || 'Здравствуйте!').trim();
const WEBHOOK_SECRET     = (process.env.WEBHOOK_SECRET || '').trim(); // если задан, ждём X-Avito-Secret === WEBHOOK_SECRET
const CLAIM_SCAN_MAX     = Math.max(1, Number(process.env.CLAIM_SCAN_MAX || 50)); // сколько свежих файлов смотреть при claim

/* ==================== helpers ==================== */
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
function nowIso() { return new Date().toISOString(); }
function genId() { return crypto.randomBytes(16).toString('hex'); }

function todayLogName() {
  const d  = new Date();
  const y  = d.getUTCFullYear();
  const m  = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `logs.${y}${m}${dd}.log`;
}

// лог в файл + консоль
async function appendLog(text) {
  console.log(text);
  await ensureDir(LOG_DIR);
  const f = path.join(LOG_DIR, todayLogName());
  await fsp.appendFile(f, text + '\n', 'utf8');
  return f;
}

function ok(res, extra = {}) { return res.send({ ok: true, ...extra }); }
function bad(res, code, msg) { return res.status(code).send({ ok: false, error: msg }); }

/* ==================== FILE QUEUE ==================== */
/**
 * Структура задачи (json):
 * {
 *   id, account, chat_id, reply_text, message_id, created_at,
 *   text,                // <— добавлено
 *   type_webhook         // "отклик" | "сообщение"  <— добавлено
 * }
 */
async function createTask({ account, chat_id, reply_text, message_id, text, type_webhook }) {
  await ensureDir(TASK_DIR);
  const id  = genId();
  const acc = (account || 'default').replace(/[^a-zA-Z0-9_-]/g, '_');

  const task = {
    id,
    account: acc,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: message_id || '',
    created_at: nowIso(),
    text: text || '',
    type_webhook: type_webhook || 'сообщение'
  };

  const file = path.join(TASK_DIR, `${acc}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(task, null, 2), 'utf8');
  return { file, task };
}

// claim: взять один из последних файлов (по mtime), макс. CLAIM_SCAN_MAX
async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith('.json'));

  // фильтр account
  if (account) {
    const pref = `${account}__`;
    files = files.filter(f => f.startsWith(pref));
  }
  if (!files.length) return null;

  // сортировка по mtime по убыванию
  files.sort((a, b) => {
    const ta = fs.statSync(path.join(TASK_DIR, a)).mtimeMs;
    const tb = fs.statSync(path.join(TASK_DIR, b)).mtimeMs;
    return tb - ta;
  });

  // ограничим окно просмотра
  files = files.slice(0, CLAIM_SCAN_MAX);

  for (const f of files) {
    const full   = path.join(TASK_DIR, f);
    const taking = full.replace(/\.json$/, '.json.taking');
    try {
      await fsp.rename(full, taking); // атомарная блокировка
      const raw = JSON.parse(await fsp.readFile(taking, 'utf8'));
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // уже кем-то взята — берём следующую
    }
  }
  return null;
}

async function doneTask(lockId) {
  try { await fsp.unlink(path.join(TASK_DIR, lockId)); } catch {}
  return true;
}

async function requeueTask(lockId) {
  const from = path.join(TASK_DIR, lockId);
  const to   = from.replace(/\.json\.taking$/, '.json');
  try { await fsp.rename(from, to); } catch {}
  return true;
}

/* ==================== APP ==================== */
const app = express();

// сохраняем оригинальное тело для лога
app.use(express.json({ limit: '1mb', verify: (req, res, buf) => { req.rawBody = buf?.toString('utf8') || ''; }}));

app.get('/', (req, res) => ok(res, { up: true }));

/* ===== DEBUG LOGS ===== */
app.get('/logs', async (req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = (await fsp.readdir(LOG_DIR))
      .filter(f => f.endsWith('.log'))
      .map(f => ({ name: f, mtime: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);
    res.send({ ok: true, files });
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

// быстрый поиск подтверждения (по двум последним логам)
app.get('/logs/has', async (req, res) => {
  const chat   = String(req.query.chat   || '').trim();
  const author = String(req.query.author || '').trim();
  if (!chat || !author) return bad(res, 400, 'chat & author required');

  await ensureDir(LOG_DIR);
  let files = (await fsp.readdir(LOG_DIR))
    .filter(f => f.endsWith('.log'))
    .map(f => ({ f, t: fs.statSync(path.join(LOG_DIR, f)).mtimeMs }))
    .sort((a, b) => b.t - a.t)
    .slice(0, 2);

  for (const it of files) {
    const full = path.join(LOG_DIR, it.f);
    let buf = await fsp.readFile(full, 'utf8');
    const has = buf.includes(`"chat_id": "${chat}"`) && buf.includes(`"author_id": ${author}`);
    if (has) return ok(res, { exists: true, files: files.map(x => x.f) });
  }
  return ok(res, { exists: false, files: files.map(x => x.f) });
});

/* ===== DEBUG TASKS ===== */
app.get('/tasks/debug', async (req, res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR)).sort();
    res.send({ ok: true, files });
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
    const json = JSON.parse(await fsp.readFile(full, 'utf8'));
    res.send(json);
  } catch (e) {
    bad(res, 500, String(e));
  }
});

/* ===== CLAIM / DONE / REQUEUE ===== */
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
    Account: task.account || '',
    Text: task.text || '',
    TypeWebhook: task.type_webhook || 'сообщение'
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

/* ==================== WEBHOOK ==================== */
/**
 * Правила создания задач:
 * - Если text содержит и "[Системное сообщение]" и "Кандидат откликнулся" — СОЗДАЁМ задачу с type_webhook="отклик".
 * - Если text НЕ содержит "[Системное сообщение]" — СОЗДАЁМ задачу с type_webhook="сообщение".
 * - Любые другие "системные" — ИГНОРИРУЕМ.
 */
app.post('/webhook/:account', async (req, res) => {
  const account = (req.params.account || 'default').trim();

  // секрет (если настроен)
  if (WEBHOOK_SECRET) {
    const headerSecret = String(req.headers['x-avito-secret'] || '').trim();
    if (headerSecret !== WEBHOOK_SECRET) {
      return bad(res, 403, 'forbidden');
    }
  }

  // логируем вход
  try {
    const hdr = JSON.stringify(req.headers || {}, null, 2);
    const bdy = req.rawBody && req.rawBody.length ? req.rawBody : JSON.stringify(req.body || {}, null, 2);
    await appendLog(`=== INCOMING WEBHOOK (${account}) @ ${nowIso()} ===\n-- HEADERS --\n${hdr}\n-- BODY --\n${bdy}\n=========================`);
  } catch { /* ignore */ }

  try {
    const payload = req.body?.payload || {};
    const val     = payload?.value || {};
    const msgType = String(val?.type || '').toLowerCase(); // 'text' | 'system' и т.д.
    const chatId  = val?.chat_id || '';
    const msgId   = val?.id || '';
    const itemId  = val?.item_id || '';
    const authorId= val?.author_id;
    const textRaw = String(val?.content?.text || '');

    // классификация
    const hasSystem = textRaw.includes('[Системное сообщение]');
    const isApply   = hasSystem && /Кандидат\s+откликнулся/i.test(textRaw);

    // правило создания задач
    let shouldCreate = false;
    let typeWebhook  = 'сообщение';

    if (isApply) {
      shouldCreate = true;
      typeWebhook  = 'отклик';
    } else if (!hasSystem) {
      shouldCreate = true;
      typeWebhook  = 'сообщение';
    } else {
      // другие системные сообщения игнорируем
      shouldCreate = false;
    }

    if (shouldCreate && chatId) {
      await createTask({
        account,
        chat_id: chatId,
        reply_text: DEFAULT_REPLY,
        message_id: msgId,
        text: textRaw,
        type_webhook: typeWebhook
      });
    }
  } catch (e) {
    // не заваливаем вебхук
  }

  return ok(res);
});

/* ==================== START ==================== */
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  console.log(`App root: ${path.resolve(__dirname)}`);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`CLAIM_SCAN_MAX=${CLAIM_SCAN_MAX}`);
  console.log(`Listening on :${PORT}`);
  // Healthcheck path можно настроить на "/"
})();
app.listen(PORT);
