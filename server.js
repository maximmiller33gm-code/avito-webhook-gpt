@@ -1,29 +1,42 @@
// server.js — простая очередь задач под вебхуки Авито (CJS)
// server.js — Avito webhook + file queue + (опц.) Redis history (ESM)

const express = require('express');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const crypto = require('crypto');
import express from 'express';
import fs from 'fs';
import { promises as fsp } from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

// ===== ESM __dirname/filename =====
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

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
const CLAIM_WINDOW = Number(process.env.CLAIM_WINDOW || 50); // смотрим до 50 файлов при claim

// ===== (optional) Redis for history =====
let redisClient = null;
const REDIS_URL = process.env.REDIS_URL || '';
if (REDIS_URL) {
  // импортируем динамически чтобы не падать, если пакета нет
  const { createClient } = await import('redis');
  redisClient = createClient({ url: REDIS_URL });
  redisClient.on('error', (e) => console.error('[Redis] error:', e));
  try {
    await redisClient.connect();
    console.log('[Redis] connected');
  } catch (e) {
    console.error('[Redis] connect failed:', e);
    redisClient = null;
  }
}

// ===== HELPERS =====
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
@@ -33,7 +46,7 @@ function genId() { return crypto.randomBytes(16).toString('hex'); }
function todayLogName() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth()+1).padStart(2, '0');
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `logs.${y}${m}${dd}.log`;
}
@@ -46,9 +59,8 @@ async function appendLog(text) {
  return file;
}

// ===== TASK QUEUE (file-based) =====
// Формат файла задачи: { id, account, chat_id, reply_text, message_id, created_at }

// ===== FILE QUEUE =====
// task file: { id, account, chat_id, reply_text, message_id, created_at }
async function createTask({ account, chat_id, reply_text, message_id }) {
  await ensureDir(TASK_DIR);
  const id  = genId();
@@ -72,20 +84,17 @@ async function claimTask(account) {
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
@@ -97,7 +106,7 @@ async function claimTask(account) {
      const lockId = path.basename(taking);
      return { task: raw, lockId };
    } catch {
      // параллельный захват — пробуем следующий
      // файл уже взяли параллельно — продолжаем
    }
  }
  return null;
@@ -120,13 +129,13 @@ async function requeueTask(lockId) {
const app = express();
app.use(express.json({ limit: '1mb' }));

function ok(res, extra = {}) { return res.json({ ok: true, ...extra }); }
function bad(res, code, msg) { return res.status(code).json({ ok: false, error: msg }); }
const ok  = (res, extra = {}) => res.json({ ok: true, ...extra });
const bad = (res, code, msg)  => res.status(code).json({ ok: false, error: msg });

// health
app.get('/', (_req, res) => ok(res, { up: true }));

// ===== LOGS API (диагностика) =====
// ===== LOGS DEBUG =====
app.get('/logs', async (_req, res) => {
  try {
    await ensureDir(LOG_DIR);
@@ -146,7 +155,7 @@ app.get('/logs/read', async (req, res) => {
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, 'bad file');
    const full = path.join(LOG_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, 'not found');
    const tail = Number(req.query.tail || 300000);
    const tail = Number(req.query.tail || 200000);
    let buf = await fsp.readFile(full, 'utf8');
    if (buf.length > tail) buf = buf.slice(buf.length - tail);
    res.type('text/plain').send(buf);
@@ -223,41 +232,40 @@ app.post('/tasks/requeue', async (req, res) => {
  ok(res);
});

// ===== WEBHOOK (ловим любые имена аккаунтов) =====
const seenSystemToday = new Set(); // антидубль по (account:chat_id) для системного "Откликнулся"
// ===== WEBHOOK: принимаем ЛЮБОЕ имя аккаунта =====
// Правило:
//  - если в одной строке есть "[Системное сообщение]" и "Кандидат откликнулся" → создать задачу
//  - если НЕТ "[Системное сообщение]" → создать задачу
//  - прочие системные → игнор
const seenSystemToday = new Set(); // антидубль по account:chat_id для системного "отклик"

app.post('/webhook/:account', async (req, res) => {
  const account = String(req.params.account || 'default');

  // ЛОГИРУЕМ ВСЁ: заголовки + тело
  // логируем вход
  try {
    const headTxt = JSON.stringify(req.headers || {}, null, 2);
    const bodyTxt = JSON.stringify(req.body || {},   null, 2);
    const head = `=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n-- HEADERS --\n${headTxt}\n-- BODY --\n${bodyTxt}\n=========================\n`;
    await appendLog(head);
    const blob = `=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n-- HEADERS --\n${headTxt}\n-- BODY --\n${bodyTxt}\n=========================\n`;
    await appendLog(blob);
  } catch {}

  try {
    const payload = req.body?.payload || {};
    const val     = payload?.value || {};
    const chatId  = val?.chat_id;
    const msgId   = val?.id || null;
    const itemId  = val?.item_id || null;

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
    const isUserText        = !hasSystemTag;

    let shouldCreate = false;
    if (isSystemCandidate) {
      // антидубль на сессию
      const key = `${account}:${chatId}`;
      if (!seenSystemToday.has(key)) {
        seenSystemToday.add(key);
@@ -267,6 +275,22 @@ app.post('/webhook/:account', async (req, res) => {
      shouldCreate = true;
    }

    // (опц.) Сохраняем историю в Redis: всё кроме системных сообщений
    if (redisClient && chatId && isUserText) {
      const entry = {
        ts: Date.now(),
        author_id: val?.author_id ?? null,
        type: val?.type || 'text',
        text: String(val?.content?.text || ''),
        item_id: itemId ?? null
      };
      // список на ключе history:<account>:<chat_id>
      const key = `history:${account}:${chatId}`;
      await redisClient.rPush(key, JSON.stringify(entry));
      // держим максимум 200 сообщений
      await redisClient.lTrim(key, -200, -1);
    }

    if (shouldCreate && chatId) {
      await createTask({
        account,
@@ -275,23 +299,23 @@ app.post('/webhook/:account', async (req, res) => {
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
  console.log(`App root: ${path.resolve(__dirname)}`);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}`);
  console.log(`TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`TASK_KEY set: ${TASK_KEY ? 'yes' : 'no'}`);
  console.log(`ONLY_FIRST_SYSTEM=${ONLY_FIRST_SYSTEM}`);
  console.log(`CLAIM_WINDOW=${CLAIM_WINDOW}`);
  app.listen(PORT, () => console.log(`Server on :${PORT}`));
  console.log(`Redis: ${redisClient ? 'enabled' : 'disabled'}`);
  const srv = app.listen(PORT, () => console.log(`Server on :${PORT}`));
  srv.setTimeout(120000); // 120s safety
})();
