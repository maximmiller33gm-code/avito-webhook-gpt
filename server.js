// server.js
import express from "express";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { createClient } from "redis";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "1mb" }));

// ==== ENV ====
const PORT              = Number(process.env.PORT || 8080);
const LOG_DIR           = process.env.LOG_DIR || path.join(__dirname, "data", "logs");
const TASK_DIR          = process.env.TASK_DIR || path.join(__dirname, "data", "tasks");
const WEBHOOK_SECRET    = process.env.WEBHOOK_SECRET || ""; // "123" и т.п.
const TASK_KEY          = process.env.TASK_KEY || "kK9f4JQ7uX2pL0aN";
const CLAIM_SCAN_LIMIT  = Number(process.env.CLAIM_SCAN_LIMIT || 50);
const DEFAULT_REPLY     = process.env.DEFAULT_REPLY || "Здравствуйте!";
const ONLY_FIRST_SYSTEM = /^(true|1)$/i.test(process.env.ONLY_FIRST_SYSTEM || "false"); // если true — на системные «Кандидат откликнулся…» реагируем 1 раз на чат

// Redis
const REDIS_URL        = process.env.REDIS_URL || "";
const HISTORY_LIMIT    = Number(process.env.HISTORY_LIMIT || 100);  // сколько сообщений держать
const HISTORY_TTL_SEC  = Number(process.env.HISTORY_TTL_SEC || 3 * 24 * 3600); // 3 суток

let redis = null;
if (REDIS_URL) {
  redis = createClient({ url: REDIS_URL });
  redis.on("error", (e) => console.error("[REDIS] error:", e));
  await redis.connect();
}

// ==== FS utils ====
function nowIso() { return new Date().toISOString(); }
function ymd() { return new Date().toISOString().slice(0,10).replace(/-/g, ""); }

function ensureDir(d) { fs.mkdirSync(d, { recursive: true }); }

function logFilePath() {
  ensureDir(LOG_DIR);
  return path.join(LOG_DIR, `logs.${new Date().toISOString().slice(0,10)}.log`);
}
function appendLog(line) {
  fs.appendFileSync(logFilePath(), line + "\n", "utf8");
}

// history helpers
function histKey(account, chat) {
  return `hist:${account}:${chat}`;
}
async function saveToHistory({ account, value }) {
  // не сохраняем «Системное сообщение»
  const txt = String(value?.content?.text || "");
  const isSystem = /\[Системное сообщение\]/i.test(txt) || String(value?.type || "") === "system";
  if (isSystem) return;

  if (!redis) return;
  const chat_id = value?.chat_id;
  if (!chat_id) return;

  const record = {
    ts: Date.now(),
    author_id: value?.author_id ?? null,
    type: value?.type ?? null,
    text: txt,
    item_id: value?.item_id ?? null,
  };

  const key = histKey(account, chat_id);
  await redis.rPush(key, JSON.stringify(record));
  await redis.lTrim(key, -HISTORY_LIMIT, -1);
  await redis.expire(key, HISTORY_TTL_SEC);
}

// create task
function taskFileName(account, id) {
  return `${account}__${id}.json`;
}
function taskLockName(name) {
  return `${name}.taking`;
}
async function createTask({ account, chat_id, reply_text, message_id, item_id }) {
  ensureDir(TASK_DIR);
  const id = crypto.createHash("md5")
    .update(`${account}:${chat_id}:${message_id || ""}:${Date.now()}`)
    .digest("hex");

  const file = taskFileName(account, id);
  const p = path.join(TASK_DIR, file);
  const payload = {
    id,
    account,
    chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: message_id || "",
    item_id: item_id || "",
    created_at: nowIso(),
  };
  fs.writeFileSync(p, JSON.stringify(payload, null, 2), "utf8");
  appendLog(`[TASK] created ${file} chat=${chat_id}`);
  return file;
}

// ==== Security helpers (secret check) ====
function safeEq(a, b) {
  const A = Buffer.from(String(a), "utf8");
  const B = Buffer.from(String(b), "utf8");
  if (A.length !== B.length) return false;
  return crypto.timingSafeEqual(A, B);
}
function hmacHex(secret, bodyStr) {
  return crypto.createHmac("sha256", secret).update(bodyStr).digest("hex");
}
function checkSecret(req) {
  // Если секрета нет — принимаем всё (стенд)
  if (!WEBHOOK_SECRET) return true;

  const raw = JSON.stringify(req.body || {});
  const s1 = req.headers["x-avito-secret"];
  if (s1 && safeEq(String(s1), WEBHOOK_SECRET)) return true;

  const sig = req.headers["x-avito-messenger-signature"];
  if (sig && safeEq(String(sig), hmacHex(WEBHOOK_SECRET, raw))) return true;

  appendLog(`[WEBHOOK] Forbidden: secret mismatch { providedLen: ${String(s1||sig||"").length}, expectedLen: ${WEBHOOK_SECRET.length}, headerKeys: ${JSON.stringify(Object.keys(req.headers).filter(k=>k.startsWith("x-")))} }`);
  return false;
}

// ==== Routes ====

// health
app.get("/", (req, res) => res.json({ ok: true, redis: Boolean(redis) }));

// history read
app.get("/history/:account/:chat", async (req, res) => {
  if (!redis) return res.json({ ok: false, error: "no redis" });
  const { account, chat } = req.params;
  const key = histKey(account, chat);
  const arr = await redis.lRange(key, 0, -1);
  const out = arr.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
  res.json({ ok: true, count: out.length, history: out });
});

// webhook — принимает любые имена аккаунтов
app.post("/webhook/:account", async (req, res) => {
  const account = req.params.account;

  if (!checkSecret(req)) return res.status(403).json({ ok: false, error: "forbidden" });

  const pretty = JSON.stringify(req.body || {}, null, 2);
  appendLog(`=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n${pretty}\n=========================`);

  // сохраняем историю (кроме системных)
  const val = req.body?.payload?.value;
  if (val) {
    try { await saveToHistory({ account, value: val }); } catch (e) { appendLog(`[HISTORY] error ${e}`); }
  }

  // Деловая логика постановки задач
  // 1) Если строка содержит одновременно [Системное сообщение] и Кандидат откликнулся — создать один раз
  // 2) Если не содержит [Системное сообщение] — это обычное сообщение — создавать задачу
  let text = "";
  if (typeof val?.content?.text === "string") text = val.content.text;
  const isSystem = /\[Системное сообщение\]/i.test(text) || String(val?.type||"") === "system";
  const isApply  = /Кандидат\s+откликнулся/i.test(text);

  let shouldCreate = false;
  if (isSystem && isApply) {
    shouldCreate = true;
    if (ONLY_FIRST_SYSTEM && redis && val?.chat_id) {
      const onceKey = `sysOnce:${account}:${val.chat_id}`;
      const was = await redis.get(onceKey);
      if (was) shouldCreate = false; // уже ставили
      else await redis.setEx(onceKey, 7*24*3600, "1"); // защитим от дублей на неделю
    }
  } else if (!isSystem) {
    shouldCreate = true;
  }

  if (shouldCreate && val?.chat_id) {
    await createTask({
      account,
      chat_id: val.chat_id,
      reply_text: DEFAULT_REPLY,
      message_id: val.id || "",
      item_id: val.item_id || ""
    });
  }

  res.json({ ok: true });
});

// ==== TASKS API ====

// список файлов
app.get("/tasks/debug", (req, res) => {
  try {
    ensureDir(TASK_DIR);
    const files = fs.readdirSync(TASK_DIR).filter(f => f.endsWith(".json") || f.endsWith(".json.taking"));
    res.json({ ok: true, files });
  } catch (e) {
    res.json({ ok: true, files: [] });
  }
});

// чтение файла очереди/лока
app.get("/tasks/read", (req, res) => {
  try {
    const name = String(req.query.file || "");
    if (!name) return res.status(400).send("file required");
    const p = path.join(TASK_DIR, name);
    if (!fs.existsSync(p)) return res.status(404).json({ ok: false, error: "not found" });
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    res.json(j);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// claim
app.get("/tasks/claim", (req, res) => {
  const key = String(req.query.key || "");
  const account = String(req.query.account || "");
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: "bad key" });

  ensureDir(TASK_DIR);
  const files = fs.readdirSync(TASK_DIR)
    .filter(f => f.endsWith(".json") && !f.endsWith(".taking"))
    .filter(f => !account || f.startsWith(`${account}__`))
    .slice(0, CLAIM_SCAN_LIMIT)
    .sort(); // детерминированность

  if (!files.length) return res.json({ ok: true, has: false });

  const file = files[0];
  const lock = taskLockName(file);
  const p = path.join(TASK_DIR, file);
  const plock = path.join(TASK_DIR, lock);

  try {
    const obj = JSON.parse(fs.readFileSync(p, "utf8"));
    // создаём lock
    fs.writeFileSync(plock, fs.readFileSync(p));
    // удаляем оригинал
    fs.unlinkSync(p);

    return res.json({
      ok: true,
      has: true,
      lockId: lock,
      ChatId: obj.chat_id || "",
      ReplyText: obj.reply_text || DEFAULT_REPLY,
      MessageId: obj.message_id || "",
      Account: obj.account || ""
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

// done (без проверок логов) — просто удаляет lock
app.post("/tasks/done", (req, res) => {
  const key = String(req.query.key || "");
  const lock = String(req.query.lock || "");
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: "bad key" });
  if (!lock) return res.status(400).json({ ok: false, error: "no lock" });

  const plock = path.join(TASK_DIR, lock);
  if (fs.existsSync(plock)) fs.unlinkSync(plock);
  return res.json({ ok: true });
});

// requeue — переводит .taking обратно в очередь
app.post("/tasks/requeue", (req, res) => {
  const key = String(req.query.key || "");
  const lock = String(req.query.lock || "");
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: "bad key" });
  if (!lock) return res.status(400).json({ ok: false, error: "no lock" });

  const plock = path.join(TASK_DIR, lock);
  if (!fs.existsSync(plock)) return res.json({ ok: true }); // уже нет — ок

  try {
    const data = fs.readFileSync(plock);
    const base = lock.replace(/\.taking$/, "");
    const p = path.join(TASK_DIR, base);
    fs.writeFileSync(p, data);
    fs.unlinkSync(plock);
    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

// doneSafe — проверяет логи и только тогда закрывает
app.post("/tasks/doneSafe", (req, res) => {
  const key = String(req.query.key || "");
  const lock = String(req.query.lock || "");
  const chat = String(req.query.chat || "");
  const author = String(req.query.author || "");

  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: "bad key" });
  if (!lock) return res.status(400).json({ ok: false, error: "no lock" });
  if (!chat) return res.status(428).json({ ok: false, error: "no chat_id in lock" });

  // проверяем последние 2 файла логов
  try {
    ensureDir(LOG_DIR);
    const all = fs.readdirSync(LOG_DIR)
      .filter(f => /^logs\.\d{4}-\d{2}-\d{2}\.log$/.test(f) || /^logs\.\d{8}\.log$/.test(f))
      .sort()
      .slice(-2);
    const found = all.some(f => {
      const s = fs.readFileSync(path.join(LOG_DIR, f), "utf8");
      // ищем текстовые исходящие сообщения от нашего автора (author_id != 0)
      return s.includes(`"chat_id": "${chat}"`) &&
             /"type"\s*:\s*"text"/.test(s) &&
             (author ? s.includes(`"author_id": ${author}`) : /"author_id":\s*(?!0)\d+/.test(s));
    });

    if (!found) {
      return res.status(428).json({ ok: false, error: "not confirmed in logs", files: all });
    }
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e) });
  }

  // подтверждение найдено — удаляем lock
  const plock = path.join(TASK_DIR, lock);
  if (fs.existsSync(plock)) fs.unlinkSync(plock);
  return res.status(204).end();
});

// ==== start ====
app.listen(PORT, () => {
  console.log(`✅ Webhook server on ${PORT}`);
  console.log(`LOG_DIR=${LOG_DIR}, TASK_DIR=${TASK_DIR}, SECRET=${WEBHOOK_SECRET ? "[set]" : "[empty]"}`);
});
