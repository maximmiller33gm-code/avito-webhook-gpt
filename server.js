// server.js (ESM)
import express from "express";
import fs from "fs";
import { promises as fsp } from "fs";
import path from "path";
import process from "process";
import crypto from "crypto";
import { fileURLToPath } from "url";
import Redis from "ioredis";

// --- __dirname в ESM ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- ENV ---
const PORT = process.env.PORT || 3000;
const LOG_DIR = process.env.LOG_DIR || "./logs";
const TASK_DIR = process.env.TASK_DIR || "./tasks";
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "my_secret_token";

// Redis
const REDIS_URL = process.env.REDIS_URL || "";
const HISTORY_TTL_SEC = Number(process.env.HISTORY_TTL_SEC || 3 * 24 * 60 * 60); // 3 суток
const HISTORY_LIMIT = Number(process.env.HISTORY_LIMIT || 100);

let redis = null;
if (REDIS_URL) {
  redis = new Redis(REDIS_URL);
  redis.on("error", (e) => console.error("[REDIS] error:", e.message));
}
const histKey = (account, chatId) => `chat:${account}:${chatId}`;

async function saveToHistory({ account, value }) {
  if (!redis) return;
  if (!value?.chat_id) return;

  const text = String(value?.content?.text || "");
  if (/Системное сообщение/i.test(text)) return; // пропускаем системные

  const item = {
    ts: value?.created ?? Math.floor(Date.now() / 1000),
    iso: new Date().toISOString(),
    type: value?.type ?? null,
    author_id: value?.author_id ?? null,
    text,
    message_id: value?.id ?? null,
    item_id: value?.item_id ?? null,
  };

  const key = histKey(account, value.chat_id);
  await redis
    .multi()
    .lpush(key, JSON.stringify(item))
    .ltrim(key, 0, HISTORY_LIMIT - 1)
    .expire(key, HISTORY_TTL_SEC)
    .exec();
}

// --- HELPERS ---
async function ensureDir(d) {
  try {
    await fsp.mkdir(d, { recursive: true });
  } catch {}
}
function nowIso() {
  return new Date().toISOString();
}
function genId() {
  return crypto.randomBytes(8).toString("hex");
}

// --- LOG ---
async function appendLog(text) {
  await ensureDir(LOG_DIR);
  const file = path.join(LOG_DIR, `logs.${new Date().toISOString().slice(0, 10)}.log`);
  await fsp.appendFile(file, text + "\n", "utf8");
  console.log(text);
}

// --- TASKS ---
async function createTask({ account, chat_id, reply_text }) {
  await ensureDir(TASK_DIR);
  const id = genId();
  const task = {
    id,
    account,
    chat_id,
    reply_text: reply_text || "Здравствуйте!",
    created_at: nowIso(),
  };
  const file = path.join(TASK_DIR, `${account}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(task, null, 2), "utf8");
  return task;
}

// --- APP ---
const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/", (req, res) => res.json({ ok: true, redis: Boolean(redis) }));

// Avito Webhook
app.post("/webhook/:account", async (req, res) => {
  const account = req.params.account;

  if (WEBHOOK_SECRET) {
    const secret = req.headers["x-avito-secret"];
    if (secret !== WEBHOOK_SECRET) return res.status(403).json({ ok: false, error: "forbidden" });
  }

  const pretty = JSON.stringify(req.body || {}, null, 2);
  await appendLog(`=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n${pretty}\n=========================`);

  const val = req.body?.payload?.value;
  if (val) {
    // сохраняем историю (кроме "Системное сообщение")
    await saveToHistory({ account, value: val });

    // ставим задачу, если "Кандидат откликнулся"
    const txt = String(val?.content?.text || "");
    if (/Кандидат откликнулся/i.test(txt)) {
      await createTask({ account, chat_id: val.chat_id, reply_text: "Здравствуйте!" });
    }
  }

  res.json({ ok: true });
});

// Debug endpoints
app.get("/logs", async (req, res) => {
  await ensureDir(LOG_DIR);
  const files = await fsp.readdir(LOG_DIR);
  res.json({ ok: true, files });
});

app.get("/tasks/debug", async (req, res) => {
  await ensureDir(TASK_DIR);
  const files = await fsp.readdir(TASK_DIR);
  res.json({ ok: true, files });
});

// --- START ---
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  app.listen(PORT, () => console.log("Server on port", PORT));
})();
