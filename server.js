import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createClient } from "redis";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "1mb" }));

// переменные окружения
const PORT = process.env.PORT || 8080;
const LOG_DIR = process.env.LOG_DIR || "./logs";
const TASK_DIR = process.env.TASK_DIR || "./tasks";
const HISTORY_LIMIT = parseInt(process.env.HISTORY_LIMIT || "100", 10);
const HISTORY_TTL_SEC = parseInt(process.env.HISTORY_TTL_SEC || "259200", 10);

let redis;
if (process.env.REDIS_URL) {
  redis = createClient({ url: process.env.REDIS_URL });
  redis.on("error", (err) => console.error("[REDIS] Error", err));
  redis.connect().then(() => console.log("[REDIS] connected"));
}

// утилита
function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
ensureDir(LOG_DIR);
ensureDir(TASK_DIR);

function nowIso() {
  return new Date().toISOString();
}

// лог в файл
async function appendLog(msg) {
  const file = path.join(LOG_DIR, "webhook.log");
  fs.appendFileSync(file, `[${nowIso()}] ${msg}\n`);
}

// сохраняем историю сообщений
async function saveToHistory(account, value) {
  if (!redis) return;
  const key = `history:${account}:${value.chat_id}`;
  const record = {
    ts: Date.now(),
    author_id: value.author_id,
    type: value.type,
    text: value.content?.text || "",
    item_id: value.item_id || "",
  };
  await redis.lPush(key, JSON.stringify(record));
  await redis.lTrim(key, 0, HISTORY_LIMIT - 1);
  await redis.expire(key, HISTORY_TTL_SEC);
}

// создаём задачу
async function createTask(task) {
  ensureDir(TASK_DIR);
  const file = path.join(
    TASK_DIR,
    `${task.account}__${task.chat_id}__${Date.now()}.json`
  );
  fs.writeFileSync(file, JSON.stringify(task, null, 2));
  console.log("[TASK] created", file);
}

// ===== Вебхук (принимаем любые имена аккаунтов) =====
app.post("/webhook/:account", async (req, res) => {
  const account = req.params.account;

  // убираем проверку секрета!
  const pretty = JSON.stringify(req.body || {}, null, 2);
  await appendLog(`RAW WEBHOOK (${account}) @ ${nowIso()}\n${pretty}`);

  const val = req.body?.payload?.value;
  if (val) {
    // сохраняем историю, кроме системных
    if (!/Системное сообщение/i.test(val.content?.text || "")) {
      await saveToHistory(account, val);
    }

    const txt = String(val.content?.text || "");
    // создаём задачу для системного "Кандидат откликнулся"
    if (/Системное сообщение/i.test(txt) && /Кандидат откликнулся/i.test(txt)) {
      await createTask({
        account,
        chat_id: val.chat_id,
        reply_text: "Здравствуйте!",
        message_id: val.message_id || "",
      });
    }

    // создаём задачу для обычных сообщений
    if (!/Системное сообщение/i.test(txt)) {
      await createTask({
        account,
        chat_id: val.chat_id,
        reply_text: "Здравствуйте!",
        message_id: val.message_id || "",
      });
    }
  }

  res.json({ ok: true });
});

// простая проверка
app.get("/", (req, res) => res.json({ ok: true, redis: Boolean(redis) }));

app.listen(PORT, () => {
  console.log(`Server on :${PORT}`);
});
