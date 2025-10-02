import express from "express";
import fs from "fs";
import path from "path";
import crypto from "crypto";

import { createClient } from "redis";

// Универсальная инициализация: поддерживает и redis:// и rediss://
const REDIS_URL = process.env.REDIS_URL || "";
const parsed = new URL(REDIS_URL);

// TLS включаем только если схема rediss://
const socket =
  parsed.protocol === "rediss:"
    ? { tls: true, servername: parsed.hostname, rejectUnauthorized: false }
    : undefined;

const redis = createClient({ url: REDIS_URL, socket });

redis.on("error", (err) => console.error("Redis Client Error", err));

// ВАЖНО: дожидаемся коннекта до старта роутов
await redis.connect();

const app = express();
// --- JSON-парсер (ставим ДО роутов/логгера)
app.use(express.json({
  limit: "1mb",
  // на случай если провайдер шлет JSON как text/plain
  type: ["application/json", "application/*+json", "text/plain"]
}));

// --- Гарантированный лог-хук для всех вебхуков (в stdout/Deploy Logs)
// ОБЯЗАТЕЛЬНО стоит ДО app.post("/webhook/:account", ...)
app.use("/webhook/:account", (req, res, next) => {
  try {
    const account = String(req.params.account || "");
    const now = new Date().toISOString();
    console.log(`[WEBHOOK][${account}] HIT @ ${now}`);
    console.log(`[WEBHOOK][${account}] HEADERS: ${JSON.stringify(req.headers, null, 2)}`);

    if (req.body && Object.keys(req.body).length) {
      console.log(`[WEBHOOK][${account}] BODY: ${JSON.stringify(req.body, null, 2)}`);
    } else {
      // Если по какой-то причине не распарсилось — выведем сырое тело
      let raw = "";
      req.setEncoding("utf8");
      req.on("data", chunk => raw += chunk);
      req.on("end", () => {
        if (raw) console.log(`[WEBHOOK][${account}] RAW: ${raw}`);
      });
    }
  } catch (e) {
    console.error("[WEBHOOK] logger error:", e);
  } finally {
    next();
  }
});

// === ENV & paths ===
const PORT = Number(process.env.PORT || 8080);
const LOG_DIR = process.env.LOG_DIR || "/mnt/data/logs";
const TASK_DIR = process.env.TASK_DIR || "/mnt/data/tasks";
const TASK_KEY = process.env.TASK_KEY || "kK9f4JQ7uX2pL0aN";
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";  // если пусто — секрет не проверяем
const MAX_CLAIM_SCAN = Number(process.env.CLAIM_SCAN_LIMIT || 50);
const HISTORY_LIMIT   = Number(process.env.HISTORY_LIMIT   || 100);
const HISTORY_TTL_SEC = Number(process.env.HISTORY_TTL_SEC || 259200); // 3 дня

// Включаем печать тела запроса в Deploy Logs по флагу
const DEBUG_WEBHOOK = String(process.env.DEBUG_WEBHOOK || "").toLowerCase() === "true";

// Утилита печати тела в Deploy Logs (ограничим размер)
function logBodyToStdout(account, body) {
  try {
    const pretty = JSON.stringify(body ?? {}, null, 2);
    const max = 4000; // чтобы не засорять логи
    const cut = pretty.length > max ? pretty.slice(0, max) + "\n…(truncated)" : pretty;
    console.log(`[WEBHOOK][${account}] RAW BODY @ ${new Date().toISOString()}\n${cut}`);
  } catch (e) {
    console.log(`[WEBHOOK][${account}] RAW BODY (string)\n${String(body).slice(0, 4000)}`);
  }
}

// ensure dirs
for (const p of [LOG_DIR, TASK_DIR]) {
  fs.mkdirSync(p, { recursive: true });
}

// === helpers ===
const saveToHistory = async (account, entry) => {
  const key = `chat:${account}:${entry.chat_id}`;
  const payload = JSON.stringify(entry);

  await redis.lPush(key, payload);                 // кладём сверху
  await redis.lTrim(key, 0, HISTORY_LIMIT - 1);    // обрезаем список
  if (HISTORY_TTL_SEC > 0) await redis.expire(key, HISTORY_TTL_SEC);

  // чтобы было видно в Deploy Logs:
  await appendLog(`[HIST] saved ${account} chat=${entry.chat_id} msg=${entry.message_id || ""}`);
};

const nowIso = () => new Date().toISOString();
const logFileName = () => `logs.${new Date().toISOString().slice(0,10).replace(/-/g,"")}.log`;
const appendLog = async (line) => {
  const f = path.join(LOG_DIR, logFileName());
  await fs.promises.appendFile(f, line + "\n", "utf8");
};

const makeId = (len=32) => crypto.randomBytes(len/2).toString("hex");

const writeTask = async (task) => {
  const id = task.id || makeId(32);
  const file = path.join(TASK_DIR, `${task.account}__${id}.json`);
  await fs.promises.writeFile(file, JSON.stringify({ id, ...task }, null, 2));
  return { id, file };
};

const listTaskFiles = async () => {
  const names = await fs.promises.readdir(TASK_DIR).catch(() => []);
  // только json и json.taking
  return names.filter(n => /\.json(\.taking)?$/.test(n)).sort();
};

const readTask = async (name) => {
  const file = path.join(TASK_DIR, name);
  return JSON.parse(await fs.promises.readFile(file, "utf8"));
};

// === routes: health ===
app.get("/", (_, res) => res.json({ ok: true, ts: nowIso() }));
app.get("/healthz", (_, res) => res.send("ok"));

// === routes: logs ===
app.get("/logs", async (_, res) => {
  const files = (await fs.promises.readdir(LOG_DIR).catch(() => []))
    .filter(n => /^logs\.\d{8}\.log$/.test(n))
    .map(n => ({ name: n }));
  res.json({ ok: true, files });
});

app.get("/logs/read", async (req, res) => {
  const file = String(req.query.file || "");
  const tail = Number(req.query.tail || 20000);
  if (!file) return res.status(400).json({ ok: false, error: "file required" });
  const p = path.join(LOG_DIR, file);
  try {
    const data = await fs.promises.readFile(p, "utf8");
    res.type("text/plain").send(data.slice(-tail));
  } catch {
    res.status(404).json({ ok: false, error: "not found" });
  }
});

// === routes: tasks debug ===
app.get("/tasks/debug", async (_, res) => {
  res.json({ ok: true, files: await listTaskFiles() });
});

app.get("/tasks/read", async (req, res) => {
  const name = String(req.query.file || "");
  if (!name) return res.status(400).json({ ok: false, error: "file required" });
  try {
    res.json(await readTask(name));
  } catch {
    res.status(404).json({ ok: false, error: "not found" });
  }
});

// === routes: claim/requeue/done ===
app.get("/tasks/claim", async (req, res) => {
  const key = String(req.query.key || "");
  const accountFilter = String(req.query.account || "");
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: "bad key" });

  const files = await listTaskFiles();
  let scanned = 0;
  for (const f of files) {
    if (!f.endsWith(".json")) continue; // свободные
    if (accountFilter && !f.startsWith(`${accountFilter}__`)) continue;
    scanned++;
    const freePath = path.join(TASK_DIR, f);
    const taking = freePath + ".taking";

    try {
      await fs.promises.rename(freePath, taking); // атомарный lock
      const task = await readTask(path.basename(taking));
      return res.json({
        ok: true,
        has: true,
        lockId: path.basename(taking),
        ChatId: task.chat_id || "",
        ReplyText: task.reply_text || "",
        MessageId: task.message_id || "",
        Account: task.account || ""
      });
    } catch {
      // файл могли схватить параллельно — пробуем дальше
    }
    if (scanned >= MAX_CLAIM_SCAN) break;
  }
  res.json({ ok: true, has: false });
});

app.post("/tasks/requeue", async (req, res) => {
  const key = String(req.query.key || "");
  const lock = String(req.query.lock || "");
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: "bad key" });
  if (!lock) return res.status(400).json({ ok: false, error: "lock required" });

  const taking = path.join(TASK_DIR, lock);
  const free = taking.replace(/\.taking$/, "");
  try {
    await fs.promises.rename(taking, free);
    res.json({ ok: true });
  } catch {
    res.status(404).json({ ok: false, error: "not found" });
  }
});

app.post("/tasks/done", async (req, res) => {
  const key = String(req.query.key || "");
  const lock = String(req.query.lock || "");
  if (key !== TASK_KEY) return res.status(403).json({ ok: false, error: "bad key" });
  if (!lock) return res.status(400).json({ ok: false, error: "lock required" });
  try {
    await fs.promises.unlink(path.join(TASK_DIR, lock));
    res.json({ ok: true });
  } catch {
    // пробуем без .taking
    try {
      await fs.promises.unlink(path.join(TASK_DIR, lock.replace(/\.taking$/, "")));
      res.json({ ok: true });
    } catch {
      res.status(404).json({ ok: false, error: "not found" });
    }
  }
});

// === WEBHOOK: принимает любые имена /webhook/:account ===
app.post("/webhook/:account", async (req, res) => {
  const account = String(req.params.account || "").trim();

  // 1) Парсим полезное
  const val      = req.body?.payload?.value || {};
  const chatId   = val.chat_id || val.chatId || "";
  const msgId    = val.id || val.message_id || "";
  const txt      = (val.content?.text || "").trim();
  const vType    = (val.type || "").toLowerCase();
  const isSystem = vType === "system" || txt.startsWith("[Системное сообщение]");
  const isApply  = isSystem && /Кандидат\s+откликнулся/i.test(txt);
  const authorId = String(val.author_id || "");

  // 2) Пишем ИСТОРИЮ (только НЕ системные)
  try {
    if (chatId && msgId && txt && !txt.includes("[Системное сообщение]")) {
      const entry = {
        chat_id:   chatId,
        ts:        val.created,
        type:      val.type,
        text:      txt,
        item_id:   val.item_id,
        message_id: msgId,
        author_id: authorId,
      };
      const key   = `chat:${account}:${chatId}`;
      const limit = Number(process.env.HISTORY_LIMIT || 100);
      const ttl   = Number(process.env.HISTORY_TTL_SEC || 259200);
      await redis.lPush(key, JSON.stringify(entry));
      await redis.lTrim(key, 0, limit - 1);
      await redis.expire(key, ttl);
    }
  } catch (e) {
    console.error("history save error:", e?.message || e);
  }

  // 3) Фильтр: создавать ли ЗАДАЧУ
  const MY_ID = String(process.env.ACCOUNT_ID || process.env.ACCOUNTID || "").trim();
  const BLOCK_AUTHOR_IDS = (process.env.BLOCK_AUTHOR_IDS || "")
    .split(",").map(s => s.trim()).filter(Boolean);

  const isFromMe =
       (authorId && MY_ID && authorId === MY_ID)
    || BLOCK_AUTHOR_IDS.includes(authorId)
    || authorId === "0"   // системный Авито
    || authorId === "1";  // системный

  // правило: создаём задачу только если
  //   — системное «Кандидат откликнулся…», ИЛИ
  //   — обычный текст от кандидата (не системное, есть текст, автор не наш)
  const shouldCreate =
       isApply
    || (!isSystem && txt && !isFromMe);

  if (!shouldCreate || !chatId) {
    const reason = isSystem
      ? (isApply ? "should-not-happen" : "system-non-apply")
      : (isFromMe ? "from-me" : "no-text");
    await appendLog(`[TASK] skipped for ${account} chat=${chatId} reason=${reason}`);
    return res.json({ ok: true });
  }

  // 4) Создаём задачу

// Определяем тип вебхука
let typeWebhook = "сообщение";
if (isSystem && txt.includes("Кандидат откликнулся")) {
  typeWebhook = "отклик";
}

const replyText = isSystem ? "" : String(txt || ""); // для системного отклика — пусто

await writeTask({
  account,
  chat_id: chatId,
  reply_text: replyText,
  message_id: msgId,
  created_at: nowIso(),
  type_webhook: typeWebhook   // <--- добавили поле
});

await appendLog(`[TASK] created for ${account} chat=${chatId} msg=${msgId} type=${typeWebhook}`);

  res.json({ ok: true });
});

// === Просмотр истории в браузере ===

// Список чатов для аккаунта (удобно найти нужный chat_id)
app.get("/history/:account", async (req, res) => {
  try {
    const { account } = req.params;
    const prefix = `chat:${account}:*`;

    // Собираем все ключи chat:<account>:<chat_id>
    const chats = [];
    for await (const key of redis.scanIterator({ MATCH: prefix, COUNT: 500 })) {
      const chat_id = key.split(":").pop();
      chats.push({ chat_id, key });
    }

    res.json({ ok: true, account, count: chats.length, chats });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

// История конкретного чата
app.get("/history/:account/:chat_id", async (req, res) => {
  try {
    const { account, chat_id } = req.params;
    const key = `chat:${account}:${chat_id}`;        // тот же ключ, куда мы писали историю
    const limit = Math.max(1, Math.min(500, Number(req.query.n || 100)));

    // Историю мы сохраняли LPUSH, поэтому читаем LRange от 0 до limit-1 и разворачиваем по времени
    const raw = await redis.lRange(key, 0, limit - 1);    // самые новые в начале
    const history = raw.map(s => {
      try { return JSON.parse(s); } catch { return { raw: s }; }
    }).sort((a, b) => (a.ts || 0) - (b.ts || 0));         // по возрастанию времени

    res.json({ ok: true, key, count: history.length, history });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

// === start ===
app.listen(PORT, () => {
  console.log("App root:", process.cwd());
  console.log("LOG_DIR=" + LOG_DIR);
  console.log("TASK_DIR=" + TASK_DIR);
  console.log("TASK_KEY set=" + Boolean(TASK_KEY));
  console.log("WEBHOOK_SECRET set=" + Boolean(WEBHOOK_SECRET));
  console.log("MAX_CLAIM_SCAN=" + MAX_CLAIM_SCAN);
  console.log("Server on :" + PORT);
});
