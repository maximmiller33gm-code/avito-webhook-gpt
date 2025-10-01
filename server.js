import express from "express";
import fs from "fs";
import path from "path";
import crypto from "crypto";

import { createClient } from "redis";

// === Redis client (TLS support for rediss://) ===
const parsed = new URL(process.env.REDIS_URL);
const useTLS = parsed.protocol === "rediss:";

const redis = createClient({
  url: process.env.REDIS_URL,
  socket: useTLS
    ? {
        tls: true,
        servername: parsed.hostname,
        rejectUnauthorized: false, // если сертификат не полный chain
      }
    : {},
});

redis.on("error", (err) => console.error("Redis Client Error", err));

// подключаемся
await redis.connect();

const app = express();
app.use(express.json({ limit: "1mb" }));

// === ENV & paths ===
const PORT = Number(process.env.PORT || 8080);
const LOG_DIR = process.env.LOG_DIR || "/mnt/data/logs";
const TASK_DIR = process.env.TASK_DIR || "/mnt/data/tasks";
const TASK_KEY = process.env.TASK_KEY || "kK9f4JQ7uX2pL0aN";
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";  // если пусто — секрет не проверяем
const MAX_CLAIM_SCAN = Number(process.env.CLAIM_SCAN_LIMIT || 50);

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

  if (DEBUG_WEBHOOK) {
  logBodyToStdout(account, req.body);
}

  // 1) Секрет (если указан в ENV)
  const providedSecret = req.headers["x-avito-secret"];
  // if (WEBHOOK_SECRET) {
//   if (!providedSecret || String(providedSecret) !== String(WEBHOOK_SECRET)) {
//     return res.status(403).json({ ok: false, error: "forbidden" });
//   }
// }

  // 2) Логируем «сырое» тело
  const pretty = JSON.stringify(req.body || {}, null, 2);
  await appendLog(`=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n${pretty}\n=========================`);

  // 3) Пытаемся вытащить полезное
  const val = req.body?.payload?.value || {};
  const chatId = val.chat_id || val.chatId || "";
  const msgId = val.id || val.message_id || "";
  const txt = (val.content?.text || "").trim();
  const isSystem = (val.type || "").toLowerCase() === "system" || txt.startsWith("[Системное сообщение]");
  const isApply = isSystem && /Кандидат\s+откликнулся/i.test(txt);

  // 4) Правило создания задач:
  //    - если системное и «Кандидат откликнулся…» → создать
  //    - если не системное (обычный текст) → создать
  //    - все остальные системные — игнор
  let shouldCreate = false;
  if (isApply) shouldCreate = true;
  else if (!isSystem && txt) shouldCreate = true;

  if (shouldCreate && chatId) {
    const replyDefault = process.env.DEFAULT_REPLY || "Здравствуйте!";
    await writeTask({
      account,
      chat_id: chatId,
      reply_text: replyDefault,
      message_id: msgId,
      created_at: nowIso()
    });
    await appendLog(`[TASK] created for ${account} chat=${chatId} msg=${msgId}`);
  } else {
    await appendLog(`[TASK] skipped for ${account} chat=${chatId} reason=${isSystem ? "system-non-apply" : "no-text"}`);
  }

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
