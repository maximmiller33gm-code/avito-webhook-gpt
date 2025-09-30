// server.js — ESM
import express from "express";
import fs from "fs";
import { promises as fsp } from "fs";
import path from "path";
import crypto from "crypto";
import process from "process";
import { fileURLToPath } from "url";

// ===== ESM dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ===== ENV
const PORT              = Number(process.env.PORT || 8080);
const TASK_KEY          = process.env.TASK_KEY || "kK9f4JQ7uX2pL0aN";
const LOG_DIR           = process.env.LOG_DIR  || "/mnt/data/logs";
const TASK_DIR          = process.env.TASK_DIR || "/mnt/data/tasks";
const DEFAULT_REPLY     = process.env.DEFAULT_REPLY || "Здравствуйте!";
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || "true").toLowerCase() === "true";
const WEBHOOK_SECRET    = (process.env.WEBHOOK_SECRET || "").trim();          // "my_secret_token"
const CLAIM_SCAN_LIMIT  = Number(process.env.CLAIM_SCAN_LIMIT || 50);

// Redis (опционально)
const REDIS_URL        = (process.env.REDIS_URL || "").trim();               // например: rediss://default:pass@host:port/0
const HISTORY_TTL_SEC  = Number(process.env.HISTORY_TTL_SEC || 60*60*24*3);  // 3 дня
const MAX_HISTORY      = Number(process.env.MAX_HISTORY || 40);              // сколько сообщений хранить в контексте

// ===== helpers
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive:true }); } catch {} }
function nowIso() { return new Date().toISOString(); }
function genId() { return crypto.randomBytes(16).toString("hex"); }
function todayLogName() {
  const d = new Date();
  const y = d.getUTCFullYear(), m = String(d.getUTCMonth()+1).padStart(2,"0"), dd = String(d.getUTCDate()).padStart(2,"0");
  return `logs.${y}${m}${dd}.log`;
}
async function appendLog(text) {
  console.log(text);
  await ensureDir(LOG_DIR);
  const f = path.join(LOG_DIR, todayLogName());
  await fsp.appendFile(f, text+"\n", "utf8");
  return f;
}
function ok(res, extra={}) { return res.send({ ok:true, ...extra }); }
function bad(res, code, msg) { return res.status(code).send({ ok:false, error:msg }); }

// ===== Redis (optional)
let rds = null;
if (REDIS_URL) {
  // ioredis лёгкий и стабильно работает на Railway/Render
  const { default: IORedis } = await import("ioredis");
  rds = new IORedis(REDIS_URL, {
    lazyConnect: true,
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
  });
  try { await rds.connect?.(); } catch {}
}
const useRedis = !!rds;

// ключ истории по chat_id
const rk = (chat) => `chat:${chat}:history`;

// сохранить событие в историю (Redis LIST -> right push JSON)
async function historyPush(chat_id, event) {
  if (!chat_id) return;
  const payload = JSON.stringify(event);
  if (useRedis) {
    await rds.rpush(rk(chat_id), payload);
    await rds.ltrim(rk(chat_id), -MAX_HISTORY, -1);
    await rds.expire(rk(chat_id), HISTORY_TTL_SEC);
  } else {
    // fallback в память (перезапуск — потеря)
    memoryHistory.set(chat_id, [...(memoryHistory.get(chat_id)||[]), payload].slice(-MAX_HISTORY));
  }
}
async function historyGet(chat_id) {
  if (!chat_id) return [];
  if (useRedis) {
    const arr = await rds.lrange(rk(chat_id), 0, -1);
    return arr.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
  } else {
    return (memoryHistory.get(chat_id)||[]).map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
  }
}
const memoryHistory = new Map(); // если Redis не подключён

// ===== Файловая очередь задач
// task JSON: { id, account, chat_id, reply_text, message_id, author_id, created_at }
async function createTask({ account, chat_id, reply_text, message_id, author_id }) {
  await ensureDir(TASK_DIR);
  const id  = genId();
  const acc = (account||"hr-main").replace(/[^a-zA-Z0-9_-]/g, "_");
  const task = {
    id, account: acc, chat_id,
    reply_text: reply_text || DEFAULT_REPLY,
    message_id: message_id || null,
    author_id: author_id || null,
    created_at: nowIso(),
  };
  const file = path.join(TASK_DIR, `${acc}__${id}.json`);
  await fsp.writeFile(file, JSON.stringify(task, null, 2), "utf8");
  return task;
}

async function claimTask(account) {
  await ensureDir(TASK_DIR);
  let files = (await fsp.readdir(TASK_DIR)).filter(f => f.endsWith(".json"));
  // свежие вперёд
  files.sort((a,b) => fs.statSync(path.join(TASK_DIR,b)).mtimeMs - fs.statSync(path.join(TASK_DIR,a)).mtimeMs);
  if (account) files = files.filter(f => f.startsWith(`${account}__`));
  files = files.slice(0, CLAIM_SCAN_LIMIT);

  for (const f of files) {
    const src = path.join(TASK_DIR, f);
    const dst = src.replace(/\.json$/, ".json.taking");
    try {
      await fsp.rename(src, dst); // атомарный захват
      const task = JSON.parse(await fsp.readFile(dst, "utf8"));
      return { lockId: path.basename(dst), task };
    } catch { /* уже забрали параллельно */ }
  }
  return null;
}

async function doneTask(lockId)   { try { await fsp.unlink(path.join(TASK_DIR, lockId)); } catch {} }
async function requeueTask(lockId) {
  const from = path.join(TASK_DIR, lockId);
  const to   = from.replace(/\.json\.taking$/, ".json");
  try { await fsp.rename(from, to); } catch {}
}

// ===== Логи (хвост читаем для дебага)
const LOG_TAIL_BYTES = 512*1024;
async function twoLatestLogs() {
  await ensureDir(LOG_DIR);
  return (await fsp.readdir(LOG_DIR))
    .filter(f => f.endsWith(".log"))
    .map(f => ({ f, t: fs.statSync(path.join(LOG_DIR,f)).mtimeMs }))
    .sort((a,b) => b.t - a.t)
    .slice(0,2)
    .map(x => path.join(LOG_DIR, x.f));
}
async function readTail(file, n=LOG_TAIL_BYTES) {
  let s = await fsp.readFile(file, "utf8");
  if (s.length > n) s = s.slice(s.length - n);
  return s;
}

// ===== App
const app = express();
app.use(express.json({ limit: "1mb" }));

// health
app.get("/", (_req,res) => ok(res, { up:true }));

// ===== ВЕБХУК АВИТО
const seenSystem = new Set(); // уникальность системных «откликнулся» на период аптайма

app.post("/webhook/:account", async (req,res) => {
  const account = req.params.account || "hr-main";

  // 1) секрет — принимаем либо X-Avito-Secret (проще), либо HMAC в X-Avito-Messenger-Signature
  if (WEBHOOK_SECRET) {
    const headerSecret = (req.headers["x-avito-secret"] || "").toString();
    const sig = (req.headers["x-avito-messenger-signature"] || "").toString();
    let okSecret = false;

    if (headerSecret) {
      okSecret = headerSecret === WEBHOOK_SECRET;
    } else if (sig) {
      // проверяем HMAC от сырого тела запроса
      try {
        const raw = JSON.stringify(req.body ?? {});
        const calc = crypto.createHmac("sha256", WEBHOOK_SECRET).update(raw).digest("hex");
        okSecret = (calc === sig.toLowerCase());
      } catch {}
    }
    if (!okSecret) return bad(res, 403, "forbidden");
  }

  // 2) логируем «как есть»
  const pretty = JSON.stringify(req.body || {}, null, 2);
  await appendLog(`=== RAW AVITO WEBHOOK (${account}) @ ${nowIso()} ===\n${pretty}\n=========================\n`);

  // 3) вынимаем полезное и сохраним историю
  try {
    const v = req.body?.payload?.value || {};
    const chat_id  = v.chat_id;
    const author   = v.author_id;
    const type     = v.type;
    const text     = v?.content?.text ?? "";
    const item_id  = v.item_id;
    const ts       = Number(req.body?.timestamp || Date.now()/1000);

    if (chat_id) {
      await historyPush(chat_id, { ts, type, author_id: author, text, item_id, account });
    }

    // 4) системное «кандидат откликнулся» → поставить задачу
    const isSystem = String(type).toLowerCase() === "system";
    const looksLikeApply = /кандидат|откликнулся|отклики/i.test(text || "");
    if (isSystem && looksLikeApply && chat_id) {
      let allow = true;
      if (ONLY_FIRST_SYSTEM) {
        const key = `${account}:${chat_id}`;
        if (seenSystem.has(key)) allow = false; else seenSystem.add(key);
      }
      if (allow) {
        await createTask({
          account,
          chat_id,
          reply_text: DEFAULT_REPLY,
          message_id: v.id || null,
          author_id: null
        });
      }
    }
  } catch (e) {
    await appendLog(`[webhook error] ${String(e)}`);
  }

  return ok(res);
});

// ===== История (для дебага)
app.get("/history/get", async (req,res) => {
  const chat = String(req.query.chat || "").trim();
  if (!chat) return bad(res, 400, "chat required");
  const data = await historyGet(chat);
  return ok(res, { chat, count: data.length, data });
});

// ===== Логи (для дебага)
app.get("/logs", async (_req,res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = (await fsp.readdir(LOG_DIR))
      .filter(f => f.endsWith(".log"))
      .map(f => ({ name:f, mtime: fs.statSync(path.join(LOG_DIR,f)).mtimeMs }))
      .sort((a,b) => b.mtime - a.mtime);
    res.send({ ok:true, files });
  } catch (e) { res.status(500).send({ ok:false, error:String(e) }); }
});
app.get("/logs/read", async (req,res) => {
  try {
    const file = String(req.query.file||"").trim();
    if (!file || !/^[\w.\-]+$/.test(file)) return bad(res, 400, "bad file");
    const full = path.join(LOG_DIR, file);
    if (!fs.existsSync(full)) return bad(res, 404, "not found");
    const tail = Number(req.query.tail || LOG_TAIL_BYTES);
    let buf = await fsp.readFile(full, "utf8");
    if (buf.length > tail) buf = buf.slice(buf.length - tail);
    res.type("text/plain").send(buf);
  } catch (e) { res.status(500).send({ ok:false, error:String(e) }); }
});

// ===== tasks: auth helper
function checkKey(req,res) {
  const key = String(req.query.key || req.body?.key || "").trim();
  if (!TASK_KEY || key !== TASK_KEY) { bad(res, 403, "bad key"); return false; }
  return true;
}

// Claim / Done / Requeue (DoneSafe можно добавить позже, если нужно подтверждение по логам)
app.all("/tasks/claim", async (req,res) => {
  if (!checkKey(req,res)) return;
  const account = String(req.query.account || req.body?.account || "").trim();
  const got = await claimTask(account);
  if (!got) return ok(res, { has:false });
  const { task, lockId } = got;
  return ok(res, {
    has:true,
    lockId,
    ChatId: task.chat_id,
    ReplyText: task.reply_text,
    MessageId: task.message_id || "",
    Account: task.account || ""
  });
});
app.post("/tasks/done", async (req,res) => {
  if (!checkKey(req,res)) return;
  const lock = String(req.query.lock || req.body?.lock || "").trim();
  if (!lock.endsWith(".json.taking")) return bad(res, 400, "lock invalid");
  await doneTask(lock);
  return ok(res);
});
app.post("/tasks/requeue", async (req,res) => {
  if (!checkKey(req,res)) return;
  const lock = String(req.query.lock || req.body?.lock || "").trim();
  if (!lock.endsWith(".json.taking")) return bad(res, 400, "lock invalid");
  await requeueTask(lock);
  return ok(res);
});

// debug: список файлов задач
app.get("/tasks/debug", async (_req,res) => {
  try {
    await ensureDir(TASK_DIR);
    const files = (await fsp.readdir(TASK_DIR)).sort();
    res.send({ ok:true, files });
  } catch (e) { res.status(500).send({ ok:false, error:String(e) }); }
});

// ===== start
(async () => {
  await ensureDir(LOG_DIR);
  await ensureDir(TASK_DIR);
  console.log(`LOG_DIR=${path.resolve(LOG_DIR)}, TASK_DIR=${path.resolve(TASK_DIR)}`);
  console.log(`SECRET=${WEBHOOK_SECRET ? "[set]" : "[empty]"}, REDIS=${useRedis ? "on" : "off"}`);
  app.listen(PORT, () => console.log(`✅ Webhook server running on :${PORT}`));
})();
