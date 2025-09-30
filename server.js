import express from "express";
import fs from "fs";
import path from "path";

const app = express();
app.use(express.json());

// ==== Настройки из переменных окружения ====
const PORT = process.env.PORT || 8080;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "my_secret_token";
const LOG_DIR = process.env.LOG_DIR || "/mnt/data/logs";
const TASK_DIR = process.env.TASK_DIR || "/mnt/data/tasks";
const TASK_KEY = process.env.TASK_KEY || "default-key";
const CLAIM_SCAN_LIMIT = parseInt(process.env.CLAIM_SCAN_LIMIT || "20", 10);

// ==== Проверим папки ====
[LOG_DIR, TASK_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// ==== Healthcheck ====
app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "avito-webhook-gpt",
    ts: new Date().toISOString(),
    secret: WEBHOOK_SECRET ? "set" : "missing"
  });
});

// ==== Хранилище задач (файлы) ====
function saveTask(chatId, payload) {
  const fileName = `${chatId}__${Date.now()}.json`;
  const filePath = path.join(TASK_DIR, fileName);
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
  console.log(`[TASK] created ${fileName}`);
  return fileName;
}

// ==== Вебхук ====
app.post("/webhook/hr-gpt", (req, res) => {
  const provided = req.header("x-avito-secret");
  if (provided !== WEBHOOK_SECRET) {
    console.error("[WEBHOOK] Forbidden: секрет не совпадает", { provided });
    return res.status(403).json({ ok: false, error: "forbidden" });
  }

  const body = req.body;
  const now = new Date().toISOString();

  // Лог в файл
  const logFile = path.join(LOG_DIR, `logs.${new Date().toISOString().slice(0,10)}.log`);
  fs.appendFileSync(logFile, `=== RAW AVITO WEBHOOK (hr-gpt) @ ${now} ===\n${JSON.stringify(body,null,2)}\n=========================\n\n`);

  console.log("=== RAW AVITO WEBHOOK (hr-gpt) @", now, "===");
  console.log(JSON.stringify(body, null, 2));
  console.log("=========================");

  // Если кандидат откликнулся → создаём задачу
  if (body?.payload?.value?.content?.flow_id === "job") {
    const chatId = body.payload.value.chat_id;
    saveTask(chatId, body);
  }

  res.json({ ok: true });
});

// ==== Отладка: список задач ====
app.get("/tasks/debug", (req, res) => {
  const files = fs.readdirSync(TASK_DIR).filter(f => f.endsWith(".json"));
  res.json({ ok: true, files });
});

// ==== Claim (взять задачу) ====
app.get("/tasks/claim", (req, res) => {
  const key = req.query.key;
  if (key !== TASK_KEY) {
    return res.status(403).json({ ok: false, error: "bad key" });
  }

  const files = fs.readdirSync(TASK_DIR).filter(f => f.endsWith(".json"));
  if (!files.length) {
    return res.json({ ok: true, has: false });
  }

  const file = files[0]; // берём первый
  const filePath = path.join(TASK_DIR, file);
  const data = JSON.parse(fs.readFileSync(filePath, "utf-8"));

  // Переименовываем в *.taking
  const lockFile = file + ".taking";
  fs.renameSync(filePath, path.join(TASK_DIR, lockFile));

  res.json({
    ok: true,
    has: true,
    lockId: lockFile,
    ChatId: data?.payload?.value?.chat_id || "",
    ReplyText: "Здравствуйте!",
    Account: "hr-gpt"
  });
});

// ==== Done ====
app.post("/tasks/done", (req, res) => {
  const { key, lock } = req.query;
  if (key !== TASK_KEY) {
    return res.status(403).json({ ok: false, error: "bad key" });
  }

  const filePath = path.join(TASK_DIR, lock);
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
    return res.json({ ok: true });
  } else {
    return res.status(404).json({ ok: false, error: "not found" });
  }
});

// ==== Старт ====
app.listen(PORT, () => {
  console.log(`✅ Webhook server running on port ${PORT}`);
  console.log(`LOG_DIR=${LOG_DIR}, TASK_DIR=${TASK_DIR}, SECRET=${WEBHOOK_SECRET}`);
});
