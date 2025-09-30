// server.js
import express from "express";
import fs from "fs";
import path from "path";
import bodyParser from "body-parser";

const app = express();
const PORT = process.env.PORT || 8080;

const __dirname = path.resolve();
const LOG_DIR = path.join(__dirname, "logs");
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

app.use(bodyParser.json());

// Универсальный webhook: примет любое имя после /webhook/*
app.post("/webhook/:account", (req, res) => {
  const account = req.params.account; // hr-gpt, personalpro и т.п.
  const logFile = path.join(LOG_DIR, `logs.${new Date().toISOString().slice(0, 10)}.log`);

  const entry = `=== INCOMING WEBHOOK (${account}) @ ${new Date().toISOString()} ===\n`
    + JSON.stringify(req.body, null, 2)
    + "\n=========================\n\n";

  fs.appendFileSync(logFile, entry, "utf8");

  console.log(`[WEBHOOK] ${account} event logged`);
  res.status(200).json({ ok: true });
});

// Для проверки
app.get("/", (req, res) => {
  res.json({ ok: true, up: true });
});

app.get("/logs", (req, res) => {
  const files = fs.readdirSync(LOG_DIR).map(name => ({
    name,
    mtime: fs.statSync(path.join(LOG_DIR, name)).mtime
  }));
  res.json({ ok: true, files });
});

app.get("/logs/read", (req, res) => {
  const file = req.query.file;
  if (!file) return res.status(400).json({ ok: false, error: "file query required" });

  const fullPath = path.join(LOG_DIR, file);
  if (!fs.existsSync(fullPath)) return res.status(404).json({ ok: false, error: "not found" });

  res.type("text/plain").send(fs.readFileSync(fullPath, "utf8"));
});

// Старт
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
