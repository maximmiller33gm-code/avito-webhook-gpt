// server.js
import express from "express";
import dotenv from "dotenv";
dotenv.config();

const app = express();
app.use(express.json({ limit: "1mb" }));

// ---------- Redis (не обязателен) ----------
let redis = null;
const REDIS_URL = process.env.REDIS_URL || "";
const HISTORY_TTL_SEC = Number(process.env.HISTORY_TTL_SEC || 60 * 60 * 24 * 3); // 3 дня
const HISTORY_MAX = Number(process.env.HISTORY_MAX || 100); // макс. сообщений в истории

async function initRedis() {
  if (!REDIS_URL) {
    console.log("Redis disabled (no REDIS_URL).");
    return;
  }
  try {
    const { createClient } = await import("redis");
    redis = createClient({ url: REDIS_URL });
    redis.on("error", (e) => console.error("[Redis] error:", e));
    await redis.connect();
    console.log("Redis connected.");
  } catch (e) {
    console.error("Redis init failed:", e);
    redis = null;
  }
}
await initRedis();

function histKey(account, chatId) {
  return `chat:${account}:${chatId}`;
}

async function saveToHistory(account, value) {
  try {
    if (!redis) return;

    // Фильтр системных сообщений
    const isSystemType = String(value?.type || "").toLowerCase() === "system";
    const text = String(value?.content?.text || "");
    const isSystemText = text.includes("Системное сообщение");

    if (isSystemType || isSystemText) return;

    const chatId = value?.chat_id;
    if (!chatId) return;

    const item = {
      ts: Date.now(),
      author_id: value?.author_id ?? null,
      type: value?.type ?? null,
      text,
      item_id: value?.item_id ?? null,
    };

    const key = histKey(account, chatId);
    // кладём в начало, ограничиваем длину, обновляем TTL
    await redis.lPush(key, JSON.stringify(item));
    await redis.lTrim(key, 0, HISTORY_MAX - 1);
    await redis.expire(key, HISTORY_TTL_SEC);
  } catch (e) {
    console.error("saveToHistory error:", e);
  }
}

// ---------- Маршруты ----------
app.get("/", (req, res) => {
  res.json({
    ok: true,
    redis: Boolean(redis),
    note: "POST /webhook/:account to receive Avito webhooks",
  });
});

// Принимаем ЛЮБОЕ имя аккаунта в пути
app.post("/webhook/:account", async (req, res) => {
  const account = req.params.account;
  // Логируем всё тело запроса, чтобы видеть «как пришло из Авито»
  try {
    console.log(
      `\n=== RAW AVITO WEBHOOK (${account}) @ ${new Date().toISOString()} ===`
    );
    console.log(JSON.stringify(req.body ?? {}, null, 2));
    console.log("===============================================");
  } catch (e) {
    console.error("Log stringify error:", e);
  }

  // Пытаемся сохранить историю (если Redis есть)
  try {
    const value = req.body?.payload?.value;
    if (value) {
      await saveToHistory(account, value);
    }
  } catch (e) {
    console.error("Webhook handler saveToHistory error:", e);
  }

  // Всегда отдаем 200, без проверки секрета — как просили
  res.json({ ok: true });
});

// Для быстрой проверки содержимого истории (необязательный хелпер)
app.get("/history/:account/:chatId", async (req, res) => {
  try {
    if (!redis) return res.status(200).json({ ok: true, history: [], redis: false });
    const key = histKey(req.params.account, req.params.chatId);
    const raw = await redis.lRange(key, 0, HISTORY_MAX - 1);
    const items = raw.map((x) => {
      try {
        return JSON.parse(x);
      } catch {
        return x;
      }
    });
    res.json({ ok: true, count: items.length, history: items });
  } catch (e) {
    console.error("Read history error:", e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

const PORT = Number(process.env.PORT || 8080);
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
