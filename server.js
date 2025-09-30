// server.js (ESM)
import express from 'express';
import crypto from 'crypto';
import { promises as fsp } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ===== ENV / CONFIG =====
const PORT            = Number(process.env.PORT || 8080);
const WEBHOOK_SECRET  = process.env.WEBHOOK_SECRET || '';        // должен совпадать с секретом в кабинете Авито
const LOG_DIR         = process.env.LOG_DIR || '/mnt/data/logs'; // куда писать логи (Railway volume)
const TASK_DIR        = process.env.TASK_DIR || '/mnt/data/tasks'; // зарезервировано на будущее
const CLAIM_SCAN_LIMIT= Number(process.env.CLAIM_SCAN_LIMIT || 50);
const ONLY_FIRST_SYSTEM = String(process.env.ONLY_FIRST_SYSTEM || 'true').toLowerCase() === 'true';

// ===== helpers =====
async function ensureDir(dir) { try { await fsp.mkdir(dir, { recursive: true }); } catch {} }
function nowIso(){ return new Date().toISOString(); }
function logFileName(){ 
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth()+1).padStart(2,'0');
  const dd = String(d.getUTCDate()).padStart(2,'0');
  return `logs.${yyyy}${mm}${dd}.log`;
}
async function appendLog(lines){
  await ensureDir(LOG_DIR);
  const file = path.join(LOG_DIR, logFileName());
  const txt  = (Array.isArray(lines)?lines:[lines]).join('') + '\n';
  await fsp.appendFile(file, txt, 'utf8');
}

// Сохраняем raw body, чтобы уметь проверять HMAC, если Авито пришлёт подпись
const app = express();
app.use(express.json({
  verify: (req, _res, buf) => { req.rawBody = buf; }
}));

// ===== Проверка секрета =====
function checkAvitoSecret(req){
  if (!WEBHOOK_SECRET) return { ok: true, mode: 'open' };

  // 1) Прямой заголовок
  const plain = req.headers['x-avito-secret'];
  if (plain && String(plain) === WEBHOOK_SECRET)
    return { ok: true, mode: 'header' };

  // 2) HMAC-подпись (если вдруг используется)
  const sig = req.headers['x-avito-messenger-signature'];
  if (sig && req.rawBody && Buffer.isBuffer(req.rawBody)) {
    const calc = crypto.createHmac('sha256', WEBHOOK_SECRET)
                       .update(req.rawBody)
                       .digest('hex');
    if (String(sig).toLowerCase() === calc.toLowerCase())
      return { ok: true, mode: 'hmac' };
  }

  return { ok: false, why: { providedHeader: plain ?? null, hasHmac: Boolean(sig) } };
}

// ===== Единая обработка для нескольких аккаунтов =====
function webhookHandler(account){
  return async (req, res) => {
    const sec = checkAvitoSecret(req);
    if (!sec.ok) {
      await appendLog(`[WEBHOOK] Forbidden (${account}): секрет не совпадает ${JSON.stringify(sec.why)}`);
      return res.status(403).json({ ok:false, error:'forbidden' });
    }

    // Лог красиво
    const stamp = nowIso();
    const headerDump = {
      host: req.headers.host,
      'user-agent': req.headers['user-agent'],
      'content-length': req.headers['content-length'],
      'content-type': req.headers['content-type'],
      'x-avito-messenger-signature': req.headers['x-avito-messenger-signature'],
      'x-avito-secret': req.headers['x-avito-secret'] ? '<set>' : undefined,
      'x-forwarded-for': req.headers['x-forwarded-for'],
      'x-railway-edge': req.headers['x-railway-edge'],
      'x-railway-request-id': req.headers['x-railway-request-id'],
    };

    await appendLog(
`=== RAW AVITO WEBHOOK (${account}) @ ${stamp} ===
-- HEADERS --
${JSON.stringify(headerDump, null, 2)}
-- BODY --
${JSON.stringify(req.body || {}, null, 2)}
=========================`
    );

    // здесь можно поставить задачу в очередь (если нужно), сейчас просто 200
    return res.json({ ok:true });
  };
}

// Роуты вебхуков
app.post('/webhook/hr-gpt',       webhookHandler('hr-gpt'));
app.post('/webhook/personalpro',  webhookHandler('personalpro'));

// ===== Служебные роуты для проверки =====
app.get('/', (_req, res) => {
  res.json({
    ok: true,
    up: true,
    port: PORT,
    LOG_DIR,
    TASK_DIR,
    secretSet: Boolean(WEBHOOK_SECRET),
  });
});

// Список лог-файлов
app.get('/logs', async (_req, res) => {
  try {
    await ensureDir(LOG_DIR);
    const files = await fsp.readdir(LOG_DIR);
    const stats = await Promise.all(files.map(async name => {
      const st = await fsp.stat(path.join(LOG_DIR, name));
      return { name, mtime: st.mtimeMs };
    }));
    res.json({ ok:true, files: stats.sort((a,b)=>b.mtime-a.mtime) });
  } catch (e) {
    res.status(500).json({ ok:false, error: String(e) });
  }
});

// Прочитать кусок лога
app.get('/logs/read', async (req, res) => {
  try {
    const file = String(req.query.file || '');
    const tail = Number(req.query.tail || 4000);
    if (!file) return res.status(400).json({ ok:false, error:'file required' });
    const full = path.join(LOG_DIR, file);
    const data = await fsp.readFile(full, 'utf8');
    const out = tail > 0 && data.length > tail ? data.slice(-tail) : data;
    res.type('text/plain').send(out);
  } catch (e) {
    res.status(500).json({ ok:false, error: String(e) });
  }
});

// ===== START =====
await ensureDir(LOG_DIR);
await ensureDir(TASK_DIR);

app.listen(PORT, () => {
  console.log(`✅ Webhook server running on port ${PORT}`);
  console.log(`LOG_DIR=${LOG_DIR}, TASK_DIR=${TASK_DIR}, SECRET=${WEBHOOK_SECRET ? '<set>' : '<empty>'}`);
});
