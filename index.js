/**
 * BIKA Pro Slot Bot — FINAL (Webhook, Render Web Service) — NO ERROR BUILD
 * -----------------------------------------------------------------------
 * ✅ Express + Webhook + UptimeRobot GET /
 * ✅ MongoDB (transactions + safe fallback) + driver-safe findOneAndUpdate handling
 * ✅ Owner via ENV OWNER_ID
 * ✅ Treasury: /settotal, /treasury (owner only)
 * ✅ /start one-time bonus 300 (ONLY if treasury has balance) — atomic claim (no duplicate payout)
 * ✅ /dailyclaim group only (Yangon day) 50~100 (ONLY if treasury has balance) — atomic day lock
 * ✅ .slot 100 (group) animated edit UI (with MAX_ACTIVE_SLOTS = 15)
 * ✅ /setrtp 90 + /rtp payout pro table
 * ✅ /shop inline buy -> Orders + receipt
 * ✅ Admin inline dashboard + Orders panel + Order actions + User DM notify
 * ✅ OWNER order alert to OWNER_ID DM (and optional ADMIN_ALERT_CHAT_ID)
 * ✅ Anti-429: bot.catch + safeTelegramCall (auto retry after retry_after)
 * ✅ Group cooldown: .mybalance + .slot to reduce spam
 *
 * NOTE:
 * - Webhook mode on Render: DO NOT call bot.launch() and DO NOT call bot.stop()
 */

require("dotenv").config();
const express = require("express");
const { Telegraf } = require("telegraf");
const { MongoClient, ObjectId } = require("mongodb");

// -------------------- ENV --------------------
const BOT_TOKEN = process.env.BOT_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || "bika_slot";
const TZ = process.env.TZ || "Asia/Yangon";
const OWNER_ID = process.env.OWNER_ID ? Number(process.env.OWNER_ID) : null;

const PUBLIC_URL = process.env.PUBLIC_URL; // e.g. https://bikagamebot.onrender.com
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET; // random string

// Optional: send new order alert to this chat too (can be group id like -100xxxx)
const ADMIN_ALERT_CHAT_ID = process.env.ADMIN_ALERT_CHAT_ID ? Number(process.env.ADMIN_ALERT_CHAT_ID) : null;

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!MONGO_URI) throw new Error("Missing MONGO_URI");
if (!OWNER_ID || !Number.isFinite(OWNER_ID)) throw new Error("Missing/Invalid OWNER_ID (must be a number)");
if (!PUBLIC_URL) throw new Error("Missing PUBLIC_URL (e.g. https://xxx.onrender.com)");
if (!WEBHOOK_SECRET) throw new Error("Missing WEBHOOK_SECRET");

// -------------------- Bot/DB --------------------
const bot = new Telegraf(BOT_TOKEN);

let mongo, db;
let users, txs, orders, configCol;
let TX_SUPPORTED = true;

// -------------------- Anti-crash global catch --------------------
bot.catch((err, ctx) => {
  const retry = err?.response?.parameters?.retry_after;
  console.log("🧯 bot.catch:", err?.message || err, "retry_after:", retry, "updateType:", ctx?.updateType);
  // NEVER throw here
});

// -------------------- UI helpers (HTML) --------------------
const COIN = "MMK";

function escHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function fmt(n) {
  const x = typeof n === "string" ? Number(n.replace(/,/g, "")) : Number(n || 0);
  return Number.isFinite(x) ? x.toLocaleString("en-US") : "0";
}

function toNum(v) {
  if (typeof v === "number") return v;
  if (typeof v === "string") return Number(v.replace(/,/g, "")) || 0;
  return 0;
}

function isGroupChat(ctx) {
  const t = ctx.chat?.type;
  return t === "group" || t === "supergroup";
}

function mentionHtml(tg) {
  const name = tg?.first_name || tg?.username || "User";
  const id = tg?.id;
  if (!id) return `<b>${escHtml(name)}</b>`;
  return `<a href="tg://user?id=${id}">${escHtml(name)}</a>`;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function getRetryAfterSeconds(e) {
  return e?.response?.parameters?.retry_after || null;
}

async function safeTelegramCall(fn, maxRetry = 1) {
  let lastErr = null;
  for (let i = 0; i <= maxRetry; i++) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      const ra = getRetryAfterSeconds(e);
      if (ra && i < maxRetry) {
        console.log(`⏳ Telegram 429 — retry after ${ra}s`);
        await sleep((ra + 1) * 1000);
        continue;
      }
      console.log("⚠️ Telegram call failed:", e?.message || e);
      return null;
    }
  }
  console.log("⚠️ Telegram call failed (final):", lastErr?.message || lastErr);
  return null;
}

async function replyHTML(ctx, html, extra = {}) {
  const payload = {
    parse_mode: "HTML",
    disable_web_page_preview: true,
    ...extra,
  };
  // If reply fails due to HTML parse, fallback to plain
  const res = await safeTelegramCall(() => ctx.reply(html, payload), 1);
  if (res) return res;
  const plain = String(html).replace(/<[^>]+>/g, "");
  return safeTelegramCall(() => ctx.reply(plain, { disable_web_page_preview: true, ...extra }), 1);
}

async function editHTML(ctx, chatId, messageId, html, extra = {}) {
  return safeTelegramCall(
    () =>
      ctx.telegram.editMessageText(chatId, messageId, undefined, html, {
        parse_mode: "HTML",
        disable_web_page_preview: true,
        ...extra,
      }),
    1
  );
}

async function sendHTML(chatId, html, extra = {}) {
  const payload = { parse_mode: "HTML", disable_web_page_preview: true, ...extra };
  const res = await safeTelegramCall(() => bot.telegram.sendMessage(chatId, html, payload), 1);
  if (res) return res;
  const plain = String(html).replace(/<[^>]+>/g, "");
  return safeTelegramCall(() => bot.telegram.sendMessage(chatId, plain, { disable_web_page_preview: true, ...extra }), 1);
}

// -------------------- Parsing --------------------
function parseAmount(text) {
  const parts = (text || "").trim().split(/\s+/);
  for (let i = 1; i < parts.length; i++) {
    const t = parts[i].replace(/,/g, "");
    if (/^\d+(\.\d+)?$/.test(t)) return Number(t);
  }
  return null;
}

function parseMentionUsername(text) {
  const parts = (text || "").trim().split(/\s+/);
  for (let i = 1; i < parts.length; i++) {
    const p = parts[i];
    if (p.startsWith("@") && p.length > 1) return p.slice(1).toLowerCase();
  }
  return null;
}

// -------------------- Yangon time helpers --------------------
function startOfDayYangon(d) {
  const ms = d.getTime();
  const offsetMs = 6.5 * 60 * 60 * 1000; // Yangon +06:30
  const local = new Date(ms + offsetMs);
  local.setUTCHours(0, 0, 0, 0);
  return new Date(local.getTime() - offsetMs);
}

function formatYangon(dt = new Date()) {
  try {
    return new Intl.DateTimeFormat("en-GB", {
      timeZone: "Asia/Yangon",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).format(dt);
  } catch {
    return dt.toISOString();
  }
}

// -------------------- Mongo connect --------------------
async function connectMongo() {
  mongo = new MongoClient(MONGO_URI, {});
  await mongo.connect();
  db = mongo.db(DB_NAME);

  users = db.collection("users");
  txs = db.collection("transactions");
  orders = db.collection("orders");
  configCol = db.collection("config");

  await users.createIndex({ userId: 1 }, { unique: true });
  await users.createIndex({ username: 1 }, { sparse: true });

  await txs.createIndex({ createdAt: -1 });
  await txs.createIndex({ type: 1, createdAt: -1 });

  await orders.createIndex({ status: 1, createdAt: -1 });
  await orders.createIndex({ userId: 1, createdAt: -1 });

  await configCol.createIndex({ key: 1 }, { unique: true });

  console.log("✅ Mongo connected");
}

// -------------------- User + Treasury primitives --------------------
async function ensureUser(tgUser) {
  const doc = {
    userId: tgUser.id,
    username: tgUser.username ? tgUser.username.toLowerCase() : null,
    firstName: tgUser.first_name || null,
    lastName: tgUser.last_name || null,
    updatedAt: new Date(),
  };

  await users.updateOne(
    { userId: tgUser.id },
    {
      $set: doc,
      $setOnInsert: {
        balance: 0,
        createdAt: new Date(),
        startBonusClaimed: false,
        lastDailyClaimAt: null,
        lastDailyClaimDayKey: null,
      },
    },
    { upsert: true }
  );

  return users.findOne({ userId: tgUser.id });
}

async function getUser(userId) {
  return users.findOne({ userId });
}

async function getUserByUsername(username) {
  return users.findOne({ username: username.toLowerCase() });
}

// -------- Treasury (Owner bank) --------
async function ensureTreasury() {
  const exist = await configCol.findOne({ key: "treasury" });

  if (exist) {
    const fixedTotal = toNum(exist.totalSupply);
    const fixedOwner = toNum(exist.ownerBalance);
    const fixedOwnerId = exist.ownerUserId || OWNER_ID;

    const needFix =
      fixedTotal !== exist.totalSupply ||
      fixedOwner !== exist.ownerBalance ||
      fixedOwnerId !== exist.ownerUserId;

    if (needFix) {
      await configCol.updateOne(
        { key: "treasury" },
        {
          $set: {
            totalSupply: fixedTotal,
            ownerBalance: fixedOwner,
            ownerUserId: fixedOwnerId,
            updatedAt: new Date(),
          },
        }
      );
      return configCol.findOne({ key: "treasury" });
    }
    return exist;
  }

  await configCol.insertOne({
    key: "treasury",
    ownerUserId: OWNER_ID,
    totalSupply: 0,
    ownerBalance: 0,
    createdAt: new Date(),
    updatedAt: new Date(),
  });
  return configCol.findOne({ key: "treasury" });
}

async function getTreasury() {
  return configCol.findOne({ key: "treasury" });
}

function isOwner(ctx, treasury) {
  return treasury?.ownerUserId && ctx.from?.id === treasury.ownerUserId;
}

async function setTotalSupply(ctx, amount) {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return { ok: false, reason: "NOT_OWNER" };

  const amt = Math.max(0, Math.floor(toNum(amount)));
  await configCol.updateOne(
    { key: "treasury" },
    { $set: { totalSupply: amt, ownerBalance: amt, updatedAt: new Date() } }
  );
  return { ok: true };
}

// -------------------- Transaction helpers --------------------
function txErrorLooksUnsupported(err) {
  const m = String(err?.message || err);
  return (
    m.includes("Transaction numbers are only allowed") ||
    m.includes("replica set") ||
    m.includes("mongos") ||
    m.includes("does not support transactions") ||
    (m.includes("Transaction") && m.includes("not supported"))
  );
}

async function withMaybeTx(work) {
  if (!TX_SUPPORTED) return work(null);

  const session = mongo.startSession();
  try {
    return await session.withTransaction(async () => work(session));
  } catch (e) {
    if (txErrorLooksUnsupported(e)) {
      TX_SUPPORTED = false;
      console.log("⚠️ TX unsupported. Falling back to non-transaction mode.");
      return await work(null);
    }
    throw e;
  } finally {
    try {
      await session.endSession();
    } catch (_) {}
  }
}

// Mongo driver safe extraction: some versions return {value}, some return doc
function extractUpdatedDoc(res) {
  if (!res) return null;
  if (res.value !== undefined) return res.value;
  if (res?.lastErrorObject && res?.ok !== undefined && res?.value !== undefined) return res.value;
  return res;
}

// Atomic: Treasury -> User
async function treasuryPayToUser(toUserId, amount, meta = {}) {
  const amt = Math.floor(toNum(amount));
  if (amt <= 0) return;

  return withMaybeTx(async (session) => {
    const opts = session ? { session, returnDocument: "after" } : { returnDocument: "after" };

    const tRes = await configCol.findOneAndUpdate(
      { key: "treasury", ownerBalance: { $gte: amt } },
      { $inc: { ownerBalance: -amt }, $set: { updatedAt: new Date() } },
      opts
    );

    const tDoc = extractUpdatedDoc(tRes);
    if (!tDoc) throw new Error("TREASURY_INSUFFICIENT");

    const uOpts = session ? { upsert: true, session } : { upsert: true };
    await users.updateOne(
      { userId: toUserId },
      { $inc: { balance: amt }, $set: { updatedAt: new Date() }, $setOnInsert: { createdAt: new Date() } },
      uOpts
    );

    const txOpts = session ? { session } : {};
    await txs.insertOne(
      {
        type: meta.type || "treasury_pay",
        fromUserId: "TREASURY",
        toUserId,
        amount: amt,
        meta,
        createdAt: new Date(),
      },
      txOpts
    );
  });
}

// Atomic: User -> Treasury
async function userPayToTreasury(fromUserId, amount, meta = {}) {
  const amt = Math.floor(toNum(amount));
  if (amt <= 0) return;

  return withMaybeTx(async (session) => {
    const opts = session ? { session, returnDocument: "after" } : { returnDocument: "after" };

    const uRes = await users.findOneAndUpdate(
      { userId: fromUserId, balance: { $gte: amt } },
      { $inc: { balance: -amt }, $set: { updatedAt: new Date() } },
      opts
    );

    const uDoc = extractUpdatedDoc(uRes);
    if (!uDoc) throw new Error("USER_INSUFFICIENT");

    const tOpts = session ? { session } : {};
    await configCol.updateOne(
      { key: "treasury" },
      { $inc: { ownerBalance: amt }, $set: { updatedAt: new Date() } },
      tOpts
    );

    const txOpts = session ? { session } : {};
    await txs.insertOne(
      {
        type: meta.type || "treasury_receive",
        fromUserId,
        toUserId: "TREASURY",
        amount: amt,
        meta,
        createdAt: new Date(),
      },
      txOpts
    );
  });
}

// Atomic: User -> User
async function transferBalance(fromUserId, toUserId, amount, meta = {}) {
  const amt = Math.floor(toNum(amount));
  if (amt <= 0) return;

  return withMaybeTx(async (session) => {
    const opts = session ? { session, returnDocument: "after" } : { returnDocument: "after" };

    const fromRes = await users.findOneAndUpdate(
      { userId: fromUserId, balance: { $gte: amt } },
      { $inc: { balance: -amt }, $set: { updatedAt: new Date() } },
      opts
    );

    const fromDoc = extractUpdatedDoc(fromRes);
    if (!fromDoc) throw new Error("INSUFFICIENT");

    const toOpts = session ? { upsert: true, session } : { upsert: true };
    await users.updateOne(
      { userId: toUserId },
      { $inc: { balance: amt }, $set: { updatedAt: new Date() }, $setOnInsert: { createdAt: new Date() } },
      toOpts
    );

    const txOpts = session ? { session } : {};
    await txs.insertOne(
      { type: "gift", fromUserId, toUserId, amount: amt, meta, createdAt: new Date() },
      txOpts
    );
  });
}

// -------------------- Treasury commands (Owner only) --------------------
bot.command("settotal", async (ctx) => {
  const amount = parseAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return replyHTML(ctx, `🏦 <b>Treasury Settings</b>\n━━━━━━━━━━━━━━\nUsage: <code>/settotal 5000000</code>`);
  }
  const r = await setTotalSupply(ctx, amount);
  if (!r.ok) return replyHTML(ctx, "⛔ Owner only command.");

  const tt = await getTreasury();
  return replyHTML(
    ctx,
    `🏦 <b>Treasury Initialized</b>\n━━━━━━━━━━━━━━\n• Total Supply: <b>${fmt(tt.totalSupply)}</b> ${COIN}\n• Owner Balance: <b>${fmt(tt.ownerBalance)}</b> ${COIN}`
  );
});

bot.command("treasury", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only.");
  const tr = await getTreasury();
  return replyHTML(
    ctx,
    `🏦 <b>Treasury Dashboard</b>\n━━━━━━━━━━━━━━\n• Total Supply: <b>${fmt(tr.totalSupply)}</b> ${COIN}\n• Owner Balance: <b>${fmt(tr.ownerBalance)}</b> ${COIN}\n• Timezone: <b>${escHtml(TZ)}</b>\n• Owner ID: <code>${tr.ownerUserId}</code>`
  );
});

// -------------------- Start bonus + /balance --------------------
const START_BONUS = 300;

// Atomic start bonus guard (prevents duplicate payouts due to webhook retries)
async function tryClaimStartBonus(userId) {
  // Set claimed = true only if currently false
  const res = await users.findOneAndUpdate(
    { userId, startBonusClaimed: false },
    { $set: { startBonusClaimed: true, updatedAt: new Date() } },
    { returnDocument: "after" }
  );
  const doc = extractUpdatedDoc(res);
  return !!doc;
}

bot.start(async (ctx) => {
  await ensureTreasury();
  const u = await ensureUser(ctx.from);

  // already claimed -> welcome back
  if (u.startBonusClaimed) {
    return replyHTML(
      ctx,
      `👋 <b>Welcome back</b>\n━━━━━━━━━━━━━\nGroup Commands:\n• <code>/dailyclaim</code>\n• <code>.slot 100</code>\n• <code>.mybalance</code>\n• <code>/shop</code>`
    );
  }

  const tr = await getTreasury();
  if (toNum(tr?.ownerBalance) < START_BONUS) {
    return replyHTML(
      ctx,
      `⚠️ <b>Treasury မသတ်မှတ်ရသေးပါ</b>\n━━━━━━━━━━━━━━\nOwner က <code>/settotal 5000000</code> လုပ်ပြီးမှ Welcome Bonus ပေးနိုင်ပါတယ်။`
    );
  }

  // lock claim first (atomic), then pay
  const locked = await tryClaimStartBonus(ctx.from.id);
  if (!locked) {
    // someone already claimed (or concurrent)
    const updated = await getUser(ctx.from.id);
    return replyHTML(
      ctx,
      `👋 <b>Welcome back</b>\n━━━━━━━━━━━━━━━━━━━━\n💼 Balance: <b>${fmt(updated?.balance)}</b> ${COIN}\n• <code>/dailyclaim</code>\n• <code>.slot 100</code>\n• <code>.mybalance</code>\n• <code>/shop</code>`
    );
  }

  try {
    await treasuryPayToUser(ctx.from.id, START_BONUS, { type: "start_bonus" });
    const updated = await getUser(ctx.from.id);

    return replyHTML(
      ctx,
      `🎉 <b>Welcome Bonus</b>\n━━━━━━━━━━━━━━\n` +
        `👤 ${mentionHtml(ctx.from)}\n` +
        `➕ Bonus: <b>${fmt(START_BONUS)}</b> ${COIN}\n` +
        `💼 Balance: <b>${fmt(updated?.balance)}</b> ${COIN}\n` +
        `━━━━━━━━━━━━━\n` +
        `Group Commands:\n• <code>/dailyclaim</code>\n• <code>.slot 100</code>\n• <code>.mybalance</code>\n• <code>/shop</code>`
    );
  } catch (e) {
    // If payout fails, revert claim flag (so user can claim later)
    await users.updateOne({ userId: ctx.from.id }, { $set: { startBonusClaimed: false, updatedAt: new Date() } });

    if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
      return replyHTML(ctx, "🏦 Treasury မလုံလောက်ပါ။ Owner က /settotal ပြန်သတ်မှတ်ပေးပါ။");
    }
    console.error("start bonus pay fail:", e);
    return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

bot.command("balance", async (ctx) => {
  const u = await ensureUser(ctx.from);
  return replyHTML(ctx, `💼 Balance: <b>${fmt(u.balance)}</b> ${COIN}`);
});

// -------------------- Daily claim (Group only, Yangon day) --------------------
const DAILY_MIN = 50;
const DAILY_MAX = 100;

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function dayKeyYangon(d) {
  // e.g. 2026-02-17 in Yangon
  const s = formatYangon(d); // "17/02/2026, 21:46:38"
  const [dd, mm, yyyy] = s.split(",")[0].trim().split("/");
  return `${yyyy}-${mm}-${dd}`;
}

bot.command("dailyclaim", async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>/dailyclaim</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  await ensureTreasury();
  await ensureUser(ctx.from);

  const now = new Date();
  const todayKey = dayKeyYangon(now);

  // atomic: set lastDailyClaimDayKey if not equal today
  const lockRes = await users.findOneAndUpdate(
    { userId: ctx.from.id, lastDailyClaimDayKey: { $ne: todayKey } },
    { $set: { lastDailyClaimDayKey: todayKey, lastDailyClaimAt: now, updatedAt: now } },
    { returnDocument: "after" }
  );
  const locked = extractUpdatedDoc(lockRes);

  if (!locked) {
    return replyHTML(
      ctx,
      `⏳ <b>Daily Claim</b>\n━━━━━━━━━━━━━━━━━━━━\nဒီနေ့ claim လုပ်ပြီးသားပါ။\nYangon time နဲ့ နေ့သစ်ဝင်ပြီးမှ ပြန် claim လုပ်နိုင်ပါတယ်။`
    );
  }

  const amount = randInt(DAILY_MIN, DAILY_MAX);
  const tr = await getTreasury();
  if (toNum(tr?.ownerBalance) < amount) {
    // revert lock so user can claim later when treasury refilled
    await users.updateOne(
      { userId: ctx.from.id },
      { $set: { lastDailyClaimDayKey: null, lastDailyClaimAt: null, updatedAt: new Date() } }
    );
    return replyHTML(ctx, "🏦 Treasury မလုံလောက်လို့ daily claim မပေးနိုင်သေးပါ။");
  }

  try {
    await treasuryPayToUser(ctx.from.id, amount, { type: "daily_claim" });
    const updated = await getUser(ctx.from.id);
    return replyHTML(
      ctx,
      `🎁 <b>Daily Claim Success</b>\n━━━━━━━━━━━━━━━━━━━━\n` +
        `👤 ${mentionHtml(ctx.from)}\n` +
        `➕ Reward: <b>${fmt(amount)}</b> ${COIN}\n` +
        `💼 Balance: <b>${fmt(updated?.balance)}</b> ${COIN}\n` +
        `🕒 ${escHtml(formatYangon(now))} (Yangon)`
    );
  } catch (e) {
    // revert lock
    await users.updateOne(
      { userId: ctx.from.id },
      { $set: { lastDailyClaimDayKey: null, lastDailyClaimAt: null, updatedAt: new Date() } }
    );

    if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) return replyHTML(ctx, "🏦 Treasury မလုံလောက်ပါ။");
    console.error("dailyclaim error:", e);
    return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

// -------------------- .mybalance Pro+ (GROUP ONLY) + COOLDOWN --------------------
function getBalanceRank(balance) {
  const b = toNum(balance);
  if (b === 0) return { title: "ဖင်ပြောင်ငမွဲ အဆင့်", badge: "🪫", color: "⚪" };
  if (b <= 500) return { title: "ဆင်းရဲသား အိမ်ခြေမဲ့ အဆင့်", badge: "🥀", color: "🟤" };
  if (b <= 1000) return { title: "အိမ်ပိုင်ဝန်းပိုင် ဆင်းရဲသားအဆင့်", badge: "🏚️", color: "🟠" };
  if (b <= 5000) return { title: "လူလတ်တန်းစားအဆင့်", badge: "🏘️", color: "🟢" };
  if (b <= 10000) return { title: "သူဌေးပေါက်စ အဆင့်", badge: "💼", color: "🔵" };
  if (b <= 100000) return { title: "သိန်းကြွယ်သူဌေး အဆင့်", badge: "💰", color: "🟣" };
  if (b <= 1000000) return { title: "သန်းကြွယ်သူဌေးအကြီးစား အဆင့်", badge: "🏦", color: "🟡" };
  if (b <= 50000000) return { title: "ကုဋေရှစ်ဆယ် သူဌေးကြီး အဆင့်", badge: "👑", color: "🟠" };
  return { title: "ကုဋေရှစ်ဆယ် သူဌေးအကြီးစား အဆင့်", badge: "👑✨", color: "🟥" };
}

function progressBar(current, min, max, blocks = 10) {
  if (max <= min) return "██████████";
  const ratio = Math.max(0, Math.min(1, (current - min) / (max - min)));
  const filled = Math.round(ratio * blocks);
  return "█".repeat(filled) + "░".repeat(blocks - filled);
}

function getRankRange(balance) {
  const b = toNum(balance);
  if (b === 0) return { min: 0, max: 0 };
  if (b <= 500) return { min: 1, max: 500 };
  if (b <= 1000) return { min: 501, max: 1000 };
  if (b <= 5000) return { min: 1001, max: 5000 };
  if (b <= 10000) return { min: 5001, max: 10000 };
  if (b <= 100000) return { min: 10001, max: 100000 };
  if (b <= 1000000) return { min: 100001, max: 1000000 };
  if (b <= 50000000) return { min: 1000001, max: 50000000 };
  return { min: 50000001, max: b };
}

// group-wide cooldown to prevent 429
const groupMyBalCooldown = new Map(); // chatId -> lastTime
const MYBAL_GROUP_COOLDOWN_MS = 6000;

bot.hears(/^\.(mybalance|bal)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>.mybalance</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  const chatId = ctx.chat?.id;
  const last = groupMyBalCooldown.get(chatId) || 0;
  if (Date.now() - last < MYBAL_GROUP_COOLDOWN_MS) {
    const sec = Math.ceil((MYBAL_GROUP_COOLDOWN_MS - (Date.now() - last)) / 1000);
    return replyHTML(
      ctx,
      `⏳ <b>ခဏစောင့်ပါ</b>\n━━━━━━━━━\nဒီ group မှာ <b>${sec}s</b> နောက်မှ <code>.mybalance</code> ပြန်သုံးနိုင်ပါတယ်။`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }
  groupMyBalCooldown.set(chatId, Date.now());

  const u = await ensureUser(ctx.from);
  const bal = toNum(u.balance);

  const rank = getBalanceRank(bal);
  const range = getRankRange(bal);
  const bar = range.max === range.min ? "██████████" : progressBar(bal, range.min, range.max, 10);

  const msg =
    `💼 <b>BIKA Pro+ Wallet</b>\n` +
    `━━━━━━━━━━━━━━\n` +
    `👤 ${mentionHtml(ctx.from)}\n` +
    `🪙 Balance: <b>${fmt(bal)}</b> ${COIN}\n` +
    `━━━━━━━━━━━━━━\n` +
    `${rank.badge} <b>Rank:</b> ${escHtml(rank.title)}\n` +
    `${rank.color} <b>Progress:</b> <code>${escHtml(bar)}</code>\n` +
    `📌 Range: <b>${fmt(range.min)}</b> → <b>${fmt(range.max)}</b> ${COIN}\n` +
    `━━━━━━━━━━━━━━\n` +
    `🕒 ${escHtml(formatYangon(new Date()))} (Yangon)`;

  return replyHTML(ctx, msg, { reply_to_message_id: ctx.message?.message_id });
});

// -------------------- Gift (User -> User) --------------------
const GIFT_COOLDOWN_MS = 10_000;
const lastGiftAt = new Map();

bot.command("gift", async (ctx) => {
  const fromTg = ctx.from;
  if (!fromTg) return;

  const last = lastGiftAt.get(fromTg.id) || 0;
  if (Date.now() - last < GIFT_COOLDOWN_MS) {
    const sec = Math.ceil((GIFT_COOLDOWN_MS - (Date.now() - last)) / 1000);
    return replyHTML(ctx, `⏳ ခဏစောင့်ပါ… (${sec}s) နောက်တစ်ခါ <code>/gift</code> လုပ်နိုင်ပါမယ်။`);
  }

  const amount = parseAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return replyHTML(
      ctx,
      `🎁 <b>Gift Usage</b>\n━━━━━━━━━━━━━━\n• Reply +  <code>/gift 500</code>\n• Mention  <code>/gift @username 500</code>`
    );
  }

  await ensureUser(fromTg);

  let toUserId = null;
  let toLabelHtml = null;

  const replyFrom = ctx.message?.reply_to_message?.from;
  if (replyFrom?.id) {
    if (replyFrom.is_bot) return replyHTML(ctx, "🤖 Bot ကို gift မပို့နိုင်ပါ။");
    if (replyFrom.id === fromTg.id) return replyHTML(ctx, "😅 ကိုယ့်ကိုကိုယ် gift မပို့နိုင်ပါ။");
    await ensureUser(replyFrom);
    toUserId = replyFrom.id;
    toLabelHtml = mentionHtml(replyFrom);
  } else {
    const uname = parseMentionUsername(ctx.message?.text || "");
    if (!uname) return replyHTML(ctx, "👤 Reply (/gift 500) သို့ /gift @username 500 သုံးပါ။");
    const toU = await getUserByUsername(uname);
    if (!toU)
      return replyHTML(ctx, "⚠️ ဒီ @username ကို မတွေ့ပါ။ (သူ bot ကို /start လုပ်ထားရမယ်) သို့ Reply နဲ့ gift ပို့ပါ။");
    if (toU.userId === fromTg.id) return replyHTML(ctx, "😅 ကိုယ့်ကိုကိုယ် gift မပို့နိုင်ပါ။");
    toUserId = toU.userId;
    toLabelHtml = `@${escHtml(uname)}`;
  }

  try {
    await transferBalance(fromTg.id, toUserId, Math.floor(amount), { chatId: ctx.chat?.id });
    lastGiftAt.set(fromTg.id, Date.now());
    const updatedFrom = await getUser(fromTg.id);

    return replyHTML(
      ctx,
      `✅ <b>Gift Sent</b>\n━━━━━━━━━━━━━━\n🎁 To: ${toLabelHtml}\nAmount: <b>${fmt(amount)}</b> ${COIN}\nYour Balance: <b>${fmt(updatedFrom?.balance)}</b> ${COIN}`
    );
  } catch (e) {
    if (String(e?.message || e).includes("INSUFFICIENT")) return replyHTML(ctx, "❌ Balance မလုံလောက်ပါ။");
    console.error("gift error:", e);
    return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

// -------------------- Owner add/remove balance --------------------
function parseTargetAndAmount(text) {
  const parts = (text || "").trim().split(/\s+/);
  const amount = parseAmount(text);

  if (parts.length === 2 && amount) return { mode: "reply", target: null, amount };
  if (parts.length >= 3) {
    const rawTarget = parts[1];
    if (rawTarget.startsWith("@"))
      return { mode: "explicit", target: { type: "username", value: rawTarget.slice(1).toLowerCase() }, amount };
    if (/^\d+$/.test(rawTarget)) return { mode: "explicit", target: { type: "userId", value: parseInt(rawTarget, 10) }, amount };
  }
  return { mode: "invalid", target: null, amount: null };
}

async function resolveTargetFromCtx(ctx, mode, target) {
  const replyFrom = ctx.message?.reply_to_message?.from;
  if (mode === "reply" && replyFrom?.id) {
    if (replyFrom.is_bot) return { ok: false, reason: "TARGET_IS_BOT" };
    await ensureUser(replyFrom);
    return { ok: true, userId: replyFrom.id, labelHtml: mentionHtml(replyFrom) };
  }

  if (mode === "explicit" && target) {
    if (target.type === "username") {
      const u = await getUserByUsername(target.value);
      if (!u) return { ok: false, reason: "NOT_FOUND_DB" };
      return { ok: true, userId: u.userId, labelHtml: `@${escHtml(target.value)}` };
    }
    if (target.type === "userId") {
      await users.updateOne(
        { userId: target.value },
        { $setOnInsert: { userId: target.value, balance: 0, createdAt: new Date() }, $set: { updatedAt: new Date() } },
        { upsert: true }
      );
      return { ok: true, userId: target.value, labelHtml: `<code>${target.value}</code>` };
    }
  }

  return { ok: false, reason: "NO_TARGET" };
}

bot.command("addbalance", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only command.");

  const { mode, target, amount } = parseTargetAndAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return replyHTML(
      ctx,
      `➕ <b>Add Balance (Owner)</b>\n━━━━━━━━━━━━━━\nReply mode:\n• Reply + <code>/addbalance 5000</code>\n\nExplicit:\n• <code>/addbalance @username 5000</code>\n• <code>/addbalance 123456789 5000</code>`
    );
  }

  const r = await resolveTargetFromCtx(ctx, mode, target);
  if (!r.ok) return replyHTML(ctx, "👤 Target မရွေးရသေးပါ။ Reply + /addbalance 5000 သို့ /addbalance @username 5000");

  try {
    await treasuryPayToUser(r.userId, Math.floor(amount), { type: "owner_addbalance", by: ctx.from.id });
    const u = await getUser(r.userId);
    const tr = await getTreasury();

    return replyHTML(
      ctx,
      `✅ <b>Balance Added</b>\n━━━━━━━━━━━━━━\nUser: ${r.labelHtml}\nAmount: <b>${fmt(amount)}</b> ${COIN}\nUser Balance: <b>${fmt(u?.balance)}</b> ${COIN}\nTreasury Left: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}`
    );
  } catch (e) {
    if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
      const tr = await getTreasury();
      return replyHTML(ctx, `❌ Treasury မလုံလောက်ပါ။ (Treasury: <b>${fmt(tr?.ownerBalance)}</b> ${COIN})`);
    }
    console.error("addbalance error:", e);
    return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

bot.command("removebalance", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only command.");

  const { mode, target, amount } = parseTargetAndAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return replyHTML(
      ctx,
      `➖ <b>Remove Balance (Owner)</b>\n━━━━━━━━━━━━━━\nReply mode:\n• Reply + <code>/removebalance 5000</code>\n\nExplicit:\n• <code>/removebalance @username 5000</code>\n• <code>/removebalance 123456789 5000</code>`
    );
  }

  const r = await resolveTargetFromCtx(ctx, mode, target);
  if (!r.ok) return replyHTML(ctx, "👤 Target မရွေးရသေးပါ။ Reply + /removebalance 5000 သို့ /removebalance @username 5000");

  try {
    await userPayToTreasury(r.userId, Math.floor(amount), { type: "owner_removebalance", by: ctx.from.id });
    const u = await getUser(r.userId);
    const tr = await getTreasury();

    return replyHTML(
      ctx,
      `✅ <b>Balance Removed</b>\n━━━━━━━━━━━━\nUser: ${r.labelHtml}\nAmount: <b>${fmt(amount)}</b> ${COIN}\nUser Balance: <b>${fmt(u?.balance)}</b> ${COIN}\nTreasury Now: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}`
    );
  } catch (e) {
    if (String(e?.message || e).includes("USER_INSUFFICIENT")) {
      const u = await getUser(r.userId);
      return replyHTML(ctx, `❌ User balance မလုံလောက်ပါ။ (Balance: <b>${fmt(u?.balance)}</b> ${COIN})`);
    }
    console.error("removebalance error:", e);
    return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

// -------------------- Shop + Orders --------------------
const SHOP_ITEMS = [
  { id: "dia11", name: "Diamonds 11 💎", price: 10000 },
  { id: "dia22", name: "Diamonds 22 💎", price: 19000 },
  { id: "dia33", name: "Diamonds 33 💎", price: 28000 },
  { id: "dia44", name: "Diamonds 44 💎", price: 37000 },
  { id: "dia55", name: "Diamonds 55 💎", price: 46000 },
  { id: "wp1", name: "Weekly Pass 🎟️", price: 70000 },
];

const ORDER_STATUS = {
  PENDING: "PENDING",
  PAID: "PAID",
  DELIVERED: "DELIVERED",
  CANCELLED: "CANCELLED",
};

function shopKeyboard() {
  const rows = [];
  for (let i = 0; i < SHOP_ITEMS.length; i += 2) {
    const a = SHOP_ITEMS[i];
    const b = SHOP_ITEMS[i + 1];
    const row = [{ text: `${a.name} • ${fmt(a.price)} ${COIN}`, callback_data: `BUY:${a.id}` }];
    if (b) row.push({ text: `${b.name} • ${fmt(b.price)} ${COIN}`, callback_data: `BUY:${b.id}` });
    rows.push(row);
  }
  rows.push([{ text: "🔄 Refresh", callback_data: "SHOP:REFRESH" }]);
  return { inline_keyboard: rows };
}

function shopText(balance) {
  const lines = SHOP_ITEMS.map((x) => `• ${escHtml(x.name)} — <b>${fmt(x.price)}</b> ${COIN}`).join("\n");
  return (
    `🛒 <b>BIKA Pro Shop</b>\n` +
    `━━━━━━━━━━━━\n` +
    `${lines}\n` +
    `━━━━━━━━━━━━\n` +
    `💼 Your Balance: <b>${fmt(balance)}</b> ${COIN}\n` +
    `Select an item below:`
  );
}

function genReceiptCode() {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let s = "";
  for (let i = 0; i < 10; i++) s += alphabet[Math.floor(Math.random() * alphabet.length)];
  return s;
}

function orderActionKeyboard(orderId) {
  return {
    inline_keyboard: [
      [
        { text: "✅ Mark PAID", callback_data: `ORDER:PAID:${orderId}` },
        { text: "📦 Mark DELIVERED", callback_data: `ORDER:DELIVERED:${orderId}` },
      ],
      [{ text: "❌ Cancel + Refund", callback_data: `ORDER:CANCEL:${orderId}` }],
      [{ text: "⬅️ Back to Admin", callback_data: "ADMIN:REFRESH" }],
    ],
  };
}

function adminOrdersKeyboard(list) {
  const rows = [];
  for (const o of list.slice(0, 6)) {
    const id = String(o._id);
    const label = `${o.itemName} • ${fmt(o.price)} ${COIN} • ${o.status}`;
    rows.push([{ text: `🧾 ${label}`, callback_data: `ORDER:OPEN:${id}` }]);
  }
  rows.push([{ text: "🔄 Refresh Orders", callback_data: "ADMIN:ORDERS" }]);
  rows.push([{ text: "⬅️ Back to Admin", callback_data: "ADMIN:REFRESH" }]);
  return { inline_keyboard: rows };
}

bot.command("shop", async (ctx) => {
  const u = await ensureUser(ctx.from);
  await ensureTreasury();
  return replyHTML(ctx, shopText(u.balance), { reply_markup: shopKeyboard() });
});

// -------------------- Slot (Animated Edit UI) + MAX_ACTIVE_SLOTS --------------------
const SLOT = {
  minBet: 50,
  maxBet: 5000,
  cooldownMs: 3000,
  capPercent: 0.30,
  reels: [
    [
      { s: "🍒", w: 3200 },
      { s: "🍋", w: 3200 },
      { s: "🍉", w: 2200 },
      { s: "🔔", w: 900 },
      { s: "⭐", w: 450 },
      { s: "BAR", w: 45 },
      { s: "7", w: 5 },
    ],
    [
      { s: "🍒", w: 3200 },
      { s: "🍋", w: 3200 },
      { s: "🍉", w: 2200 },
      { s: "🔔", w: 900 },
      { s: "⭐", w: 450 },
      { s: "BAR", w: 45 },
      { s: "7", w: 5 },
    ],
    [
      { s: "🍒", w: 3200 },
      { s: "🍋", w: 3200 },
      { s: "🍉", w: 2200 },
      { s: "🔔", w: 900 },
      { s: "⭐", w: 450 },
      { s: "BAR", w: 95 },
      { s: "7", w: 50 },
    ],
  ],
  payouts: {
    "7,7,7": 30,
    "BAR,BAR,BAR": 20,
    "⭐,⭐,⭐": 10,
    "🔔,🔔,🔔": 8,
    "🍉,🍉,🍉": 6,
    "🍋,🍋,🍋": 4,
    "🍒,🍒,🍒": 2,
    ANY2: 1.3,
  },
};

// ✅ Max concurrent slot spins
const activeSlots = new Set(); // userId set
const MAX_ACTIVE_SLOTS = 15; // ✅ as requested
console.log("🎰 MAX_ACTIVE_SLOTS:", MAX_ACTIVE_SLOTS);

const lastSlotAt = new Map();

function weightedPick(items) {
  let total = 0;
  for (const it of items) total += it.w;
  let r = Math.random() * total;
  for (const it of items) {
    r -= it.w;
    if (r <= 0) return it.s;
  }
  return items[items.length - 1].s;
}

function randomSymbolFromReel(reel) {
  const syms = reel.map((x) => x.s);
  return syms[Math.floor(Math.random() * syms.length)];
}

function isAnyTwo(a, b, c) {
  return (a === b && a !== c) || (a === c && a !== b) || (b === c && b !== a);
}

function calcMultiplier(a, b, c) {
  const key = `${a},${b},${c}`;
  if (SLOT.payouts[key] != null) return SLOT.payouts[key];
  if (isAnyTwo(a, b, c)) return SLOT.payouts.ANY2 || 0;
  return 0;
}

function slotArt(a, b, c) {
  const box = (x) => (x === "BAR" ? "BAR" : x === "7" ? "7️⃣" : x);
  return `┏━━━━━━━━━━━━━━━━━━┓\n┃  ${box(a)}  |  ${box(b)}  |  ${box(c)}  ┃\n┗━━━━━━━━━━━━━━━━━━┛`;
}

function spinFrame(a, b, c, note = "Spinning...", vibe = "spin") {
  const art = slotArt(a, b, c);

  const vibeHeader =
    vibe === "glow"
      ? "🏆✨ WIN GLOW! ✨🏆"
      : vibe === "lose"
      ? "🥀 BAD LUCK… 🥀"
      : vibe === "jackpot1"
      ? "🎉🎉🎉 JACKPOT HIT! 🎉🎉🎉"
      : vibe === "jackpot2"
      ? "💎🏆 777 MEGA WIN! 🏆💎"
      : "🎰 BIKA Pro Slot";

  const sound =
    vibe === "spin"
      ? "🔊 KRRR… KRRR…  🎛️"
      : vibe === "lock"
      ? "🔊 KLAK!  🔒"
      : vibe === "glow"
      ? "✨✨✨"
      : vibe === "lose"
      ? "🔇 whomp… whomp…  💔"
      : vibe.startsWith("jackpot")
      ? "💥🔥🎆"
      : "🔊";

  return (
    `<b>${escHtml(vibeHeader)}</b>\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `<pre>${escHtml(art)}</pre>\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${escHtml(sound)} ${escHtml(note)}`
  );
}

// group-wide cooldown for slot command to reduce spam (still per user gameplay)
const groupSlotCooldown = new Map(); // chatId -> lastTime
const SLOT_GROUP_COOLDOWN_MS = 700; // tiny delay to avoid stampede

async function runSlotSpinAnimated(ctx, bet) {
  const userId = ctx.from?.id;
  if (!userId) return;

  // ✅ max concurrent
  if (!activeSlots.has(userId)) {
    if (activeSlots.size >= MAX_ACTIVE_SLOTS) {
      return replyHTML(
        ctx,
        `🚦 <b>Slot Busy</b>\n━━━━━━━━━━━━━━\nလက်ရှိမှာ လူများလို့ <b>${MAX_ACTIVE_SLOTS}</b> ယောက်ထက်ပိုပြီး မဆော့နိုင်သေးပါ။\n⏳ ခဏစောင့်ပြီး ပြန်ကြိုးစားပါ။`,
        { reply_to_message_id: ctx.message?.message_id }
      );
    }
    activeSlots.add(userId);
  } else {
    return replyHTML(
      ctx,
      `⏳ <b>မင်း slot က မပြီးသေးပါ</b>\n━━━━━━━━━━━━━━\nတစ်ခါ ဆော့ပြီးမှ နောက်တစ်ခါ ပြန်ဆော့ပါ။`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  try {
    // per-user cooldown
    const last = lastSlotAt.get(userId) || 0;
    if (Date.now() - last < SLOT.cooldownMs) {
      const sec = Math.ceil((SLOT.cooldownMs - (Date.now() - last)) / 1000);
      return replyHTML(ctx, `⏳ ခဏစောင့်ပါ… (${sec}s) နောက်တစ်ခါ spin လုပ်နိုင်ပါမယ်။`, {
        reply_to_message_id: ctx.message?.message_id,
      });
    }

    if (bet < SLOT.minBet || bet > SLOT.maxBet) {
      return replyHTML(
        ctx,
        `🎰 <b>BIKA Pro Slot</b>\n━━━━━━━━━━━━━━━━━━━━\nUsage: <code>.slot 1000</code>\nMin: <b>${fmt(SLOT.minBet)}</b> ${COIN}\nMax: <b>${fmt(SLOT.maxBet)}</b> ${COIN}`,
        { reply_to_message_id: ctx.message?.message_id }
      );
    }

    await ensureUser(ctx.from);
    await ensureTreasury();

    try {
      await userPayToTreasury(userId, bet, { type: "slot_bet", bet, chatId: ctx.chat?.id });
    } catch (e) {
      if (String(e?.message || e).includes("USER_INSUFFICIENT")) {
        return replyHTML(
          ctx,
          `❌ <b>Balance မလုံလောက်ပါ</b>\n━━━━━━━━━━━━━━━━━━━━\nSlot ဆော့ဖို့ လက်ကျန်ငွေ မလုံလောက်ပါ။\nDaily claim / gift / addbalance နဲ့ ငွေစုဆောင်းပြီးမှ ပြန်လာပါ။`,
          { reply_to_message_id: ctx.message?.message_id }
        );
      }
      console.error("slot bet error:", e);
      return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။", { reply_to_message_id: ctx.message?.message_id });
    }

    const finalA = weightedPick(SLOT.reels[0]);
    const finalB = weightedPick(SLOT.reels[1]);
    const finalC = weightedPick(SLOT.reels[2]);

    const mult = calcMultiplier(finalA, finalB, finalC);
    let payout = mult > 0 ? Math.floor(bet * mult) : 0;

    if (payout > 0) {
      const tr = await getTreasury();
      const ownerBal = toNum(tr?.ownerBalance);
      const maxPay = Math.floor(ownerBal * SLOT.capPercent);
      payout = Math.min(payout, maxPay, ownerBal);
    }

    const win = payout > 0;
    const isJackpot = finalA === "7" && finalB === "7" && finalC === "7";

    const initA = randomSymbolFromReel(SLOT.reels[0]);
    const initB = randomSymbolFromReel(SLOT.reels[1]);
    const initC = randomSymbolFromReel(SLOT.reels[2]);

    const sent = await replyHTML(ctx, spinFrame(initA, initB, initC, "reels spinning…", "spin"), {
      reply_to_message_id: ctx.message?.message_id,
    });

    const chatId = ctx.chat?.id;
    const messageId = sent?.message_id;

    const frames = [
      { a: randomSymbolFromReel(SLOT.reels[0]), b: randomSymbolFromReel(SLOT.reels[1]), c: randomSymbolFromReel(SLOT.reels[2]), note: "speed up!", vibe: "spin", delay: 650 },
      { a: randomSymbolFromReel(SLOT.reels[0]), b: randomSymbolFromReel(SLOT.reels[1]), c: randomSymbolFromReel(SLOT.reels[2]), note: "rolling…", vibe: "spin", delay: 650 },
      { a: finalA, b: randomSymbolFromReel(SLOT.reels[1]), c: randomSymbolFromReel(SLOT.reels[2]), note: "locking 1st reel…", vibe: "lock", delay: 850 },
      { a: finalA, b: finalB, c: randomSymbolFromReel(SLOT.reels[2]), note: "locking 2nd reel…", vibe: "lock", delay: 850 },
      { a: finalA, b: finalB, c: finalC, note: "result!", vibe: "lock", delay: 900 },
    ];

    if (win && !isJackpot) frames.push({ a: finalA, b: finalB, c: finalC, note: "shining… payout loading…", vibe: "glow", delay: 900 });
    if (isJackpot) {
      frames.push({ a: finalA, b: finalB, c: finalC, note: "BOOM! 🔥🔥🔥", vibe: "jackpot1", delay: 900 });
      frames.push({ a: finalA, b: finalB, c: finalC, note: "paying now…", vibe: "jackpot2", delay: 900 });
    }
    if (!win) frames.push({ a: finalA, b: finalB, c: finalC, note: "try again… 🍀", vibe: "lose", delay: 900 });

    for (const f of frames) {
      await sleep(f.delay);
      if (chatId && messageId) await editHTML(ctx, chatId, messageId, spinFrame(f.a, f.b, f.c, f.note, f.vibe));
    }

    if (payout > 0) {
      try {
        await treasuryPayToUser(userId, payout, { type: "slot_win", bet, payout, combo: `${finalA},${finalB},${finalC}` });
      } catch (e) {
        console.error("slot payout error:", e);
        // refund bet if payout failed
        try {
          await treasuryPayToUser(userId, bet, { type: "slot_refund", reason: "payout_fail" });
        } catch (_) {}
        if (chatId && messageId) {
          await editHTML(
            ctx,
            chatId,
            messageId,
            `🎰 <b>BIKA Pro Slot</b>\n━━━━━━━━━━━━━━━━━━━━\n<pre>${escHtml(slotArt(finalA, finalB, finalC))}</pre>\n━━━━━━━━━━━━━━━━━━━━\n⚠️ Payout error ဖြစ်လို့ refund ပြန်ပေးလိုက်ပါတယ်။`
          );
        }
        lastSlotAt.set(userId, Date.now());
        return;
      }
    }

    lastSlotAt.set(userId, Date.now());

    const net = payout - bet;
    const headline = payout === 0 ? "❌ LOSE" : isJackpot ? "🏆 JACKPOT 777!" : "✅ WIN";

    const finalMsg =
      `🎰 <b>BIKA Pro Slot</b>\n` +
      `━━━━━━━━━━━━━━\n` +
      `<pre>${escHtml(slotArt(finalA, finalB, finalC))}</pre>\n` +
      `━━━━━━━━━━━━━━\n` +
      `<b>${escHtml(headline)}</b>\n` +
      `Bet: <b>${fmt(bet)}</b> ${COIN}\n` +
      `Payout: <b>${fmt(payout)}</b> ${COIN}\n` +
      `Net: <b>${fmt(net)}</b> ${COIN}`;

    if (chatId && messageId) await editHTML(ctx, chatId, messageId, finalMsg);
  } finally {
    // ✅ always release slot
    activeSlots.delete(userId);
  }
}

bot.hears(/^\.(slot)\s+(\d+)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>.slot</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  // tiny group anti-stampede
  const chatId = ctx.chat?.id;
  const last = groupSlotCooldown.get(chatId) || 0;
  if (Date.now() - last < SLOT_GROUP_COOLDOWN_MS) {
    return; // silently ignore burst
  }
  groupSlotCooldown.set(chatId, Date.now());

  const bet = parseInt(ctx.match[2], 10);
  if (!Number.isFinite(bet) || bet <= 0) return;
  return runSlotSpinAnimated(ctx, bet);
});

// -------------------- RTP monitor + /setrtp --------------------
function padRight(s, n) {
  s = String(s);
  return s.length >= n ? s : s + " ".repeat(n - s.length);
}
function padLeft(s, n) {
  s = String(s);
  return s.length >= n ? s : " ".repeat(n - s.length) + s;
}

function renderPayoutsTable() {
  const rows = [
    ["COMBO", "MULTI", "BET 1,000 → PAYOUT"],
    ["7 7 7", SLOT.payouts["7,7,7"], Math.floor(1000 * SLOT.payouts["7,7,7"])],
    ["BAR BAR BAR", SLOT.payouts["BAR,BAR,BAR"], Math.floor(1000 * SLOT.payouts["BAR,BAR,BAR"])],
    ["⭐ ⭐ ⭐", SLOT.payouts["⭐,⭐,⭐"], Math.floor(1000 * SLOT.payouts["⭐,⭐,⭐"])],
    ["🔔 🔔 🔔", SLOT.payouts["🔔,🔔,🔔"], Math.floor(1000 * SLOT.payouts["🔔,🔔,🔔"])],
    ["🍉 🍉 🍉", SLOT.payouts["🍉,🍉,🍉"], Math.floor(1000 * SLOT.payouts["🍉,🍉,🍉"])],
    ["🍋 🍋 🍋", SLOT.payouts["🍋,🍋,🍋"], Math.floor(1000 * SLOT.payouts["🍋,🍋,🍋"])],
    ["🍒 🍒 🍒", SLOT.payouts["🍒,🍒,🍒"], Math.floor(1000 * SLOT.payouts["🍒,🍒,🍒"])],
    ["ANY2 (2 same)", SLOT.payouts["ANY2"], Math.floor(1000 * SLOT.payouts["ANY2"])],
  ];

  const colW = [16, 10, 20];
  const line = `┌${"─".repeat(colW[0])}┬${"─".repeat(colW[1])}┬${"─".repeat(colW[2])}┐`;
  const mid = `├${"─".repeat(colW[0])}┼${"─".repeat(colW[1])}┼${"─".repeat(colW[2])}┤`;
  const end = `└${"─".repeat(colW[0])}┴${"─".repeat(colW[1])}┴${"─".repeat(colW[2])}┘`;

  const out = [];
  out.push(line);
  rows.forEach((r, i) => {
    const c0 = padRight(r[0], colW[0]);
    const c1 = padLeft(r[1], colW[1]);
    const c2 = padLeft(fmt(r[2]), colW[2]);
    out.push(`│${c0}│${c1}│${c2}│`);
    if (i === 0) out.push(mid);
  });
  out.push(end);
  return out.join("\n");
}

function buildProbMap(reel) {
  const total = reel.reduce((a, x) => a + x.w, 0);
  const map = new Map();
  for (const it of reel) map.set(it.s, it.w / total);
  return map;
}

function calcBaseRTP() {
  const p1 = buildProbMap(SLOT.reels[0]);
  const p2 = buildProbMap(SLOT.reels[1]);
  const p3 = buildProbMap(SLOT.reels[2]);
  const syms1 = [...p1.keys()],
    syms2 = [...p2.keys()],
    syms3 = [...p3.keys()];
  let expectedMultiplier = 0;
  for (const a of syms1)
    for (const b of syms2)
      for (const c of syms3) {
        const prob = p1.get(a) * p2.get(b) * p3.get(c);
        const mult = calcMultiplier(a, b, c);
        expectedMultiplier += prob * (mult || 0);
      }
  return expectedMultiplier;
}

function approx777Odds() {
  const p1 = buildProbMap(SLOT.reels[0]).get("7") || 0;
  const p2 = buildProbMap(SLOT.reels[1]).get("7") || 0;
  const p3 = buildProbMap(SLOT.reels[2]).get("7") || 0;
  const p777 = p1 * p2 * p3;
  if (p777 <= 0) return "N/A";
  return `~1 / ${fmt(Math.round(1 / p777))}`;
}

function scalePayouts(factor) {
  for (const k of Object.keys(SLOT.payouts)) {
    SLOT.payouts[k] = Number((SLOT.payouts[k] * factor).toFixed(4));
  }
}

bot.command("rtp", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only.");

  const tr = await getTreasury();
  const base = calcBaseRTP();
  const odds777 = approx777Odds();

  const msg =
    `🧮 <b>Slot RTP Dashboard</b>\n` +
    `━━━━━━━━━━━━━━\n` +
    `Treasury: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}\n` +
    `Total Supply: <b>${fmt(tr?.totalSupply)}</b> ${COIN}\n` +
    `Base RTP: <b>${(base * 100).toFixed(2)}%</b>\n` +
    `House Edge: <b>${((1 - base) * 100).toFixed(2)}%</b>\n` +
    `777 Odds: <b>${escHtml(odds777)}</b>\n` +
    `Cap: <b>${Math.round(SLOT.capPercent * 100)}%</b> of Treasury / spin\n` +
    `🕒 ${escHtml(formatYangon(new Date()))} (Yangon)\n` +
    `━━━━━━━━━━━━━━\n` +
    `<b>Payout Table (Bet = 1,000)</b>\n` +
    `<pre>${escHtml(renderPayoutsTable())}</pre>`;

  return replyHTML(ctx, msg);
});

bot.command("setrtp", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only.");

  const parts = (ctx.message?.text || "").trim().split(/\s+/);
  if (parts.length < 2) {
    return replyHTML(ctx, `⚙️ <b>Set RTP</b>\n━━━━━━━━━━━━━━━━━━━━\nUsage:\n• <code>/setrtp 90</code>\n• <code>/setrtp 0.90</code>`);
  }

  let target = Number(parts[1]);
  if (!Number.isFinite(target)) return replyHTML(ctx, "Invalid number.");

  if (target > 1) target = target / 100;
  target = Math.max(0.5, Math.min(0.98, target));

  const before = calcBaseRTP();
  if (before <= 0) return replyHTML(ctx, "Base RTP is 0 (check weights/payouts).");

  const factor = target / before;
  scalePayouts(factor);

  const after = calcBaseRTP();
  const odds777 = approx777Odds();
  const tr = await getTreasury();

  const msg =
    `✅ <b>RTP Updated (Owner)</b>\n` +
    `━━━━━━━━━━━━━━━\n` +
    `Target RTP: <b>${(target * 100).toFixed(2)}%</b>\n` +
    `Old Base RTP: <b>${(before * 100).toFixed(2)}%</b>\n` +
    `New Base RTP: <b>${(after * 100).toFixed(2)}%</b>\n` +
    `Scale Factor: <b>${factor.toFixed(4)}</b>\n` +
    `777 Odds: <b>${escHtml(odds777)}</b>\n` +
    `Treasury: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}\n` +
    `━━━━━━━━━━━━━━━\n` +
    `<b>Payout Table (Bet = 1,000)</b>\n` +
    `<pre>${escHtml(renderPayoutsTable())}</pre>`;

  return replyHTML(ctx, msg);
});

// -------------------- Admin dashboard (inline + guided input) --------------------
const ADMIN = { panelTitle: "🛡️ BIKA • Pro Admin Dashboard", sessionTTL: 5 * 60 * 1000 };
const adminSessions = new Map();

function setAdminSession(ownerId, session) {
  adminSessions.set(ownerId, { ...session, expiresAt: Date.now() + ADMIN.sessionTTL });
}
function getAdminSession(ownerId) {
  const s = adminSessions.get(ownerId);
  if (!s) return null;
  if (Date.now() > (s.expiresAt || 0)) {
    adminSessions.delete(ownerId);
    return null;
  }
  return s;
}
function clearAdminSession(ownerId) {
  adminSessions.delete(ownerId);
}

function adminKeyboard() {
  return {
    inline_keyboard: [
      [
        { text: "🏦 Treasury", callback_data: "ADMIN:TREASURY" },
        { text: "🧾 Orders", callback_data: "ADMIN:ORDERS" },
      ],
      [
        { text: "➕ Add Balance", callback_data: "ADMIN:ADD" },
        { text: "➖ Remove Balance", callback_data: "ADMIN:REMOVE" },
      ],
      [
        { text: "🔎 Set Target (ID/@)", callback_data: "ADMIN:TARGET_MANUAL" },
        { text: "🧹 Clear Target", callback_data: "ADMIN:CLEAR_TARGET" },
      ],
      [{ text: "🔄 Refresh", callback_data: "ADMIN:REFRESH" }],
    ],
  };
}

async function renderAdminPanel(ctx, note = "") {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only.");

  const tr = await getTreasury();
  const s = getAdminSession(ctx.from.id);

  const targetLine = s?.targetUserId
    ? `Target: <b>${escHtml(String(s.targetLabel))}</b> (ID: <code>${s.targetUserId}</code>)`
    : `Target: <i>Not set</i>`;

  const extra = note ? `\n${note}\n` : "\n";

  const text =
    `${ADMIN.panelTitle}\n` +
    `━━━━━━━━━━━━━━━\n` +
    `Treasury Balance: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}\n` +
    `Total Supply: <b>${fmt(tr?.totalSupply)}</b> ${COIN}\n` +
    `🕒 ${escHtml(formatYangon(new Date()))} (Yangon)\n` +
    `━━━━━━━━━━━━━━━\n` +
    `${targetLine}\n` +
    `━━━━━━━━━━━━━━━` +
    `${extra}` +
    `Choose an action below:`;

  if (ctx.updateType === "callback_query") {
    return safeTelegramCall(
      () => ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: adminKeyboard(), disable_web_page_preview: true }),
      1
    );
  }
  return replyHTML(ctx, text, { reply_markup: adminKeyboard() });
}

bot.command("admin", async (ctx) => renderAdminPanel(ctx));

async function askManualTarget(ctx) {
  setAdminSession(ctx.from.id, { mode: "await_target" });
  return replyHTML(
    ctx,
    `🔎 <b>Set Target User</b>\n━━━━━━━━━━━━━━━\nSend one:\n• <code>@username</code>\n• <code>123456789</code> (userId)\nExample: <code>@Official_Bika</code>`,
    { reply_markup: { force_reply: true } }
  );
}

async function askAmount(ctx, type) {
  const s = getAdminSession(ctx.from.id);
  if (!s?.targetUserId) return renderAdminPanel(ctx, "⚠️ <b>Target user မရွေးရသေးပါ။</b> Set Target လုပ်ပါ။");

  setAdminSession(ctx.from.id, { ...s, mode: type === "add" ? "await_add_amount" : "await_remove_amount" });

  const header = type === "add" ? "➕ <b>Add Balance</b>" : "➖ <b>Remove Balance</b>";
  const hint = type === "add" ? "Treasury → User" : "User → Treasury";

  return replyHTML(
    ctx,
    `${header}\n━━━━━━━━━━━━━━━━\nTarget: <b>${escHtml(String(s.targetLabel))}</b>\nFlow: <i>${escHtml(hint)}</i>\n━━━━━━━━━━━━━━━━━━━━\nAmount ပို့ပါ (numbers only)\nExample: <code>5000</code>`,
    { reply_markup: { force_reply: true } }
  );
}

bot.on("text", async (ctx, next) => {
  const s = getAdminSession(ctx.from.id);
  if (!s) return next();

  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return next();

  const text = (ctx.message?.text || "").trim();

  if (s.mode === "await_target") {
    let targetUserId = null;
    let targetLabel = null;

    if (text.startsWith("@") && text.length > 1) {
      const uname = text.slice(1).toLowerCase();
      const u = await getUserByUsername(uname);
      if (!u) {
        clearAdminSession(ctx.from.id);
        return renderAdminPanel(ctx, "⚠️ DB ထဲမှာမတွေ့ပါ။ သူ bot ကို /start လုပ်ထားရမယ်။");
      }
      targetUserId = u.userId;
      targetLabel = "@" + uname;
    } else if (/^\d+$/.test(text)) {
      targetUserId = parseInt(text, 10);
      targetLabel = text;

      await users.updateOne(
        { userId: targetUserId },
        { $setOnInsert: { userId: targetUserId, balance: 0, createdAt: new Date() }, $set: { updatedAt: new Date() } },
        { upsert: true }
      );
    } else {
      clearAdminSession(ctx.from.id);
      return renderAdminPanel(ctx, "⚠️ Target format မမှန်ပါ။ <code>@username</code> သို့ <code>userId</code> ပို့ပါ။");
    }

    setAdminSession(ctx.from.id, { mode: "idle", targetUserId, targetLabel });
    return renderAdminPanel(ctx, `✅ Target set: <b>${escHtml(String(targetLabel))}</b>`);
  }

  if (s.mode === "await_add_amount") {
    const amt = parseInt(text.replace(/,/g, ""), 10);
    setAdminSession(ctx.from.id, { ...s, mode: "idle" });
    if (!Number.isFinite(amt) || amt <= 0) return renderAdminPanel(ctx, "⚠️ Amount မမှန်ပါ။ ဥပမာ <code>5000</code> လိုပို့ပါ။");

    try {
      await treasuryPayToUser(s.targetUserId, amt, { type: "owner_addbalance_admin", by: ctx.from.id });
      const u = await getUser(s.targetUserId);
      const tr = await getTreasury();
      return renderAdminPanel(
        ctx,
        `✅ <b>Added Successfully</b>\nUser: <b>${escHtml(String(s.targetLabel))}</b>\nAmount: <b>${fmt(amt)}</b> ${COIN}\nUser Balance: <b>${fmt(u?.balance)}</b> ${COIN}\nTreasury Left: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}`
      );
    } catch (e) {
      if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
        const tr = await getTreasury();
        return renderAdminPanel(ctx, `❌ Treasury မလုံလောက်ပါ။ (Treasury: <b>${fmt(tr?.ownerBalance)}</b> ${COIN})`);
      }
      console.error("admin add error:", e);
      return renderAdminPanel(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
    }
  }

  if (s.mode === "await_remove_amount") {
    const amt = parseInt(text.replace(/,/g, ""), 10);
    setAdminSession(ctx.from.id, { ...s, mode: "idle" });
    if (!Number.isFinite(amt) || amt <= 0) return renderAdminPanel(ctx, "⚠️ Amount မမှန်ပါ။ ဥပမာ <code>5000</code> လိုပို့ပါ။");

    try {
      await userPayToTreasury(s.targetUserId, amt, { type: "owner_removebalance_admin", by: ctx.from.id });
      const u = await getUser(s.targetUserId);
      const tr = await getTreasury();
      return renderAdminPanel(
        ctx,
        `✅ <b>Removed Successfully</b>\nUser: <b>${escHtml(String(s.targetLabel))}</b>\nAmount: <b>${fmt(amt)}</b> ${COIN}\nUser Balance: <b>${fmt(u?.balance)}</b> ${COIN}\nTreasury Now: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}`
      );
    } catch (e) {
      if (String(e?.message || e).includes("USER_INSUFFICIENT")) {
        const u = await getUser(s.targetUserId);
        return renderAdminPanel(ctx, `❌ User balance မလုံလောက်ပါ။ (User: <b>${fmt(u?.balance)}</b> ${COIN})`);
      }
      console.error("admin remove error:", e);
      return renderAdminPanel(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
    }
  }

  return next();
});

// -------------------- Orders helpers (Admin + User notice + Owner alert) --------------------
async function getRecentOrders(statuses, limit = 10) {
  return orders
    .find({ status: { $in: statuses } })
    .sort({ createdAt: -1 })
    .limit(limit)
    .toArray();
}

function orderReceiptText(o) {
  const who = o.username ? `@${escHtml(o.username)}` : `<code>${o.userId}</code>`;
  return (
    `🧾 <b>Order Receipt</b>\n` +
    `━━━━━━━━━━━━━━━━\n` +
    `Order ID: <code>${escHtml(String(o._id))}</code>\n` +
    `Receipt: <code>${escHtml(o.receiptCode || "-")}</code>\n` +
    `Item: <b>${escHtml(o.itemName)}</b>\n` +
    `Price: <b>${fmt(o.price)}</b> ${COIN}\n` +
    `Status: <b>${escHtml(o.status)}</b>\n` +
    `User: ${who}\n` +
    `Time: <b>${escHtml(formatYangon(new Date(o.createdAt)))}</b> (Yangon)\n` +
    `━━━━━━━━━━━━━━━━`
  );
}

async function notifyUserOrderUpdate(o, noteLine = "") {
  try {
    const note = noteLine ? `\n${noteLine}\n` : "\n";
    const msg =
      `🧾 <b>Order Update</b>\n` +
      `━━━━━━━━━━━━━━━━\n` +
      `Order ID: <code>${escHtml(String(o._id))}</code>\n` +
      `Receipt: <code>${escHtml(o.receiptCode || "-")}</code>\n` +
      `Item: <b>${escHtml(o.itemName)}</b>\n` +
      `Price: <b>${fmt(o.price)}</b> ${COIN}\n` +
      `Status: <b>${escHtml(o.status)}</b>\n` +
      `${note}` +
      `Time: <b>${escHtml(formatYangon(new Date()))}</b> (Yangon)`;
    await sendHTML(o.userId, msg);
  } catch (e) {
    console.log("notify user failed (maybe user blocked bot):", e?.message || e);
  }
}

async function alertOwnerNewOrder(o) {
  const who = o.username ? `@${escHtml(o.username)}` : `<code>${o.userId}</code>`;
  const msg =
    `🧾 <b>New Order Created</b>\n` +
    `━━━━━━━━━━━━━━━━\n` +
    `Item: <b>${escHtml(o.itemName)}</b>\n` +
    `Price: <b>${fmt(o.price)}</b> ${COIN}\n` +
    `User: ${who}\n` +
    `Order ID: <code>${escHtml(String(o._id))}</code>\n` +
    `Receipt: <code>${escHtml(o.receiptCode || "-")}</code>\n` +
    `Status: <b>${escHtml(o.status)}</b>\n` +
    `Time: <b>${escHtml(formatYangon(new Date(o.createdAt)))}</b> (Yangon)\n` +
    `━━━━━━━━━━━━━━━━\n` +
    `➡️ Manage: <code>/admin</code> → Orders`;

  // always try owner DM
  await sendHTML(OWNER_ID, msg);

  // optional extra admin group/channel
  if (ADMIN_ALERT_CHAT_ID && ADMIN_ALERT_CHAT_ID !== OWNER_ID) {
    await sendHTML(ADMIN_ALERT_CHAT_ID, msg);
  }
}

// -------------------- Callback Query (Shop + Admin + Orders) --------------------
bot.on("callback_query", async (ctx) => {
  const data = ctx.callbackQuery?.data || "";

  // ---- SHOP ----
  if (data === "SHOP:REFRESH") {
    const u = await ensureUser(ctx.from);
    await safeTelegramCall(() => ctx.answerCbQuery("Refreshed"), 1);
    return safeTelegramCall(
      () =>
        ctx.editMessageText(shopText(u.balance), {
          parse_mode: "HTML",
          reply_markup: shopKeyboard(),
          disable_web_page_preview: true,
        }),
      1
    );
  }

  if (data.startsWith("BUY:")) {
    const itemId = data.split(":")[1];
    const item = SHOP_ITEMS.find((x) => x.id === itemId);
    if (!item) return safeTelegramCall(() => ctx.answerCbQuery("Item not found", { show_alert: true }), 1);

    await ensureUser(ctx.from);
    await ensureTreasury();

    const receiptCode = genReceiptCode();
    try {
      // deduct user -> treasury
      await userPayToTreasury(ctx.from.id, item.price, { type: "shop_buy", itemId: item.id, itemName: item.name });

      // create order (PENDING)
      const ins = await orders.insertOne({
        userId: ctx.from.id,
        username: ctx.from.username ? ctx.from.username.toLowerCase() : null,
        itemId: item.id,
        itemName: item.name,
        price: item.price,
        receiptCode,
        status: ORDER_STATUS.PENDING,
        createdAt: new Date(),
        updatedAt: new Date(),
        history: [{ status: ORDER_STATUS.PENDING, at: new Date(), by: "SYSTEM" }],
      });

      const orderId = ins.insertedId;
      const u = await getUser(ctx.from.id);

      await safeTelegramCall(() => ctx.answerCbQuery("✅ Order created!"), 1);

      // ✅ owner alert (THIS FIXES "admin chat ဆီမရောက်လာဘူး")
      const o = await orders.findOne({ _id: orderId });
      if (o) await alertOwnerNewOrder(o);

      // user receipt message
      return replyHTML(
        ctx,
        `✅ <b>Order Created</b>\n━━━━━━━━━━━━━━━\n` +
          `Order ID: <code>${escHtml(String(orderId))}</code>\n` +
          `Receipt: <code>${escHtml(receiptCode)}</code>\n` +
          `Item: <b>${escHtml(item.name)}</b>\n` +
          `Paid: <b>${fmt(item.price)}</b> ${COIN}\n` +
          `Your Balance: <b>${fmt(u?.balance)}</b> ${COIN}\n` +
          `Status: <b>${ORDER_STATUS.PENDING}</b>\n` +
          `━━━━━━━━━━━━━━━\n` +
          `📌 Admin က confirm / deliver လုပ်ပြီးရင် DM နဲ့ အကြောင်းကြားပေးပါမယ်။`
      );
    } catch (e) {
      if (String(e?.message || e).includes("USER_INSUFFICIENT")) {
        const u = await getUser(ctx.from.id);
        const bal = toNum(u?.balance);
        const need = toNum(item.price);
        const lack = Math.max(0, need - bal);

        await safeTelegramCall(() => ctx.answerCbQuery(`❌ မလုံလောက်ပါ (${fmt(lack)} ${COIN} လိုနေပါသေးတယ်)`, { show_alert: true }), 1);

        return replyHTML(
          ctx,
          `❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>\n` +
            `━━━━━━━━━━━━━━\n` +
            `Item: <b>${escHtml(item.name)}</b>\n` +
            `Price: <b>${fmt(need)}</b> ${COIN}\n` +
            `Your Balance: <b>${fmt(bal)}</b> ${COIN}\n` +
            `Need More: <b>${fmt(lack)}</b> ${COIN}\n` +
            `━━━━━━━━━━━━━━━\n` +
            `💡 slot ဆော့ရင်း ပိုက်ဆံဆုဆောင်းပြီးမှ ပြန်လာပါ။\n` +
            `• Daily claim: <code>/dailyclaim</code>\n` +
            `• Wallet: <code>.mybalance</code>\n` +
            `• Shop: <code>/shop</code>`
        );
      }

      console.error("BUY error:", e);
      return safeTelegramCall(() => ctx.answerCbQuery("Error", { show_alert: true }), 1);
    }
  }

  // ---- ADMIN ----
  if (data.startsWith("ADMIN:")) {
    const t = await ensureTreasury();
    if (!isOwner(ctx, t)) {
      await safeTelegramCall(() => ctx.answerCbQuery("Owner only", { show_alert: true }), 1);
      return;
    }

    if (data === "ADMIN:REFRESH") {
      await safeTelegramCall(() => ctx.answerCbQuery("Refreshed"), 1);
      return renderAdminPanel(ctx);
    }

    if (data === "ADMIN:TREASURY") {
      await safeTelegramCall(() => ctx.answerCbQuery("Treasury"), 1);
      return renderAdminPanel(ctx, "📌 Treasury status shown above.");
    }

    if (data === "ADMIN:TARGET_MANUAL") {
      await safeTelegramCall(() => ctx.answerCbQuery("Manual target"), 1);
      return askManualTarget(ctx);
    }

    if (data === "ADMIN:CLEAR_TARGET") {
      await safeTelegramCall(() => ctx.answerCbQuery("Cleared"), 1);
      clearAdminSession(ctx.from.id);
      return renderAdminPanel(ctx, "🧹 Target cleared.");
    }

    if (data === "ADMIN:ADD") {
      await safeTelegramCall(() => ctx.answerCbQuery("Add"), 1);
      return askAmount(ctx, "add");
    }

    if (data === "ADMIN:REMOVE") {
      await safeTelegramCall(() => ctx.answerCbQuery("Remove"), 1);
      return askAmount(ctx, "remove");
    }

    if (data === "ADMIN:ORDERS") {
      await safeTelegramCall(() => ctx.answerCbQuery("Orders"), 1);
      const list = await getRecentOrders([ORDER_STATUS.PENDING, ORDER_STATUS.PAID], 10);
      if (!list.length) return renderAdminPanel(ctx, "🧾 Orders: <i>None</i>");

      const lines = list
        .slice(0, 10)
        .map((o, i) => {
          const who = o.username ? `@${escHtml(o.username)}` : `<code>${o.userId}</code>`;
          const when = escHtml(formatYangon(new Date(o.createdAt)));
          return (
            `${i + 1}. <b>${escHtml(o.itemName)}</b> — <b>${fmt(o.price)}</b> ${COIN}\n` +
            `   Status: <b>${escHtml(o.status)}</b>\n` +
            `   User: ${who}\n` +
            `   Order: <code>${escHtml(String(o._id))}</code>\n` +
            `   Receipt: <code>${escHtml(o.receiptCode || "-")}</code>\n` +
            `   Time: ${when}`
          );
        })
        .join("\n\n");

      const panel =
        `🧾 <b>Orders (PENDING / PAID)</b>\n━━━━━━━━━━━━━━━\n` +
        `${lines}\n` +
        `━━━━━━━━━━━━━━━\n` +
        `Tap an order below to manage:`;

      return safeTelegramCall(
        () =>
          ctx.editMessageText(panel, {
            parse_mode: "HTML",
            disable_web_page_preview: true,
            reply_markup: adminOrdersKeyboard(list),
          }),
        1
      );
    }

    await safeTelegramCall(() => ctx.answerCbQuery("OK"), 1);
    return;
  }

  // ---- ORDER ACTIONS (Owner only) ----
  if (data.startsWith("ORDER:")) {
    const t = await ensureTreasury();
    if (!isOwner(ctx, t)) {
      await safeTelegramCall(() => ctx.answerCbQuery("Owner only", { show_alert: true }), 1);
      return;
    }

    const parts = data.split(":");
    const action = parts[1];
    const id = parts[2];

    if (action === "OPEN") {
      await safeTelegramCall(() => ctx.answerCbQuery("Open"), 1);
      let oid = null;
      try {
        oid = new ObjectId(id);
      } catch (_) {
        return safeTelegramCall(() => ctx.editMessageText("Invalid Order ID", { reply_markup: adminKeyboard() }), 1);
      }
      const o = await orders.findOne({ _id: oid });
      if (!o) return safeTelegramCall(() => ctx.editMessageText("Order not found", { reply_markup: adminKeyboard() }), 1);

      const text = orderReceiptText(o) + "\nSelect action:";
      return safeTelegramCall(
        () =>
          ctx.editMessageText(text, {
            parse_mode: "HTML",
            disable_web_page_preview: true,
            reply_markup: orderActionKeyboard(String(o._id)),
          }),
        1
      );
    }

    if (action === "PAID" || action === "DELIVERED" || action === "CANCEL") {
      await safeTelegramCall(() => ctx.answerCbQuery("Working..."), 1);
      let oid = null;
      try {
        oid = new ObjectId(id);
      } catch (_) {
        return safeTelegramCall(() => ctx.answerCbQuery("Invalid ID", { show_alert: true }), 1);
      }

      const o = await orders.findOne({ _id: oid });
      if (!o) return safeTelegramCall(() => ctx.answerCbQuery("Order not found", { show_alert: true }), 1);

      if (action === "PAID") {
        if (o.status === ORDER_STATUS.CANCELLED || o.status === ORDER_STATUS.DELIVERED) {
          return safeTelegramCall(() => ctx.answerCbQuery("Already closed", { show_alert: true }), 1);
        }
        const now = new Date();
        await orders.updateOne(
          { _id: oid },
          {
            $set: { status: ORDER_STATUS.PAID, updatedAt: now },
            $push: { history: { status: ORDER_STATUS.PAID, at: now, by: ctx.from.id } },
          }
        );
        const updated = await orders.findOne({ _id: oid });
        await notifyUserOrderUpdate(updated, "✅ Admin က order ကို <b>PAID</b> လို့ confirm လုပ်ပြီးပါပြီ။");

        const text = orderReceiptText(updated) + "\n✅ Updated.";
        return safeTelegramCall(
          () =>
            ctx.editMessageText(text, {
              parse_mode: "HTML",
              disable_web_page_preview: true,
              reply_markup: orderActionKeyboard(String(updated._id)),
            }),
          1
        );
      }

      if (action === "DELIVERED") {
        if (o.status === ORDER_STATUS.CANCELLED) return safeTelegramCall(() => ctx.answerCbQuery("Cancelled order", { show_alert: true }), 1);

        const now = new Date();
        await orders.updateOne(
          { _id: oid },
          {
            $set: { status: ORDER_STATUS.DELIVERED, updatedAt: now },
            $push: { history: { status: ORDER_STATUS.DELIVERED, at: now, by: ctx.from.id } },
          }
        );
        const updated = await orders.findOne({ _id: oid });
        await notifyUserOrderUpdate(updated, "📦 Order ကို <b>DELIVERED</b> လုပ်ပြီးပါပြီ။ ကျေးဇူးတင်ပါတယ်။");

        const text = orderReceiptText(updated) + "\n📦 Delivered.";
        return safeTelegramCall(
          () =>
            ctx.editMessageText(text, {
              parse_mode: "HTML",
              disable_web_page_preview: true,
              reply_markup: { inline_keyboard: [[{ text: "⬅️ Back to Admin", callback_data: "ADMIN:REFRESH" }]] },
            }),
          1
        );
      }

      if (action === "CANCEL") {
        if (o.status === ORDER_STATUS.CANCELLED) return safeTelegramCall(() => ctx.answerCbQuery("Already cancelled", { show_alert: true }), 1);
        if (o.status === ORDER_STATUS.DELIVERED) return safeTelegramCall(() => ctx.answerCbQuery("Already delivered", { show_alert: true }), 1);

        try {
          await treasuryPayToUser(o.userId, o.price, { type: "order_refund", orderId: String(o._id), itemId: o.itemId });
        } catch (e) {
          if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
            return safeTelegramCall(() => ctx.answerCbQuery("Treasury insufficient for refund", { show_alert: true }), 1);
          }
          console.error("refund error:", e);
          return safeTelegramCall(() => ctx.answerCbQuery("Refund error", { show_alert: true }), 1);
        }

        const now = new Date();
        await orders.updateOne(
          { _id: oid },
          {
            $set: { status: ORDER_STATUS.CANCELLED, updatedAt: now },
            $push: { history: { status: ORDER_STATUS.CANCELLED, at: now, by: ctx.from.id } },
          }
        );

        const updated = await orders.findOne({ _id: oid });
        await notifyUserOrderUpdate(
          updated,
          `❌ Admin က order ကို <b>CANCELLED</b> လုပ်ပြီး refund <b>${fmt(updated.price)}</b> ${COIN} ပြန်ပေးပြီးပါပြီ။`
        );

        const text = orderReceiptText(updated) + "\n❌ Cancelled + Refunded.";
        return safeTelegramCall(
          () =>
            ctx.editMessageText(text, {
              parse_mode: "HTML",
              disable_web_page_preview: true,
              reply_markup: { inline_keyboard: [[{ text: "⬅️ Back to Admin", callback_data: "ADMIN:REFRESH" }]] },
            }),
          1
        );
      }
    }

    await safeTelegramCall(() => ctx.answerCbQuery("OK"), 1);
    return;
  }

  await safeTelegramCall(() => ctx.answerCbQuery("OK"), 1);
});

// -------------------- Webhook Boot (Render Web Service) --------------------
let server = null;

(async () => {
  await connectMongo();
  await ensureTreasury();

  const app = express();
  app.use(express.json());

  const PORT = process.env.PORT || 3000;

  const webhookPath = `/telegraf/${WEBHOOK_SECRET}`;
  const webhookUrl = `${PUBLIC_URL}${webhookPath}`;

  // Health check endpoint for UptimeRobot
  app.get("/", (req, res) => res.status(200).send("OK"));

  // Telegram will POST updates here
  app.post(webhookPath, (req, res) => {
    // IMPORTANT: never throw here
    safeTelegramCall(() => bot.handleUpdate(req.body, res), 0);
  });

  server = app.listen(PORT, async () => {
    console.log("✅ Web server listening on", PORT);

    try {
      await bot.telegram.deleteWebhook({ drop_pending_updates: true });
    } catch (_) {}

    await bot.telegram.setWebhook(webhookUrl);
    console.log("✅ Webhook set to:", webhookUrl);
    console.log(`🕒 TZ env: ${TZ}`);
    console.log(`🛡️ Owner ID (env): ${OWNER_ID}`);
    console.log(`🧩 TX supported: ${TX_SUPPORTED}`);
    console.log(`🎰 MAX_ACTIVE_SLOTS: ${MAX_ACTIVE_SLOTS}`);
    if (ADMIN_ALERT_CHAT_ID) console.log(`📣 ADMIN_ALERT_CHAT_ID: ${ADMIN_ALERT_CHAT_ID}`);
  });

  console.log("🤖 Bot started (Webhook mode)");
})().catch((e) => {
  console.error("BOOT ERROR:", e);
  process.exit(1);
});

// ✅ Safe shutdown (Webhook mode: DO NOT call bot.stop())
async function safeShutdown(signal) {
  console.log(`🧯 Shutdown signal: ${signal}`);
  try {
    if (server) server.close();
  } catch (_) {}
  try {
    if (mongo) await mongo.close();
  } catch (_) {}
  process.exit(0);
}

process.once("SIGINT", () => safeShutdown("SIGINT"));
process.once("SIGTERM", () => safeShutdown("SIGTERM"));
