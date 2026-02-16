/**
 * BIKA Pro Slot Bot — FINAL (Single File, Render + Mongo)
 * ------------------------------------------------------------
 * ✅ ENV OWNER_ID sets bot owner
 * ✅ Treasury Total Supply (Owner bank) + Atomic transfers
 * ✅ /settotal 5000000, /treasury (owner only)
 * ✅ /start first time only bonus: +300 (Treasury -> User)
 * ✅ /dailyclaim group only, Yangon day reset: +50~100 (Treasury -> User)
 * ✅ Gift: /gift @user amount OR reply /gift amount (User -> User)
 * ✅ Owner: /addbalance & /removebalance (reply/@/id)
 * ✅ Shop: /shop inline buy -> creates PENDING orders
 * ✅ Admin dashboard: /admin inline + guided inputs
 * ✅ Slot: .slot 100 (group), animated edit UI:
 *    - sound-like emoji effect
 *    - win glow frame
 *    - jackpot celebration 2 frames
 *    - lose sad frame
 * ✅ RTP: /rtp and /setrtp 90 + Pro table (payout multipliers auto scale)
 * ✅ Payout safety: cap max 30% of treasury per spin + never exceed treasury
 * ✅ .mybalance (group only) Pro+ UI rank system
 */

require("dotenv").config();
const { Telegraf } = require("telegraf");
const { MongoClient } = require("mongodb");

// -------------------- ENV --------------------
const BOT_TOKEN = process.env.BOT_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || "bika_slot";
const TZ = process.env.TZ || "Asia/Yangon";
const OWNER_ID = process.env.OWNER_ID ? Number(process.env.OWNER_ID) : null;

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!MONGO_URI) throw new Error("Missing MONGO_URI");
if (!OWNER_ID || !Number.isFinite(OWNER_ID)) throw new Error("Missing/Invalid OWNER_ID (must be a number)");

// -------------------- Bot/DB --------------------
const bot = new Telegraf(BOT_TOKEN);

let mongo, db;
let users, txs, orders, configCol;

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
  await configCol.createIndex({ key: 1 }, { unique: true });

  console.log("✅ Mongo connected");
}

// -------------------- UI helpers --------------------
const COIN = "🪙";

function fmt(n) {
  return Number(n || 0).toLocaleString();
}
function displayName(tg) {
  if (!tg) return "User";
  if (tg.username) return "@" + tg.username;
  return tg.first_name || "User";
}
function isGroupChat(ctx) {
  const t = ctx.chat?.type;
  return t === "group" || t === "supergroup";
}
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
  // Yangon UTC+6:30
  const ms = d.getTime();
  const offsetMs = 6.5 * 60 * 60 * 1000;
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
    // If ownerUserId missing, set it from ENV (one-time fix)
    if (!exist.ownerUserId) {
      await configCol.updateOne(
        { key: "treasury" },
        { $set: { ownerUserId: OWNER_ID, updatedAt: new Date() } }
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

  await configCol.updateOne(
    { key: "treasury" },
    { $set: { totalSupply: amount, ownerBalance: amount, updatedAt: new Date() } }
  );
  return { ok: true };
}

// Atomic: Treasury -> User
async function treasuryPayToUser(toUserId, amount, meta = {}) {
  const session = mongo.startSession();
  try {
    await session.withTransaction(async () => {
      const tRes = await configCol.findOneAndUpdate(
        { key: "treasury", ownerBalance: { $gte: amount } },
        { $inc: { ownerBalance: -amount }, $set: { updatedAt: new Date() } },
        { returnDocument: "after", session }
      );
      if (!tRes.value) throw new Error("TREASURY_INSUFFICIENT");

      await users.updateOne(
        { userId: toUserId },
        { $inc: { balance: amount }, $set: { updatedAt: new Date() }, $setOnInsert: { createdAt: new Date() } },
        { upsert: true, session }
      );

      await txs.insertOne(
        { type: meta.type || "treasury_pay", fromUserId: "TREASURY", toUserId, amount, meta, createdAt: new Date() },
        { session }
      );
    });
  } finally {
    await session.endSession();
  }
}

// Atomic: User -> Treasury
async function userPayToTreasury(fromUserId, amount, meta = {}) {
  const session = mongo.startSession();
  try {
    await session.withTransaction(async () => {
      const uRes = await users.findOneAndUpdate(
        { userId: fromUserId, balance: { $gte: amount } },
        { $inc: { balance: -amount }, $set: { updatedAt: new Date() } },
        { returnDocument: "after", session }
      );
      if (!uRes.value) throw new Error("USER_INSUFFICIENT");

      await configCol.updateOne(
        { key: "treasury" },
        { $inc: { ownerBalance: amount }, $set: { updatedAt: new Date() } },
        { session }
      );

      await txs.insertOne(
        { type: meta.type || "treasury_receive", fromUserId, toUserId: "TREASURY", amount, meta, createdAt: new Date() },
        { session }
      );
    });
  } finally {
    await session.endSession();
  }
}

// Atomic: User -> User
async function transferBalance(fromUserId, toUserId, amount, meta = {}) {
  const session = mongo.startSession();
  try {
    await session.withTransaction(async () => {
      const fromRes = await users.findOneAndUpdate(
        { userId: fromUserId, balance: { $gte: amount } },
        { $inc: { balance: -amount }, $set: { updatedAt: new Date() } },
        { returnDocument: "after", session }
      );
      if (!fromRes.value) throw new Error("INSUFFICIENT");

      await users.updateOne(
        { userId: toUserId },
        { $inc: { balance: amount }, $set: { updatedAt: new Date() }, $setOnInsert: { createdAt: new Date() } },
        { upsert: true, session }
      );

      await txs.insertOne(
        { type: "gift", fromUserId, toUserId, amount, meta, createdAt: new Date() },
        { session }
      );
    });
  } finally {
    await session.endSession();
  }
}

// -------------------- Treasury commands (Owner only) --------------------
bot.command("settotal", async (ctx) => {
  const amount = parseAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return ctx.reply(`🏦 *Treasury Settings*\n━━━━━━━━━━━━━━\nUsage: /settotal 5000000`, { parse_mode: "Markdown" });
  }
  const r = await setTotalSupply(ctx, Math.floor(amount));
  if (!r.ok) return ctx.reply("⛔ Owner only command.");

  const tt = await getTreasury();
  return ctx.reply(
    `🏦 *Treasury Initialized*\n━━━━━━━━━━━━━━\n• Total Supply: *${fmt(tt.totalSupply)}* ${COIN}\n• Owner Balance: *${fmt(tt.ownerBalance)}* ${COIN}`,
    { parse_mode: "Markdown" }
  );
});

bot.command("treasury", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return ctx.reply("⛔ Owner only.");
  const tr = await getTreasury();
  return ctx.reply(
    `🏦 *Treasury Dashboard*\n━━━━━━━━━━━━━━\n• Total Supply: *${fmt(tr.totalSupply)}* ${COIN}\n• Owner Balance: *${fmt(tr.ownerBalance)}* ${COIN}\n• Timezone: *${TZ}*\n• Owner ID: *${tr.ownerUserId}*`,
    { parse_mode: "Markdown" }
  );
});

// -------------------- Start bonus + balance --------------------
const START_BONUS = 300;

bot.start(async (ctx) => {
  await ensureTreasury();
  const u = await ensureUser(ctx.from);

  if (!u.startBonusClaimed) {
    try {
      await treasuryPayToUser(ctx.from.id, START_BONUS, { type: "start_bonus" });
    } catch (e) {
      // still mark claimed to avoid repeated attempts
      console.error("start bonus pay fail:", e);
    }

    await users.updateOne(
      { userId: ctx.from.id },
      { $set: { startBonusClaimed: true, updatedAt: new Date() } }
    );

    const updated = await getUser(ctx.from.id);

    return ctx.reply(
      `🎉 *Welcome Bonus*\n━━━━━━━━━━━━━━━━━━━━\n` +
        `➕ Bonus: *${fmt(START_BONUS)}* ${COIN}\n` +
        `💼 Balance: *${fmt(updated?.balance)}* ${COIN}\n` +
        `━━━━━━━━━━━━━━━━━━━━\n` +
        `Group ထဲမှာ:\n• /dailyclaim — daily bonus\n• .slot 100 — slot\n• .mybalance — wallet\n• /shop — shop`,
      { parse_mode: "Markdown" }
    );
  }

  return ctx.reply(
    `👋 *Welcome back*\n━━━━━━━━━━━━━━━━━━━━\nGroup ထဲမှာ:\n• /dailyclaim\n• .slot 100\n• .mybalance\n• /shop`,
    { parse_mode: "Markdown" }
  );
});

bot.command("balance", async (ctx) => {
  const u = await ensureUser(ctx.from);
  return ctx.reply(`💼 Balance: *${fmt(u.balance)}* ${COIN}`, { parse_mode: "Markdown" });
});

// -------------------- Daily claim (Group only, Yangon day) --------------------
const DAILY_MIN = 50;
const DAILY_MAX = 100;

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

bot.command("dailyclaim", async (ctx) => {
  if (!isGroupChat(ctx)) return ctx.reply("ℹ️ /dailyclaim ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  await ensureTreasury();
  const u = await ensureUser(ctx.from);

  const now = new Date();
  const todayStart = startOfDayYangon(now);
  const last = u.lastDailyClaimAt ? new Date(u.lastDailyClaimAt) : null;

  if (last && last >= todayStart) {
    return ctx.reply(
      `⏳ *Daily Claim*\n━━━━━━━━━━━━━━━━━━━━\n` +
        `ဒီနေ့ claim လုပ်ပြီးသားပါ။\n` +
        `Yangon time နဲ့ နေ့သစ်ဝင်ပြီးမှ ပြန် claim လုပ်နိုင်ပါတယ်။`,
      { parse_mode: "Markdown" }
    );
  }

  const amount = randInt(DAILY_MIN, DAILY_MAX);

  try {
    await treasuryPayToUser(ctx.from.id, amount, { type: "daily_claim" });
    await users.updateOne({ userId: ctx.from.id }, { $set: { lastDailyClaimAt: now, updatedAt: now } });

    const updated = await getUser(ctx.from.id);
    return ctx.reply(
      `🎁 *Daily Claim Success*\n━━━━━━━━━━━━━━━━━━━━\n` +
        `👤 ${displayName(ctx.from)}\n` +
        `➕ Reward: *${fmt(amount)}* ${COIN}\n` +
        `💼 Balance: *${fmt(updated?.balance)}* ${COIN}\n` +
        `🕒 ${formatYangon(now)} (Yangon)`,
      { parse_mode: "Markdown" }
    );
  } catch (e) {
    if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
      return ctx.reply("🏦 Treasury မလုံလောက်လို့ daily claim မပေးနိုင်သေးပါ။");
    }
    console.error("dailyclaim error:", e);
    return ctx.reply("⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

// -------------------- .mybalance Pro+ (GROUP ONLY) --------------------
function getBalanceRank(balance) {
  const b = Number(balance || 0);

  if (b === 0) return { title: "ဖင်ပြောင်ငမွဲ အဆင့်", badge: "🪫", color: "⚪" };
  if (b >= 1 && b <= 500) return { title: "ဆင်းရဲသား အိမ်ခြေမဲ့ အဆင့်", badge: "🥀", color: "🟤" };
  if (b >= 501 && b <= 1000) return { title: "အိမ်ပိုင်ဝန်းပိုင် ဆင်းရဲသားအဆင့်", badge: "🏚️", color: "🟠" };
  if (b >= 1001 && b <= 5000) return { title: "လူလတ်တန်းစားအဆင့်", badge: "🏘️", color: "🟢" };
  if (b >= 5001 && b <= 10000) return { title: "သူဌေးပေါက်စ အဆင့်", badge: "💼", color: "🔵" };
  if (b >= 10001 && b <= 100000) return { title: "သိန်းကြွယ်သူဌေး အဆင့်", badge: "💰", color: "🟣" };
  if (b >= 100001 && b <= 1000000) return { title: "သန်းကြွယ်သူဌေးအကြီးစား အဆင့်", badge: "🏦", color: "🟡" };
  if (b >= 1000001 && b <= 50000000) return { title: "ကုဋေရှစ်ဆယ် သူဌေးကြီး အဆင့်", badge: "👑", color: "🟠" };
  return { title: "လွန်ကဲသော သူဌေးကြီး အဆင့်", badge: "👑✨", color: "🟥" };
}

function progressBar(current, min, max, blocks = 10) {
  if (max <= min) return "██████████";
  const ratio = Math.max(0, Math.min(1, (current - min) / (max - min)));
  const filled = Math.round(ratio * blocks);
  return "█".repeat(filled) + "░".repeat(blocks - filled);
}

function getRankRange(balance) {
  const b = Number(balance || 0);
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

bot.hears(/^\.(mybalance|bal)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return ctx.reply("ℹ️ .mybalance ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  const u = await ensureUser(ctx.from);
  const bal = Number(u.balance || 0);

  const rank = getBalanceRank(bal);
  const range = getRankRange(bal);
  const bar = range.max === range.min ? "██████████" : progressBar(bal, range.min, range.max, 10);

  const msg =
    `💼 *BIKA Pro+ Wallet*\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `👤 ${displayName(ctx.from)}\n` +
    `🪙 Balance: *${fmt(bal)}* ${COIN}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${rank.badge} *Rank:* ${rank.title}\n` +
    `${rank.color} *Progress:* \`${bar}\`\n` +
    `📌 Range: *${fmt(range.min)}* → *${fmt(range.max)}* ${COIN}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `🕒 ${formatYangon(new Date())} (Yangon)`;

  return ctx.reply(msg, { parse_mode: "Markdown" });
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
    return ctx.reply(`⏳ ခဏစောင့်ပါ… (${sec}s) နောက်တစ်ခါ /gift လုပ်နိုင်ပါမယ်။`);
  }

  const amount = parseAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return ctx.reply(
      `🎁 *Gift Usage*\n━━━━━━━━━━━━━━\n• Reply +  /gift 500\n• Mention  /gift @username 500`,
      { parse_mode: "Markdown" }
    );
  }

  await ensureUser(fromTg);

  let toUserId = null;
  let toLabel = null;

  const replyFrom = ctx.message?.reply_to_message?.from;
  if (replyFrom?.id) {
    if (replyFrom.is_bot) return ctx.reply("🤖 Bot ကို gift မပို့နိုင်ပါ။");
    if (replyFrom.id === fromTg.id) return ctx.reply("😅 ကိုယ့်ကိုကိုယ် gift မပို့နိုင်ပါ။");
    await ensureUser(replyFrom);
    toUserId = replyFrom.id;
    toLabel = displayName(replyFrom);
  } else {
    const uname = parseMentionUsername(ctx.message?.text || "");
    if (!uname) return ctx.reply("👤 Reply (/gift 500) သို့ /gift @username 500 သုံးပါ။");
    const toU = await getUserByUsername(uname);
    if (!toU) return ctx.reply("⚠️ ဒီ @username ကို မတွေ့ပါ။ (သူ bot ကို /start လုပ်ထားရမယ်) သို့ Reply နဲ့ gift ပို့ပါ။");
    if (toU.userId === fromTg.id) return ctx.reply("😅 ကိုယ့်ကိုကိုယ် gift မပို့နိုင်ပါ။");
    toUserId = toU.userId;
    toLabel = "@" + uname;
  }

  try {
    await transferBalance(fromTg.id, toUserId, Math.floor(amount), { chatId: ctx.chat?.id });
    lastGiftAt.set(fromTg.id, Date.now());
    const updatedFrom = await getUser(fromTg.id);

    return ctx.reply(
      `✅ *Gift Sent*\n━━━━━━━━━━━━━━\n🎁 To: *${toLabel}*\n💸 Amount: *${fmt(amount)}* ${COIN}\n💼 Your Balance: *${fmt(updatedFrom?.balance)}* ${COIN}`,
      { parse_mode: "Markdown" }
    );
  } catch (e) {
    if (String(e?.message || e).includes("INSUFFICIENT")) return ctx.reply("❌ Balance မလုံလောက်ပါ။");
    console.error("gift error:", e);
    return ctx.reply("⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

// -------------------- Owner add/remove balance --------------------
function parseTargetAndAmount(text) {
  const parts = (text || "").trim().split(/\s+/);
  const amount = parseAmount(text);

  if (parts.length === 2 && amount) return { mode: "reply", target: null, amount };
  if (parts.length >= 3) {
    const rawTarget = parts[1];
    if (rawTarget.startsWith("@")) return { mode: "explicit", target: { type: "username", value: rawTarget.slice(1).toLowerCase() }, amount };
    if (/^\d+$/.test(rawTarget)) return { mode: "explicit", target: { type: "userId", value: parseInt(rawTarget, 10) }, amount };
  }
  return { mode: "invalid", target: null, amount: null };
}

async function resolveTargetFromCtx(ctx, mode, target) {
  const replyFrom = ctx.message?.reply_to_message?.from;
  if (mode === "reply" && replyFrom?.id) {
    if (replyFrom.is_bot) return { ok: false, reason: "TARGET_IS_BOT" };
    await ensureUser(replyFrom);
    return { ok: true, userId: replyFrom.id, label: displayName(replyFrom) };
  }

  if (mode === "explicit" && target) {
    if (target.type === "username") {
      const u = await getUserByUsername(target.value);
      if (!u) return { ok: false, reason: "NOT_FOUND_DB" };
      return { ok: true, userId: u.userId, label: "@" + target.value };
    }
    if (target.type === "userId") {
      await users.updateOne(
        { userId: target.value },
        { $setOnInsert: { userId: target.value, balance: 0, createdAt: new Date() }, $set: { updatedAt: new Date() } },
        { upsert: true }
      );
      return { ok: true, userId: target.value, label: String(target.value) };
    }
  }

  return { ok: false, reason: "NO_TARGET" };
}

bot.command("addbalance", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return ctx.reply("⛔ Owner only command.");

  const { mode, target, amount } = parseTargetAndAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return ctx.reply(
      `➕ *Add Balance (Owner)*\n━━━━━━━━━━━━━━\nReply mode:\n• Reply + /addbalance 5000\n\nExplicit:\n• /addbalance @username 5000\n• /addbalance 123456789 5000`,
      { parse_mode: "Markdown" }
    );
  }

  const r = await resolveTargetFromCtx(ctx, mode, target);
  if (!r.ok) return ctx.reply("👤 Target မရွေးရသေးပါ။ Reply + /addbalance 5000 သို့ /addbalance @username 5000");

  try {
    await treasuryPayToUser(r.userId, Math.floor(amount), { type: "owner_addbalance", by: ctx.from.id });
    const u = await getUser(r.userId);
    const tr = await getTreasury();

    return ctx.reply(
      `✅ *Balance Added*\n━━━━━━━━━━━━━━\n👤 User: *${r.label}*\n➕ Amount: *${fmt(amount)}* ${COIN}\n💼 User Balance: *${fmt(u?.balance)}* ${COIN}\n🏦 Treasury Left: *${fmt(tr?.ownerBalance)}* ${COIN}`,
      { parse_mode: "Markdown" }
    );
  } catch (e) {
    if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
      const tr = await getTreasury();
      return ctx.reply(`❌ Treasury မလုံလောက်ပါ။ (Treasury: ${fmt(tr?.ownerBalance)} ${COIN})`);
    }
    console.error("addbalance error:", e);
    return ctx.reply("⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

bot.command("removebalance", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return ctx.reply("⛔ Owner only command.");

  const { mode, target, amount } = parseTargetAndAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return ctx.reply(
      `➖ *Remove Balance (Owner)*\n━━━━━━━━━━━━━━\nReply mode:\n• Reply + /removebalance 5000\n\nExplicit:\n• /removebalance @username 5000\n• /removebalance 123456789 5000`,
      { parse_mode: "Markdown" }
    );
  }

  const r = await resolveTargetFromCtx(ctx, mode, target);
  if (!r.ok) return ctx.reply("👤 Target မရွေးရသေးပါ။ Reply + /removebalance 5000 သို့ /removebalance @username 5000");

  try {
    await userPayToTreasury(r.userId, Math.floor(amount), { type: "owner_removebalance", by: ctx.from.id });
    const u = await getUser(r.userId);
    const tr = await getTreasury();

    return ctx.reply(
      `✅ *Balance Removed*\n━━━━━━━━━━━━━━\n👤 User: *${r.label}*\n➖ Amount: *${fmt(amount)}* ${COIN}\n💼 User Balance: *${fmt(u?.balance)}* ${COIN}\n🏦 Treasury Now: *${fmt(tr?.ownerBalance)}* ${COIN}`,
      { parse_mode: "Markdown" }
    );
  } catch (e) {
    if (String(e?.message || e).includes("USER_INSUFFICIENT")) {
      const u = await getUser(r.userId);
      return ctx.reply(`❌ User balance မလုံလောက်ပါ။ (Balance: ${fmt(u?.balance)} ${COIN})`);
    }
    console.error("removebalance error:", e);
    return ctx.reply("⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

// -------------------- Shop + Orders --------------------
const SHOP_ITEMS = [
  { id: "dia11", name: "Diamonds 11 💎", price: 1000 },
  { id: "dia22", name: "Diamonds 22 💎", price: 1900 },
  { id: "dia33", name: "Diamonds 33 💎", price: 2800 },
  { id: "dia44", name: "Diamonds 44 💎", price: 3700 },
  { id: "dia55", name: "Diamonds 55 💎", price: 4600 },
  { id: "wp1", name: "Weekly Pass 🎟️", price: 7000 },
];

function shopKeyboard() {
  const rows = [];
  for (let i = 0; i < SHOP_ITEMS.length; i += 2) {
    const a = SHOP_ITEMS[i];
    const b = SHOP_ITEMS[i + 1];
    const row = [{ text: `${a.name} • ${fmt(a.price)}${COIN}`, callback_data: `BUY:${a.id}` }];
    if (b) row.push({ text: `${b.name} • ${fmt(b.price)}${COIN}`, callback_data: `BUY:${b.id}` });
    rows.push(row);
  }
  rows.push([{ text: "🔄 Refresh", callback_data: "SHOP:REFRESH" }]);
  return { inline_keyboard: rows };
}

function shopText(balance) {
  const lines = SHOP_ITEMS.map((x) => `• ${x.name} — *${fmt(x.price)}* ${COIN}`).join("\n");
  return (
    `🛒 *BIKA Pro Shop*\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${lines}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `💼 Your Balance: *${fmt(balance)}* ${COIN}\n` +
    `Select an item below:`
  );
}

bot.command("shop", async (ctx) => {
  const u = await ensureUser(ctx.from);
  await ensureTreasury();
  return ctx.reply(shopText(u.balance), { parse_mode: "Markdown", reply_markup: shopKeyboard() });
});

// -------------------- Slot (Animated Edit UI) --------------------
const SLOT = {
  minBet: 100,
  maxBet: 50000,
  cooldownMs: 6000,
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
      { s: "BAR", w: 45 },
      { s: "7", w: 5 },
    ],
  ],

  payouts: {
    "7,7,7": 50,
    "BAR,BAR,BAR": 35,
    "⭐,⭐,⭐": 30,
    "🔔,🔔,🔔": 20,
    "🍉,🍉,🍉": 10,
    "🍋,🍋,🍋": 5,
    "🍒,🍒,🍒": 3,
    ANY2: 1.2,
  },
};

const lastSlotAt = new Map();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
  const box = (x) => (x === "BAR" ? "🟥BAR🟥" : x === "7" ? "7️⃣" : x);
  return `┏━━━━━━━━━━━━━━┓\n┃  ${box(a)}  |  ${box(b)}  |  ${box(c)}  ┃\n┗━━━━━━━━━━━━━━┛`;
}

function spinFrame(a, b, c, note = "Spinning...", vibe = "spin") {
  const art = slotArt(a, b, c);

  const vibeHeader =
    vibe === "glow" ? "🏆✨ *WIN GLOW!* ✨🏆" :
    vibe === "lose" ? "🥀 *BAD LUCK…* 🥀" :
    vibe === "jackpot1" ? "🎉🎉🎉 *JACKPOT HIT!* 🎉🎉🎉" :
    vibe === "jackpot2" ? "💎🏆 *777 MEGA WIN!* 🏆💎" :
    "🎰 *BIKA Pro Slot*";

  const sound =
    vibe === "spin" ? "🔊 *KRRR… KRRR…*  🎛️" :
    vibe === "lock" ? "🔊 *KLAK!*  🔒" :
    vibe === "glow" ? "✨✨✨" :
    vibe === "lose" ? "🔇 *whomp… whomp…*  💔" :
    vibe.startsWith("jackpot") ? "💥🔥🎆" :
    "🔊";

  return (
    `${vibeHeader}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${art}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${sound} ${note}`
  );
}

async function runSlotSpinAnimated(ctx, bet) {
  const userId = ctx.from?.id;

  const last = lastSlotAt.get(userId) || 0;
  if (Date.now() - last < SLOT.cooldownMs) {
    const sec = Math.ceil((SLOT.cooldownMs - (Date.now() - last)) / 1000);
    return ctx.reply(`⏳ ခဏစောင့်ပါ… (${sec}s) နောက်တစ်ခါ spin လုပ်နိုင်ပါမယ်။`);
  }

  if (bet < SLOT.minBet || bet > SLOT.maxBet) {
    return ctx.reply(
      `🎰 *BIKA Pro Slot*\n━━━━━━━━━━━━━━━━━━━━\nUsage: .slot 1000\nMin: *${fmt(SLOT.minBet)}* ${COIN}\nMax: *${fmt(SLOT.maxBet)}* ${COIN}`,
      { parse_mode: "Markdown" }
    );
  }

  await ensureUser(ctx.from);
  await ensureTreasury();

  // take bet (atomic)
  try {
    await userPayToTreasury(userId, bet, { type: "slot_bet", bet, chatId: ctx.chat?.id });
  } catch (e) {
    if (String(e?.message || e).includes("USER_INSUFFICIENT")) return ctx.reply("❌ Balance မလုံလောက်ပါ။");
    console.error("slot bet error:", e);
    return ctx.reply("⚠️ Error ဖြစ်သွားပါတယ်။");
  }

  // decide final
  const finalA = weightedPick(SLOT.reels[0]);
  const finalB = weightedPick(SLOT.reels[1]);
  const finalC = weightedPick(SLOT.reels[2]);

  const mult = calcMultiplier(finalA, finalB, finalC);
  let payout = mult > 0 ? Math.floor(bet * mult) : 0;

  // cap payout
  if (payout > 0) {
    const tr = await getTreasury();
    const ownerBal = tr?.ownerBalance || 0;
    const maxPay = Math.floor(ownerBal * SLOT.capPercent);
    payout = Math.min(payout, maxPay);
    payout = Math.min(payout, ownerBal);
  }

  const win = payout > 0;
  const isJackpot = finalA === "7" && finalB === "7" && finalC === "7";

  const initA = randomSymbolFromReel(SLOT.reels[0]);
  const initB = randomSymbolFromReel(SLOT.reels[1]);
  const initC = randomSymbolFromReel(SLOT.reels[2]);

  const sent = await ctx.reply(spinFrame(initA, initB, initC, "reels spinning…", "spin"), { parse_mode: "Markdown" });
  const chatId = ctx.chat?.id;
  const messageId = sent?.message_id;

  async function safeEdit(text) {
    try {
      await ctx.telegram.editMessageText(chatId, messageId, undefined, text, { parse_mode: "Markdown" });
    } catch (_) {}
  }

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
    await safeEdit(spinFrame(f.a, f.b, f.c, f.note, f.vibe));
  }

  // payout after animation
  if (payout > 0) {
    try {
      await treasuryPayToUser(userId, payout, { type: "slot_win", bet, payout, combo: `${finalA},${finalB},${finalC}` });
    } catch (e) {
      console.error("slot payout error:", e);
      try {
        await treasuryPayToUser(userId, bet, { type: "slot_refund", reason: "payout_fail" });
      } catch (_) {}
      await safeEdit(
        `🎰 *BIKA Pro Slot*\n━━━━━━━━━━━━━━━━━━━━\n${slotArt(finalA, finalB, finalC)}\n━━━━━━━━━━━━━━━━━━━━\n⚠️ Payout error ဖြစ်လို့ refund ပြန်ပေးလိုက်ပါတယ်။`
      );
      lastSlotAt.set(userId, Date.now());
      return;
    }
  }

  lastSlotAt.set(userId, Date.now());

  const net = payout - bet;
  const headline = payout === 0 ? "❌ *LOSE*" : isJackpot ? "🏆 *JACKPOT 777!*" : "✅ *WIN*";

  const finalMsg =
    `🎰 *BIKA Pro Slot*\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${slotArt(finalA, finalB, finalC)}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${headline}\n` +
    `• Bet: *${fmt(bet)}* ${COIN}\n` +
    `• Payout: *${fmt(payout)}* ${COIN}\n` +
    `• Net: *${fmt(net)}* ${COIN}`;

  await safeEdit(finalMsg);
}

bot.hears(/^\.(slot)\s+(\d+)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return ctx.reply("ℹ️ .slot ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");
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

  const syms1 = [...p1.keys()], syms2 = [...p2.keys()], syms3 = [...p3.keys()];
  let expectedMultiplier = 0;

  for (const a of syms1) {
    for (const b of syms2) {
      for (const c of syms3) {
        const prob = p1.get(a) * p2.get(b) * p3.get(c);
        const mult = calcMultiplier(a, b, c);
        expectedMultiplier += prob * (mult || 0);
      }
    }
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
  if (!isOwner(ctx, t)) return ctx.reply("⛔ Owner only.");

  const tr = await getTreasury();
  const base = calcBaseRTP();
  const odds777 = approx777Odds();

  const msg =
    `🧮 *Slot RTP Dashboard*\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `🏦 Treasury: *${fmt(tr?.ownerBalance)}* ${COIN}\n` +
    `📦 Total Supply: *${fmt(tr?.totalSupply)}* ${COIN}\n` +
    `🎯 Base RTP: *${(base * 100).toFixed(2)}%*\n` +
    `📉 House Edge: *${((1 - base) * 100).toFixed(2)}%*\n` +
    `🎰 777 Odds: *${odds777}*\n` +
    `🛡️ Cap: *${Math.round(SLOT.capPercent * 100)}% of Treasury / spin*\n` +
    `🕒 ${formatYangon(new Date())} (Yangon)\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `*Payout Table (Bet = 1,000)*\n` +
    "```text\n" +
    renderPayoutsTable() +
    "\n```";

  return ctx.reply(msg, { parse_mode: "Markdown" });
});

bot.command("setrtp", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return ctx.reply("⛔ Owner only.");

  const parts = (ctx.message?.text || "").trim().split(/\s+/);
  if (parts.length < 2) {
    return ctx.reply(
      `⚙️ *Set RTP*\n━━━━━━━━━━━━━━━━━━━━\nUsage:\n• /setrtp 90\n• /setrtp 0.90`,
      { parse_mode: "Markdown" }
    );
  }

  let target = Number(parts[1]);
  if (!Number.isFinite(target)) return ctx.reply("Invalid number.");

  if (target > 1) target = target / 100;
  target = Math.max(0.5, Math.min(0.98, target));

  const before = calcBaseRTP();
  if (before <= 0) return ctx.reply("Base RTP is 0 (check weights/payouts).");

  const factor = target / before;
  scalePayouts(factor);

  const after = calcBaseRTP();
  const odds777 = approx777Odds();
  const tr = await getTreasury();

  const msg =
    `✅ *RTP Updated (Owner)*\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `🎯 Target RTP: *${(target * 100).toFixed(2)}%*\n` +
    `📌 Old Base RTP: *${(before * 100).toFixed(2)}%*\n` +
    `✅ New Base RTP: *${(after * 100).toFixed(2)}%*\n` +
    `🔧 Scale Factor: *${factor.toFixed(4)}*\n` +
    `🎰 777 Odds: *${odds777}*\n` +
    `🏦 Treasury: *${fmt(tr?.ownerBalance)}* ${COIN}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `*Payout Table (Bet = 1,000)*\n` +
    "```text\n" +
    renderPayoutsTable() +
    "\n```";

  return ctx.reply(msg, { parse_mode: "Markdown" });
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
  if (!isOwner(ctx, t)) return ctx.reply("⛔ Owner only.");

  const tr = await getTreasury();
  const s = getAdminSession(ctx.from.id);

  const targetLine = s?.targetUserId
    ? `👤 Target: *${s.targetLabel}*  (ID: \`${s.targetUserId}\`)`
    : `👤 Target: _Not set_`;

  const extra = note ? `\n${note}\n` : "\n";

  const text =
    `${ADMIN.panelTitle}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `🏦 Treasury Balance: *${fmt(tr?.ownerBalance)}* ${COIN}\n` +
    `📦 Total Supply: *${fmt(tr?.totalSupply)}* ${COIN}\n` +
    `🕒 ${formatYangon(new Date())} (Yangon)\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${targetLine}\n` +
    `━━━━━━━━━━━━━━━━━━━━` +
    `${extra}` +
    `Choose an action below:`;

  if (ctx.updateType === "callback_query") {
    return ctx.editMessageText(text, { parse_mode: "Markdown", reply_markup: adminKeyboard() });
  }
  return ctx.reply(text, { parse_mode: "Markdown", reply_markup: adminKeyboard() });
}

bot.command("admin", async (ctx) => renderAdminPanel(ctx));

async function getPendingOrders(limit = 10) {
  return orders.find({ status: "PENDING" }).sort({ createdAt: -1 }).limit(limit).toArray();
}

async function askManualTarget(ctx) {
  setAdminSession(ctx.from.id, { mode: "await_target" });
  return ctx.reply(
    `🔎 *Set Target User*\n━━━━━━━━━━━━━━━━━━━━\nSend one:\n• \`@username\`\n• \`123456789\` (userId)\nExample: \`@Official_Bika\``,
    { parse_mode: "Markdown", reply_markup: { force_reply: true } }
  );
}

async function askAmount(ctx, type) {
  const s = getAdminSession(ctx.from.id);
  if (!s?.targetUserId) return renderAdminPanel(ctx, "⚠️ *Target user မရွေးရသေးပါ။* Set Target လုပ်ပါ။");

  setAdminSession(ctx.from.id, { ...s, mode: type === "add" ? "await_add_amount" : "await_remove_amount" });

  const header = type === "add" ? "➕ *Add Balance*" : "➖ *Remove Balance*";
  const hint = type === "add" ? "Treasury → User" : "User → Treasury";

  return ctx.reply(
    `${header}\n━━━━━━━━━━━━━━━━━━━━\n👤 Target: *${s.targetLabel}*\n🔁 Flow: _${hint}_\n━━━━━━━━━━━━━━━━━━━━\nAmount ပို့ပါ (numbers only)\nExample: \`5000\``,
    { parse_mode: "Markdown", reply_markup: { force_reply: true } }
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
      return renderAdminPanel(ctx, "⚠️ Target format မမှန်ပါ။ `@username` သို့ `userId` ပို့ပါ။");
    }

    setAdminSession(ctx.from.id, { mode: "idle", targetUserId, targetLabel });
    return renderAdminPanel(ctx, `✅ Target set: *${targetLabel}*`);
  }

  if (s.mode === "await_add_amount") {
    const amt = parseInt(text.replace(/,/g, ""), 10);
    setAdminSession(ctx.from.id, { ...s, mode: "idle" });
    if (!Number.isFinite(amt) || amt <= 0) return renderAdminPanel(ctx, "⚠️ Amount မမှန်ပါ။ ဥပမာ `5000` လိုပို့ပါ။");

    try {
      await treasuryPayToUser(s.targetUserId, amt, { type: "owner_addbalance_admin", by: ctx.from.id });
      const u = await getUser(s.targetUserId);
      const tr = await getTreasury();
      return renderAdminPanel(
        ctx,
        `✅ *Added Successfully*\n• User: *${s.targetLabel}*\n• Amount: *${fmt(amt)}* ${COIN}\n• User Balance: *${fmt(u?.balance)}* ${COIN}\n• Treasury Left: *${fmt(tr?.ownerBalance)}* ${COIN}`
      );
    } catch (e) {
      if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
        const tr = await getTreasury();
        return renderAdminPanel(ctx, `❌ Treasury မလုံလောက်ပါ။ (Treasury: *${fmt(tr?.ownerBalance)}* ${COIN})`);
      }
      console.error("admin add error:", e);
      return renderAdminPanel(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
    }
  }

  if (s.mode === "await_remove_amount") {
    const amt = parseInt(text.replace(/,/g, ""), 10);
    setAdminSession(ctx.from.id, { ...s, mode: "idle" });
    if (!Number.isFinite(amt) || amt <= 0) return renderAdminPanel(ctx, "⚠️ Amount မမှန်ပါ။ ဥပမာ `5000` လိုပို့ပါ။");

    try {
      await userPayToTreasury(s.targetUserId, amt, { type: "owner_removebalance_admin", by: ctx.from.id });
      const u = await getUser(s.targetUserId);
      const tr = await getTreasury();
      return renderAdminPanel(
        ctx,
        `✅ *Removed Successfully*\n• User: *${s.targetLabel}*\n• Amount: *${fmt(amt)}* ${COIN}\n• User Balance: *${fmt(u?.balance)}* ${COIN}\n• Treasury Now: *${fmt(tr?.ownerBalance)}* ${COIN}`
      );
    } catch (e) {
      if (String(e?.message || e).includes("USER_INSUFFICIENT")) {
        const u = await getUser(s.targetUserId);
        return renderAdminPanel(ctx, `❌ User balance မလုံလောက်ပါ။ (User: *${fmt(u?.balance)}* ${COIN})`);
      }
      console.error("admin remove error:", e);
      return renderAdminPanel(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
    }
  }

  return next();
});

// -------------------- Callback Query (Shop + Admin) --------------------
bot.on("callback_query", async (ctx) => {
  const data = ctx.callbackQuery?.data || "";

  if (data === "SHOP:REFRESH") {
    const u = await ensureUser(ctx.from);
    await ctx.answerCbQuery("Refreshed");
    return ctx.editMessageText(shopText(u.balance), { parse_mode: "Markdown", reply_markup: shopKeyboard() });
  }

  if (data.startsWith("BUY:")) {
    const itemId = data.split(":")[1];
    const item = SHOP_ITEMS.find((x) => x.id === itemId);
    if (!item) return ctx.answerCbQuery("Item not found", { show_alert: true });

    await ensureUser(ctx.from);
    await ensureTreasury();

    try {
      await userPayToTreasury(ctx.from.id, item.price, { type: "shop_buy", itemId: item.id, itemName: item.name });
      await orders.insertOne({
        userId: ctx.from.id,
        username: ctx.from.username ? ctx.from.username.toLowerCase() : null,
        itemId: item.id,
        itemName: item.name,
        price: item.price,
        status: "PENDING",
        createdAt: new Date(),
      });

      const u = await getUser(ctx.from.id);
      await ctx.answerCbQuery("✅ Purchased!");

      return ctx.reply(
        `✅ *Order Created*\n━━━━━━━━━━━━━━━━━━━━\n🧾 Item: *${item.name}*\n💳 Paid: *${fmt(item.price)}* ${COIN}\n💼 Balance: *${fmt(u?.balance)}* ${COIN}\n━━━━━━━━━━━━━━━━━━━━\n⏳ Status: *PENDING*`,
        { parse_mode: "Markdown" }
      );
    } catch (e) {
      if (String(e?.message || e).includes("USER_INSUFFICIENT")) return ctx.answerCbQuery("Insufficient balance", { show_alert: true });
      console.error("BUY error:", e);
      return ctx.answerCbQuery("Error", { show_alert: true });
    }
  }

  if (data.startsWith("ADMIN:")) {
    const t = await ensureTreasury();
    if (!isOwner(ctx, t)) {
      await ctx.answerCbQuery("Owner only", { show_alert: true });
      return;
    }

    if (data === "ADMIN:REFRESH") {
      await ctx.answerCbQuery("Refreshed");
      return renderAdminPanel(ctx);
    }

    if (data === "ADMIN:TREASURY") {
      await ctx.answerCbQuery("Treasury");
      return renderAdminPanel(ctx, "📌 Treasury status shown above.");
    }

    if (data === "ADMIN:ORDERS") {
      const list = await getPendingOrders(10);
      await ctx.answerCbQuery("Orders");
      if (!list.length) return renderAdminPanel(ctx, "🧾 Pending Orders: _None_");

      const lines = list
        .map((o, i) => {
          const who = o.username ? `@${o.username}` : String(o.userId);
          const when = formatYangon(new Date(o.createdAt));
          return `${i + 1}. *${o.itemName}* — *${fmt(o.price)}* ${COIN}\n   👤 ${who}  •  ⏱ ${when}`;
        })
        .join("\n");

      return renderAdminPanel(ctx, `🧾 *Pending Orders (Top 10)*\n━━━━━━━━━━━━━━━━━━━━\n${lines}`);
    }

    if (data === "ADMIN:TARGET_MANUAL") {
      await ctx.answerCbQuery("Manual target");
      return askManualTarget(ctx);
    }

    if (data === "ADMIN:CLEAR_TARGET") {
      await ctx.answerCbQuery("Cleared");
      clearAdminSession(ctx.from.id);
      return renderAdminPanel(ctx, "🧹 Target cleared.");
    }

    if (data === "ADMIN:ADD") {
      await ctx.answerCbQuery("Add");
      return askAmount(ctx, "add");
    }

    if (data === "ADMIN:REMOVE") {
      await ctx.answerCbQuery("Remove");
      return askAmount(ctx, "remove");
    }

    await ctx.answerCbQuery("OK");
    return;
  }

  await ctx.answerCbQuery("OK");
});

// -------------------- Boot --------------------
(async () => {
  await connectMongo();
  await ensureTreasury();
  await bot.launch();
  console.log("🤖 Bot started");
  console.log(`🕒 TZ env: ${TZ} (recommend: TZ=Asia/Yangon on Render)`);
  console.log(`🛡️ Owner ID (env): ${OWNER_ID}`);
})();

process.once("SIGINT", () => bot.stop("SIGINT"));
process.once("SIGTERM", () => bot.stop("SIGTERM"));
