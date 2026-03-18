/*
 * BIKA Pro Slot Bot — COMPLETE
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

const PUBLIC_URL = process.env.PUBLIC_URL;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

const WEB_ORIGIN = process.env.WEB_ORIGIN || "https://officialbika.github.io";
const WEB_API_KEY = process.env.WEB_API_KEY || "";

if (!BOT_TOKEN) throw new Error("Missing BOT_TOKEN");
if (!MONGO_URI) throw new Error("Missing MONGO_URI");
if (!OWNER_ID || !Number.isFinite(OWNER_ID)) throw new Error("Missing/Invalid OWNER_ID (must be a number)");
if (!PUBLIC_URL) throw new Error("Missing PUBLIC_URL (e.g. https://xxx.onrender.com)");
if (!WEBHOOK_SECRET) throw new Error("Missing WEBHOOK_SECRET");

// -------------------- Bot/DB --------------------
const bot = new Telegraf(BOT_TOKEN);

let mongo, db;
let users, txs, orders, configCol, groupsCol;
let TX_SUPPORTED = true;
let BOT_INFO = null;

// -------------------- UI helpers --------------------
const COIN = "MMK";
const HOUSE_CUT_PERCENT = 0.05;

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

function getRetryAfterSec(err) {
  const retry = err?.response?.parameters?.retry_after;
  const m = String(err?.message || err);
  if (typeof retry === "number" && retry > 0) return retry;
  const match = m.match(/retry after (\d+)/i);
  if (match) return Number(match[1]) || 0;
  return 0;
}

async function safeTelegram(fn, { maxRetries = 3 } = {}) {
  let lastErr = null;
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      const retryAfter = getRetryAfterSec(e);
      if (String(e?.message || e).includes("429") || retryAfter > 0) {
        const waitMs = Math.max(1000, (retryAfter || 2) * 1000) + Math.floor(Math.random() * 350);
        await sleep(waitMs);
        continue;
      }
      break;
    }
  }
  throw lastErr;
}

async function replyHTML(ctx, html, extra = {}) {
  try {
    return await safeTelegram(() =>
      ctx.reply(html, { parse_mode: "HTML", disable_web_page_preview: true, ...extra })
    );
  } catch (e) {
    try {
      return await safeTelegram(() => ctx.reply(String(html).replace(/<[^>]+>/g, ""), extra));
    } catch (_) {
      return null;
    }
  }
}

async function editHTML(ctx, chatId, messageId, html, extra = {}) {
  try {
    return await safeTelegram(() =>
      ctx.telegram.editMessageText(chatId, messageId, undefined, html, {
        parse_mode: "HTML",
        disable_web_page_preview: true,
        ...extra,
      })
    );
  } catch (_) {}
}

async function safeEditCurrent(ctx, html, extra = {}) {
  try {
    return await safeTelegram(() =>
      ctx.editMessageText(html, {
        parse_mode: "HTML",
        disable_web_page_preview: true,
        ...extra,
      })
    );
  } catch (e) {
    const msg = String(e?.message || e || "");
    if (msg.includes("message is not modified") || msg.includes("message to edit not found")) return null;
    console.error("safeEditCurrent error:", msg);
    return null;
  }
}

async function safeEditByIds(chatId, messageId, html, extra = {}) {
  try {
    return await safeTelegram(() =>
      bot.telegram.editMessageText(chatId, messageId, undefined, html, {
        parse_mode: "HTML",
        disable_web_page_preview: true,
        ...extra,
      })
    );
  } catch (e) {
    const msg = String(e?.message || e || "");
    if (msg.includes("message is not modified") || msg.includes("message to edit not found")) return null;
    console.error("safeEditByIds error:", msg);
    return null;
  }
}

const callbackLocks = new Set();

async function withCallbackLock(key, fn) {
  if (callbackLocks.has(key)) return false;
  callbackLocks.add(key);
  try {
    await fn();
    return true;
  } finally {
    callbackLocks.delete(key);
  }
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

function startOfDayYangon(d) {
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

async function connectMongo() {
  mongo = new MongoClient(MONGO_URI, {});
  await mongo.connect();
  db = mongo.db(DB_NAME);

  users = db.collection("users");
  txs = db.collection("transactions");
  orders = db.collection("orders");
  configCol = db.collection("config");
  groupsCol = db.collection("groups");

  await users.createIndex({ userId: 1 }, { unique: true });
  await users.createIndex({ username: 1 }, { sparse: true });
  await txs.createIndex({ createdAt: -1 });
  await txs.createIndex({ type: 1, createdAt: -1 });
  await orders.createIndex({ status: 1, createdAt: -1 });
  await orders.createIndex({ userId: 1, createdAt: -1 });
  await configCol.createIndex({ key: 1 }, { unique: true });
  await groupsCol.createIndex({ groupId: 1 }, { unique: true });
  await groupsCol.createIndex({ username: 1 }, { sparse: true });
  await groupsCol.createIndex({ updatedAt: -1 });

  console.log("✅ Mongo connected");
}

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
        isVip: false,
      },
    },
    { upsert: true }
  );

  return users.findOne({ userId: tgUser.id });
}

async function ensureGroup(chat) {
  if (!chat) return null;
  const type = chat.type;
  if (type !== "group" && type !== "supergroup") return null;

  const doc = {
    groupId: chat.id,
    title: chat.title || "Untitled Group",
    username: chat.username ? String(chat.username).toLowerCase() : null,
    type,
    updatedAt: new Date(),
  };

  await groupsCol.updateOne(
    { groupId: chat.id },
    {
      $set: doc,
      $setOnInsert: {
        createdAt: new Date(),
        approvalStatus: "pending",
        approvedAt: null,
        approvedBy: null,
        rejectedAt: null,
        rejectedBy: null,
        botIsAdmin: false,
        inviteLink: null,
        ownerInviteNotifiedAt: null,
      },
    },
    { upsert: true }
  );

  return groupsCol.findOne({ groupId: chat.id });
}

function isApproveCommandText(text) {
  return /^\/(approve|reject|groupstatus)(?:@\w+)?(?:\s|$)/i.test(String(text || "").trim());
}

function shouldCheckGroupApproval(ctx) {
  if (!isGroupChat(ctx)) return false;
  if (ctx.updateType === "callback_query") return true;
  const text = String(ctx.message?.text || "").trim();
  if (!text) return false;
  return text.startsWith("/") || text.startsWith(".");
}

async function getGroupDoc(groupId) {
  return groupsCol.findOne({ groupId });
}

async function approveGroupById(groupId, approverId) {
  await groupsCol.updateOne(
    { groupId },
    {
      $set: {
        approvalStatus: "approved",
        approvedAt: new Date(),
        approvedBy: approverId,
        rejectedAt: null,
        rejectedBy: null,
        updatedAt: new Date(),
      },
    }
  );
  return getGroupDoc(groupId);
}

async function rejectGroupById(groupId, approverId) {
  await groupsCol.updateOne(
    { groupId },
    {
      $set: {
        approvalStatus: "rejected",
        rejectedAt: new Date(),
        rejectedBy: approverId,
        approvedAt: null,
        approvedBy: null,
        updatedAt: new Date(),
      },
    }
  );
  return getGroupDoc(groupId);
}

async function setGroupBotAdminState(groupId, isAdmin) {
  await groupsCol.updateOne(
    { groupId },
    {
      $set: {
        botIsAdmin: !!isAdmin,
        updatedAt: new Date(),
      },
    },
    { upsert: true }
  );
}

async function tryGetGroupInviteLink(chatId) {
  try {
    const link = await safeTelegram(() => bot.telegram.exportChatInviteLink(chatId));
    return link || null;
  } catch (e) {
    console.error("invite link export error:", String(e?.message || e));
    return null;
  }
}

async function notifyOwnerGroupReadyForApproval(chat) {
  if (!chat?.id) return;

  const group = await getGroupDoc(chat.id);
  const alreadyApproved = group?.approvalStatus === "approved";
  if (alreadyApproved) return;

  const inviteLink = await tryGetGroupInviteLink(chat.id);
  await groupsCol.updateOne(
    { groupId: chat.id },
    {
      $set: {
        inviteLink: inviteLink || null,
        ownerInviteNotifiedAt: new Date(),
        updatedAt: new Date(),
      },
    }
  );

  const groupTitle = chat.title || group?.title || "Untitled Group";
  const groupUsername = chat.username ? `@${chat.username}` : null;
  const text =
    `🔔 <b>New Group Approval Needed</b>
━━━━━━━━━━━━
` +
    `Group: <b>${escHtml(groupTitle)}</b>
` +
    `Group ID: <code>${chat.id}</code>
` +
    `${groupUsername ? `Username: <b>${escHtml(groupUsername)}</b>
` : ""}` +
    `Status: <b>PENDING APPROVAL</b>

` +
    `ဒီ group ကိုအသုံးပြုဖို့ Owner approve လိုပါတယ်။
` +
    `Group ထဲဝင်ပြီး <code>/approve</code> ရိုက်ပေးပါ။`;

  const keyboard = inviteLink
    ? { inline_keyboard: [[{ text: "🔗 Join Group", url: inviteLink }]] }
    : undefined;

  try {
    await safeTelegram(() =>
      bot.telegram.sendMessage(OWNER_ID, text, {
        parse_mode: "HTML",
        disable_web_page_preview: true,
        ...(keyboard ? { reply_markup: keyboard } : {}),
      })
    );
  } catch (e) {
    console.error("owner notify dm error:", String(e?.message || e));
  }
}

async function sendGroupApprovalWarning(ctx, group) {
  const text =
    group?.botIsAdmin
      ? `⛔ <b>သင့် Group Chat မှာ Bot Owner Approve မပေးထားပါ</b>
━━━━━━━━━━━━
Bot ကို အသုံးပြုဖို့ Owner က ဒီ group ထဲဝင်ပြီး <code>/approve</code> ပေးရပါမယ်။`
      : `⚠️ <b>ဒီ Group မှာ Bot ကို အရင် Admin ပေးပါ</b>
━━━━━━━━━━━━
Admin ပေးပြီးသွားတာနဲ့ Owner DM မှာ group invite link button ပို့ပေးပါမယ်။

ပြီးရင် Owner က group ထဲဝင်ပြီး <code>/approve</code> ပေးမှ အသုံးပြုလို့ရပါမယ်။`;

  if (ctx.updateType === "callback_query") {
    try {
      await ctx.answerCbQuery("Owner approval required", { show_alert: true });
    } catch (_) {}
    return null;
  }

  return replyHTML(ctx, text, {
    reply_to_message_id: ctx.message?.message_id,
  });
}

bot.use(async (ctx, next) => {
  try {
    if (ctx.from?.id) await ensureUser(ctx.from);
    if (ctx.chat) await ensureGroup(ctx.chat);
  } catch (e) {
    console.error("ensure user/group middleware error:", e);
  }
  return next();
});

bot.use(async (ctx, next) => {
  try {
    if (!shouldCheckGroupApproval(ctx)) return next();
    if (!ctx.chat?.id) return next();

    const treasury = await ensureTreasury();
    if (isOwner(ctx, treasury)) return next();

    const group = await getGroupDoc(ctx.chat.id);
    if (!group) return next();

    if (group.approvalStatus === "approved") return next();

    const text = String(ctx.message?.text || "").trim();
    if (isApproveCommandText(text)) {
      return replyHTML(ctx, `⛔ <b>Owner only</b>
━━━━━━━━━━━━
ဒီ command ကို bot owner ပဲ အသုံးပြုနိုင်ပါတယ်။`);
    }

    await sendGroupApprovalWarning(ctx, group);
    return;
  } catch (e) {
    console.error("group approval middleware error:", e);
    return next();
  }
});

bot.on("my_chat_member", async (ctx, next) => {
  try {
    const upd = ctx.update?.my_chat_member;
    const chat = upd?.chat;
    if (!chat || (chat.type !== "group" && chat.type !== "supergroup")) return next();

    const newStatus = upd?.new_chat_member?.status;
    const oldStatus = upd?.old_chat_member?.status;
    const newUserId = upd?.new_chat_member?.user?.id;

    if (BOT_INFO?.id && newUserId && newUserId !== BOT_INFO.id) return next();

    await ensureGroup(chat);

    const isAdmin = newStatus === "administrator";
    const wasAdmin = oldStatus === "administrator";

    await setGroupBotAdminState(chat.id, isAdmin);

    if (isAdmin && !wasAdmin) {
      await notifyOwnerGroupReadyForApproval(chat);
      try {
        await safeTelegram(() =>
          bot.telegram.sendMessage(
            chat.id,
            `✅ <b>Bot ကို Admin ပေးပြီးပါပြီ</b>
━━━━━━━━━━━━
Owner DM ထဲကို group invite link button ပို့ပြီးပါပြီ။
Owner က ဒီ group ထဲဝင်ပြီး <code>/approve</code> ပေးမှ bot ကို အသုံးပြုလို့ရပါမယ်။`,
            { parse_mode: "HTML", disable_web_page_preview: true }
          )
        );
      } catch (_) {}
    }
  } catch (e) {
    console.error("my_chat_member handler error:", e);
  }
  return next();
});

async function getUser(userId) {
  return users.findOne({ userId });
}

async function getUserByUsername(username) {
  return users.findOne({ username: username.toLowerCase() });
}

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

function extractUpdatedDoc(res) {
  if (!res) return null;
  if (res.value !== undefined) return res.value;
  if (res?.lastErrorObject && res?.ok !== undefined && res?.value !== undefined) return res.value;
  return res;
}

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
      { type: meta.type || "treasury_pay", fromUserId: "TREASURY", toUserId, amount: amt, meta, createdAt: new Date() },
      txOpts
    );
  });
}

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
      { type: meta.type || "treasury_receive", fromUserId, toUserId: "TREASURY", amount: amt, meta, createdAt: new Date() },
      txOpts
    );
  });
}

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
    await txs.insertOne({ type: "gift", fromUserId, toUserId, amount: amt, meta, createdAt: new Date() }, txOpts);
  });
}


function chooseWeighted(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function spinSlotOutcomeForUser(u) {
  // Normal users use the original slot behavior: pure weighted reel spins.
  if (!u?.isVip) {
    return [
      weightedPick(SLOT.reels[0]),
      weightedPick(SLOT.reels[1]),
      weightedPick(SLOT.reels[2]),
    ];
  }

  // VIP users keep the boosted chance to land a winning combo.
  const wantsWin = Math.random() < 0.90;

  if (wantsWin) {
    const winningCombos = Object.entries(SLOT.payouts)
      .filter(([k, v]) => k !== "ANY2" && Number(v) > 0)
      .map(([k]) => k.split(","));
    if (winningCombos.length) return chooseWeighted(winningCombos);

    const reels = SLOT.reels.map((reel) => reel.map((x) => x.s));
    const sym = chooseWeighted(reels[0]);
    return [sym, sym, sym];
  }

  // VIP misses fall back to the normal original weighted spin result.
  return [
    weightedPick(SLOT.reels[0]),
    weightedPick(SLOT.reels[1]),
    weightedPick(SLOT.reels[2]),
  ];
}

function decideDiceDuelValues(userA, userB) {
  let d1 = randInt(1, 6);
  let d2 = randInt(1, 6);

  const vipA = !!userA?.isVip;
  const vipB = !!userB?.isVip;

  if (vipA && !vipB && Math.random() < 0.90) {
    const pairs = [
      [6, 1], [6, 2], [6, 3], [6, 4], [6, 5],
      [5, 1], [5, 2], [5, 3], [5, 4],
      [4, 1], [4, 2], [4, 3],
      [3, 1], [3, 2],
      [2, 1],
    ];
    [d1, d2] = chooseWeighted(pairs);
  } else if (vipB && !vipA && Math.random() < 0.90) {
    const pairs = [
      [1, 6], [2, 6], [3, 6], [4, 6], [5, 6],
      [1, 5], [2, 5], [3, 5], [4, 5],
      [1, 4], [2, 4], [3, 4],
      [1, 3], [2, 3],
      [1, 2],
    ];
    [d1, d2] = chooseWeighted(pairs);
  }

  return { d1, d2 };
}

function drawShanHandsForUsers(userA, userB) {
  const tryOnce = () => {
    const deck = shuffle(buildDeck());
    const cardsA = drawCards(deck, 3);
    const cardsB = drawCards(deck, 3);
    const result = compareHands(cardsA, cardsB);
    return { cardsA, cardsB, result };
  };

  const vipA = !!userA?.isVip;
  const vipB = !!userB?.isVip;

  if (vipA && !vipB && Math.random() < 0.90) {
    for (let i = 0; i < 120; i++) {
      const out = tryOnce();
      if (out.result.winner === "A") return out;
    }
  }

  if (vipB && !vipA && Math.random() < 0.90) {
    for (let i = 0; i < 120; i++) {
      const out = tryOnce();
      if (out.result.winner === "B") return out;
    }
  }

  return tryOnce();
}

// -------------------- VIP System (Owner Only) --------------------
bot.command("addvip", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only command.");

  const { mode, target } = parseTargetOnly(ctx.message?.text || "");
  const r = await resolveTargetFromCtx(ctx, mode, target);

  if (!r.ok) {
    return replyHTML(
      ctx,
      `🌟 <b>Add VIP</b>\n━━━━━━━━━━━━\n` +
        `Usage:\n` +
        `• Reply + <code>/addvip</code>\n` +
        `• <code>/addvip @username</code>\n` +
        `• <code>/addvip 123456789</code>`
    );
  }

  try {
    await users.updateOne(
      { userId: r.userId },
      {
        $set: {
          isVip: true,
          updatedAt: new Date(),
        },
        $setOnInsert: {
          userId: r.userId,
          balance: 0,
          createdAt: new Date(),
          startBonusClaimed: false,
          lastDailyClaimAt: null,
        },
      },
      { upsert: true }
    );

    return replyHTML(
      ctx,
      `🌟 <b>VIP Added Successfully</b>\n━━━━━━━━━━━━━━━━\n` +
        `User: ${r.labelHtml}\n` +
        `Status: <b>VIP Member</b>`
    );
  } catch (e) {
    console.error("addvip error:", e);
    return replyHTML(ctx, "⚠️ VIP ထည့်ရာတွင် အမှားအယွင်းရှိပါသည်။");
  }
});

bot.command("removevip", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only command.");

  const { mode, target } = parseTargetOnly(ctx.message?.text || "");
  const r = await resolveTargetFromCtx(ctx, mode, target);

  if (!r.ok) {
    return replyHTML(
      ctx,
      `❌ <b>Remove VIP</b>\n━━━━━━━━━━━━\n` +
        `Usage:\n` +
        `• Reply + <code>/removevip</code>\n` +
        `• <code>/removevip @username</code>\n` +
        `• <code>/removevip 123456789</code>`
    );
  }

  try {
    const existing = await getUser(r.userId);
    if (!existing) {
      return replyHTML(ctx, `⚠️ User not found: ${r.labelHtml}`);
    }

    await users.updateOne(
      { userId: r.userId },
      {
        $set: {
          isVip: false,
          updatedAt: new Date(),
        },
      }
    );

    return replyHTML(
      ctx,
      `❌ <b>VIP Status Removed</b>\n━━━━━━━━━━━━━━━━\n` +
        `User: ${r.labelHtml}\n` +
        `Status: <b>Normal User</b>`
    );
  } catch (e) {
    console.error("removevip error:", e);
    return replyHTML(ctx, "⚠️ VIP ဖြုတ်ရာတွင် အမှားအယွင်းရှိပါသည်။");
  }
});
// -------------------- Treasury commands --------------------
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

// -------------------- Start bonus + balance --------------------
const START_BONUS = 300;

bot.start(async (ctx) => {
  await ensureTreasury();
  const u = await ensureUser(ctx.from);

  if (!u.startBonusClaimed) {
    const tr = await getTreasury();
    if (toNum(tr?.ownerBalance) < START_BONUS) {
      return replyHTML(
        ctx,
        `⚠️ <b>Treasury မသတ်မှတ်ရသေးပါ</b>\n━━━━━━━━━━━━━━━━\nOwner က <code>/settotal 5000000</code> လုပ်ပြီးမှ Welcome Bonus ပေးနိုင်ပါတယ်။`
      );
    }

    try {
      await treasuryPayToUser(ctx.from.id, START_BONUS, { type: "start_bonus" });
      await users.updateOne({ userId: ctx.from.id }, { $set: { startBonusClaimed: true, updatedAt: new Date() } });

      const updated = await getUser(ctx.from.id);
      return replyHTML(
        ctx,
        `🎉 <b>Welcome Bonus</b>\n━━━━━━━━━━━━━━━\n` +
          `👤 ${mentionHtml(ctx.from)}\n` +
          `➕ Bonus: <b>${fmt(START_BONUS)}</b> ${COIN}\n` +
          `💼 Balance: <b>${fmt(updated?.balance)}</b> ${COIN}\n` +
          `━━━━━━━━━━━━━━\n` +
          `Group Commands:\n• <code>/dailyclaim</code>\n• <code>.slot 100</code>\n• <code>.dice 200</code>\n• <code>.shan 500</code>\n• <code>.mybalance</code>\n• <code>.top10</code>\n• <code>/shop</code>`
      );
    } catch (e) {
      if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
        return replyHTML(ctx, "🏦 Treasury မလုံလောက်ပါ။ Owner က /settotal ပြန်သတ်မှတ်ပေးပါ။");
      }
      console.error("start bonus pay fail:", e);
      return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
    }
  }

  return replyHTML(
    ctx,
    `👋 <b>Welcome back</b>\n━━━━━━━━━━━━━━━\nGroup Commands:\n• <code>/dailyclaim</code>\n• <code>.slot 100</code>\n• <code>.dice 200</code>\n• <code>.shan 500</code>\n• <code>.mybalance</code>\n• <code>.top10</code>\n• <code>/shop</code>`
  );
});

bot.command("balance", async (ctx) => {
  const u = await ensureUser(ctx.from);
  return replyHTML(ctx, `💼 Balance: <b>${fmt(u.balance)}</b> ${COIN}`);
});

// -------------------- Daily claim --------------------
const DAILY_MIN = 500;
const DAILY_MAX = 2000;

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

bot.command("dailyclaim", async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>/dailyclaim</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  await ensureTreasury();
  const u = await ensureUser(ctx.from);

  const now = new Date();
  const todayStart = startOfDayYangon(now);
  const last = u.lastDailyClaimAt ? new Date(u.lastDailyClaimAt) : null;

  if (last && last >= todayStart) {
    return replyHTML(
      ctx,
      `⏳ <b>Daily Claim</b>\n━━━━━━━━━━━━━━\nဒီနေ့ claim လုပ်ပြီးပြီလေ တစ်ရက် ဘယ်နှကြိမ်ယူချင်နေတာလဲ။\n လစ်လစ် !! နောက်နေ့မှ ပြန် claim လုပ်။`
    );
  }

  const amount = randInt(DAILY_MIN, DAILY_MAX);
  const tr = await getTreasury();
  if (toNum(tr?.ownerBalance) < amount) {
    return replyHTML(ctx, "🏦 ဘဏ်ငွေလက်ကျန် မလုံလောက်လို့ daily claim မပေးနိုင်သေးပါ။");
  }

  try {
    await treasuryPayToUser(ctx.from.id, amount, { type: "daily_claim" });
    await users.updateOne({ userId: ctx.from.id }, { $set: { lastDailyClaimAt: now, updatedAt: now } });

    const updated = await getUser(ctx.from.id);
    return replyHTML(
      ctx,
      `🎁 <b>Daily Claim Success</b>\n━━━━━━━━━━━━━━\n` +
        `👤 ${mentionHtml(ctx.from)}\n` +
        `➕ Reward: <b>${fmt(amount)}</b> ${COIN}\n` +
        `💼 Balance: <b>${fmt(updated?.balance)}</b> ${COIN}\n` +
        `🕒 ${escHtml(formatYangon(now))} (Yangon Time)`
    );
  } catch (e) {
    if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) return replyHTML(ctx, "🏦 ဘဏ်ငွေလက်ကျန် မလုံလောက်ပါ။");
    console.error("dailyclaim error:", e);
    return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});

// -------------------- Rank / Wallet UI --------------------
function getBalanceRank(balance) {
  const b = toNum(balance);
  if (b === 0) return { tier: 0, title: "ဖင်ပြောင်ငမွဲ", badge: "🪫", crown: "⚪", aura: "▫️" };
  if (b <= 500) return { tier: 1, title: "အိမ်​ခြေမဲ့ ဆင်းရဲသား", badge: "🥀", crown: "🟤", aura: "🟤" };
  if (b <= 1000) return { tier: 2, title: "အိမ်ပိုင်ဝန်းပိုင် ဆင်းရဲသား", badge: "🏚️", crown: "🟠", aura: "🟠" };
  if (b <= 5000) return { tier: 3, title: "လူလတ်တန်းစား", badge: "🏘️", crown: "🟢", aura: "🟢" };
  if (b <= 10000) return { tier: 4, title: "သူဌေးပေါက်စ", badge: "💼", crown: "🔵", aura: "🔵" };
  if (b <= 100000) return { tier: 5, title: "သိန်းကြွယ်သူဌေး", badge: "💰", crown: "🟣", aura: "🟣" };
  if (b <= 1000000) return { tier: 6, title: "သန်းကြွယ်သူဌေး", badge: "🏦", crown: "🟡", aura: "🟡" };
  if (b <= 50000000) return { tier: 7, title: "ကုဋေ၈၀ သူဌေးကြီး", badge: "👑", crown: "🟠", aura: "🟠" };
  return { tier: 8, title: "ကမ္ဘာ့အချမ်းသာဆုံး လူသား", badge: "👑✨", crown: "🟥", aura: "🟥" };
}

function progressBar(current, min, max, blocks = 12) {
  if (max <= min) return "████████████";
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

bot.hears(/^\.(mybalance|bal)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>.mybalance</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  const u = await ensureUser(ctx.from);
  const bal = toNum(u.balance);

  const rank = getBalanceRank(bal);
  const range = getRankRange(bal);
  const bar = range.max === range.min ? "████████████" : progressBar(bal, range.min, range.max, 12);

  const msg =
    `${rank.badge} <b>BIKA Pro+ Wallet</b> ${rank.crown}\n` +
    `━━━━━━━━━━━━━━━━\n` +
    `👤 ${mentionHtml(ctx.from)}\n\n` +
    `🪙 လက်ကျန်ငွေ: <b>${fmt(bal)}</b> ${COIN}\n` +
    `━━━━━━━━━━━━━━━━\n` +
    `🏷️ Rank: <b>${escHtml(rank.title)}</b>\n\n` +
    `${rank.aura} Progress: <code>${escHtml(bar)}</code>\n\n` +
    `📌 Range: <b>${fmt(range.min)}</b> → <b>${fmt(range.max)}</b> ${COIN}\n` +
    `━━━━━━━━━━━━━━━━\n` +
    `🕒 ${escHtml(formatYangon(new Date()))} (Yangon Time)`;

  return replyHTML(ctx, msg);
});

// -------------------- Top10 --------------------
function topBadge(i) {
  if (i === 0) return "🥇👑";
  if (i === 1) return "🥈";
  if (i === 2) return "🥉";
  if (i < 10) return "🏅";
  return "•";
}

async function sendTop10(ctx) {
  const list = await users.find({}).sort({ balance: -1 }).limit(10).toArray();
  if (!list.length) return replyHTML(ctx, "📊 Top10 မရှိသေးပါ။");

  const lines = list.map((u, idx) => {
    const name = u.username ? `@${escHtml(u.username)}` : `<code>${u.userId}</code>`;
    const r = getBalanceRank(u.balance);
    return `${topBadge(idx)} <b>#${idx + 1}</b> ${r.badge} ${name} — <b>${fmt(u.balance)}</b> ${COIN}`;
  });

  const msg =
    `📊 <b>BIKA • Top 10 Players</b>\n` +
    `━━━━━━━━━━━━━━━━\n` +
    lines.join("\n") +
    `\n━━━━━━━━━━━━━━━\n` +
    `🕒 ${escHtml(formatYangon(new Date()))} (Yangon Time)`;

  return replyHTML(ctx, msg);
}

bot.hears(/^\.(top10)(\s+players)?\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>.top10</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");
  return sendTop10(ctx);
});

bot.command("top10", sendTop10);

// -------------------- Broadcast --------------------
bot.command("broadcast", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only.");

  let text = (ctx.message?.text || "").replace(/^\/broadcast(@\w+)?\s*/i, "").trim();
  if (!text) {
    const rep = ctx.message?.reply_to_message;
    if (rep?.text) text = rep.text;
    else if (rep?.caption) text = rep.caption;
  }

  if (!text) {
    return replyHTML(
      ctx,
      `📣 <b>Broadcast</b>\n━━━━━━━━━━━━\nUsage:\n• <code>/broadcast မင်္ဂလာပါ...</code>\n• (or) Reply to a message + <code>/broadcast</code>\n━━━━━━━━━━━━━`
    );
  }

  const cursor = users.find({}, { projection: { userId: 1 } });
  let ok = 0, fail = 0;

  await replyHTML(ctx, `📣 Broadcasting…\nTarget: users collection`);

  while (await cursor.hasNext()) {
    const u = await cursor.next();
    try {
      await safeTelegram(
        () =>
          bot.telegram.sendMessage(u.userId, `📣 <b>BIKA Broadcast</b>\n━━━━━━━━━━━━━━\n${escHtml(text)}`, {
            parse_mode: "HTML",
            disable_web_page_preview: true,
          }),
        { maxRetries: 3 }
      );
      ok++;
    } catch (_) {
      fail++;
    }
    await sleep(35);
  }

  return replyHTML(ctx, `✅ Broadcast done.\n• Sent: <b>${ok}</b>\n• Failed: <b>${fail}</b>`);
});

// -------------------- Gift --------------------
const GIFT_COOLDOWN_MS = 10_000;
const lastGiftAt = new Map();

async function doGift(ctx, toUserId, amount, toLabelHtml) {
  const fromTg = ctx.from;
  const last = lastGiftAt.get(fromTg.id) || 0;
  if (Date.now() - last < GIFT_COOLDOWN_MS) {
    const sec = Math.ceil((GIFT_COOLDOWN_MS - (Date.now() - last)) / 1000);
    return replyHTML(ctx, `⏳ ခဏစောင့်ပါ… (${sec}s) ပီးမှ နောက်တစ်ခါ gift လုပ်နိုင်ပါမယ်။`, {
      reply_to_message_id: ctx.message?.message_id,
    });
  }

  try {
    await transferBalance(fromTg.id, toUserId, Math.floor(amount), { chatId: ctx.chat?.id });
    lastGiftAt.set(fromTg.id, Date.now());

    const updatedFrom = await getUser(fromTg.id);
    const fromHtml = mentionHtml(fromTg);

    return replyHTML(
      ctx,
      `🎁 <b>Gift Success</b>\n━━━━━━━━━━━━━━━━\n` +
        `ပေးပို့သူ: ${fromHtml}\n` +
        `လက်ခံရရှိသူ: ${toLabelHtml}\n` +
        `လင်ဆောင်ပမာဏ: <b>${fmt(amount)}</b> ${COIN}\n` +
        `စုစုပေါင်း လက်ကျန်ငွေ: <b>${fmt(updatedFrom?.balance)}</b> ${COIN}`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  } catch (e) {
    if (String(e?.message || e).includes("INSUFFICIENT")) {
      return replyHTML(ctx, "❌ လက်ကျန်ငွေ မလုံလောက်ပါ။", { reply_to_message_id: ctx.message?.message_id });
    }
    console.error("gift error:", e);
    return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။", { reply_to_message_id: ctx.message?.message_id });
  }
}

bot.command("gift", async (ctx) => {
  const fromTg = ctx.from;
  if (!fromTg) return;

  const amount = parseAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return replyHTML(
      ctx,
      `🎁 <b>Gift Usage</b>\n━━━━━━━━━━━━━\n• Reply + <code>/gift 500</code>\n• Mention + <code>/gift @username 500</code>\n• Reply + <code>.gift 500</code> (group)`
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
    if (!toU) return replyHTML(ctx, "⚠️ ဒီ @username ကို မတွေ့ပါ။ (သူ bot ကို /start လုပ်ထားရမယ်) သို့ Reply နဲ့ gift ပို့ပါ။");
    if (toU.userId === fromTg.id) return replyHTML(ctx, "😅 ကိုယ့်ကိုကိုယ် gift မပို့နိုင်ပါ။");
    toUserId = toU.userId;
    toLabelHtml = `@${escHtml(uname)}`;
  }

  return doGift(ctx, toUserId, amount, toLabelHtml);
});

bot.hears(/^\.(gift)\s+(\d+)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>.gift</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  const bet = parseInt(ctx.match[2], 10);
  if (!Number.isFinite(bet) || bet <= 0) return;

  const replyFrom = ctx.message?.reply_to_message?.from;
  if (!replyFrom?.id) {
    return replyHTML(ctx, `⚠️ <b>Reply လုပ်ပြီးသုံးပါ</b>\n━━━━━━━━━━━━━━\nExample: Reply + <code>.gift 200</code>`, {
      reply_to_message_id: ctx.message?.message_id,
    });
  }
  if (replyFrom.is_bot) return replyHTML(ctx, "🤖 Bot ကို gift မပို့နိုင်ပါ။", { reply_to_message_id: ctx.message?.message_id });
  if (replyFrom.id === ctx.from.id) return replyHTML(ctx, "😅 ကိုယ့်ကိုကိုယ် gift မပို့နိုင်ပါ။", { reply_to_message_id: ctx.message?.message_id });

  await ensureUser(ctx.from);
  await ensureUser(replyFrom);

  return doGift(ctx, replyFrom.id, bet, mentionHtml(replyFrom));
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

function parseTargetOnly(text) {
  const parts = String(text || "").trim().split(/\s+/);

  // Reply mode: /addvip  သို့ /removevip
  if (parts.length === 1) {
    return { mode: "reply", target: null };
  }

  // Explicit mode: /addvip @username  သို့ /addvip 123456789
  if (parts.length >= 2) {
    const rawTarget = parts[1];

    if (rawTarget.startsWith("@") && rawTarget.length > 1) {
      return {
        mode: "explicit",
        target: { type: "username", value: rawTarget.slice(1).toLowerCase() },
      };
    }

    if (/^\d+$/.test(rawTarget)) {
      return {
        mode: "explicit",
        target: { type: "userId", value: parseInt(rawTarget, 10) },
      };
    }
  }

  return { mode: "invalid", target: null };
}

bot.command("addbalance", async (ctx) => {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only command.");

  const { mode, target, amount } = parseTargetAndAmount(ctx.message?.text || "");
  if (!amount || amount <= 0) {
    return replyHTML(
      ctx,
      `➕ <b>Add Balance (Owner)</b>\n━━━━━━━━━━━━\nReply mode:\n• Reply + <code>/addbalance 5000</code>\n\nExplicit:\n• <code>/addbalance @username 5000</code>\n• <code>/addbalance 123456789 5000</code>`
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
      `✅ <b>Balance Added</b>\n━━━━━━━━━━━━\nUser: ${r.labelHtml}\nထပ်ဖြည့်လိုက်သောငွေ: <b>${fmt(amount)}</b> ${COIN}\nလက်ကျန်ငွေစုစုပေါင်း: <b>${fmt(u?.balance)}</b> ${COIN}\nဘဏ်ငွေ လက်ကျန်: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}`
    );
  } catch (e) {
    if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) {
      const tr = await getTreasury();
      return replyHTML(ctx, `❌ ဘဏ်ငွေလက်ကျန် မလုံလောက်ပါ။ (Treasury: <b>${fmt(tr?.ownerBalance)}</b> ${COIN})`);
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
      `➖ <b>Remove Balance (Owner)</b>\n━━━━━━━━━━━━━\nReply mode:\n• Reply + <code>/removebalance 5000</code>\n\nExplicit:\n• <code>/removebalance @username 5000</code>\n• <code>/removebalance 123456789 5000</code>`
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
      return replyHTML(ctx, `❌ လက်ကျန်ငွေ မလုံလောက်ပါ။ (Balance: <b>${fmt(u?.balance)}</b> ${COIN})`);
    }
    console.error("removebalance error:", e);
    return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။");
  }
});
// -------------------- Shop + Orders --------------------
const SHOP_ITEMS = [
  { id: "dia11", name: "Diamonds 11 💎", price: 500000 },
  { id: "dia22", name: "Diamonds 22 💎", price: 1000000 },
  { id: "dia33", name: "Diamonds 33 💎", price: 1500000 },
  { id: "dia44", name: "Diamonds 44 💎", price: 2000000 },
  { id: "dia55", name: "Diamonds 55 💎", price: 2500000 },
  { id: "wp1", name: "Weekly Pass 🎟️", price: 9000000 },
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
    `━━━━━━━━━━━━━━\n` +
    `${lines}\n` +
    `━━━━━━━━━━━━━━\n` +
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
  for (const o of list.slice(0, 8)) {
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

// -------------------- Slot --------------------
const MAX_ACTIVE_SLOTS = 3;
const activeSlots = new Set();
console.log(`🎰 MAX_ACTIVE_SLOTS: ${MAX_ACTIVE_SLOTS}`);

const SLOT = {
  minBet: 50,
  maxBet: 5000,
  cooldownMs: 1500,
  capPercent: 0.30,
  reels: [
    [
      { s: "🍒", w: 3200 },
      { s: "🍋", w: 2200 },
      { s: "🍉", w: 1500 },
      { s: "🔔", w: 900 },
      { s: "⭐", w: 450 },
      { s: "BAR", w: 200 },
      { s: "7", w: 100 },
    ],
    [
      { s: "🍒", w: 3200 },
      { s: "🍋", w: 2200 },
      { s: "🍉", w: 1500 },
      { s: "🔔", w: 900 },
      { s: "⭐", w: 450 },
      { s: "BAR", w: 200 },
      { s: "7", w: 100 },
    ],
    [
      { s: "🍒", w: 3200 },
      { s: "🍋", w: 2200 },
      { s: "🍉", w: 1500 },
      { s: "🔔", w: 900 },
      { s: "⭐", w: 450 },
      { s: "BAR", w: 200 },
      { s: "7", w: 100 },
    ],
  ],
  payouts: {
    "7,7,7": 20,
    "BAR,BAR,BAR": 15,
    "⭐,⭐,⭐": 12,
    "🔔,🔔,🔔": 9,
    "🍉,🍉,🍉": 7,
    "🍋,🍋,🍋": 5,
    "🍒,🍒,🍒": 3,
    ANY2: 1.5,
  },
};

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
      : vibe === "jackpot"
      ? "💎🏆 777 JACKPOT! 🏆💎"
      : "🎰 BIKA Pro Slot";

  return `<b>${escHtml(vibeHeader)}</b>\n━━━━━━━━━━━━\n<pre>${escHtml(art)}</pre>\n━━━━━━━━━━━━\n${escHtml(note)}`;
}

async function runSlotSpinAnimated(ctx, bet) {
  const userId = ctx.from?.id;
  if (!userId) return;

  if (activeSlots.size >= MAX_ACTIVE_SLOTS && !activeSlots.has(userId)) {
    return replyHTML(
      ctx,
      `⛔ <b>Slot Busy</b>\n━━━━━━━━━━━━━━\nအခုတလော တစ်ပြိုင်နက် ဆော့နေသူများလို့ ခဏနားပြီး ပြန်ကြိုးစားပါ။\n(Max active: <b>${MAX_ACTIVE_SLOTS}</b>)`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

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
      `🎰 <b>BIKA Pro Slot</b>\n━━━━━━━━━━━━━\nUsage: <code>.slot 1000</code>\nMin: <b>${fmt(SLOT.minBet)}</b> ${COIN}\nMax: <b>${fmt(SLOT.maxBet)}</b> ${COIN}`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  await ensureUser(ctx.from);
  await ensureTreasury();

  activeSlots.add(userId);
  try {
    try {
      await userPayToTreasury(userId, bet, { type: "slot_bet", bet, chatId: ctx.chat?.id });
    } catch (e) {
      if (String(e?.message || e).includes("USER_INSUFFICIENT")) {
        return replyHTML(
          ctx,
          `❌ <b>Balance မလုံလောက်ပါ</b>\n━━━━━━━━━━━━━━\nSlot ဆော့ဖို့ လက်ကျန်ငွေ မလုံလောက်ပါ။\nDaily claim / gift / addbalance နဲ့ ငွေစုဆောင်းပြီးမှ ပြန်လာပါ။`,
          { reply_to_message_id: ctx.message?.message_id }
        );
      }
      console.error("slot bet error:", e);
      return replyHTML(ctx, "⚠️ Error ဖြစ်သွားပါတယ်။", { reply_to_message_id: ctx.message?.message_id });
    }

    const slotUser = await ensureUser(ctx.from);
    const [finalA, finalB, finalC] = spinSlotOutcomeForUser(slotUser);

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
      { a: randomSymbolFromReel(SLOT.reels[0]), b: randomSymbolFromReel(SLOT.reels[1]), c: randomSymbolFromReel(SLOT.reels[2]), note: "rolling…", vibe: "spin", delay: 320 },
      { a: finalA, b: randomSymbolFromReel(SLOT.reels[1]), c: randomSymbolFromReel(SLOT.reels[2]), note: "locking…", vibe: "spin", delay: 380 },
      { a: finalA, b: finalB, c: finalC, note: "result!", vibe: isJackpot ? "jackpot" : win ? "glow" : "lose", delay: 450 },
    ];

    for (const f of frames) {
      await sleep(f.delay);
      await editHTML(ctx, chatId, messageId, spinFrame(f.a, f.b, f.c, f.note, f.vibe));
    }

    if (payout > 0) {
      try {
        await treasuryPayToUser(userId, payout, { type: "slot_win", bet, payout, combo: `${finalA},${finalB},${finalC}` });
      } catch (e) {
        console.error("slot payout error:", e);
        try {
          await treasuryPayToUser(userId, bet, { type: "slot_refund", reason: "payout_fail" });
        } catch (_) {}
        await editHTML(
          ctx,
          chatId,
          messageId,
          `🎰 <b>BIKA Pro Slot</b>\n━━━━━━━━━━━━━━\n<pre>${escHtml(slotArt(finalA, finalB, finalC))}</pre>\n━━━━━━━━━━━━━━\n⚠️ Payout error ဖြစ်လို့ refund ပြန်ပေးလိုက်ပါတယ်။`
        );
        lastSlotAt.set(userId, Date.now());
        return;
      }
    }

    lastSlotAt.set(userId, Date.now());
    const net = payout - bet;
    const headline = payout === 0 ? "❌ LOSE" : isJackpot ? "🏆 JACKPOT 777!" : "✅ WIN";

    const finalMsg =
      `🎰 <b>BIKA Pro Slot</b>\n` +
      `━━━━━━━━━━━\n` +
      `<pre>${escHtml(slotArt(finalA, finalB, finalC))}</pre>\n` +
      `━━━━━━━━━━━\n` +
      `<b>${escHtml(headline)}</b>\n` +
      `Bet: <b>${fmt(bet)}</b> ${COIN}\n` +
      `Payout: <b>${fmt(payout)}</b> ${COIN}\n` +
      `Net: <b>${fmt(net)}</b> ${COIN}`;

    await editHTML(ctx, chatId, messageId, finalMsg);
  } finally {
    activeSlots.delete(userId);
  }
}

bot.hears(/^\.(slot)\s+(\d+)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) {
    return replyHTML(ctx, "ℹ️ <code>.slot</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");
  }

  const bet = parseInt(ctx.match[2], 10);
  if (!Number.isFinite(bet) || bet <= 0) return;

  runSlotSpinAnimated(ctx, bet).catch((err) => {
    console.error("slot spin error:", err);
  });
});

// -------------------- RTP --------------------
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
    `━━━━━━━━━━━━\n` +
    `Treasury: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}\n` +
    `Total Supply: <b>${fmt(tr?.totalSupply)}</b> ${COIN}\n` +
    `Base RTP: <b>${(base * 100).toFixed(2)}%</b>\n` +
    `House Edge: <b>${((1 - base) * 100).toFixed(2)}%</b>\n` +
    `777 Odds: <b>${escHtml(odds777)}</b>\n` +
    `Cap: <b>${Math.round(SLOT.capPercent * 100)}%</b> of Treasury / spin\n` +
    `🕒 ${escHtml(formatYangon(new Date()))} (Yangon)\n` +
    `━━━━━━━━━━━━\n` +
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
    `━━━━━━━━━━━━\n` +
    `Target RTP: <b>${(target * 100).toFixed(2)}%</b>\n` +
    `Old Base RTP: <b>${(before * 100).toFixed(2)}%</b>\n` +
    `New Base RTP: <b>${(after * 100).toFixed(2)}%</b>\n` +
    `Scale Factor: <b>${factor.toFixed(4)}</b>\n` +
    `777 Odds: <b>${escHtml(odds777)}</b>\n` +
    `Treasury: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}\n` +
    `━━━━━━━━━━━━\n` +
    `<b>Payout Table (Bet = 1,000)</b>\n` +
    `<pre>${escHtml(renderPayoutsTable())}</pre>`;

  return replyHTML(ctx, msg);
});
// -------------------- Admin dashboard --------------------
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
        { text: "👥 Group List", callback_data: "ADMIN:GROUPS" },
        { text: "🔎 Set Target (ID/@)", callback_data: "ADMIN:TARGET_MANUAL" },
      ],
      [
        { text: "🧹 Clear Target", callback_data: "ADMIN:CLEAR_TARGET" },
        { text: "🔄 Refresh", callback_data: "ADMIN:REFRESH" },
      ],
    ],
  };
}

async function renderAdminPanel(ctx, note = "") {
  const t = await ensureTreasury();
  if (!isOwner(ctx, t)) return replyHTML(ctx, "⛔ Owner only.");

  const tr = await getTreasury();
  const s = getAdminSession(ctx.from.id);
  const userCount = await users.countDocuments();
  const groupCount = await groupsCol.countDocuments();

  const targetLine = s?.targetUserId
    ? `👤 Target: <b>${escHtml(String(s.targetLabel))}</b> (ID: <code>${s.targetUserId}</code>)`
    : `👤 Target: <i>Not set</i>`;

  const extra = note ? `\n${note}\n` : "\n";

  const text =
    `${ADMIN.panelTitle}\n` +
    `━━━━━━━━━━━━\n` +
    `🏦 Treasury Balance: <b>${fmt(tr?.ownerBalance)}</b> ${COIN}\n` +
    `📦 Total Supply: <b>${fmt(tr?.totalSupply)}</b> ${COIN}\n` +
    `👤 Users: <b>${fmt(userCount)}</b>\n` +
    `👥 Groups: <b>${fmt(groupCount)}</b>\n` +
    `🕒 ${escHtml(formatYangon(new Date()))} (Yangon Time)\n` +
    `━━━━━━━━━━━━\n` +
    `${targetLine}\n` +
    `━━━━━━━━━━━━` +
    `${extra}` +
    `Choose an action below:`;

  if (ctx.updateType === "callback_query") {
    return ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: adminKeyboard(), disable_web_page_preview: true });
  }
  return replyHTML(ctx, text, { reply_markup: adminKeyboard() });
}

async function renderAdminGroups(ctx) {
  const list = await groupsCol.find({}).sort({ updatedAt: -1 }).limit(30).toArray();

  if (!list.length) {
    const text =
      `👥 <b>Known Groups</b>\n━━━━━━━━━━━━\n` +
      `No groups saved yet.\n` +
      `Bot ကို group တွေထဲထည့်ပြီး message/command run လိုက်ပါ။`;

    const extra = { reply_markup: { inline_keyboard: [[{ text: "⬅️ Back to Admin", callback_data: "ADMIN:REFRESH" }]] } };
    if (ctx.updateType === "callback_query") {
      return ctx.editMessageText(text, { parse_mode: "HTML", disable_web_page_preview: true, ...extra });
    }
    return replyHTML(ctx, text, extra);
  }

  const lines = list.map((g, i) => {
    const main = g.username ? `@${escHtml(g.username)}` : `<b>${escHtml(g.title || "Untitled Group")}</b>`;
    const sub = g.username ? `(${escHtml(g.title || "Untitled Group")})` : `<code>${g.groupId}</code>`;
    const status = g.approvalStatus ? String(g.approvalStatus).toUpperCase() : "PENDING";
    const admin = g.botIsAdmin ? "ADMIN" : "NO-ADMIN";
    return `${i + 1}. ${main} ${sub} — <b>${status}</b> / <i>${admin}</i>`;
  });

  const text =
    `👥 <b>Known Groups</b>\n━━━━━━━━━━━━\n` +
    lines.join("\n") +
    `\n━━━━━━━━━━━━\nTotal shown: <b>${fmt(list.length)}</b>`;

  const extra = { reply_markup: { inline_keyboard: [[{ text: "⬅️ Back to Admin", callback_data: "ADMIN:REFRESH" }]] } };
  if (ctx.updateType === "callback_query") {
    return ctx.editMessageText(text, { parse_mode: "HTML", disable_web_page_preview: true, ...extra });
  }
  return replyHTML(ctx, text, extra);
}

bot.command("groupstatus", async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ ဒီ command ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");
  const g = await getGroupDoc(ctx.chat.id);
  if (!g) return replyHTML(ctx, "Group data not found.");

  const statusMap = { approved: "APPROVED", rejected: "REJECTED", pending: "PENDING" };
  return replyHTML(
    ctx,
    `👥 <b>Group Status</b>
━━━━━━━━━━━━
` +
      `Group: <b>${escHtml(g.title || "Untitled Group")}</b>
` +
      `Group ID: <code>${g.groupId}</code>
` +
      `Bot Admin: <b>${g.botIsAdmin ? "YES" : "NO"}</b>
` +
      `Owner Approval: <b>${statusMap[g.approvalStatus] || "PENDING"}</b>`
  );
});

bot.command("approve", async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ ဒီ command ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");
  const treasury = await ensureTreasury();
  if (!isOwner(ctx, treasury)) return replyHTML(ctx, "⛔ <b>Owner only</b>");

  const g = await getGroupDoc(ctx.chat.id);
  if (!g) return replyHTML(ctx, "Group data not found.");
  if (!g.botIsAdmin) return replyHTML(ctx, `⚠️ <b>Bot ကို Admin မပေးရသေးပါ</b>
━━━━━━━━━━━━
အရင်ဆုံး bot ကို admin ပေးပါ။`);

  await approveGroupById(ctx.chat.id, ctx.from.id);
  return replyHTML(
    ctx,
    `✅ <b>Group Approved</b>
━━━━━━━━━━━━
` +
      `ဒီ group မှာ bot ကို အသုံးပြုလို့ရပါပြီ။`
  );
});

bot.command("reject", async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ ဒီ command ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");
  const treasury = await ensureTreasury();
  if (!isOwner(ctx, treasury)) return replyHTML(ctx, "⛔ <b>Owner only</b>");

  await rejectGroupById(ctx.chat.id, ctx.from.id);
  return replyHTML(
    ctx,
    `❌ <b>Group Rejected</b>
━━━━━━━━━━━━
` +
      `ဒီ group ကို approve မပေးထားပါ။`
  );
});

bot.command("admin", async (ctx) => renderAdminPanel(ctx));

async function askManualTarget(ctx) {
  setAdminSession(ctx.from.id, { mode: "await_target" });
  return replyHTML(
    ctx,
    `🔎 <b>Set Target User</b>\n━━━━━━━━━━━━\nSend one:\n• <code>@username</code>\n• <code>123456789</code> (userId)\nExample: <code>@Official_Bika</code>`,
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
    `${header}\n━━━━━━━━━━━━━\nTarget: <b>${escHtml(String(s.targetLabel))}</b>\nFlow: <i>${escHtml(hint)}</i>\n━━━━━━━━━━━━━━━━\nAmount ပို့ပါ (numbers only)\nExample: <code>5000</code>`,
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

// -------------------- Orders helpers --------------------
async function getRecentOrders(statuses, limit = 10) {
  return orders.find({ status: { $in: statuses } }).sort({ createdAt: -1 }).limit(limit).toArray();
}

function orderReceiptText(o) {
  const who = o.username ? `@${escHtml(o.username)}` : `<code>${o.userId}</code>`;
  return (
    `🧾 <b>Order Receipt</b>\n` +
    `━━━━━━━━━━━━\n` +
    `Order ID: <code>${escHtml(String(o._id))}</code>\n` +
    `Receipt: <code>${escHtml(o.receiptCode || "-")}</code>\n` +
    `Item: <b>${escHtml(o.itemName)}</b>\n` +
    `Price: <b>${fmt(o.price)}</b> ${COIN}\n` +
    `Status: <b>${escHtml(o.status)}</b>\n` +
    `User: ${who}\n` +
    `Time: <b>${escHtml(formatYangon(new Date(o.createdAt)))}</b> (Yangon)\n` +
    `━━━━━━━━━━━━`
  );
}

async function notifyUserOrderUpdate(o, noteLine = "") {
  try {
    const note = noteLine ? `\n${noteLine}\n` : "\n";
    const msg =
      `🧾 <b>Order Update</b>\n` +
      `━━━━━━━━━━━━\n` +
      `Order ID: <code>${escHtml(String(o._id))}</code>\n` +
      `Receipt: <code>${escHtml(o.receiptCode || "-")}</code>\n` +
      `Item: <b>${escHtml(o.itemName)}</b>\n` +
      `Price: <b>${fmt(o.price)}</b> ${COIN}\n` +
      `Status: <b>${escHtml(o.status)}</b>\n` +
      `${note}` +
      `Time: <b>${escHtml(formatYangon(new Date()))}</b> (Yangon)`;

    await safeTelegram(() =>
      bot.telegram.sendMessage(o.userId, msg, { parse_mode: "HTML", disable_web_page_preview: true })
    );
  } catch (e) {
    console.log("notify user failed:", e?.message || e);
  }
}
// -------------------- Dice PvP --------------------
const DICE = {
  minBet: 10,
  maxBet: 40000,
  timeoutMs: 60_000,
  maxActive: 4,
};

const activeDiceChallenges = new Map();

function makeDiceChallengeId(chatId, msgId) {
  return `${chatId}:${msgId}`;
}

function diceChallengeKeyboard(challengeId) {
  return {
    inline_keyboard: [
      [{ text: "✅ Accept Dice Duel", callback_data: `DICE:ACCEPT:${challengeId}` }],
      [{ text: "❌ Cancel", callback_data: `DICE:CANCEL:${challengeId}` }],
    ],
  };
}

function diceChallengeText(challenger, target, bet) {
  const challengerName = challenger?.username ? `@${challenger.username}` : challenger?.first_name || "Player";
  const targetName = target?.username ? `@${target.username}` : target?.first_name || "Player";

  return (
    `🎲 <b>Dice Duel Challenge</b>\n` +
    `━━━━━━━━━━━━\n` +
    `စိန်ခေါ်သူ: <b>${escHtml(challengerName)}</b>\n` +
    `လက်ခံသူ: <b>${escHtml(targetName)}</b>\n` +
    `Bet: <b>${fmt(bet)}</b> ${COIN}\n` +
    `Winner gets: <b>98%</b> (House cut: <b>2%</b>)\n` +
    `━━━━━━━━━━━━\n` +
    `Reply ထောက်ထားတဲ့သူပဲ Accept လုပ်နိုင်ပါတယ်။\n` +
    `⏳ Timeout: <b>${Math.floor(DICE.timeoutMs / 1000)}s</b>`
  );
}

async function sendDice(chatId, replyToMsgId) {
  return safeTelegram(() => bot.telegram.sendDice(chatId, { reply_to_message_id: replyToMsgId }));
}

bot.hears(/^\.(dice)\s+(\d+)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>.dice</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  const bet = parseInt(ctx.match[2], 10);
  if (!Number.isFinite(bet) || bet <= 0) return;

  if (bet < DICE.minBet || bet > DICE.maxBet) {
    return replyHTML(
      ctx,
      `🎲 <b>Dice Duel</b>\n━━━━━━━━━━━\nUsage: Reply + <code>.dice 200</code>\nMin: <b>${fmt(DICE.minBet)}</b> ${COIN}\nMax: <b>${fmt(DICE.maxBet)}</b> ${COIN}`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  const replyFrom = ctx.message?.reply_to_message?.from;
  if (!replyFrom?.id) {
    return replyHTML(
      ctx,
      `⚠️ <b>Reply လုပ်ပြီးသုံးပါ</b>\n━━━━━━━━━━━━\nExample: Reply + <code>.dice 200</code>`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  if (replyFrom.is_bot) {
    return replyHTML(ctx, "🤖 Bot ကို challenge မလုပ်နိုင်ပါ။", {
      reply_to_message_id: ctx.message?.message_id,
    });
  }

  if (replyFrom.id === ctx.from.id) {
    return replyHTML(ctx, "😅 ကိုယ့်ကိုကိုယ် challenge မလုပ်နိုင်ပါ။", {
      reply_to_message_id: ctx.message?.message_id,
    });
  }

  if (activeDiceChallenges.size >= DICE.maxActive) {
    return replyHTML(
      ctx,
      `⛔ <b>Dice Busy</b>\n━━━━━━━━━━━\nအခု Dice challenge များလွန်းနေပါတယ်။ ခဏနားပြီး ပြန်ကြိုးစားပါ။`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  await ensureUser(ctx.from);
  await ensureUser(replyFrom);

  const challengerUser = await getUser(ctx.from.id);
  if (toNum(challengerUser?.balance) < bet) {
    const lack = Math.max(0, bet - toNum(challengerUser?.balance));
    return replyHTML(
      ctx,
      `❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>\n━━━━━━━━━━━\nBet: <b>${fmt(bet)}</b> ${COIN}\nYour Balance: <b>${fmt(challengerUser?.balance)}</b> ${COIN}\nNeed More: <b>${fmt(lack)}</b> ${COIN}`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  const sent = await replyHTML(ctx, diceChallengeText(ctx.from, replyFrom, bet), {
    reply_markup: { inline_keyboard: [[{ text: "✅ Accept Dice Duel", callback_data: "DICE:TEMP" }]] },
    reply_to_message_id: ctx.message?.message_id,
  });

  if (!sent?.message_id) return;

  const challengeId = makeDiceChallengeId(ctx.chat.id, sent.message_id);

  await safeTelegram(() =>
    ctx.telegram.editMessageReplyMarkup(
      ctx.chat.id,
      sent.message_id,
      undefined,
      diceChallengeKeyboard(challengeId)
    )
  );

  activeDiceChallenges.set(challengeId, {
    challengeId,
    chatId: ctx.chat.id,
    msgId: sent.message_id,
    bet,
    challengerId: ctx.from.id,
    challengerName: ctx.from.first_name || ctx.from.username || "Player",
    challengerUsername: ctx.from.username || null,
    targetUserId: replyFrom.id,
    targetName: replyFrom.first_name || replyFrom.username || "Player",
    targetUsername: replyFrom.username || null,
    createdAt: Date.now(),
    status: "OPEN",
    timeoutHandle: setTimeout(async () => {
      const c = activeDiceChallenges.get(challengeId);
      if (!c || c.status !== "OPEN") return;
      c.status = "EXPIRED";
      activeDiceChallenges.set(challengeId, c);
      try {
        await safeEditByIds(
          c.chatId,
          c.msgId,
          `⏳ <b>Dice Duel Expired</b>
━━━━━━━━━━━━
စိန်ခေါ်မှု အချိန်ကုန်သွားပါတယ်။
Bet: <b>${fmt(c.bet)}</b> ${COIN}`
        );
      } catch (_) {}
      activeDiceChallenges.delete(challengeId);
    }, DICE.timeoutMs),
  });
});

// -------------------- Shan Koe Mee PvP --------------------
const SHAN = {
  minBet: 10,
  maxBet: 40000,
  timeoutMs: 60_000,
  maxActive: 4,
};

const activeShanChallenges = new Map();

function makeShanChallengeId(chatId, msgId) {
  return `${chatId}:${msgId}`;
}

function shanKeyboard(challengeId) {
  return {
    inline_keyboard: [
      [{ text: "✅ Accept Shan Duel", callback_data: `SHAN:ACCEPT:${challengeId}` }],
      [{ text: "❌ Cancel", callback_data: `SHAN:CANCEL:${challengeId}` }],
    ],
  };
}

const SUITS = ["♠", "♥", "♦", "♣"];
const RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"];

function buildDeck() {
  const deck = [];
  for (const suit of SUITS) {
    for (const rank of RANKS) deck.push({ rank, suit });
  }
  return deck;
}

function shuffle(deck) {
  const arr = [...deck];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function drawCards(deck, n) {
  return deck.splice(0, n);
}

function rankValue(rank) {
  if (rank === "A") return 1;
  if (["10", "J", "Q", "K"].includes(rank)) return 0;
  return Number(rank) || 0;
}

function calcPoints(cards) {
  return cards.reduce((sum, c) => sum + rankValue(c.rank), 0) % 10;
}

function isShanKoeMee(cards) {
  return cards.length === 3 && cards.every((c) => c.rank === cards[0].rank);
}

function isZatToe(cards) {
  return cards.length === 3 && cards.every((c) => ["J", "Q", "K"].includes(c.rank));
}

function isSuitTriple(cards) {
  return cards.length === 3 && cards.every((c) => c.suit === cards[0].suit);
}

function highCardWeight(rank) {
  if (rank === "A") return 1;
  if (rank === "J") return 11;
  if (rank === "Q") return 12;
  if (rank === "K") return 13;
  return Number(rank) || 0;
}

function sortedHighRanks(cards) {
  return [...cards].map((c) => highCardWeight(c.rank)).sort((a, b) => b - a);
}

function handInfo(cards) {
  if (isShanKoeMee(cards)) return { category: 4, name: "Shan Koe Mee", points: calcPoints(cards), tieBreaker: sortedHighRanks(cards) };
  if (isZatToe(cards)) return { category: 3, name: "Zat Toe", points: calcPoints(cards), tieBreaker: sortedHighRanks(cards) };
  if (isSuitTriple(cards)) return { category: 2, name: "Suit Triple", points: calcPoints(cards), tieBreaker: sortedHighRanks(cards) };
  return { category: 1, name: `Point ${calcPoints(cards)}`, points: calcPoints(cards), tieBreaker: sortedHighRanks(cards) };
}

function compareTieBreaker(a, b) {
  const len = Math.max(a.length, b.length);
  for (let i = 0; i < len; i++) {
    const av = a[i] || 0;
    const bv = b[i] || 0;
    if (av > bv) return 1;
    if (bv > av) return -1;
  }
  return 0;
}

function compareHands(cardsA, cardsB) {
  const A = handInfo(cardsA);
  const B = handInfo(cardsB);

  if (A.category > B.category) return { winner: "A", infoA: A, infoB: B };
  if (B.category > A.category) return { winner: "B", infoA: A, infoB: B };
  if (A.points > B.points) return { winner: "A", infoA: A, infoB: B };
  if (B.points > A.points) return { winner: "B", infoA: A, infoB: B };
  const tb = compareTieBreaker(A.tieBreaker, B.tieBreaker);
  if (tb > 0) return { winner: "A", infoA: A, infoB: B };
  if (tb < 0) return { winner: "B", infoA: A, infoB: B };
  return { winner: "TIE", infoA: A, infoB: B };
}

function cardBox(card) {
  const rank = String(card.rank);
  const suit = String(card.suit);
  const left = rank.padEnd(2, " ");
  const right = rank.padStart(2, " ");
  return [
    "┌───────┐",
    `│ ${left}    │`,
    `│   ${suit}  │`,
    `│    ${right}│`,
    "└───────┘",
  ];
}

function renderCardsRow(cards) {
  const boxes = cards.map(cardBox);
  const lines = [];
  for (let i = 0; i < 5; i++) lines.push(boxes.map((b) => b[i]).join(" "));
  return lines.join("\n");
}

function shanChallengeText(challenger, target, bet) {
  const challengerName = challenger?.username ? `@${challenger.username}` : challenger?.first_name || "Player";
  const targetName = target?.username ? `@${target.username}` : target?.first_name || "Player";

  return (
    `🃏 <b>Shan Koe Mee Challenge</b>\n` +
    `━━━━━━━━━━━━\n` +
    `စိန်ခေါ်သူ: <b>${escHtml(challengerName)}</b>\n` +
    `လက်ခံသူ: <b>${escHtml(targetName)}</b>\n` +
    `Bet: <b>${fmt(bet)}</b> ${COIN}\n` +
    `Winner gets: <b>98%</b> (normal)\n` +
    `Suit Triple: <b>pot + extra one bet</b>\n` +
    `━━━━━━━━━━━━\n` +
    `Reply ထောက်ထားတဲ့သူပဲ Accept လုပ်နိုင်ပါတယ်။\n` +
    `⏳ Timeout: <b>${Math.floor(SHAN.timeoutMs / 1000)}s</b>`
  );
}

bot.hears(/^\.(shan)\s+(\d+)\s*$/i, async (ctx) => {
  if (!isGroupChat(ctx)) return replyHTML(ctx, "ℹ️ <code>.shan</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");

  const bet = parseInt(ctx.match[2], 10);
  if (!Number.isFinite(bet) || bet <= 0) return;

  if (bet < SHAN.minBet || bet > SHAN.maxBet) {
    return replyHTML(
      ctx,
      `🃏 <b>Shan Duel</b>\n━━━━━━━━━━━\nUsage: Reply + <code>.shan 500</code>\nMin: <b>${fmt(SHAN.minBet)}</b> ${COIN}\nMax: <b>${fmt(SHAN.maxBet)}</b> ${COIN}`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  const replyFrom = ctx.message?.reply_to_message?.from;
  if (!replyFrom?.id) {
    return replyHTML(
      ctx,
      `⚠️ <b>Reply လုပ်ပြီးသုံးပါ</b>\n━━━━━━━━━━━━\nExample: Reply + <code>.shan 500</code>`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  if (replyFrom.is_bot) return replyHTML(ctx, "🤖 Bot ကို challenge မလုပ်နိုင်ပါ။", { reply_to_message_id: ctx.message?.message_id });
  if (replyFrom.id === ctx.from.id) return replyHTML(ctx, "😅 ကိုယ့်ကိုကိုယ် challenge မလုပ်နိုင်ပါ။", { reply_to_message_id: ctx.message?.message_id });

  if (activeShanChallenges.size >= SHAN.maxActive) {
    return replyHTML(
      ctx,
      `⛔ <b>Shan Busy</b>\n━━━━━━━━━━━\nအခု Shan challenge များနေပါတယ်။ ခဏနားပြီး ပြန်ကြိုးစားပါ။`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  await ensureUser(ctx.from);
  await ensureUser(replyFrom);

  const challengerUser = await getUser(ctx.from.id);
  if (toNum(challengerUser?.balance) < bet) {
    const lack = Math.max(0, bet - toNum(challengerUser?.balance));
    return replyHTML(
      ctx,
      `❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>\n━━━━━━━━━━━\nBet: <b>${fmt(bet)}</b> ${COIN}\nYour Balance: <b>${fmt(challengerUser?.balance)}</b> ${COIN}\nNeed More: <b>${fmt(lack)}</b> ${COIN}`,
      { reply_to_message_id: ctx.message?.message_id }
    );
  }

  const sent = await replyHTML(ctx, shanChallengeText(ctx.from, replyFrom, bet), {
    reply_markup: { inline_keyboard: [[{ text: "✅ Accept Shan Duel", callback_data: "SHAN:TEMP" }]] },
    reply_to_message_id: ctx.message?.message_id,
  });

  if (!sent?.message_id) return;

  const challengeId = makeShanChallengeId(ctx.chat.id, sent.message_id);
  await safeTelegram(() => ctx.telegram.editMessageReplyMarkup(ctx.chat.id, sent.message_id, undefined, shanKeyboard(challengeId)));

  activeShanChallenges.set(challengeId, {
    challengeId,
    chatId: ctx.chat.id,
    msgId: sent.message_id,
    bet,
    challengerId: ctx.from.id,
    challengerName: ctx.from.first_name || ctx.from.username || "Player",
    challengerUsername: ctx.from.username || null,
    targetUserId: replyFrom.id,
    targetName: replyFrom.first_name || replyFrom.username || "Player",
    targetUsername: replyFrom.username || null,
    createdAt: Date.now(),
    status: "OPEN",
    timeoutHandle: setTimeout(async () => {
      const c = activeShanChallenges.get(challengeId);
      if (!c || c.status !== "OPEN") return;
      c.status = "EXPIRED";
      activeShanChallenges.set(challengeId, c);
      try {
        await safeEditByIds(
          c.chatId,
          c.msgId,
          `⏳ <b>Shan Duel Expired</b>
━━━━━━━━━━━━
စိန်ခေါ်မှု အချိန်ကုန်သွားပါတယ်။
Bet: <b>${fmt(c.bet)}</b> ${COIN}`
        );
      } catch (_) {}
      activeShanChallenges.delete(challengeId);
    }, SHAN.timeoutMs),
  });
});
// -------------------- Callback Query --------------------
bot.on("callback_query", async (ctx) => {
  const data = ctx.callbackQuery?.data || "";

  // ---- SHOP ----
  if (data === "SHOP:REFRESH") {
    const u = await ensureUser(ctx.from);
    await safeTelegram(() => ctx.answerCbQuery("Refreshed"));
    return ctx.editMessageText(shopText(u.balance), { parse_mode: "HTML", reply_markup: shopKeyboard(), disable_web_page_preview: true });
  }

  if (data.startsWith("BUY:")) {
    const itemId = data.split(":")[1];
    const item = SHOP_ITEMS.find((x) => x.id === itemId);
    if (!item) return ctx.answerCbQuery("Item not found", { show_alert: true });

    await ensureUser(ctx.from);
    await ensureTreasury();

    const receiptCode = genReceiptCode();
    try {
      await userPayToTreasury(ctx.from.id, item.price, { type: "shop_buy", itemId: item.id, itemName: item.name });

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
      await ctx.answerCbQuery("✅ Order created!");

      return replyHTML(
        ctx,
        `✅ <b>Order Created</b>\n━━━━━━━━━━━━━\n` +
          `Order ID: <code>${escHtml(String(orderId))}</code>\n` +
          `Receipt: <code>${escHtml(receiptCode)}</code>\n` +
          `Item: <b>${escHtml(item.name)}</b>\n` +
          `Paid: <b>${fmt(item.price)}</b> ${COIN}\n` +
          `Your Balance: <b>${fmt(u?.balance)}</b> ${COIN}\n` +
          `Status: <b>${ORDER_STATUS.PENDING}</b>\n` +
          `━━━━━━━━━━━━━\n` +
          `📌 Admin က confirm / deliver လုပ်ပြီးရင် DM နဲ့ အကြောင်းကြားပေးပါမယ်။`
      );
    } catch (e) {
      if (String(e?.message || e).includes("USER_INSUFFICIENT")) {
        const u = await getUser(ctx.from.id);
        const bal = toNum(u?.balance);
        const need = toNum(item.price);
        const lack = Math.max(0, need - bal);

        await ctx.answerCbQuery(`❌ မလုံလောက်ပါ (${fmt(lack)} ${COIN} လိုနေပါသေးတယ်)`, { show_alert: true });

        return replyHTML(
          ctx,
          `❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>\n` +
            `━━━━━━━━━━━\n` +
            `Item: <b>${escHtml(item.name)}</b>\n` +
            `Price: <b>${fmt(need)}</b> ${COIN}\n` +
            `Your Balance: <b>${fmt(bal)}</b> ${COIN}\n` +
            `Need More: <b>${fmt(lack)}</b> ${COIN}\n` +
            `━━━━━━━━━━━━\n` +
            `💡 slot ဆော့ရင်း ပိုက်ဆံဆုဆောင်းပြီးမှ ပြန်လာပါ။\n` +
            `• Daily claim: <code>/dailyclaim</code>\n` +
            `• Wallet: <code>.mybalance</code>\n` +
            `• Shop: <code>/shop</code>`
        );
      }

      console.error("BUY error:", e);
      return ctx.answerCbQuery("Error", { show_alert: true });
    }
  }

  // ---- ADMIN ----
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
    if (data === "ADMIN:GROUPS") {
      await ctx.answerCbQuery("Groups");
      return renderAdminGroups(ctx);
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
    if (data === "ADMIN:ORDERS") {
      await ctx.answerCbQuery("Orders");
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
        `🧾 <b>Orders (PENDING / PAID)</b>\n━━━━━━━━━━━\n` +
        `${lines}\n` +
        `━━━━━━━━━━━\n` +
        `Tap an order below to manage:`;

      return ctx.editMessageText(panel, {
        parse_mode: "HTML",
        disable_web_page_preview: true,
        reply_markup: adminOrdersKeyboard(list),
      });
    }

    await ctx.answerCbQuery("OK");
    return;
  }

  // ---- ORDER ACTIONS ----
  if (data.startsWith("ORDER:")) {
    const t = await ensureTreasury();
    if (!isOwner(ctx, t)) {
      await ctx.answerCbQuery("Owner only", { show_alert: true });
      return;
    }

    const parts = data.split(":");
    const action = parts[1];
    const id = parts[2];

    if (action === "OPEN") {
      await ctx.answerCbQuery("Open");
      let oid = null;
      try { oid = new ObjectId(id); } catch (_) {
        return ctx.editMessageText("Invalid Order ID", { reply_markup: adminKeyboard() });
      }
      const o = await orders.findOne({ _id: oid });
      if (!o) return ctx.editMessageText("Order not found", { reply_markup: adminKeyboard() });
      const text = orderReceiptText(o) + "\nSelect action:";
      return ctx.editMessageText(text, {
        parse_mode: "HTML",
        disable_web_page_preview: true,
        reply_markup: orderActionKeyboard(String(o._id)),
      });
    }

    if (action === "PAID" || action === "DELIVERED" || action === "CANCEL") {
      await ctx.answerCbQuery("Working...");
      let oid = null;
      try { oid = new ObjectId(id); } catch (_) {
        return ctx.answerCbQuery("Invalid ID", { show_alert: true });
      }

      const o = await orders.findOne({ _id: oid });
      if (!o) return ctx.answerCbQuery("Order not found", { show_alert: true });

      if (action === "PAID") {
        if (o.status === ORDER_STATUS.CANCELLED || o.status === ORDER_STATUS.DELIVERED) return ctx.answerCbQuery("Already closed", { show_alert: true });
        const now = new Date();
        await orders.updateOne({ _id: oid }, { $set: { status: ORDER_STATUS.PAID, updatedAt: now }, $push: { history: { status: ORDER_STATUS.PAID, at: now, by: ctx.from.id } } });
        const updated = await orders.findOne({ _id: oid });
        await notifyUserOrderUpdate(updated, "✅ Owner က order ကို <b>PAID</b> လို့ confirm လုပ်ပြီးပါပြီ။ @BikaMlbbDiamondShopChat ဒီ  group မှာ သင့် id+svid လာပို့ပေးပါ။");
        return ctx.editMessageText(orderReceiptText(updated) + "\n✅ Updated.", {
          parse_mode: "HTML",
          disable_web_page_preview: true,
          reply_markup: orderActionKeyboard(String(updated._id)),
        });
      }

      if (action === "DELIVERED") {
        if (o.status === ORDER_STATUS.CANCELLED) return ctx.answerCbQuery("Cancelled order", { show_alert: true });
        const now = new Date();
        await orders.updateOne({ _id: oid }, { $set: { status: ORDER_STATUS.DELIVERED, updatedAt: now }, $push: { history: { status: ORDER_STATUS.DELIVERED, at: now, by: ctx.from.id } } });
        const updated = await orders.findOne({ _id: oid });
        await notifyUserOrderUpdate(updated, "📦 သင့် Order ကို <b>DELIVERED</b> လုပ်ပြီးပါပြီ။ဂိမ်းထဲဝင်စစ်ကြည့်ပါအုံး ကျေးဇူးတင်ပါတယ်ဗျ။");
        return ctx.editMessageText(orderReceiptText(updated) + "\n📦 Delivered.", {
          parse_mode: "HTML",
          disable_web_page_preview: true,
          reply_markup: { inline_keyboard: [[{ text: "⬅️ Back to Admin", callback_data: "ADMIN:REFRESH" }]] },
        });
      }

      if (action === "CANCEL") {
        if (o.status === ORDER_STATUS.CANCELLED) return ctx.answerCbQuery("Already cancelled", { show_alert: true });
        if (o.status === ORDER_STATUS.DELIVERED) return ctx.answerCbQuery("Already delivered", { show_alert: true });

        try {
          await treasuryPayToUser(o.userId, o.price, { type: "order_refund", orderId: String(o._id), itemId: o.itemId });
        } catch (e) {
          if (String(e?.message || e).includes("TREASURY_INSUFFICIENT")) return ctx.answerCbQuery("Treasury insufficient for refund", { show_alert: true });
          console.error("refund error:", e);
          return ctx.answerCbQuery("Refund error", { show_alert: true });
        }

        const now = new Date();
        await orders.updateOne({ _id: oid }, { $set: { status: ORDER_STATUS.CANCELLED, updatedAt: now }, $push: { history: { status: ORDER_STATUS.CANCELLED, at: now, by: ctx.from.id } } });

        const updated = await orders.findOne({ _id: oid });
        await notifyUserOrderUpdate(updated, `❌ Admin က order ကို <b>CANCELLED</b> လုပ်ပြီး refund <b>${fmt(updated.price)}</b> ${COIN} ပြန်ပေးပြီးပါပြီ။`);

        return ctx.editMessageText(orderReceiptText(updated) + "\n❌ Cancelled + Refunded.", {
          parse_mode: "HTML",
          disable_web_page_preview: true,
          reply_markup: { inline_keyboard: [[{ text: "⬅️ Back to Admin", callback_data: "ADMIN:REFRESH" }]] },
        });
      }
    }

    await ctx.answerCbQuery("OK");
    return;
  }

  // ---- DICE ----
  if (data.startsWith("DICE:")) {
    const parts = data.split(":");
    const action = parts[1];
    const challengeId = parts.slice(2).join(":");

    const c = activeDiceChallenges.get(challengeId);
    if (!c) {
      await ctx.answerCbQuery("Challenge not found / expired", { show_alert: true });
      return;
    }

    if (action === "CANCEL") {
      if (ctx.from.id !== c.challengerId && ctx.from.id !== OWNER_ID) {
        await ctx.answerCbQuery("Only challenger can cancel", { show_alert: true });
        return;
      }
      c.status = "CANCELLED";
      clearTimeout(c.timeoutHandle);
      activeDiceChallenges.delete(challengeId);
      await ctx.answerCbQuery("Cancelled");
      return safeEditCurrent(
        ctx,
        `❌ <b>Dice Duel Cancelled</b>
━━━━━━━━━━━━
စိန်ခေါ်မှုကို ဖျက်လိုက်ပါတယ်။
Bet: <b>${fmt(c.bet)}</b> ${COIN}`,
        { parse_mode: "HTML", disable_web_page_preview: true }
      );
    }

    if (action === "ACCEPT") {
      const locked = await withCallbackLock(`dice:${challengeId}`, async () => {
        if (c.status !== "OPEN") {
          await ctx.answerCbQuery("Already closed", { show_alert: true });
          return;
        }

        if (ctx.from.id !== c.targetUserId) {
          await ctx.answerCbQuery("ဒီ duel ကို reply ထောက်ထားတဲ့သူပဲ Accept လုပ်နိုင်ပါတယ်", { show_alert: true });
          return;
        }

        c.status = "LOCKED";
        activeDiceChallenges.set(challengeId, c);
        await ctx.answerCbQuery("Processing...");
        await ensureUser(ctx.from);

        const challenger = await getUser(c.challengerId);
        const opponent = await getUser(ctx.from.id);

        if (toNum(challenger?.balance) < c.bet) {
          c.status = "FAILED";
          clearTimeout(c.timeoutHandle);
          activeDiceChallenges.delete(challengeId);
          return safeEditCurrent(ctx, `⚠️ <b>Challenge Failed</b>
━━━━━━━━━━━━
စိန်ခေါ်သူ balance မလုံလောက်ပါ။`);
        }

        if (toNum(opponent?.balance) < c.bet) {
          c.status = "OPEN";
          activeDiceChallenges.set(challengeId, c);
          const lack = Math.max(0, c.bet - toNum(opponent?.balance));
          await replyHTML(
            ctx,
            `❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>
━━━━━━━━━━━━
Bet: <b>${fmt(c.bet)}</b> ${COIN}
Your Balance: <b>${fmt(opponent?.balance)}</b> ${COIN}
Need More: <b>${fmt(lack)}</b> ${COIN}`,
            { reply_to_message_id: c.msgId }
          );
          return;
        }

        c.status = "PLAYING";
        c.opponentId = ctx.from.id;
        clearTimeout(c.timeoutHandle);
        activeDiceChallenges.set(challengeId, c);

        try {
          await ensureTreasury();
          await userPayToTreasury(c.challengerId, c.bet, { type: "dice_bet", challengeId });
          await userPayToTreasury(c.opponentId, c.bet, { type: "dice_bet", challengeId });
        } catch (e) {
          console.error("dice bet take error:", e);
          c.status = "FAILED";
          activeDiceChallenges.delete(challengeId);
          return safeEditCurrent(ctx, `⚠️ <b>Error</b>
━━━━━━━━━━━━
Bet process error.`);
        }

        const pot = c.bet * 2;
        const payout = Math.floor(pot * (1 - HOUSE_CUT_PERCENT));
        const houseCut = pot - payout;

        const challengerLabel = c.challengerUsername ? `@${c.challengerUsername}` : c.challengerName;
        const opponentLabel = c.targetUsername ? `@${c.targetUsername}` : c.targetName;

        await safeEditCurrent(
          ctx,
          `🎲 <b>Dice Duel Started!</b>
━━━━━━━━━━━━
` +
            `စိန်ခေါ်သူ: <b>${escHtml(challengerLabel)}</b>
` +
            `လက်ခံသူ: <b>${escHtml(opponentLabel)}</b>
` +
            `Bet: <b>${fmt(c.bet)}</b> ${COIN}
` +
            `Pot: <b>${fmt(pot)}</b> ${COIN}
` +
            `House cut: <b>2%</b> (${fmt(houseCut)} ${COIN})
` +
            `━━━━━━━━━━━━
Rolling dice...`,
          { parse_mode: "HTML", disable_web_page_preview: true }
        );

        try {
          await sendDice(c.chatId, c.msgId);
          await sleep(900);
          await sendDice(c.chatId, c.msgId);
        } catch (e) {
          console.error("sendDice error:", e);
        }

        const challengerLatest = await getUser(c.challengerId);
        const opponentLatest = await getUser(c.opponentId);
        const { d1, d2 } = decideDiceDuelValues(challengerLatest, opponentLatest);

        let winnerId = null;
        let winnerLabel = "";

        if (d1 > d2) {
          winnerId = c.challengerId;
          winnerLabel = challengerLabel;
        } else if (d2 > d1) {
          winnerId = c.opponentId;
          winnerLabel = opponentLabel;
        } else {
          try {
            await treasuryPayToUser(c.challengerId, c.bet, { type: "dice_refund", challengeId, reason: "tie" });
            await treasuryPayToUser(c.opponentId, c.bet, { type: "dice_refund", challengeId, reason: "tie" });
          } catch (_) {}
          c.status = "DONE";
          activeDiceChallenges.delete(challengeId);

          return safeEditCurrent(
            ctx,
            `🎲 <b>Dice Duel Result</b>
━━━━━━━━━━━━
` +
              `စိန်ခေါ်သူ: <b>${escHtml(challengerLabel)}</b> → <b>${d1}</b>
` +
              `လက်ခံသူ: <b>${escHtml(opponentLabel)}</b> → <b>${d2}</b>
` +
              `━━━━━━━━━━━━
` +
              `🤝 <b>TIE!</b> — Bet refund ပြန်ပေးပြီးပါပြီ။`,
            { parse_mode: "HTML", disable_web_page_preview: true }
          );
        }

        try {
          await treasuryPayToUser(winnerId, payout, { type: "dice_win", challengeId, pot, payout, houseCut });
        } catch (e) {
          console.error("dice payout error:", e);
          try {
            await treasuryPayToUser(c.challengerId, c.bet, { type: "dice_refund", challengeId, reason: "payout_fail" });
            await treasuryPayToUser(c.opponentId, c.bet, { type: "dice_refund", challengeId, reason: "payout_fail" });
          } catch (_) {}
          c.status = "DONE";
          activeDiceChallenges.delete(challengeId);
          return safeEditCurrent(ctx, `⚠️ <b>Dice Duel Error</b>
━━━━━━━━━━━━━━━━
Payout error ဖြစ်လို့ refund ပြန်ပေးလိုက်ပါတယ်။`, { parse_mode: "HTML", disable_web_page_preview: true });
        }

        c.status = "DONE";
        activeDiceChallenges.delete(challengeId);

        return safeEditCurrent(
          ctx,
          `🎲 <b>Dice Duel Result</b>
━━━━━━━━━━━━━━━━
` +
            `စိန်ခေါ်သူ: <b>${escHtml(challengerLabel)}</b> → <b>${d1}</b>
` +
            `လက်ခံသူ: <b>${escHtml(opponentLabel)}</b> → <b>${d2}</b>
` +
            `━━━━━━━━━━━━━━━━
` +
            `🏆 Winner: <b>${escHtml(winnerLabel)}</b>
` +
            `💰 Pot: <b>${fmt(pot)}</b> ${COIN}
` +
            `✅ Winner gets: <b>${fmt(payout)}</b> ${COIN} (98%)
` +
            `🏦 House cut: <b>2%</b> (${fmt(houseCut)} ${COIN})`,
          { parse_mode: "HTML", disable_web_page_preview: true }
        );
      });

      if (!locked) {
        await ctx.answerCbQuery("Processing...");
      }
      return;
    }

    await ctx.answerCbQuery("OK");
    return;
  }

  // ---- SHAN ----
  if (data.startsWith("SHAN:")) {
    const parts = data.split(":");
    const action = parts[1];
    const challengeId = parts.slice(2).join(":");

    const c = activeShanChallenges.get(challengeId);
    if (!c) {
      await ctx.answerCbQuery("Challenge not found / expired", { show_alert: true });
      return;
    }

    if (action === "CANCEL") {
      if (ctx.from.id !== c.challengerId && ctx.from.id !== OWNER_ID) {
        await ctx.answerCbQuery("Only challenger can cancel", { show_alert: true });
        return;
      }
      c.status = "CANCELLED";
      clearTimeout(c.timeoutHandle);
      activeShanChallenges.delete(challengeId);
      await ctx.answerCbQuery("Cancelled");
      return safeEditCurrent(
        ctx,
        `❌ <b>Shan Duel Cancelled</b>
━━━━━━━━━━━━
စိန်ခေါ်မှုကို ဖျက်လိုက်ပါတယ်။
Bet: <b>${fmt(c.bet)}</b> ${COIN}`,
        { parse_mode: "HTML", disable_web_page_preview: true }
      );
    }

    if (action === "ACCEPT") {
      const locked = await withCallbackLock(`shan:${challengeId}`, async () => {
        if (c.status !== "OPEN") {
          await ctx.answerCbQuery("Already closed", { show_alert: true });
          return;
        }

        if (ctx.from.id !== c.targetUserId) {
          await ctx.answerCbQuery("ဒီ duel ကို reply ထောက်ထားတဲ့သူပဲ Accept လုပ်နိုင်ပါတယ်", { show_alert: true });
          return;
        }

        c.status = "LOCKED";
        activeShanChallenges.set(challengeId, c);
        await ctx.answerCbQuery("Processing...");
        await ensureUser(ctx.from);

        const challenger = await getUser(c.challengerId);
        const opponent = await getUser(ctx.from.id);

        if (toNum(challenger?.balance) < c.bet) {
          c.status = "FAILED";
          clearTimeout(c.timeoutHandle);
          activeShanChallenges.delete(challengeId);
          return safeEditCurrent(ctx, `⚠️ <b>Challenge Failed</b>
━━━━━━━━━━━━
စိန်ခေါ်သူ balance မလုံလောက်ပါ။`);
        }

        if (toNum(opponent?.balance) < c.bet) {
          c.status = "OPEN";
          activeShanChallenges.set(challengeId, c);
          const lack = Math.max(0, c.bet - toNum(opponent?.balance));
          await replyHTML(
            ctx,
            `❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>
━━━━━━━━━━━━
Bet: <b>${fmt(c.bet)}</b> ${COIN}
Your Balance: <b>${fmt(opponent?.balance)}</b> ${COIN}
Need More: <b>${fmt(lack)}</b> ${COIN}`,
            { reply_to_message_id: c.msgId }
          );
          return;
        }

        c.status = "PLAYING";
        c.opponentId = ctx.from.id;
        clearTimeout(c.timeoutHandle);
        activeShanChallenges.set(challengeId, c);

        try {
          await ensureTreasury();
          await userPayToTreasury(c.challengerId, c.bet, { type: "shan_bet", challengeId });
          await userPayToTreasury(c.opponentId, c.bet, { type: "shan_bet", challengeId });
        } catch (e) {
          console.error("shan bet take error:", e);
          c.status = "FAILED";
          activeShanChallenges.delete(challengeId);
          return safeEditCurrent(ctx, `⚠️ <b>Error</b>
━━━━━━━━━━━━
Bet process error.`);
        }

        const challengerLatest = await getUser(c.challengerId);
        const opponentLatest = await getUser(c.opponentId);
        const shanRound = drawShanHandsForUsers(challengerLatest, opponentLatest);
        const cardsA = shanRound.cardsA;
        const cardsB = shanRound.cardsB;
        await sleep(700);

        const result = shanRound.result;
        const infoA = result.infoA;
        const infoB = result.infoB;

        const challengerLabel = c.challengerUsername ? `@${c.challengerUsername}` : c.challengerName;
        const opponentLabel = c.targetUsername ? `@${c.targetUsername}` : c.targetName;

        const pot = c.bet * 2;
        const normalPayout = Math.floor(pot * (1 - HOUSE_CUT_PERCENT));

        if (result.winner === "TIE") {
          try {
            await treasuryPayToUser(c.challengerId, c.bet, { type: "shan_refund", challengeId, reason: "tie" });
            await treasuryPayToUser(c.opponentId, c.bet, { type: "shan_refund", challengeId, reason: "tie" });
          } catch (_) {}

          c.status = "DONE";
          activeShanChallenges.delete(challengeId);

          return safeEditCurrent(
            ctx,
            `🃏 <b>Shan Duel Result</b>
━━━━━━━━━━━━
` +
              `စိန်ခေါ်သူ: <b>${escHtml(challengerLabel)}</b>
` +
              `<pre>${escHtml(renderCardsRow(cardsA))}</pre>
` +
              `Hand: <b>${escHtml(infoA.name)}</b>
` +
              `Point: <b>${infoA.points}</b>

` +
              `လက်ခံသူ: <b>${escHtml(opponentLabel)}</b>
` +
              `<pre>${escHtml(renderCardsRow(cardsB))}</pre>
` +
              `Hand: <b>${escHtml(infoB.name)}</b>
` +
              `Point: <b>${infoB.points}</b>
` +
              `━━━━━━━━━━━━
` +
              `🤝 <b>TIE!</b> — Bet refund ပြန်ပေးပြီးပါပြီ။`,
            { parse_mode: "HTML", disable_web_page_preview: true }
          );
        }

        let winnerId = null;
        let winnerLabel = "";
        let winnerInfo = null;
        let loserId = null;
        let payout = normalPayout;
        let extraPenalty = 0;

        if (result.winner === "A") {
          winnerId = c.challengerId;
          winnerLabel = challengerLabel;
          winnerInfo = infoA;
          loserId = c.opponentId;
        } else {
          winnerId = c.opponentId;
          winnerLabel = opponentLabel;
          winnerInfo = infoB;
          loserId = c.challengerId;
        }

        if (winnerInfo.name === "Suit Triple") {
          extraPenalty = c.bet;
          try {
            await userPayToTreasury(loserId, extraPenalty, { type: "shan_suit_triple_extra", challengeId });
          } catch (_) {
            extraPenalty = 0;
          }
          payout = pot + extraPenalty;
        }

        try {
          await treasuryPayToUser(winnerId, payout, { type: "shan_win", challengeId, pot, payout, extraPenalty, hand: winnerInfo.name });
        } catch (e) {
          console.error("shan payout error:", e);
          try {
            await treasuryPayToUser(c.challengerId, c.bet, { type: "shan_refund", challengeId, reason: "payout_fail" });
            await treasuryPayToUser(c.opponentId, c.bet, { type: "shan_refund", challengeId, reason: "payout_fail" });
          } catch (_) {}
          c.status = "DONE";
          activeShanChallenges.delete(challengeId);
          return safeEditCurrent(ctx, `⚠️ <b>Shan Duel Error</b>
━━━━━━━━━━━━━━━━
Payout error ဖြစ်လို့ refund ပြန်ပေးလိုက်ပါတယ်။`, { parse_mode: "HTML", disable_web_page_preview: true });
        }

        c.status = "DONE";
        activeShanChallenges.delete(challengeId);

        return safeEditCurrent(
          ctx,
          `🃏 <b>Shan Duel Result</b>
━━━━━━━━━━━━
` +
            `စိန်ခေါ်သူ: <b>${escHtml(challengerLabel)}</b>
` +
            `<pre>${escHtml(renderCardsRow(cardsA))}</pre>
` +
            `Hand: <b>${escHtml(infoA.name)}</b>
` +
            `Point: <b>${infoA.points}</b>

` +
            `လက်ခံသူ: <b>${escHtml(opponentLabel)}</b>
` +
            `<pre>${escHtml(renderCardsRow(cardsB))}</pre>
` +
            `Hand: <b>${escHtml(infoB.name)}</b>
` +
            `Point: <b>${infoB.points}</b>
` +
            `━━━━━━━━━━━━
` +
            `🏆 Winner: <b>${escHtml(winnerLabel)}</b>
` +
            `Winning Hand: <b>${escHtml(winnerInfo.name)}</b>
` +
            `💰 Pot: <b>${fmt(pot)}</b> ${COIN}
` +
            `${winnerInfo.name === "Suit Triple" ? `🔥 Extra Bet: <b>${fmt(extraPenalty)}</b> ${COIN}
` : `🏦 House cut: <b>2%</b>
`}` +
            `✅ Winner gets: <b>${fmt(payout)}</b> ${COIN}`,
          { parse_mode: "HTML", disable_web_page_preview: true }
        );
      });

      if (!locked) {
        await ctx.answerCbQuery("Processing...");
      }
      return;
    }

    await ctx.answerCbQuery("OK");
    return;
  }

  await ctx.answerCbQuery("OK");
});


bot.catch(async (err, ctx) => {
  const msg = String(err?.message || err || "");
  const retryAfter = getRetryAfterSec(err);
  console.error("Bot error:", msg);

  if (msg.includes("429") || retryAfter > 0) {
    console.log(`⚠️ Rate limited. Retry after ${retryAfter || "?"}s`);
    try {
      await ctx?.answerCbQuery?.("Processing...", { show_alert: false });
    } catch (_) {}
    return;
  }
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught Exception:", err);
});

// -------------------- Webhook Boot --------------------
let server = null;

function isAllowedOrigin(origin) {
  if (!origin) return false;
  const o = String(origin);
  if (o === WEB_ORIGIN) return true;
  if (o.startsWith("http://localhost:")) return true;
  if (o.startsWith("http://127.0.0.1:")) return true;
  return false;
}

function requireApiKey(req) {
  if (!WEB_API_KEY) return false;
  const key = req.headers["x-api-key"];
  return key && String(key) === String(WEB_API_KEY);
}

(async () => {
  await connectMongo();
  await ensureTreasury();
  BOT_INFO = await bot.telegram.getMe();

  const app = express();
  app.use(express.json());

  const PORT = process.env.PORT || 3000;

  const webhookPath = `/telegraf/${WEBHOOK_SECRET}`;
  const webhookUrl = `${PUBLIC_URL}${webhookPath}`;

  app.use((req, res, next) => {
    const origin = req.headers.origin;

    if (origin && isAllowedOrigin(origin)) {
      res.setHeader("Access-Control-Allow-Origin", origin);
      res.setHeader("Vary", "Origin");
    }

    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-API-KEY");

    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
  });

  app.get("/", (req, res) => res.status(200).send("OK"));

  app.get("/api/ping", (req, res) => {
    return res.json({ ok: true, msg: "pong", time: new Date().toISOString() });
  });

  app.get("/api/balance", async (req, res) => {
    try {
      if (!requireApiKey(req)) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

      const userId = Number(req.query.userId);
      if (!userId || !Number.isFinite(userId)) return res.status(400).json({ ok: false, error: "INVALID_USER_ID" });

      const u = await users.findOne({ userId });
      return res.json({
        ok: true,
        userId,
        username: u?.username || null,
        firstName: u?.firstName || null,
        balance: u?.balance || 0,
        updatedAt: u?.updatedAt || null,
      });
    } catch (e) {
      console.error("api/balance error:", e);
      return res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
  });

  app.get("/api/top10", async (req, res) => {
    try {
      if (!requireApiKey(req)) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

      const list = await users
        .find({}, { projection: { userId: 1, username: 1, firstName: 1, balance: 1 } })
        .sort({ balance: -1 })
        .limit(10)
        .toArray();

      return res.json({ ok: true, top10: list });
    } catch (e) {
      console.error("api/top10 error:", e);
      return res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
  });

  app.post(webhookPath, (req, res) => {
    bot.handleUpdate(req.body, res);
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
    console.log(`🌐 WEB_ORIGIN: ${WEB_ORIGIN}`);
    console.log(`🔐 WEB_API_KEY set: ${WEB_API_KEY ? "YES" : "NO"}`);
    console.log(`🤖 Bot username: @${BOT_INFO?.username || "unknown"}`);
  });

  console.log("🤖 Bot started (Webhook mode)");
})().catch((e) => {
  console.error("BOOT ERROR:", e);
  process.exit(1);
});

// -------------------- Safe shutdown --------------------
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
