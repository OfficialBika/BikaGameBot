/*
 * BIKA Shan Koe Mee
 */

function registerShanGame(deps) {
  const {
    bot,
    ensureUser,
    getUser,
    ensureTreasury,
    treasuryPayToUser,
    userPayToTreasury,
    replyHTML,
    safeTelegram,
    sleep,
    fmt,
    escHtml,
    COIN,
    HOUSE_CUT_PERCENT,
    OWNER_ID,
    isGroupChat,
    toNum,
  } = deps;

  const SHAN = {
    minBet: 50,
    maxBet: 5000,
    timeoutMs: 60_000,
    maxActive: 20,
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
      for (const rank of RANKS) {
        deck.push({ rank, suit });
      }
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
    const total = cards.reduce((sum, c) => sum + rankValue(c.rank), 0);
    return total % 10;
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
    return [...cards]
      .map((c) => highCardWeight(c.rank))
      .sort((a, b) => b - a);
  }

  function handInfo(cards) {
    if (isShanKoeMee(cards)) {
      return {
        category: 4,
        name: "Shan Koe Mee",
        points: calcPoints(cards),
        tieBreaker: sortedHighRanks(cards),
      };
    }

    if (isZatToe(cards)) {
      return {
        category: 3,
        name: "Zat Toe",
        points: calcPoints(cards),
        tieBreaker: sortedHighRanks(cards),
      };
    }

    if (isSuitTriple(cards)) {
      return {
        category: 2,
        name: "Suit Triple",
        points: calcPoints(cards),
        tieBreaker: sortedHighRanks(cards),
      };
    }

    return {
      category: 1,
      name: `Point ${calcPoints(cards)}`,
      points: calcPoints(cards),
      tieBreaker: sortedHighRanks(cards),
    };
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
      `│   ${suit}   │`,
      `│    ${right} │`,
      "└───────┘",
    ];
  }

  function renderCardsRow(cards) {
    const boxes = cards.map(cardBox);
    const lines = [];
    for (let i = 0; i < 5; i++) {
      lines.push(boxes.map((b) => b[i]).join(" "));
    }
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
      `Reply ထောက်ထားတဲ့သူပဲ Accept လုပ်နိုင်ပါတယ်。\n` +
      `⏳ Timeout: <b>${Math.floor(SHAN.timeoutMs / 1000)}s</b>`
    );
  }

  bot.hears(/^\.(shan)\s+(\d+)\s*$/i, async (ctx) => {
    if (!isGroupChat(ctx)) {
      return replyHTML(ctx, "ℹ️ <code>.shan</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။");
    }

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

    await safeTelegram(() =>
      ctx.telegram.editMessageReplyMarkup(
        ctx.chat.id,
        sent.message_id,
        undefined,
        shanKeyboard(challengeId)
      )
    );

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
          await safeTelegram(() =>
            bot.telegram.editMessageText(
              c.chatId,
              c.msgId,
              undefined,
              `⏳ <b>Shan Duel Expired</b>\n━━━━━━━━━━━━\nစိန်ခေါ်မှု အချိန်ကုန်သွားပါတယ်。\nBet: <b>${fmt(c.bet)}</b> ${COIN}`,
              { parse_mode: "HTML", disable_web_page_preview: true }
            )
          );
        } catch (_) {}
        activeShanChallenges.delete(challengeId);
      }, SHAN.timeoutMs),
    });
  });

  bot.on("callback_query", async (ctx, next) => {
    const data = ctx.callbackQuery?.data || "";
    if (!data.startsWith("SHAN:")) return next();

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
      return ctx.editMessageText(
        `❌ <b>Shan Duel Cancelled</b>\n━━━━━━━━━━━━\nစိန်ခေါ်မှုကို ဖျက်လိုက်ပါတယ်。\nBet: <b>${fmt(c.bet)}</b> ${COIN}`,
        { parse_mode: "HTML", disable_web_page_preview: true }
      );
    }

    if (action === "ACCEPT") {
      if (c.status !== "OPEN") {
        await ctx.answerCbQuery("Already closed", { show_alert: true });
        return;
      }

      if (ctx.from.id !== c.targetUserId) {
        await ctx.answerCbQuery("ဒီ duel ကို reply ထောက်ထားတဲ့သူပဲ Accept လုပ်နိုင်ပါတယ်", { show_alert: true });
        return;
      }

      await ensureUser(ctx.from);

      const challenger = await getUser(c.challengerId);
      const opponent = await getUser(ctx.from.id);

      if (toNum(challenger?.balance) < c.bet) {
        c.status = "FAILED";
        clearTimeout(c.timeoutHandle);
        activeShanChallenges.delete(challengeId);
        await ctx.answerCbQuery("Challenger has insufficient balance", { show_alert: true });
        return ctx.editMessageText(
          `⚠️ <b>Challenge Failed</b>\n━━━━━━━━━━━━\nစိန်ခေါ်သူ balance မလုံလောက်ပါ။`,
          { parse_mode: "HTML", disable_web_page_preview: true }
        );
      }

      if (toNum(opponent?.balance) < c.bet) {
        await ctx.answerCbQuery("Insufficient balance", { show_alert: true });
        const lack = Math.max(0, c.bet - toNum(opponent?.balance));
        return replyHTML(
          ctx,
          `❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>\n━━━━━━━━━━━━\nBet: <b>${fmt(c.bet)}</b> ${COIN}\nYour Balance: <b>${fmt(opponent?.balance)}</b> ${COIN}\nNeed More: <b>${fmt(lack)}</b> ${COIN}`,
          { reply_to_message_id: c.msgId }
        );
      }

      c.status = "PLAYING";
      c.opponentId = ctx.from.id;
      clearTimeout(c.timeoutHandle);
      activeShanChallenges.set(challengeId, c);

      await ctx.answerCbQuery("Accepted!");

      try {
        await ensureTreasury();
        await userPayToTreasury(c.challengerId, c.bet, { type: "shan_bet", challengeId });
        await userPayToTreasury(c.opponentId, c.bet, { type: "shan_bet", challengeId });
      } catch (e) {
        console.error("shan bet take error:", e);
        c.status = "FAILED";
        activeShanChallenges.delete(challengeId);
        return ctx.editMessageText(`⚠️ <b>Error</b>\n━━━━━━━━━━━━\nBet process error.`, {
          parse_mode: "HTML",
        });
      }

      const deck = shuffle(buildDeck());
      const cardsA = drawCards(deck, 3);
      const cardsB = drawCards(deck, 3);

      await sleep(700);

      const result = compareHands(cardsA, cardsB);
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

        return ctx.editMessageText(
          `🃏 <b>Shan Duel Result</b>\n━━━━━━━━━━━━\n` +
            `စိန်ခေါ်သူ: <b>${escHtml(challengerLabel)}</b>\n` +
            `<pre>${escHtml(renderCardsRow(cardsA))}</pre>\n` +
            `Hand: <b>${escHtml(infoA.name)}</b>\n` +
            `Point: <b>${infoA.points}</b>\n\n` +
            `လက်ခံသူ: <b>${escHtml(opponentLabel)}</b>\n` +
            `<pre>${escHtml(renderCardsRow(cardsB))}</pre>\n` +
            `Hand: <b>${escHtml(infoB.name)}</b>\n` +
            `Point: <b>${infoB.points}</b>\n` +
            `━━━━━━━━━━━━\n` +
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
        } catch (e) {
          extraPenalty = 0;
        }
        payout = pot + extraPenalty;
      }

      try {
        await treasuryPayToUser(winnerId, payout, {
          type: "shan_win",
          challengeId,
          pot,
          payout,
          extraPenalty,
          hand: winnerInfo.name,
        });
      } catch (e) {
        console.error("shan payout error:", e);
        try {
          await treasuryPayToUser(c.challengerId, c.bet, { type: "shan_refund", challengeId, reason: "payout_fail" });
          await treasuryPayToUser(c.opponentId, c.bet, { type: "shan_refund", challengeId, reason: "payout_fail" });
        } catch (_) {}

        c.status = "DONE";
        activeShanChallenges.delete(challengeId);

        return ctx.editMessageText(
          `⚠️ <b>Shan Duel Error</b>\n━━━━━━━━━━━━━━━━\nPayout error ဖြစ်လို့ refund ပြန်ပေးလိုက်ပါတယ်။`,
          { parse_mode: "HTML", disable_web_page_preview: true }
        );
      }

      c.status = "DONE";
      activeShanChallenges.delete(challengeId);

      return ctx.editMessageText(
        `🃏 <b>Shan Duel Result</b>\n━━━━━━━━━━━━\n` +
          `စိန်ခေါ်သူ: <b>${escHtml(challengerLabel)}</b>\n` +
          `<pre>${escHtml(renderCardsRow(cardsA))}</pre>\n` +
          `Hand: <b>${escHtml(infoA.name)}</b>\n` +
          `Point: <b>${infoA.points}</b>\n\n` +
          `လက်ခံသူ: <b>${escHtml(opponentLabel)}</b>\n` +
          `<pre>${escHtml(renderCardsRow(cardsB))}</pre>\n` +
          `Hand: <b>${escHtml(infoB.name)}</b>\n` +
          `Point: <b>${infoB.points}</b>\n` +
          `━━━━━━━━━━━━\n` +
          `🏆 Winner: <b>${escHtml(winnerLabel)}</b>\n` +
          `Winning Hand: <b>${escHtml(winnerInfo.name)}</b>\n` +
          `💰 Pot: <b>${fmt(pot)}</b> ${COIN}\n` +
          `${winnerInfo.name === "Suit Triple" ? `🔥 Extra Bet: <b>${fmt(extraPenalty)}</b> ${COIN}\n` : `🏦 House cut: <b>2%</b>\n`}` +
          `✅ Winner gets: <b>${fmt(payout)}</b> ${COIN}`,
        { parse_mode: "HTML", disable_web_page_preview: true }
      );
    }

    await ctx.answerCbQuery("OK");
  });
}

module.exports = { registerShanGame };
