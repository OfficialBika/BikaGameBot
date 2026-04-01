
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type App struct {
	Bot    *tgbotapi.BotAPI
	Mongo  *mongo.Client
	DB     *mongo.Database
	Users  *mongo.Collection
	Groups *mongo.Collection
	Config *mongo.Collection
	Tx     *mongo.Collection
	Orders *mongo.Collection

	BotToken   string
	MongoURI   string
	DBName     string
	TZName     string
	OwnerID    int64
	Coin       string
	StartBonus int64
	StartedAt  time.Time
	TZ         *time.Location

	mu sync.Mutex

	ActiveSlots          map[int64]bool
	LastSlotAt           map[int64]int64
	LastGiftAt           map[int64]int64
	ActiveDiceChallenges map[string]*Challenge
	ActiveShanChallenges map[string]*Challenge
	CurrentBroadcast     *BroadcastRun
}

type BroadcastRun struct {
	ID                string
	Cancelled         bool
	OwnerChatID       int64
	ProgressMessageID int
	StartedAt         time.Time
}

type Challenge struct {
	ID           string
	ChatID       int64
	MsgID        int
	ChallengerID int64
	OpponentID   int64
	Bet          int64
	Status       string
	CreatedAt    time.Time
}

type UserDoc struct {
	UserID             int64      `bson:"userId"`
	Username           *string    `bson:"username,omitempty"`
	FirstName          string     `bson:"firstName,omitempty"`
	LastName           string     `bson:"lastName,omitempty"`
	FullName           string     `bson:"fullName,omitempty"`
	Balance            int64      `bson:"balance"`
	IsVIP              bool       `bson:"isVip"`
	StartedBot         bool       `bson:"startedBot"`
	StartBonusClaimed  bool       `bson:"startBonusClaimed"`
	LastDailyClaimDate *string    `bson:"lastDailyClaimDate,omitempty"`
	CreatedAt          time.Time  `bson:"createdAt"`
	UpdatedAt          time.Time  `bson:"updatedAt"`
}

type GroupDoc struct {
	GroupID        int64      `bson:"groupId"`
	Title          string     `bson:"title"`
	BotIsAdmin     bool       `bson:"botIsAdmin"`
	ApprovalStatus string     `bson:"approvalStatus"`
	ApprovedBy     *int64     `bson:"approvedBy,omitempty"`
	CreatedAt      time.Time  `bson:"createdAt"`
	UpdatedAt      time.Time  `bson:"updatedAt"`
}

type TreasuryDoc struct {
	Key              string    `bson:"key"`
	OwnerUserID      int64     `bson:"ownerUserId"`
	TotalSupply      int64     `bson:"totalSupply"`
	OwnerBalance     int64     `bson:"ownerBalance"`
	MaintenanceMode  bool      `bson:"maintenanceMode"`
	VIPWinRate       int       `bson:"vipWinRate"`
	ShopEnabled      bool      `bson:"shopEnabled"`
	BroadcastRunning bool      `bson:"broadcastRunning"`
	BroadcastRunID   *string   `bson:"broadcastRunId,omitempty"`
	SlotRTP          float64   `bson:"slotRtp"`
	CreatedAt        time.Time `bson:"createdAt"`
	UpdatedAt        time.Time `bson:"updatedAt"`
}

type ReelItem struct {
	S string
	W int
}

type Card struct {
	Rank string
	Suit string
}

type HandInfo struct {
	Category   int
	Name       string
	Points     int
	TieBreaker []int
}

type CompareResult struct {
	Winner string
	InfoA  HandInfo
	InfoB  HandInfo
}

type ShanDeal struct {
	CardsA []Card
	CardsB []Card
	Result CompareResult
}

type ShopItem struct {
	ID    string
	Name  string
	Price int64
}

var shopItems = []ShopItem{
	{ID: "dia11", Name: "Diamonds 11 💎", Price: 500000},
	{ID: "dia22", Name: "Diamonds 22 💎", Price: 1000000},
	{ID: "dia33", Name: "Diamonds 33 💎", Price: 1500000},
	{ID: "dia44", Name: "Diamonds 44 💎", Price: 2000000},
	{ID: "dia55", Name: "Diamonds 55 💎", Price: 2500000},
	{ID: "wp1", Name: "Weekly Pass 🎟️", Price: 9000000},
}

var slotReels = [][]ReelItem{
	{{"🍒", 3200}, {"🍋", 2200}, {"🍉", 1500}, {"🔔", 900}, {"⭐", 450}, {"BAR", 200}, {"7", 100}},
	{{"🍒", 3200}, {"🍋", 2200}, {"🍉", 1500}, {"🔔", 900}, {"⭐", 450}, {"BAR", 200}, {"7", 100}},
	{{"🍒", 3200}, {"🍋", 2200}, {"🍉", 1500}, {"🔔", 900}, {"⭐", 450}, {"BAR", 200}, {"7", 100}},
}

var slotBasePayouts = map[string]float64{
	"7,7,7":       20.0,
	"BAR,BAR,BAR": 15.0,
	"⭐,⭐,⭐":        12.0,
	"🔔,🔔,🔔":        9.0,
	"🍉,🍉,🍉":        7.0,
	"🍋,🍋,🍋":        5.0,
	"🍒,🍒,🍒":        3.0,
	"ANY2":        1.5,
}

var (
	maxActiveSlots = 5
	giftCooldownMS int64 = 10_000
	dailyMin int64 = 500
	dailyMax int64 = 5000
	diceMinBet int64 = 100
	diceMaxBet int64 = 50000
	diceMaxActive = 20
	shanMinBet int64 = 100
	shanMaxBet int64 = 50000
	shanMaxActive = 20
	slotMinBet int64 = 50
	slotMaxBet int64 = 5000
	slotCooldownMS int64 = 700
	slotCapPercent = 0.30
	suits = []string{"♥", "♦", "♣", "♠"}
	ranks = []string{"A","2","3","4","5","6","7","8","9","10","J","Q","K"}
)

func main() {
	rand.Seed(time.Now().UnixNano())
	_ = godotenv.Load()

	app, err := NewApp()
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := app.EnsureIndexes(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := app.EnsureTreasury(ctx); err != nil {
		log.Fatal(err)
	}

	log.Printf("bot started as @%s", app.Bot.Self.UserName)
	app.RunPolling()
}

func NewApp() (*App, error) {
	botToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	mongoURI := firstNonEmpty(os.Getenv("MONGODB_URI"), os.Getenv("MONGO_URI"))
	dbName := firstNonEmpty(os.Getenv("DB_NAME"), "bika_slot")
	tzName := firstNonEmpty(os.Getenv("TZ"), "Asia/Yangon")
	ownerID, _ := strconv.ParseInt(firstNonEmpty(os.Getenv("OWNER_ID"), "0"), 10, 64)
	startBonus, _ := strconv.ParseInt(firstNonEmpty(os.Getenv("START_BONUS"), "30000"), 10, 64)
	coin := firstNonEmpty(os.Getenv("STORE_CURRENCY"), "MMK")

	if botToken == "" {
		return nil, errors.New("missing BOT_TOKEN")
	}
	if mongoURI == "" {
		return nil, errors.New("missing MONGODB_URI / MONGO_URI")
	}
	if ownerID == 0 {
		return nil, errors.New("missing OWNER_ID")
	}

	loc, err := time.LoadLocation(tzName)
	if err != nil {
		loc = time.FixedZone("Asia/Yangon", 6*3600+30*60)
	}

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	mc, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, err
	}

	db := mc.Database(dbName)
	return &App{
		Bot: bot, Mongo: mc, DB: db,
		Users: db.Collection("users"),
		Groups: db.Collection("groups"),
		Config: db.Collection("config"),
		Tx: db.Collection("transactions"),
		Orders: db.Collection("orders"),
		BotToken: botToken, MongoURI: mongoURI, DBName: dbName, TZName: tzName,
		OwnerID: ownerID, Coin: coin, StartBonus: startBonus, StartedAt: time.Now(), TZ: loc,
		ActiveSlots: map[int64]bool{}, LastSlotAt: map[int64]int64{}, LastGiftAt: map[int64]int64{},
		ActiveDiceChallenges: map[string]*Challenge{}, ActiveShanChallenges: map[string]*Challenge{},
	}, nil
}

func (a *App) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.Mongo.Disconnect(ctx)
}

func (a *App) RunPolling() {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	updates := a.Bot.GetUpdatesChan(u)
	for update := range updates {
		if update.Message != nil {
			go a.HandleMessage(update.Message)
		}
		if update.CallbackQuery != nil {
			go a.HandleCallback(update.CallbackQuery)
		}
	}
}

func (a *App) HandleMessage(m *tgbotapi.Message) {
	if m == nil || m.From == nil {
		return
	}
	text := strings.TrimSpace(m.Text)

	switch {
	case strings.HasPrefix(text, "/start"):
		a.CmdStart(m)
	case text == "☰ Menu":
		a.CmdStart(m)
	case strings.HasPrefix(text, "/ping"):
		a.CmdPing(m)
	case strings.HasPrefix(text, "/status"):
		a.CmdStatus(m)
	case strings.HasPrefix(text, "/balance"):
		a.CmdBalance(m)
	case strings.HasPrefix(text, "/dailyclaim"):
		a.CmdDailyClaim(m)
	case text == ".top10" || strings.HasPrefix(text, "/top10"):
		a.CmdTop10(m)
	case text == ".mybalance" || text == ".bal" || strings.HasPrefix(text, "/bal"):
		a.CmdMyBalance(m)
	case strings.HasPrefix(text, "/gift"):
		a.CmdGift(m)
	case strings.HasPrefix(text, ".gift "):
		a.CmdDotGift(m)
	case strings.HasPrefix(text, ".slot "):
		a.CmdSlot(m)
	case strings.HasPrefix(text, ".dice "):
		a.CmdDice(m)
	case strings.HasPrefix(text, ".shan "):
		a.CmdShan(m)
	case strings.HasPrefix(text, "/broadcastend"):
		a.CmdBroadcastEnd(m)
	case strings.HasPrefix(text, "/broadcast"):
		a.CmdBroadcast(m)
	case strings.HasPrefix(text, "/approve"):
		a.CmdApprove(m)
	case strings.HasPrefix(text, "/reject"):
		a.CmdReject(m)
	case strings.HasPrefix(text, "/groupstatus"):
		a.CmdGroupStatus(m)
	case strings.HasPrefix(text, "/on"):
		a.CmdOn(m)
	case strings.HasPrefix(text, "/off"):
		a.CmdOff(m)
	case strings.HasPrefix(text, "/settotal"):
		a.CmdSetTotal(m)
	case strings.HasPrefix(text, "/treasury"):
		a.CmdTreasury(m)
	case strings.HasPrefix(text, "/addvip"):
		a.CmdAddVIP(m)
	case strings.HasPrefix(text, "/removevip"):
		a.CmdRemoveVIP(m)
	case strings.HasPrefix(text, "/viplist"):
		a.CmdVIPList(m)
	case strings.HasPrefix(text, "/setvipwr"):
		a.CmdSetVIPWR(m)
	case strings.HasPrefix(text, "/vipwr"):
		a.CmdVIPWR(m)
	case strings.HasPrefix(text, "/shop"):
		a.CmdShop(m)
	case strings.HasPrefix(text, "/orders"):
		a.CmdOrders(m)
	case strings.HasPrefix(text, "/admin"):
		a.CmdAdmin(m)
	}
}

func (a *App) HandleCallback(cb *tgbotapi.CallbackQuery) {
	if cb == nil || cb.Message == nil || cb.From == nil {
		return
	}
	data := cb.Data
	switch {
	case strings.HasPrefix(data, "shop:"):
		a.HandleShopOrder(cb, strings.TrimPrefix(data, "shop:"))
	case strings.HasPrefix(data, "dice:"):
		a.CBDice(cb)
	case strings.HasPrefix(data, "shan:"):
		a.CBShan(cb)
	default:
		a.AnswerCallback(cb.ID, "Unknown action", false)
	}
}

func (a *App) EnsureIndexes(ctx context.Context) error {
	_, err := a.Users.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.M{"userId": 1}, Options: options.Index().SetUnique(true)},
		{Keys: bson.M{"username": 1}},
		{Keys: bson.M{"startedBot": 1}},
		{Keys: bson.M{"isVip": 1}},
	})
	if err != nil { return err }
	_, _ = a.Groups.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.M{"groupId": 1}, Options: options.Index().SetUnique(true)},
		{Keys: bson.M{"approvalStatus": 1}},
	})
	_, _ = a.Config.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.M{"key": 1}, Options: options.Index().SetUnique(true),
	})
	_, _ = a.Tx.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.M{"createdAt": 1}})
	_, _ = a.Orders.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.M{"orderId": 1}, Options: options.Index().SetUnique(true)},
		{Keys: bson.M{"status": 1}},
	})
	return nil
}

func (a *App) now() time.Time { return time.Now().In(a.TZ) }
func (a *App) formatYangon(t time.Time) string { return t.In(a.TZ).Format("02/01/2006, 15:04:05") }

func esc(v any) string { return html.EscapeString(fmt.Sprint(v)) }
func fmtInt(n int64) string {
	sign := ""
	if n < 0 { sign = "-"; n = -n }
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 { return sign + s }
	var out strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		out.WriteString(s[:pre])
		if len(s) > pre { out.WriteByte(',') }
	}
	for i := pre; i < len(s); i += 3 {
		out.WriteString(s[i:i+3])
		if i+3 < len(s) { out.WriteByte(',') }
	}
	return sign + out.String()
}
func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" { return s }
	}
	return ""
}
func (a *App) uptimeText() string {
	sec := int(time.Since(a.StartedAt).Seconds())
	h, rem := sec/3600, sec%3600
	m, s := rem/60, rem%60
	if h > 0 { return fmt.Sprintf("%dh %dm %ds", h, m, s) }
	if m > 0 { return fmt.Sprintf("%dm %ds", m, s) }
	return fmt.Sprintf("%ds", s)
}
func nonZeroFloat(v, fallback float64) float64 { if v != 0 { return v }; return fallback }
func firstNonZero(v, fallback int64) int64 { if v != 0 { return v }; return fallback }
func normalizeVIPWinRate(v int) int { if v < 0 { return 0 }; if v > 100 { return 100 }; return v }
func normalizeRTP(v float64) float64 {
	if v > 1 { v = v / 100.0 }
	if v < 0.50 { return 0.50 }
	if v > 0.98 { return 0.98 }
	return v
}
func tokenHex(n int) string {
	buf := make([]byte, n)
	if _, err := crand.Read(buf); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func (a *App) ReplyHTML(m *tgbotapi.Message, text string) (*tgbotapi.Message, error) {
	msg := tgbotapi.NewMessage(m.Chat.ID, text)
	msg.ParseMode = "HTML"
	msg.ReplyToMessageID = m.MessageID
	sent, err := a.Bot.Send(msg)
	return &sent, err
}

func (a *App) ReplyHTMLWithMarkup(m *tgbotapi.Message, text string, markup any) (*tgbotapi.Message, error) {
	msg := tgbotapi.NewMessage(m.Chat.ID, text)
	msg.ParseMode = "HTML"
	msg.ReplyToMessageID = m.MessageID
	switch x := markup.(type) {
	case tgbotapi.InlineKeyboardMarkup:
		msg.ReplyMarkup = x
	case *tgbotapi.InlineKeyboardMarkup:
		msg.ReplyMarkup = x
	}
	sent, err := a.Bot.Send(msg)
	return &sent, err
}

func (a *App) SendHTML(chatID int64, text string) (*tgbotapi.Message, error) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "HTML"
	sent, err := a.Bot.Send(msg)
	return &sent, err
}

func (a *App) EditHTML(chatID int64, messageID int, text string, markup *tgbotapi.InlineKeyboardMarkup) error {
	msg := tgbotapi.NewEditMessageText(chatID, messageID, text)
	msg.ParseMode = "HTML"
	if markup != nil {
		msg.ReplyMarkup = markup
	}
	_, err := a.Bot.Send(msg)
	return err
}

func (a *App) DeleteMessage(chatID int64, messageID int) {
	_, _ = a.Bot.Request(tgbotapi.NewDeleteMessage(chatID, messageID))
}

func (a *App) AnswerCallback(id, text string, alert bool) {
	cfg := tgbotapi.NewCallback(id, text)
	cfg.ShowAlert = alert
	_, _ = a.Bot.Request(cfg)
}

func (a *App) CopyMessage(toChatID, fromChatID int64, messageID int) bool {
	cfg := tgbotapi.NewCopyMessage(toChatID, fromChatID, messageID)
	_, err := a.Bot.CopyMessage(cfg)
	return err == nil
}

func (a *App) SendText(chatID int64, text string) bool {
	msg := tgbotapi.NewMessage(chatID, text)
	_, err := a.Bot.Send(msg)
	return err == nil
}

func (a *App) MentionHTML(u *tgbotapi.User) string {
	name := strings.TrimSpace(strings.Join([]string{u.FirstName, u.LastName}, " "))
	if name == "" {
		if u.UserName != "" { name = u.UserName } else { name = "User" }
	}
	return fmt.Sprintf(`<a href="tg://user?id=%d">%s</a>`, u.ID, esc(name))
}

func (a *App) UserLabelHTML(u *UserDoc) string {
	if u == nil { return "Unknown" }
	name := strings.TrimSpace(u.FullName)
	if name == "" { name = strings.TrimSpace(u.FirstName + " " + u.LastName) }
	if name == "" { name = "User" }
	return fmt.Sprintf(`<a href="tg://user?id=%d">%s</a>`, u.UserID, esc(name))
}

func userLabelText(u *UserDoc) string {
	if u == nil { return "Unknown" }
	name := strings.TrimSpace(u.FullName)
	if name == "" { name = strings.TrimSpace(u.FirstName + " " + u.LastName) }
	if name == "" { name = "User" }
	return name
}

func (a *App) IsGroupMessage(m *tgbotapi.Message) bool {
	return m.Chat != nil && (m.Chat.IsGroup() || m.Chat.IsSuperGroup())
}

func (a *App) EnsureUser(ctx context.Context, tgUser *tgbotapi.User) (*UserDoc, error) {
	now := a.now()
	var username *string
	if tgUser.UserName != "" {
		v := strings.ToLower(tgUser.UserName)
		username = &v
	}
	fullName := strings.TrimSpace(strings.Join([]string{tgUser.FirstName, tgUser.LastName}, " "))
	if fullName == "" { fullName = tgUser.FirstName }

	_, err := a.Users.UpdateOne(ctx, bson.M{"userId": tgUser.ID}, bson.M{
		"$set": bson.M{
			"userId": tgUser.ID, "username": username, "firstName": tgUser.FirstName,
			"lastName": tgUser.LastName, "fullName": fullName, "updatedAt": now,
		},
		"$setOnInsert": bson.M{
			"balance": int64(0), "isVip": false, "startedBot": false, "startBonusClaimed": false,
			"createdAt": now, "lastDailyClaimDate": nil,
		},
	}, options.Update().SetUpsert(true))
	if err != nil { return nil, err }
	return a.GetUser(ctx, tgUser.ID)
}

func (a *App) GetUser(ctx context.Context, userID int64) (*UserDoc, error) {
	var u UserDoc
	err := a.Users.FindOne(ctx, bson.M{"userId": userID}).Decode(&u)
	if err == mongo.ErrNoDocuments { return nil, nil }
	if err != nil { return nil, err }
	return &u, nil
}

func (a *App) GetUserByUsername(ctx context.Context, username string) (*UserDoc, error) {
	un := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(username), "@"))
	var u UserDoc
	err := a.Users.FindOne(ctx, bson.M{"username": un}).Decode(&u)
	if err == mongo.ErrNoDocuments { return nil, nil }
	if err != nil { return nil, err }
	return &u, nil
}

func (a *App) EnsureGroup(ctx context.Context, chat *tgbotapi.Chat) (*GroupDoc, error) {
	now := a.now()
	botIsAdmin := false
	member, err := a.Bot.GetChatMember(tgbotapi.GetChatMemberConfig{ChatConfigWithUser: tgbotapi.ChatConfigWithUser{
		ChatID: chat.ID, UserID: a.Bot.Self.ID,
	}})
	if err == nil {
		switch member.Status {
		case "administrator", "creator":
			botIsAdmin = true
		}
	}
	_, err = a.Groups.UpdateOne(ctx, bson.M{"groupId": chat.ID}, bson.M{
		"$set": bson.M{
			"groupId": chat.ID, "title": chat.Title, "botIsAdmin": botIsAdmin, "updatedAt": now,
		},
		"$setOnInsert": bson.M{
			"approvalStatus": "pending", "approvedBy": nil, "createdAt": now,
		},
	}, options.Update().SetUpsert(true))
	if err != nil { return nil, err }
	var g GroupDoc
	err = a.Groups.FindOne(ctx, bson.M{"groupId": chat.ID}).Decode(&g)
	if err != nil { return nil, err }
	return &g, nil
}

func (a *App) EnsureTreasury(ctx context.Context) (*TreasuryDoc, error) {
	var t TreasuryDoc
	err := a.Config.FindOne(ctx, bson.M{"key": "treasury"}).Decode(&t)
	if err == mongo.ErrNoDocuments {
		doc := TreasuryDoc{
			Key: "treasury", OwnerUserID: a.OwnerID, TotalSupply: 0, OwnerBalance: 0,
			MaintenanceMode: false, VIPWinRate: 90, ShopEnabled: true, BroadcastRunning: false,
			SlotRTP: 0.90, CreatedAt: a.now(), UpdatedAt: a.now(),
		}
		if _, err := a.Config.InsertOne(ctx, doc); err != nil { return nil, err }
		return &doc, nil
	}
	if err != nil { return nil, err }

	_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{
		"ownerUserId": firstNonZero(t.OwnerUserID, a.OwnerID),
		"vipWinRate": normalizeVIPWinRate(t.VIPWinRate),
		"slotRtp": normalizeRTP(nonZeroFloat(t.SlotRTP, 0.90)),
		"updatedAt": a.now(),
	}})
	err = a.Config.FindOne(ctx, bson.M{"key": "treasury"}).Decode(&t)
	if err != nil { return nil, err }
	return &t, nil
}

func (a *App) GetTreasury(ctx context.Context) (*TreasuryDoc, error) {
	return a.EnsureTreasury(ctx)
}

func (a *App) IsOwner(userID int64, t *TreasuryDoc) bool {
	return t != nil && t.OwnerUserID == userID
}

func (a *App) LogTx(ctx context.Context, kind string, data bson.M) {
	data["type"] = kind
	data["createdAt"] = a.now()
	_, _ = a.Tx.InsertOne(ctx, data)
}

func (a *App) TreasuryPayToUser(ctx context.Context, userID int64, amount int64, meta bson.M) error {
	if amount <= 0 { return nil }
	now := a.now()
	res, err := a.Config.UpdateOne(ctx, bson.M{"key":"treasury","ownerBalance":bson.M{"$gte": amount}}, bson.M{
		"$inc": bson.M{"ownerBalance": -amount},
		"$set": bson.M{"updatedAt": now},
	})
	if err != nil { return err }
	if res.ModifiedCount == 0 { return errors.New("TREASURY_INSUFFICIENT") }

	_, err = a.Users.UpdateOne(ctx, bson.M{"userId": userID}, bson.M{
		"$inc": bson.M{"balance": amount},
		"$set": bson.M{"updatedAt": now},
		"$setOnInsert": bson.M{
			"userId": userID, "username": nil, "firstName": "", "lastName": "",
			"fullName": fmt.Sprint(userID), "isVip": false, "startedBot": false,
			"startBonusClaimed": false, "createdAt": now,
		},
	}, options.Update().SetUpsert(true))
	if err != nil { return err }
	a.LogTx(ctx, "treasury_to_user", bson.M{"userId": userID, "amount": amount, "meta": meta})
	return nil
}

func (a *App) UserPayToTreasury(ctx context.Context, userID, amount int64, meta bson.M) error {
	if amount <= 0 { return nil }
	now := a.now()
	res, err := a.Users.UpdateOne(ctx, bson.M{"userId": userID, "balance": bson.M{"$gte": amount}}, bson.M{
		"$inc": bson.M{"balance": -amount}, "$set": bson.M{"updatedAt": now},
	})
	if err != nil { return err }
	if res.ModifiedCount == 0 { return errors.New("USER_INSUFFICIENT") }

	_, err = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{
		"$inc": bson.M{"ownerBalance": amount}, "$set": bson.M{"updatedAt": now},
	})
	if err != nil { return err }
	a.LogTx(ctx, "user_to_treasury", bson.M{"userId": userID, "amount": amount, "meta": meta})
	return nil
}

func (a *App) TransferBalance(ctx context.Context, fromUserID, toUserID, amount int64, meta bson.M) error {
	if amount <= 0 { return nil }
	now := a.now()
	res, err := a.Users.UpdateOne(ctx, bson.M{"userId": fromUserID, "balance": bson.M{"$gte": amount}}, bson.M{
		"$inc": bson.M{"balance": -amount}, "$set": bson.M{"updatedAt": now},
	})
	if err != nil { return err }
	if res.ModifiedCount == 0 { return errors.New("USER_INSUFFICIENT") }
	_, err = a.Users.UpdateOne(ctx, bson.M{"userId": toUserID}, bson.M{
		"$inc": bson.M{"balance": amount}, "$set": bson.M{"updatedAt": now},
	}, options.Update().SetUpsert(true))
	if err != nil { return err }
	a.LogTx(ctx, "user_to_user", bson.M{"fromUserId": fromUserID, "toUserId": toUserID, "amount": amount, "meta": meta})
	return nil
}

func (a *App) NextOrderID(ctx context.Context) (string, error) {
	var doc bson.M
	err := a.Config.FindOneAndUpdate(ctx, bson.M{"key":"order_seq"}, bson.M{
		"$inc": bson.M{"value": 1},
		"$setOnInsert": bson.M{"value":0, "createdAt": a.now()},
		"$set": bson.M{"updatedAt": a.now()},
	}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&doc)
	if err != nil { return "", err }
	switch v := doc["value"].(type) {
	case int32:
		return fmt.Sprintf("BKS%06d", v), nil
	case int64:
		return fmt.Sprintf("BKS%06d", v), nil
	case float64:
		return fmt.Sprintf("BKS%06d", int64(v)), nil
	default:
		return "BKS000001", nil
	}
}

func (a *App) EnsureNotMaintenance(m *tgbotapi.Message) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, err := a.EnsureTreasury(ctx)
	if err != nil {
		a.ReplyHTML(m, "⚠️ Treasury error")
		return false
	}
	if !t.MaintenanceMode || a.IsOwner(m.From.ID, t) { return true }
	a.ReplyHTML(m, "🛠️ <b>Bot ပြုပြင်နေပါတယ်</b>\n━━━━━━━━━━━━\nခေတ္တစောင့်ဆိုင်းပေးပါ။")
	return false
}

func (a *App) EnsureGroupApproved(m *tgbotapi.Message) bool {
	if !a.IsGroupMessage(m) { return true }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g, err := a.EnsureGroup(ctx, m.Chat)
	if err != nil {
		a.ReplyHTML(m, "⚠️ Group check error")
		return false
	}
	if g.ApprovalStatus == "approved" { return true }
	a.ReplyHTML(m, "⚠️ <b>Bot Owner Approve မပေးထားပါ</b>\n━━━━━━━━━━━━\nဒီ group မှာ bot ကိုအသုံးပြုရန် owner က <code>/approve</code> လုပ်ပေးရပါမယ်။")
	return false
}

func (a *App) CmdStart(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	t, err := a.EnsureTreasury(ctx)
	if err != nil { a.ReplyHTML(m, "Treasury error"); return }
	u, err := a.EnsureUser(ctx, m.From)
	if err != nil { a.ReplyHTML(m, "User error"); return }
	_, _ = a.Users.UpdateOne(ctx, bson.M{"userId": m.From.ID}, bson.M{"$set": bson.M{"startedBot": true, "updatedAt": a.now()}})
	if !u.StartBonusClaimed {
		if t.OwnerBalance < a.StartBonus {
			a.ReplyHTML(m, "⚠️ <b>Treasury မသတ်မှတ်ရသေးပါ</b>\n━━━━━━━━━━━━\nOwner က <code>/settotal 5000000</code> လုပ်ပြီးမှ Welcome Bonus ပေးနိုင်ပါတယ်။")
			return
		}
		if err := a.TreasuryPayToUser(ctx, m.From.ID, a.StartBonus, bson.M{"type":"start_bonus"}); err != nil {
			a.ReplyHTML(m, "🏦 Treasury မလုံလောက်ပါ။ Owner က /settotal ပြန်သတ်မှတ်ပေးပါ။")
			return
		}
		_, _ = a.Users.UpdateOne(ctx, bson.M{"userId": m.From.ID}, bson.M{"$set": bson.M{"startBonusClaimed": true, "updatedAt": a.now()}})
		u2, _ := a.GetUser(ctx, m.From.ID)
		a.ReplyHTML(m, fmt.Sprintf("🎉 <b>Welcome Bonus</b>\n━━━━━━━━━━━━━━━\n👤 %s\n➕ Bonus: <b>%s</b> %s\n💼 Balance: <b>%s</b> %s\n━━━━━━━━━━━━━━\nGroup Commands:\n• <code>/dailyclaim</code>\n• <code>.slot 100</code>\n• <code>.dice 200</code>\n• <code>.shan 500</code>\n• <code>.mybalance</code>\n• <code>.top10</code>\n• <code>/shop</code>",
			a.MentionHTML(m.From), fmtInt(a.StartBonus), a.Coin, fmtInt(u2.Balance), a.Coin))
		return
	}
	a.ReplyHTML(m, "👋 <b>Welcome back</b>\n━━━━━━━━━━━━━━━\nGroup Commands:\n• <code>/dailyclaim</code>\n• <code>.slot 100</code>\n• <code>.dice 200</code>\n• <code>.shan 500</code>\n• <code>.mybalance</code>\n• <code>.top10</code>\n• <code>/shop</code>")
}

func (a *App) CmdPing(m *tgbotapi.Message) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = a.EnsureUser(ctx, m.From)
	db0 := time.Now()
	_, _ = a.EnsureTreasury(ctx)
	dbMS := time.Since(db0).Milliseconds()
	botMS := time.Since(start).Milliseconds()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	a.ReplyHTML(m, fmt.Sprintf("🏓 <b>PING</b>\n━━━━━━━━━━━━\n⚡ Bot: <b>%d ms</b>\n🗄 DB: <b>%d ms</b>\n⏱ Uptime: <b>%s</b>\n💻 Memory: <b>%d MB Alloc / %d MB Sys</b>\n🕒 Yangon: <b>%s</b>",
		botMS, dbMS, esc(a.uptimeText()), ms.Alloc/1024/1024, ms.Sys/1024/1024, esc(a.formatYangon(a.now()))))
}

func (a *App) CmdStatus(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	_, _ = a.EnsureUser(ctx, m.From)
	t, _ := a.EnsureTreasury(ctx)
	usersCount, _ := a.Users.CountDocuments(ctx, bson.M{})
	groupsCount, _ := a.Groups.CountDocuments(ctx, bson.M{})
	vipCount, _ := a.Users.CountDocuments(ctx, bson.M{"isVip": true})
	ms := time.Since(start).Milliseconds()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	bstatus := "IDLE"
	if a.CurrentBroadcast != nil && !a.CurrentBroadcast.Cancelled { bstatus = "RUNNING" }
	maint := "OFF"
	vipwr := 90
	if t != nil {
		if t.MaintenanceMode { maint = "ON" }
		vipwr = t.VIPWinRate
	}
	a.ReplyHTML(m, fmt.Sprintf("📊 <b>BIKA Bot Status</b>\n━━━━━━━━━━━━\n⚡ Bot: <b>%d ms</b>\n🗄 DB: <b>%d ms</b>\n⏱ Uptime: <b>%s</b>\n💻 Memory: <b>%d MB Alloc / %d MB Sys</b>\n🕒 Yangon: <b>%s</b>\n━━━━━━━━━━━━\n👥 Users: <b>%d</b>\n👨‍👩‍👧‍👦 Groups: <b>%d</b>\n🌟 VIP: <b>%d</b>\n🎲 Open Dice: <b>%d</b>\n🀄 Open Shan: <b>%d</b>\n📣 Broadcast: <b>%s</b>\n🛠 Maintenance: <b>%s</b>\n🎯 VIP WR: <b>%d%%</b>",
		ms, ms, esc(a.uptimeText()), mem.Alloc/1024/1024, mem.Sys/1024/1024, esc(a.formatYangon(a.now())),
		usersCount, groupsCount, vipCount, len(a.ActiveDiceChallenges), len(a.ActiveShanChallenges), bstatus, maint, vipwr))
}

func (a *App) CmdBalance(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	u, err := a.EnsureUser(ctx, m.From)
	if err != nil { a.ReplyHTML(m, "User error"); return }
	a.ReplyHTML(m, fmt.Sprintf("💼 Balance: <b>%s</b> %s", fmtInt(u.Balance), a.Coin))
}

func (a *App) CmdMyBalance(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	if a.IsGroupMessage(m) && !a.EnsureGroupApproved(m) { return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	u, err := a.EnsureUser(ctx, m.From)
	if err != nil { a.ReplyHTML(m, "User error"); return }
	a.ReplyHTML(m, fmt.Sprintf("💼 <b>My Balance</b>\n━━━━━━━━━━━━\nUser: %s\nBalance: <b>%s</b> %s", a.MentionHTML(m.From), fmtInt(u.Balance), a.Coin))
}

func (a *App) CmdOn(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }
	_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{"maintenanceMode": false, "updatedAt": a.now()}})
	a.ReplyHTML(m, "✅ <b>Bot Online</b>\n━━━━━━━━━━━━\nBot ကို <b>ON</b> ပြန်လုပ်ပြီးပါပြီ။ User များ ပုံမှန်သုံးနိုင်ပါပြီ။")
}

func (a *App) CmdOff(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }
	_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{"maintenanceMode": true, "updatedAt": a.now()}})
	a.ReplyHTML(m, "🛠️ <b>Bot Maintenance Mode</b>\n━━━━━━━━━━━━\nBot ကို <b>OFF</b> လုပ်ပြီးပါပြီ။\nUser command အားလုံးကို ခေတ္တပိတ်ထားပါမယ်။")
}

func (a *App) CmdSetTotal(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }
	parts := strings.Fields(m.Text)
	if len(parts) < 2 { a.ReplyHTML(m, "Usage: <code>/settotal 5000000</code>"); return }
	total, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || total < 0 { a.ReplyHTML(m, "Usage: <code>/settotal 5000000</code>"); return }
	_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{
		"totalSupply": total, "ownerBalance": total, "ownerUserId": a.OwnerID, "updatedAt": a.now(),
	}}, options.Update().SetUpsert(true))
	a.ReplyHTML(m, fmt.Sprintf("✅ Treasury total set.\n• Total Supply: <b>%s</b> %s\n• Treasury: <b>%s</b> %s", fmtInt(total), a.Coin, fmtInt(total), a.Coin))
}

func (a *App) CmdTreasury(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }
	a.ReplyHTML(m, fmt.Sprintf("🏦 <b>Treasury</b>\n━━━━━━━━━━━━\nBalance: <b>%s</b> %s\nTotal Supply: <b>%s</b> %s\nOwner ID: <code>%d</code>\n🕒 %s",
		fmtInt(t.OwnerBalance), a.Coin, fmtInt(t.TotalSupply), a.Coin, t.OwnerUserID, esc(a.formatYangon(a.now()))))
}

func (a *App) resolveVIPTarget(ctx context.Context, m *tgbotapi.Message) (*UserDoc, error) {
	parts := strings.Fields(m.Text)
	if m.ReplyToMessage != nil && m.ReplyToMessage.From != nil && !m.ReplyToMessage.From.IsBot {
		return a.EnsureUser(ctx, m.ReplyToMessage.From)
	}
	if len(parts) < 2 { return nil, errors.New("missing target") }
	raw := strings.TrimSpace(parts[1])
	if strings.HasPrefix(raw, "@") { return a.GetUserByUsername(ctx, raw) }
	if id, err := strconv.ParseInt(raw, 10, 64); err == nil { return a.GetUser(ctx, id) }
	return nil, errors.New("invalid target")
}

func (a *App) CmdAddVIP(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only command."); return }
	u, err := a.resolveVIPTarget(ctx, m)
	if err != nil || u == nil {
		a.ReplyHTML(m, "👤 VIP ပေးမယ့်သူကို Reply လုပ်ပါ သို့ <code>/addvip @username</code> / <code>/addvip userId</code> သုံးပါ။")
		return
	}
	_, _ = a.Users.UpdateOne(ctx, bson.M{"userId": u.UserID}, bson.M{"$set": bson.M{"isVip": true, "updatedAt": a.now()}}, options.Update().SetUpsert(true))
	a.ReplyHTML(m, fmt.Sprintf("🌟 <b>VIP Added Successfully</b>\n━━━━━━━━━━━━━━━━\nUser: %s\nအဆင့်အတန်း: <b>VIP Member</b>", a.UserLabelHTML(u)))
}

func (a *App) CmdRemoveVIP(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only command."); return }
	u, err := a.resolveVIPTarget(ctx, m)
	if err != nil || u == nil {
		a.ReplyHTML(m, "👤 VIP ဖြုတ်မယ့်သူကို Reply လုပ်ပါ သို့ <code>/removevip @username</code> / <code>/removevip userId</code> သုံးပါ။")
		return
	}
	_, _ = a.Users.UpdateOne(ctx, bson.M{"userId": u.UserID}, bson.M{"$set": bson.M{"isVip": false, "updatedAt": a.now()}}, options.Update().SetUpsert(true))
	a.ReplyHTML(m, fmt.Sprintf("❌ <b>VIP Status Removed</b>\n━━━━━━━━━━━━━━━━\nUser: %s", a.UserLabelHTML(u)))
}

func (a *App) CmdVIPList(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only command."); return }
	cur, err := a.Users.Find(ctx, bson.M{"isVip": true}, options.Find().SetSort(bson.M{"updatedAt": -1}).SetLimit(50))
	if err != nil { a.ReplyHTML(m, "DB error"); return }
	defer cur.Close(ctx)
	lines := []string{"🌟 <b>VIP List</b>", "━━━━━━━━━━━━"}
	i := 0
	for cur.Next(ctx) {
		var u UserDoc
		if cur.Decode(&u) == nil {
			i++
			lines = append(lines, fmt.Sprintf("%d. %s — <code>%d</code>", i, a.UserLabelHTML(&u), u.UserID))
		}
	}
	if i == 0 { a.ReplyHTML(m, "📭 VIP list ထဲမှာ ဘယ်သူမှမရှိသေးပါ။"); return }
	a.ReplyHTML(m, strings.Join(lines, "\n"))
}

func (a *App) CmdSetVIPWR(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only command."); return }
	parts := strings.Fields(m.Text)
	if len(parts) < 2 {
		a.ReplyHTML(m, fmt.Sprintf("⚙️ <b>Set VIP Win Rate</b>\n━━━━━━━━━━━━\nUsage: <code>/setvipwr 60</code>\nCurrent: <b>%d%%</b>\nApplies to: <b>Slot / Dice / Shan</b>", t.VIPWinRate))
		return
	}
	rate, err := strconv.Atoi(parts[1])
	if err != nil { a.ReplyHTML(m, "⚠️ Usage: <code>/setvipwr 0-100</code>"); return }
	rate = normalizeVIPWinRate(rate)
	_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{"vipWinRate": rate, "updatedAt": a.now()}})
	a.ReplyHTML(m, fmt.Sprintf("✅ <b>VIP Win Rate Updated</b>\n━━━━━━━━━━━━\nNew Rate: <b>%d%%</b>\nApplied to: <b>Slot / Dice / Shan</b>", rate))
}

func (a *App) CmdVIPWR(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only command."); return }
	a.ReplyHTML(m, fmt.Sprintf("📊 <b>VIP Win Rate</b>\n━━━━━━━━━━━━\nCurrent: <b>%d%%</b>\nApplies to: <b>Slot / Dice / Shan</b>\nSet with: <code>/setvipwr 0-100</code>", t.VIPWinRate))
}

func (a *App) CmdDailyClaim(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	if !a.IsGroupMessage(m) { a.ReplyHTML(m, "ℹ️ ဒီ command ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။"); return }
	if !a.EnsureGroupApproved(m) { return }
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = a.EnsureUser(ctx, m.From)
	u, _ := a.GetUser(ctx, m.From.ID)
	dateKey := a.now().Format("2006-01-02")
	if u != nil && u.LastDailyClaimDate != nil && *u.LastDailyClaimDate == dateKey {
		a.ReplyHTML(m, "⏳ ဒီနေ့ Daily Claim ယူပြီးပါပြီ။ Myanmar day ပြောင်းမှ ပြန်ယူလို့ရပါမယ်။")
		return
	}
	amount := rand.Int63n(dailyMax-dailyMin+1) + dailyMin
	if err := a.TreasuryPayToUser(ctx, m.From.ID, amount, bson.M{"type":"daily_claim","chatId":m.Chat.ID}); err != nil {
		a.ReplyHTML(m, "🏦 Treasury မလုံလောက်ပါ။")
		return
	}
	_, _ = a.Users.UpdateOne(ctx, bson.M{"userId": m.From.ID}, bson.M{"$set": bson.M{"lastDailyClaimDate": dateKey, "updatedAt": a.now()}})
	u2, _ := a.GetUser(ctx, m.From.ID)
	a.ReplyHTML(m, fmt.Sprintf("🎁 <b>Daily Claim</b>\n━━━━━━━━━━━━\nUser: %s\nReceived: <b>%s</b> %s\nBalance: <b>%s</b> %s",
		a.MentionHTML(m.From), fmtInt(amount), a.Coin, fmtInt(u2.Balance), a.Coin))
}

func (a *App) CmdTop10(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cur, err := a.Users.Find(ctx, bson.M{}, options.Find().SetSort(bson.M{"balance": -1}).SetLimit(10))
	if err != nil { a.ReplyHTML(m, "No users yet."); return }
	defer cur.Close(ctx)
	lines := []string{"🏆 <b>Top 10 Richest</b>", "━━━━━━━━━━━━"}
	i := 0
	for cur.Next(ctx) {
		var u UserDoc
		if cur.Decode(&u) == nil {
			i++
			lines = append(lines, fmt.Sprintf("%d. %s — <b>%s</b> %s", i, a.UserLabelHTML(&u), fmtInt(u.Balance), a.Coin))
		}
	}
	if i == 0 { a.ReplyHTML(m, "No users yet."); return }
	a.ReplyHTML(m, strings.Join(lines, "\n"))
}

func (a *App) doGift(ctx context.Context, m *tgbotapi.Message, toUserID, amount int64, toLabel string) {
	a.mu.Lock()
	last := a.LastGiftAt[m.From.ID]
	nowMS := time.Now().UnixMilli()
	if nowMS-last < giftCooldownMS {
		a.mu.Unlock()
		sec := int((giftCooldownMS - (nowMS-last) + 999) / 1000)
		a.ReplyHTML(m, fmt.Sprintf("⏳ ခဏစောင့်ပါ… (%ds) ပီးမှ နောက်တစ်ခါ gift လုပ်နိုင်ပါမယ်။", sec))
		return
	}
	a.mu.Unlock()

	if err := a.TransferBalance(ctx, m.From.ID, toUserID, amount, bson.M{"chatId": m.Chat.ID}); err != nil {
		if strings.Contains(err.Error(), "USER_INSUFFICIENT") {
			u, _ := a.GetUser(ctx, m.From.ID)
			bal := int64(0)
			if u != nil { bal = u.Balance }
			a.ReplyHTML(m, fmt.Sprintf("❌ လက်ကျန်ငွေ မလုံလောက်ပါ။ (Balance: <b>%s</b> %s)", fmtInt(bal), a.Coin))
			return
		}
		a.ReplyHTML(m, "Gift failed.")
		return
	}
	a.mu.Lock()
	a.LastGiftAt[m.From.ID] = time.Now().UnixMilli()
	a.mu.Unlock()

	u, _ := a.GetUser(ctx, m.From.ID)
	a.ReplyHTML(m, fmt.Sprintf("🎁 <b>Gift Success</b>\n━━━━━━━━━━━━\nFrom: %s\nTo: %s\nAmount: <b>%s</b> %s\nYour Balance: <b>%s</b> %s",
		a.MentionHTML(m.From), toLabel, fmtInt(amount), a.Coin, fmtInt(u.Balance), a.Coin))
}

func (a *App) CmdGift(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = a.EnsureUser(ctx, m.From)
	parts := strings.Fields(m.Text)
	if len(parts) < 2 {
		a.ReplyHTML(m, "🎁 <b>Gift Usage</b>\n━━━━━━━━━━━━━\n• Reply + <code>/gift 500</code>\n• Mention + <code>/gift @username 500</code>\n• Reply + <code>.gift 500</code> (group)")
		return
	}
	amount, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil || amount <= 0 {
		a.ReplyHTML(m, "🎁 <b>Gift Usage</b>\n━━━━━━━━━━━━━\n• Reply + <code>/gift 500</code>\n• Mention + <code>/gift @username 500</code>\n• Reply + <code>.gift 500</code> (group)")
		return
	}
	var toUserID int64
	var toLabel string
	if m.ReplyToMessage != nil && m.ReplyToMessage.From != nil && !m.ReplyToMessage.From.IsBot && m.ReplyToMessage.From.ID != m.From.ID {
		_, _ = a.EnsureUser(ctx, m.ReplyToMessage.From)
		toUserID = m.ReplyToMessage.From.ID
		toLabel = a.MentionHTML(m.ReplyToMessage.From)
	} else {
		if len(parts) < 3 || !strings.HasPrefix(parts[1], "@") {
			a.ReplyHTML(m, "👤 Reply (/gift 500) သို့ /gift @username 500 သုံးပါ။")
			return
		}
		u, _ := a.GetUserByUsername(ctx, parts[1])
		if u == nil {
			a.ReplyHTML(m, "⚠️ ဒီ @username ကို မတွေ့ပါ။ (သူ bot ကို /start လုပ်ထားရမယ်) သို့ Reply နဲ့ gift ပို့ပါ။")
			return
		}
		if u.UserID == m.From.ID {
			a.ReplyHTML(m, "😅 ကိုယ့်ကိုကိုယ် gift မပို့နိုင်ပါ။")
			return
		}
		toUserID = u.UserID
		toLabel = a.UserLabelHTML(u)
	}
	a.doGift(ctx, m, toUserID, amount, toLabel)
}

func (a *App) CmdDotGift(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	if !a.IsGroupMessage(m) { a.ReplyHTML(m, "ℹ️ <code>.gift</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။"); return }
	if !a.EnsureGroupApproved(m) { return }
	if m.ReplyToMessage == nil || m.ReplyToMessage.From == nil {
		a.ReplyHTML(m, "⚠️ <b>Reply လုပ်ပြီးသုံးပါ</b>\n━━━━━━━━━━━━━━\nExample: Reply + <code>.gift 200</code>")
		return
	}
	if m.ReplyToMessage.From.IsBot { a.ReplyHTML(m, "🤖 Bot ကို gift မပို့နိုင်ပါ။"); return }
	if m.ReplyToMessage.From.ID == m.From.ID { a.ReplyHTML(m, "😅 ကိုယ့်ကိုကိုယ် gift မပို့နိုင်ပါ။"); return }
	parts := strings.Fields(m.Text)
	if len(parts) < 2 { a.ReplyHTML(m, "Example: Reply + <code>.gift 200</code>"); return }
	amount, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || amount <= 0 { a.ReplyHTML(m, "Example: Reply + <code>.gift 200</code>"); return }
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = a.EnsureUser(ctx, m.ReplyToMessage.From)
	a.doGift(ctx, m, m.ReplyToMessage.From.ID, amount, a.MentionHTML(m.ReplyToMessage.From))
}

func currentSlotPayouts(slotRTP float64) map[string]float64 {
	out := map[string]float64{}
	factor := slotRTP / 0.90
	for k, v := range slotBasePayouts { out[k] = math.Round(v*factor*10000) / 10000 }
	return out
}
func weightedPick(items []ReelItem) string {
	total := 0
	for _, it := range items { total += it.W }
	if total <= 0 { return items[len(items)-1].S }
	r := rand.Float64() * float64(total)
	for _, it := range items {
		r -= float64(it.W)
		if r <= 0 { return it.S }
	}
	return items[len(items)-1].S
}
func randomSymbolFromReel(reel []ReelItem) string {
	return reel[rand.Intn(len(reel))].S
}
func isAnyTwo(a,b,c string) bool {
	return (a==b && a!=c) || (a==c && a!=b) || (b==c && b!=a)
}
func calcMultiplier(a,b,c string, payouts map[string]float64) float64 {
	key := a + "," + b + "," + c
	if v, ok := payouts[key]; ok { return v }
	if isAnyTwo(a,b,c) { return payouts["ANY2"] }
	return 0
}
func slotArt(a,b,c string) string {
	box := func(x string) string {
		if x == "7" { return "7️⃣" }
		return x
	}
	return fmt.Sprintf("┏━━━━━━━━━━━━━━━━━━┓\n┃  %s  |  %s  |  %s  ┃\n┗━━━━━━━━━━━━━━━━━━┛", box(a), box(b), box(c))
}
func spinFrame(a,b,c,note,vibe string) string {
	header := "🎰 BIKA Pro Slot"
	if vibe == "glow" { header = "🏆✨ WIN GLOW! ✨🏆" }
	if vibe == "lose" { header = "🥀 BAD LUCK… 🥀" }
	if vibe == "jackpot" { header = "💎🏆 777 JACKPOT! 🏆💎" }
	return fmt.Sprintf("<b>%s</b>\n━━━━━━━━━━━━\n<pre>%s</pre>\n━━━━━━━━━━━━\n%s", esc(header), esc(slotArt(a,b,c)), esc(note))
}
func spinSlotOutcomeNormal(slotRTP float64, payouts map[string]float64) (string,string,string) {
	for i:=0; i<25; i++ {
		x,y,z := weightedPick(slotReels[0]), weightedPick(slotReels[1]), weightedPick(slotReels[2])
		mult := calcMultiplier(x,y,z,payouts)
		if mult <= 0 { return x,y,z }
		if rand.Float64() < slotRTP { return x,y,z }
	}
	for {
		x,y,z := randomSymbolFromReel(slotReels[0]), randomSymbolFromReel(slotReels[1]), randomSymbolFromReel(slotReels[2])
		if calcMultiplier(x,y,z,payouts) <= 0 { return x,y,z }
	}
}
func spinSlotOutcomeVIP(vipWinRate int, payouts map[string]float64) (string,string,string) {
	if rand.Float64() < float64(vipWinRate)/100.0 {
		var combos [][]string
		for k, v := range payouts {
			if k != "ANY2" && v > 0 { combos = append(combos, strings.Split(k, ",")) }
		}
		if len(combos) > 0 {
			c := combos[rand.Intn(len(combos))]
			return c[0], c[1], c[2]
		}
	}
	return spinSlotOutcomeNormal(0.90, payouts)
}
func spinSlotOutcomeForUser(u *UserDoc, vipWinRate int, slotRTP float64, payouts map[string]float64) (string,string,string) {
	if u != nil && u.IsVIP { return spinSlotOutcomeVIP(vipWinRate, payouts) }
	return spinSlotOutcomeNormal(slotRTP, payouts)
}

func (a *App) CmdSlot(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	if !a.IsGroupMessage(m) { a.ReplyHTML(m, "ℹ️ <code>.slot</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။"); return }
	if !a.EnsureGroupApproved(m) { return }

	parts := strings.Fields(m.Text)
	if len(parts) < 2 { a.ReplyHTML(m, "Usage: <code>.slot 1000</code>"); return }
	bet, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil { a.ReplyHTML(m, "Usage: <code>.slot 1000</code>"); return }

	userID := m.From.ID
	a.mu.Lock()
	if len(a.ActiveSlots) >= maxActiveSlots && !a.ActiveSlots[userID] {
		a.mu.Unlock()
		a.ReplyHTML(m, fmt.Sprintf("⛔ <b>Slot Busy</b>\n━━━━━━━━━━━━━━\nအခုတလော တစ်ပြိုင်နက် ဆော့နေသူများလို့ ခဏနားပြီး ပြန်ကြိုးစားပါ။\n(Max active: <b>%d</b>)", maxActiveSlots))
		return
	}
	last := a.LastSlotAt[userID]
	if time.Now().UnixMilli()-last < slotCooldownMS {
		sec := int((slotCooldownMS - (time.Now().UnixMilli()-last) + 999) / 1000)
		a.mu.Unlock()
		a.ReplyHTML(m, fmt.Sprintf("⏳ ခဏစောင့်ပါ… (%ds) နောက်တစ်ခါ spin လုပ်နိုင်ပါမယ်။", sec))
		return
	}
	a.ActiveSlots[userID] = true
	a.mu.Unlock()
	defer func(){
		a.mu.Lock()
		delete(a.ActiveSlots, userID)
		a.mu.Unlock()
	}()

	if bet < slotMinBet || bet > slotMaxBet {
		a.ReplyHTML(m, fmt.Sprintf("🎰 <b>BIKA Pro Slot</b>\n━━━━━━━━━━━━━\nUsage: <code>.slot 1000</code>\nMin: <b>%s</b> %s\nMax: <b>%s</b> %s", fmtInt(slotMinBet), a.Coin, fmtInt(slotMaxBet), a.Coin))
		return
	}

	initA, initB, initC := randomSymbolFromReel(slotReels[0]), randomSymbolFromReel(slotReels[1]), randomSymbolFromReel(slotReels[2])
	sent, _ := a.ReplyHTML(m, spinFrame(initA, initB, initC, "reels spinning…", "spin"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, _ = a.EnsureUser(ctx, m.From)
	t, _ := a.EnsureTreasury(ctx)
	u, _ := a.GetUser(ctx, m.From.ID)

	if err := a.UserPayToTreasury(ctx, userID, bet, bson.M{"type":"slot_bet","bet":bet,"chatId":m.Chat.ID}); err != nil {
		if sent != nil {
			_ = a.EditHTML(sent.Chat.ID, sent.MessageID, "❌ <b>Balance မလုံလောက်ပါ</b>\n━━━━━━━━━━━━━━\nSlot ဆော့ဖို့ လက်ကျန်ငွေ မလုံလောက်ပါ။\nDaily claim / gift / addbalance နဲ့ ငွေစုဆောင်းပြီးမှ ပြန်လာပါ။", nil)
		} else {
			a.ReplyHTML(m, "❌ Balance မလုံလောက်ပါ")
		}
		return
	}

	slotRTP, vipWR := 0.90, 90
	if t != nil {
		slotRTP = normalizeRTP(t.SlotRTP)
		vipWR = normalizeVIPWinRate(t.VIPWinRate)
	}
	payouts := currentSlotPayouts(slotRTP)
	finalA, finalB, finalC := spinSlotOutcomeForUser(u, vipWR, slotRTP, payouts)
	mult := calcMultiplier(finalA, finalB, finalC, payouts)
	payout := int64(0)
	if mult > 0 { payout = int64(float64(bet) * mult) }
	if payout > 0 {
		tr, _ := a.GetTreasury(ctx)
		ownerBal := int64(0)
		if tr != nil { ownerBal = tr.OwnerBalance }
		maxPay := int64(float64(ownerBal) * slotCapPercent)
		if payout > maxPay { payout = maxPay }
		if payout > ownerBal { payout = ownerBal }
	}
	win := payout > 0
	isJackpot := finalA == "7" && finalB == "7" && finalC == "7"

	type frame struct { A,B,C,Note,Vibe string; Delay time.Duration }
	frames := []frame{
		{randomSymbolFromReel(slotReels[0]), randomSymbolFromReel(slotReels[1]), randomSymbolFromReel(slotReels[2]), "rolling…", "spin", 220 * time.Millisecond},
		{finalA, randomSymbolFromReel(slotReels[1]), randomSymbolFromReel(slotReels[2]), "locking…", "spin", 240 * time.Millisecond},
		{finalA, finalB, finalC, "result!", func() string {
			if isJackpot { return "jackpot" }
			if win { return "glow" }
			return "lose"
		}(), 260 * time.Millisecond},
	}
	for _, f := range frames {
		time.Sleep(f.Delay)
		if sent != nil { _ = a.EditHTML(sent.Chat.ID, sent.MessageID, spinFrame(f.A, f.B, f.C, f.Note, f.Vibe), nil) }
	}
	if payout > 0 {
		if err := a.TreasuryPayToUser(ctx, userID, payout, bson.M{"type":"slot_win","bet":bet,"payout":payout,"combo": finalA+","+finalB+","+finalC}); err != nil {
			_ = a.TreasuryPayToUser(ctx, userID, bet, bson.M{"type":"slot_refund","reason":"payout_fail"})
			if sent != nil {
				_ = a.EditHTML(sent.Chat.ID, sent.MessageID, fmt.Sprintf("🎰 <b>BIKA Pro Slot</b>\n━━━━━━━━━━━━━━\n<pre>%s</pre>\n━━━━━━━━━━━━━━\n⚠️ Payout error ဖြစ်လို့ refund ပြန်ပေးလိုက်ပါတယ်။", esc(slotArt(finalA, finalB, finalC))), nil)
			}
			a.mu.Lock(); a.LastSlotAt[userID] = time.Now().UnixMilli(); a.mu.Unlock()
			return
		}
	}
	a.mu.Lock(); a.LastSlotAt[userID] = time.Now().UnixMilli(); a.mu.Unlock()
	net := payout - bet
	headline := "❌ LOSE"
	if payout > 0 { headline = "✅ WIN" }
	if isJackpot { headline = "🏆 JACKPOT 777!" }
	finalMsg := fmt.Sprintf("🎰 <b>BIKA Pro Slot</b>\n━━━━━━━━━━━\n<pre>%s</pre>\n━━━━━━━━━━━\n<b>%s</b>\nBet: <b>%s</b> %s\nPayout: <b>%s</b> %s\nNet: <b>%s</b> %s",
		esc(slotArt(finalA, finalB, finalC)), esc(headline), fmtInt(bet), a.Coin, fmtInt(payout), a.Coin, fmtInt(net), a.Coin)
	if sent != nil { _ = a.EditHTML(sent.Chat.ID, sent.MessageID, finalMsg, nil) } else { a.ReplyHTML(m, finalMsg) }
}

func challengeKeyboard(kind, id string) tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Accept", kind + ":accept:" + id),
			tgbotapi.NewInlineKeyboardButtonData("❌ Cancel", kind + ":cancel:" + id),
		),
	)
}

func diceChallengeText(a *App, challenger, target *tgbotapi.User, bet int64) string {
	return fmt.Sprintf("🎲 <b>Dice Duel Challenge</b>\n━━━━━━━━━━━━\nစိန်ခေါ်သူ: %s\nလက်ခံသူ: %s\nBet: <b>%s</b> %s\nWinner gets: <b>98%%</b>\n━━━━━━━━━━━━\nReply ထောက်ထားတဲ့သူပဲ Accept လုပ်နိုင်ပါတယ်။",
		a.MentionHTML(challenger), a.MentionHTML(target), fmtInt(bet), a.Coin)
}

func shanChallengeText(a *App, challenger, target *tgbotapi.User, bet int64) string {
	return fmt.Sprintf("🃏 <b>Shan Koe Mee Challenge</b>\n━━━━━━━━━━━━\nစိန်ခေါ်သူ: %s\nလက်ခံသူ: %s\nBet: <b>%s</b> %s\nWinner gets: <b>98%%</b> (normal)\nSuit Triple: <b>pot + extra one bet</b>\n━━━━━━━━━━━━\nReply ထောက်ထားတဲ့သူပဲ Accept လုပ်နိုင်ပါတယ်။",
		a.MentionHTML(challenger), a.MentionHTML(target), fmtInt(bet), a.Coin)
}

func (a *App) CmdDice(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	if !a.IsGroupMessage(m) { a.ReplyHTML(m, "ℹ️ <code>.dice</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။"); return }
	if !a.EnsureGroupApproved(m) { return }
	parts := strings.Fields(m.Text)
	if len(parts) < 2 { return }
	bet, _ := strconv.ParseInt(parts[1], 10, 64)
	if bet < diceMinBet || bet > diceMaxBet {
		a.ReplyHTML(m, fmt.Sprintf("🎲 <b>Dice Duel</b>\n━━━━━━━━━━━\nUsage: Reply + <code>.dice 200</code>\nMin: <b>%s</b> %s\nMax: <b>%s</b> %s", fmtInt(diceMinBet), a.Coin, fmtInt(diceMaxBet), a.Coin))
		return
	}
	replyFrom := m.ReplyToMessage
	if replyFrom == nil || replyFrom.From == nil {
		a.ReplyHTML(m, "⚠️ <b>Reply လုပ်ပြီးသုံးပါ</b>\n━━━━━━━━━━━━\nExample: Reply + <code>.dice 200</code>")
		return
	}
	if replyFrom.From.IsBot { a.ReplyHTML(m, "🤖 Bot ကို challenge မလုပ်နိုင်ပါ။"); return }
	if replyFrom.From.ID == m.From.ID { a.ReplyHTML(m, "😅 ကိုယ့်ကိုကိုယ် challenge မလုပ်နိုင်ပါ။"); return }
	a.mu.Lock()
	busy := len(a.ActiveDiceChallenges) >= diceMaxActive
	a.mu.Unlock()
	if busy {
		a.ReplyHTML(m, "⛔ <b>Dice Busy</b>\n━━━━━━━━━━━\nအခု Dice challenge များလွန်းနေပါတယ်။ ခဏနားပြီး ပြန်ကြိုးစားပါ။")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = a.EnsureUser(ctx, m.From)
	_, _ = a.EnsureUser(ctx, replyFrom.From)
	challenger, _ := a.GetUser(ctx, m.From.ID)
	if challenger == nil || challenger.Balance < bet {
		lack := bet
		if challenger != nil { lack = bet - challenger.Balance }
		if lack < 0 { lack = 0 }
		bal := int64(0)
		if challenger != nil { bal = challenger.Balance }
		a.ReplyHTML(m, fmt.Sprintf("❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>\n━━━━━━━━━━━\nBet: <b>%s</b> %s\nYour Balance: <b>%s</b> %s\nNeed More: <b>%s</b> %s",
			fmtInt(bet), a.Coin, fmtInt(bal), a.Coin, fmtInt(lack), a.Coin))
		return
	}
	cid := tokenHex(6)
	sent, _ := a.ReplyHTMLWithMarkup(m, diceChallengeText(a, m.From, replyFrom.From, bet), challengeKeyboard("dice", cid))
	if sent == nil { return }
	a.mu.Lock()
	a.ActiveDiceChallenges[cid] = &Challenge{ID: cid, ChatID: m.Chat.ID, MsgID: sent.MessageID, ChallengerID: m.From.ID, OpponentID: replyFrom.From.ID, Bet: bet, Status: "PENDING", CreatedAt: time.Now()}
	a.mu.Unlock()
}

func (a *App) CBDice(cb *tgbotapi.CallbackQuery) {
	parts := strings.Split(cb.Data, ":")
	if len(parts) != 3 { a.AnswerCallback(cb.ID, "", false); return }
	action, cid := parts[1], parts[2]
	a.mu.Lock()
	c := a.ActiveDiceChallenges[cid]
	a.mu.Unlock()
	if c == nil { a.AnswerCallback(cb.ID, "Challenge not found.", true); return }

	if action == "cancel" {
		if cb.From.ID != c.ChallengerID && cb.From.ID != a.OwnerID {
			a.AnswerCallback(cb.ID, "Only challenger can cancel.", true); return
		}
		a.mu.Lock()
		delete(a.ActiveDiceChallenges, cid)
		a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "❌ <b>Dice Duel Cancelled</b>\n━━━━━━━━━━━━\nChallenge cancelled.", nil)
		a.AnswerCallback(cb.ID, "", false)
		return
	}
	if action != "accept" { a.AnswerCallback(cb.ID, "", false); return }
	if cb.From.ID != c.OpponentID { a.AnswerCallback(cb.ID, "Reply target only.", true); return }

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, _ = a.EnsureUser(ctx, cb.From)
	challenger, _ := a.GetUser(ctx, c.ChallengerID)
	opponent, _ := a.GetUser(ctx, c.OpponentID)
	if challenger == nil || challenger.Balance < c.Bet {
		a.mu.Lock(); delete(a.ActiveDiceChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "❌ Challenger balance insufficient.", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}
	if opponent == nil || opponent.Balance < c.Bet {
		a.mu.Lock(); delete(a.ActiveDiceChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "❌ Opponent balance insufficient.", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}
	if err := a.UserPayToTreasury(ctx, c.ChallengerID, c.Bet, bson.M{"type":"dice_bet","challengeId": cid}); err != nil {
		a.mu.Lock(); delete(a.ActiveDiceChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "⚠️ Bet transfer error.", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}
	if err := a.UserPayToTreasury(ctx, c.OpponentID, c.Bet, bson.M{"type":"dice_bet","challengeId": cid}); err != nil {
		_ = a.TreasuryPayToUser(ctx, c.ChallengerID, c.Bet, bson.M{"type":"dice_refund","challengeId": cid, "reason":"partial_fail"})
		a.mu.Lock(); delete(a.ActiveDiceChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "⚠️ Bet transfer error.", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}

	_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "🎲 <b>Dice Duel Result</b>\n━━━━━━━━━━━━\nRolling dice...", nil)
	time.Sleep(800 * time.Millisecond)

	d1msg, err1 := a.Bot.Send(tgbotapi.DiceConfig{BaseChat: tgbotapi.BaseChat{ChatID: c.ChatID, ReplyToMessageID: c.MsgID}, Emoji: "🎲"})
	time.Sleep(1100 * time.Millisecond)
	d2msg, err2 := a.Bot.Send(tgbotapi.DiceConfig{BaseChat: tgbotapi.BaseChat{ChatID: c.ChatID, ReplyToMessageID: c.MsgID}, Emoji: "🎲"})
	if err1 != nil || err2 != nil || d1msg.Dice == nil || d2msg.Dice == nil {
		_ = a.TreasuryPayToUser(ctx, c.ChallengerID, c.Bet, bson.M{"type":"dice_refund","challengeId": cid})
		_ = a.TreasuryPayToUser(ctx, c.OpponentID, c.Bet, bson.M{"type":"dice_refund","challengeId": cid})
		a.mu.Lock(); delete(a.ActiveDiceChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "⚠️ Dice roll failed. Bet refund ပြန်ပေးပါပြီ။", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}

	d1, d2 := int(d1msg.Dice.Value), int(d2msg.Dice.Value)
	challengerLabel, opponentLabel := a.UserLabelHTML(challenger), a.UserLabelHTML(opponent)
	pot := c.Bet * 2
	houseCut := int64(math.Round(float64(pot) * 0.02))
	var text string

	if d1 > d2 {
		payout := pot - houseCut
		if err := a.TreasuryPayToUser(ctx, c.ChallengerID, payout, bson.M{"type":"dice_win","challengeId": cid, "pot": pot}); err != nil {
			payout = 0
		}
		text = fmt.Sprintf("🎲 <b>Dice Duel Result</b>\n━━━━━━━━━━━━\nစိန်ခေါ်သူ: %s → <b>%d</b>\nလက်ခံသူ: %s → <b>%d</b>\n━━━━━━━━━━━━\n🏆 Winner: %s\n💰 Pot: <b>%s</b> %s\n✅ Winner gets: <b>%s</b> %s (98%%)\n🏦 House cut: <b>2%%</b> (%s %s)",
			challengerLabel, d1, opponentLabel, d2, challengerLabel, fmtInt(pot), a.Coin, fmtInt(payout), a.Coin, fmtInt(houseCut), a.Coin)
	} else if d2 > d1 {
		payout := pot - houseCut
		if err := a.TreasuryPayToUser(ctx, c.OpponentID, payout, bson.M{"type":"dice_win","challengeId": cid, "pot": pot}); err != nil {
			payout = 0
		}
		text = fmt.Sprintf("🎲 <b>Dice Duel Result</b>\n━━━━━━━━━━━━\nစိန်ခေါ်သူ: %s → <b>%d</b>\nလက်ခံသူ: %s → <b>%d</b>\n━━━━━━━━━━━━\n🏆 Winner: %s\n💰 Pot: <b>%s</b> %s\n✅ Winner gets: <b>%s</b> %s (98%%)\n🏦 House cut: <b>2%%</b> (%s %s)",
			challengerLabel, d1, opponentLabel, d2, opponentLabel, fmtInt(pot), a.Coin, fmtInt(payout), a.Coin, fmtInt(houseCut), a.Coin)
	} else {
		_ = a.TreasuryPayToUser(ctx, c.ChallengerID, c.Bet, bson.M{"type":"dice_refund","challengeId": cid})
		_ = a.TreasuryPayToUser(ctx, c.OpponentID, c.Bet, bson.M{"type":"dice_refund","challengeId": cid})
		text = fmt.Sprintf("🎲 <b>Dice Duel Result</b>\n━━━━━━━━━━━━\nစိန်ခေါ်သူ: %s → <b>%d</b>\nလက်ခံသူ: %s → <b>%d</b>\n━━━━━━━━━━━━\n🤝 <b>TIE!</b> — Bet refund ပြန်ပေးပါပြီ",
			challengerLabel, d1, opponentLabel, d2)
	}
	a.mu.Lock(); delete(a.ActiveDiceChallenges, cid); a.mu.Unlock()
	_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, text, nil)
	a.AnswerCallback(cb.ID, "", false)
}

func buildDeck() []Card {
	var deck []Card
	for _, s := range suits {
		for _, r := range ranks {
			deck = append(deck, Card{Rank:r, Suit:s})
		}
	}
	return deck
}
func shuffleDeck(deck []Card) []Card {
	out := append([]Card(nil), deck...)
	rand.Shuffle(len(out), func(i,j int){ out[i], out[j] = out[j], out[i] })
	return out
}
func drawCards(deck *[]Card, n int) []Card {
	out := append([]Card(nil), (*deck)[:n]...)
	*deck = (*deck)[n:]
	return out
}
func rankValue(rank string) int {
	switch rank {
	case "A": return 1
	case "10","J","Q","K": return 0
	default:
		v, _ := strconv.Atoi(rank)
		return v
	}
}
func calcPoints(cards []Card) int {
	sum := 0
	for _, c := range cards { sum += rankValue(c.Rank) }
	return sum % 10
}
func isShanKoeMee(cards []Card) bool {
	return len(cards) == 3 && cards[0].Rank == cards[1].Rank && cards[1].Rank == cards[2].Rank
}
func isZatToe(cards []Card) bool {
	if len(cards) != 3 { return false }
	for _, c := range cards {
		if c.Rank != "J" && c.Rank != "Q" && c.Rank != "K" { return false }
	}
	return true
}
func isSuitTriple(cards []Card) bool {
	return len(cards) == 3 && cards[0].Suit == cards[1].Suit && cards[1].Suit == cards[2].Suit
}
func highCardWeight(rank string) int {
	switch rank {
	case "A": return 1
	case "J": return 11
	case "Q": return 12
	case "K": return 13
	default:
		v, _ := strconv.Atoi(rank)
		return v
	}
}
func sortedHighRanks(cards []Card) []int {
	var out []int
	for _, c := range cards { out = append(out, highCardWeight(c.Rank)) }
	sort.Slice(out, func(i,j int) bool { return out[i] > out[j] })
	return out
}
func handInfo(cards []Card) HandInfo {
	if isShanKoeMee(cards) {
		return HandInfo{Category:4, Name:"Shan Koe Mee", Points: calcPoints(cards), TieBreaker: sortedHighRanks(cards)}
	}
	if isZatToe(cards) {
		return HandInfo{Category:3, Name:"Zat Toe", Points: calcPoints(cards), TieBreaker: sortedHighRanks(cards)}
	}
	if isSuitTriple(cards) {
		return HandInfo{Category:2, Name:"Suit Triple", Points: calcPoints(cards), TieBreaker: sortedHighRanks(cards)}
	}
	pts := calcPoints(cards)
	return HandInfo{Category:1, Name: fmt.Sprintf("Point %d", pts), Points: pts, TieBreaker: sortedHighRanks(cards)}
}
func compareTieBreaker(a,b []int) int {
	maxl := len(a)
	if len(b) > maxl { maxl = len(b) }
	for i:=0; i<maxl; i++ {
		av, bv := -1, -1
		if i < len(a) { av = a[i] }
		if i < len(b) { bv = b[i] }
		if av > bv { return 1 }
		if bv > av { return -1 }
	}
	return 0
}
func compareHands(cardsA, cardsB []Card) CompareResult {
	ia, ib := handInfo(cardsA), handInfo(cardsB)
	if ia.Category > ib.Category { return CompareResult{Winner:"A", InfoA: ia, InfoB: ib} }
	if ib.Category > ia.Category { return CompareResult{Winner:"B", InfoA: ia, InfoB: ib} }
	if ia.Points > ib.Points { return CompareResult{Winner:"A", InfoA: ia, InfoB: ib} }
	if ib.Points > ia.Points { return CompareResult{Winner:"B", InfoA: ia, InfoB: ib} }
	tb := compareTieBreaker(ia.TieBreaker, ib.TieBreaker)
	if tb > 0 { return CompareResult{Winner:"A", InfoA: ia, InfoB: ib} }
	if tb < 0 { return CompareResult{Winner:"B", InfoA: ia, InfoB: ib} }
	return CompareResult{Winner:"TIE", InfoA: ia, InfoB: ib}
}
func cardBox(card Card) []string {
	left := fmt.Sprintf("%-2s", card.Rank)
	right := fmt.Sprintf("%2s", card.Rank)
	return []string{
		"┌───────┐",
		fmt.Sprintf("│ %s    │", left),
		fmt.Sprintf("│   %s  │", card.Suit),
		fmt.Sprintf("│    %s│", right),
		"└───────┘",
	}
}
func renderCardsRow(cards []Card) string {
	boxes := make([][]string, 0, len(cards))
	for _, c := range cards { boxes = append(boxes, cardBox(c)) }
	var lines []string
	for i:=0; i<5; i++ {
		var parts []string
		for _, b := range boxes { parts = append(parts, b[i]) }
		lines = append(lines, strings.Join(parts, " "))
	}
	return strings.Join(lines, "\n")
}
func drawShanHandsForUsers(userA, userB *UserDoc, vipWinRate int) ShanDeal {
	tryOnce := func() ShanDeal {
		deck := shuffleDeck(buildDeck())
		cardsA := drawCards(&deck, 3)
		cardsB := drawCards(&deck, 3)
		res := compareHands(cardsA, cardsB)
		return ShanDeal{CardsA: cardsA, CardsB: cardsB, Result: res}
	}
	vipChance := float64(vipWinRate) / 100.0
	vipA := userA != nil && userA.IsVIP
	vipB := userB != nil && userB.IsVIP

	if vipA && !vipB && rand.Float64() < vipChance {
		for i:=0; i<120; i++ {
			out := tryOnce()
			if out.Result.Winner == "A" { return out }
		}
	}
	if vipB && !vipA && rand.Float64() < vipChance {
		for i:=0; i<120; i++ {
			out := tryOnce()
			if out.Result.Winner == "B" { return out }
		}
	}
	return tryOnce()
}

func (a *App) CmdShan(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	if !a.IsGroupMessage(m) { a.ReplyHTML(m, "ℹ️ <code>.shan</code> ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။"); return }
	if !a.EnsureGroupApproved(m) { return }
	parts := strings.Fields(m.Text)
	if len(parts) < 2 { return }
	bet, _ := strconv.ParseInt(parts[1], 10, 64)
	if bet < shanMinBet || bet > shanMaxBet {
		a.ReplyHTML(m, fmt.Sprintf("🃏 <b>Shan Duel</b>\n━━━━━━━━━━━\nUsage: Reply + <code>.shan 500</code>\nMin: <b>%s</b> %s\nMax: <b>%s</b> %s", fmtInt(shanMinBet), a.Coin, fmtInt(shanMaxBet), a.Coin))
		return
	}
	replyFrom := m.ReplyToMessage
	if replyFrom == nil || replyFrom.From == nil {
		a.ReplyHTML(m, "⚠️ <b>Reply လုပ်ပြီးသုံးပါ</b>\n━━━━━━━━━━━━\nExample: Reply + <code>.shan 500</code>")
		return
	}
	if replyFrom.From.IsBot { a.ReplyHTML(m, "🤖 Bot ကို challenge မလုပ်နိုင်ပါ။"); return }
	if replyFrom.From.ID == m.From.ID { a.ReplyHTML(m, "😅 ကိုယ့်ကိုကိုယ် challenge မလုပ်နိုင်ပါ။"); return }
	a.mu.Lock()
	busy := len(a.ActiveShanChallenges) >= shanMaxActive
	a.mu.Unlock()
	if busy {
		a.ReplyHTML(m, "⛔ <b>Shan Busy</b>\n━━━━━━━━━━━\nအခု Shan challenge များနေပါတယ်။ ခဏနားပြီး ပြန်ကြိုးစားပါ။")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = a.EnsureUser(ctx, m.From)
	_, _ = a.EnsureUser(ctx, replyFrom.From)
	challenger, _ := a.GetUser(ctx, m.From.ID)
	if challenger == nil || challenger.Balance < bet {
		lack := bet
		if challenger != nil { lack = bet - challenger.Balance }
		if lack < 0 { lack = 0 }
		bal := int64(0)
		if challenger != nil { bal = challenger.Balance }
		a.ReplyHTML(m, fmt.Sprintf("❌ <b>လက်ကျန်ငွေ မလုံလောက်ပါ</b>\n━━━━━━━━━━━\nBet: <b>%s</b> %s\nYour Balance: <b>%s</b> %s\nNeed More: <b>%s</b> %s",
			fmtInt(bet), a.Coin, fmtInt(bal), a.Coin, fmtInt(lack), a.Coin))
		return
	}
	cid := tokenHex(6)
	sent, _ := a.ReplyHTMLWithMarkup(m, shanChallengeText(a, m.From, replyFrom.From, bet), challengeKeyboard("shan", cid))
	if sent == nil { return }
	a.mu.Lock()
	a.ActiveShanChallenges[cid] = &Challenge{ID: cid, ChatID: m.Chat.ID, MsgID: sent.MessageID, ChallengerID: m.From.ID, OpponentID: replyFrom.From.ID, Bet: bet, Status: "PENDING", CreatedAt: time.Now()}
	a.mu.Unlock()
}

func (a *App) CBShan(cb *tgbotapi.CallbackQuery) {
	parts := strings.Split(cb.Data, ":")
	if len(parts) != 3 { a.AnswerCallback(cb.ID, "", false); return }
	action, cid := parts[1], parts[2]
	a.mu.Lock()
	c := a.ActiveShanChallenges[cid]
	a.mu.Unlock()
	if c == nil { a.AnswerCallback(cb.ID, "Challenge not found.", true); return }

	if action == "cancel" {
		if cb.From.ID != c.ChallengerID && cb.From.ID != a.OwnerID {
			a.AnswerCallback(cb.ID, "Only challenger can cancel.", true); return
		}
		a.mu.Lock(); delete(a.ActiveShanChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "❌ <b>Shan Duel Cancelled</b>\n━━━━━━━━━━━━\nChallenge cancelled.", nil)
		a.AnswerCallback(cb.ID, "", false)
		return
	}
	if cb.From.ID != c.OpponentID {
		a.AnswerCallback(cb.ID, "Reply target only.", true); return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	challenger, _ := a.GetUser(ctx, c.ChallengerID)
	opponent, _ := a.GetUser(ctx, c.OpponentID)
	if challenger == nil || challenger.Balance < c.Bet {
		a.mu.Lock(); delete(a.ActiveShanChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "❌ Challenger balance insufficient.", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}
	if opponent == nil || opponent.Balance < c.Bet {
		a.mu.Lock(); delete(a.ActiveShanChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "❌ Opponent balance insufficient.", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}
	if err := a.UserPayToTreasury(ctx, c.ChallengerID, c.Bet, bson.M{"type":"shan_bet","challengeId":cid}); err != nil {
		a.mu.Lock(); delete(a.ActiveShanChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "⚠️ Bet transfer error.", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}
	if err := a.UserPayToTreasury(ctx, c.OpponentID, c.Bet, bson.M{"type":"shan_bet","challengeId":cid}); err != nil {
		_ = a.TreasuryPayToUser(ctx, c.ChallengerID, c.Bet, bson.M{"type":"shan_refund","challengeId": cid, "reason":"partial_fail"})
		a.mu.Lock(); delete(a.ActiveShanChallenges, cid); a.mu.Unlock()
		_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, "⚠️ Bet transfer error.", nil)
		a.AnswerCallback(cb.ID, "", false); return
	}

	t, _ := a.EnsureTreasury(ctx)
	vipWR := 90
	if t != nil { vipWR = normalizeVIPWinRate(t.VIPWinRate) }
	out := drawShanHandsForUsers(challenger, opponent, vipWR)
	cardsA, cardsB, result := out.CardsA, out.CardsB, out.Result
	pot := c.Bet * 2
	houseCut := int64(math.Round(float64(pot) * 0.02))

	extra := int64(0)
	payout := int64(0)
	winnerLabel := ""
	if result.Winner == "A" {
		winnerLabel = a.UserLabelHTML(challenger)
		payout = pot - houseCut
		if result.InfoA.Name == "Suit Triple" { extra = c.Bet }
		tr, _ := a.GetTreasury(ctx)
		ownerBal := int64(0); if tr != nil { ownerBal = tr.OwnerBalance }
		if payout + extra > ownerBal { payout = ownerBal } else { payout = payout + extra }
		_ = a.TreasuryPayToUser(ctx, c.ChallengerID, payout, bson.M{"type":"shan_win","challengeId":cid,"pot":pot})
	} else if result.Winner == "B" {
		winnerLabel = a.UserLabelHTML(opponent)
		payout = pot - houseCut
		if result.InfoB.Name == "Suit Triple" { extra = c.Bet }
		tr, _ := a.GetTreasury(ctx)
		ownerBal := int64(0); if tr != nil { ownerBal = tr.OwnerBalance }
		if payout + extra > ownerBal { payout = ownerBal } else { payout = payout + extra }
		_ = a.TreasuryPayToUser(ctx, c.OpponentID, payout, bson.M{"type":"shan_win","challengeId":cid,"pot":pot})
	} else {
		_ = a.TreasuryPayToUser(ctx, c.ChallengerID, c.Bet, bson.M{"type":"shan_refund","challengeId":cid})
		_ = a.TreasuryPayToUser(ctx, c.OpponentID, c.Bet, bson.M{"type":"shan_refund","challengeId":cid})
	}

	chLabel, opLabel := a.UserLabelHTML(challenger), a.UserLabelHTML(opponent)
	text := fmt.Sprintf("🃏 <b>Shan Duel Result</b>\n━━━━━━━━━━━━\nစိန်ခေါ်သူ: %s\n<pre>%s</pre>\nHand: <b>%s</b>\nPoint: <b>%d</b>\n\nလက်ခံသူ: %s\n<pre>%s</pre>\nHand: <b>%s</b>\nPoint: <b>%d</b>\n━━━━━━━━━━━━\n",
		chLabel, esc(renderCardsRow(cardsA)), esc(result.InfoA.Name), result.InfoA.Points,
		opLabel, esc(renderCardsRow(cardsB)), esc(result.InfoB.Name), result.InfoB.Points)
	if result.Winner == "TIE" {
		text += "🤝 <b>TIE!</b> — Bet refund ပြန်ပေးပါပြီ"
	} else {
		text += fmt.Sprintf("🏆 Winner: %s\n💰 Pot: <b>%s</b> %s\n✅ Winner gets: <b>%s</b> %s", winnerLabel, fmtInt(pot), a.Coin, fmtInt(payout), a.Coin)
		if extra > 0 { text += fmt.Sprintf("\n🎁 Suit Triple Bonus: <b>%s</b> %s", fmtInt(extra), a.Coin) }
		text += fmt.Sprintf("\n🏦 House cut: <b>2%%</b> (%s %s)", fmtInt(houseCut), a.Coin)
	}
	a.mu.Lock(); delete(a.ActiveShanChallenges, cid); a.mu.Unlock()
	_ = a.EditHTML(cb.Message.Chat.ID, cb.Message.MessageID, text, nil)
	a.AnswerCallback(cb.ID, "", false)
}

func (a *App) CmdBroadcast(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }

	text := ""
	parts := strings.SplitN(m.Text, " ", 2)
	if len(parts) > 1 { text = strings.TrimSpace(parts[1]) }
	sourceMessage := m.ReplyToMessage

	if text == "" && sourceMessage == nil {
		a.ReplyHTML(m, "📣 <b>Broadcast</b>\n━━━━━━━━━━━━\nUsage:\n• <code>/broadcast မင်္ဂလာပါ...</code>\n• (or) Reply to a message + <code>/broadcast</code>")
		return
	}

	a.mu.Lock()
	if a.CurrentBroadcast != nil && !a.CurrentBroadcast.Cancelled {
		a.mu.Unlock()
		a.ReplyHTML(m, "⚠️ Broadcast တစ်ခု လက်ရှိ run နေပါတယ်။ ရပ်ချင်ရင် <code>/broadcastend</code> သုံးပါ။")
		return
	}
	a.mu.Unlock()

	runID := fmt.Sprintf("%d_%s", time.Now().Unix(), tokenHex(3))
	progress, _ := a.ReplyHTML(m, fmt.Sprintf("📣 Broadcasting…\nTarget: users + groups\nStatus: <b>Running</b>\nRun ID: <code>%s</code>", runID))
	if progress == nil { return }

	a.mu.Lock()
	a.CurrentBroadcast = &BroadcastRun{ID: runID, Cancelled: false, OwnerChatID: m.Chat.ID, ProgressMessageID: progress.MessageID, StartedAt: time.Now()}
	a.mu.Unlock()
	_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{"broadcastRunning": true, "broadcastRunId": runID, "updatedAt": a.now()}})

	var targets []struct{ ChatID int64; Kind string }
	seen := map[int64]bool{}
	cur1, _ := a.Users.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"userId":1}))
	if cur1 != nil {
		defer cur1.Close(ctx)
		for cur1.Next(ctx) {
			var row struct{ UserID int64 `bson:"userId"` }
			if cur1.Decode(&row) == nil && row.UserID != 0 && !seen[row.UserID] {
				seen[row.UserID] = true
				targets = append(targets, struct{ChatID int64; Kind string}{row.UserID, "user"})
			}
		}
	}
	cur2, _ := a.Groups.Find(ctx, bson.M{"approvalStatus":"approved"}, options.Find().SetProjection(bson.M{"groupId":1}))
	if cur2 != nil {
		defer cur2.Close(ctx)
		for cur2.Next(ctx) {
			var row struct{ GroupID int64 `bson:"groupId"` }
			if cur2.Decode(&row) == nil && row.GroupID != 0 && !seen[row.GroupID] {
				seen[row.GroupID] = true
				targets = append(targets, struct{ChatID int64; Kind string}{row.GroupID, "group"})
			}
		}
	}

	go func(source *tgbotapi.Message, text string, ownerChatID int64, progressMessageID int, runID string, targets []struct{ChatID int64; Kind string}) {
		ok, fail, skipped, userSent, groupSent := 0,0,0,0,0
		stopped := false
		for i, t := range targets {
			a.mu.Lock()
			stoppedNow := a.CurrentBroadcast == nil || a.CurrentBroadcast.Cancelled
			a.mu.Unlock()
			if stoppedNow { stopped = true; break }

			success := false
			if source != nil {
				success = a.CopyMessage(t.ChatID, source.Chat.ID, source.MessageID)
			} else {
				success = a.SendText(t.ChatID, text)
			}
			if success {
				ok++
				if t.Kind == "user" { userSent++ } else { groupSent++ }
			} else {
				fail++
			}
			if (i+1)%25 == 0 {
				_ = a.EditHTML(ownerChatID, progressMessageID, fmt.Sprintf("📣 <b>Broadcast Progress</b>\n━━━━━━━━━━━━\nRun ID: <code>%s</code>\nProcessed: <b>%d</b> / <b>%d</b>\nUsers sent: <b>%d</b>\nGroups sent: <b>%d</b>\nSkipped: <b>%d</b>\nFailed: <b>%d</b>\nStatus: <b>Running</b>",
					runID, i+1, len(targets), userSent, groupSent, skipped, fail), nil)
			}
			time.Sleep(20 * time.Millisecond)
		}

		a.mu.Lock()
		progressID := 0
		if a.CurrentBroadcast != nil {
			progressID = a.CurrentBroadcast.ProgressMessageID
		}
		a.CurrentBroadcast = nil
		a.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{"broadcastRunning": false, "broadcastRunId": nil, "updatedAt": a.now()}})
		if progressID != 0 { a.DeleteMessage(ownerChatID, progressID) }

		if stopped {
			_, _ = a.SendHTML(ownerChatID, fmt.Sprintf("🛑 Broadcast stopped.\n• Sent: <b>%d</b>\n• Users: <b>%d</b>\n• Groups: <b>%d</b>\n• Skipped: <b>%d</b>\n• Failed: <b>%d</b>", ok, userSent, groupSent, skipped, fail))
		} else {
			_, _ = a.SendHTML(ownerChatID, fmt.Sprintf("✅ Broadcast done.\n• Total Sent: <b>%d</b>\n• Users: <b>%d</b>\n• Groups: <b>%d</b>\n• Skipped: <b>%d</b>\n• Failed: <b>%d</b>", ok, userSent, groupSent, skipped, fail))
		}
	}(sourceMessage, text, m.Chat.ID, progress.MessageID, runID, targets)
}

func (a *App) CmdBroadcastEnd(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }
	a.mu.Lock()
	if a.CurrentBroadcast == nil || a.CurrentBroadcast.Cancelled {
		a.mu.Unlock()
		_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{"broadcastRunning": false, "broadcastRunId": nil, "updatedAt": a.now()}})
		a.ReplyHTML(m, "ℹ️ လက်ရှိ run နေတဲ့ broadcast မရှိပါ။")
		return
	}
	a.CurrentBroadcast.Cancelled = true
	ownerChatID := a.CurrentBroadcast.OwnerChatID
	progressID := a.CurrentBroadcast.ProgressMessageID
	a.mu.Unlock()
	if ownerChatID != 0 && progressID != 0 { a.DeleteMessage(ownerChatID, progressID) }
	_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{"broadcastRunning": false, "broadcastRunId": nil, "updatedAt": a.now()}})
	a.ReplyHTML(m, "🛑 လက်ရှိ broadcast ကို ရပ်တန့်ပြီး clear လုပ်ပြီးပါပြီ။")
}

func (a *App) CmdApprove(m *tgbotapi.Message) {
	if !a.IsGroupMessage(m) { a.ReplyHTML(m, "ℹ️ ဒီ command ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။"); return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ <b>Owner only</b>"); return }
	g, err := a.EnsureGroup(ctx, m.Chat)
	if err != nil { a.ReplyHTML(m, "Group error"); return }
	if !g.BotIsAdmin {
		a.ReplyHTML(m, "⚠️ <b>Bot ကို Admin မပေးရသေးပါ</b>\n━━━━━━━━━━━━\nအရင်ဆုံး bot ကို admin ပေးပါ။")
		return
	}
	_, _ = a.Groups.UpdateOne(ctx, bson.M{"groupId": m.Chat.ID}, bson.M{"$set": bson.M{"approvalStatus":"approved","approvedBy": m.From.ID, "updatedAt": a.now()}}, options.Update().SetUpsert(true))
	a.ReplyHTML(m, "✅ <b>Group Approved</b>\n━━━━━━━━━━━━\nဒီ group မှာ bot ကို အသုံးပြုလို့ရပါပြီ။")
}

func (a *App) CmdReject(m *tgbotapi.Message) {
	if !a.IsGroupMessage(m) { a.ReplyHTML(m, "ℹ️ ဒီ command ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။"); return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ <b>Owner only</b>"); return }
	_, _ = a.Groups.UpdateOne(ctx, bson.M{"groupId": m.Chat.ID}, bson.M{"$set": bson.M{"approvalStatus":"rejected","approvedBy": m.From.ID, "updatedAt": a.now()}}, options.Update().SetUpsert(true))
	a.ReplyHTML(m, "❌ <b>Group Rejected</b>\n━━━━━━━━━━━━\nဒီ group ကို approve မပေးထားပါ။")
}

func (a *App) CmdGroupStatus(m *tgbotapi.Message) {
	if !a.IsGroupMessage(m) { a.ReplyHTML(m, "ℹ️ ဒီ command ကို group ထဲမှာပဲ သုံးနိုင်ပါတယ်။"); return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g, err := a.EnsureGroup(ctx, m.Chat)
	if err != nil { a.ReplyHTML(m, "Group error"); return }
	admin := "NO"; if g.BotIsAdmin { admin = "YES" }
	a.ReplyHTML(m, fmt.Sprintf("👥 <b>Group Status</b>\n━━━━━━━━━━━━\nTitle: <b>%s</b>\nApproved: <b>%s</b>\nBot Admin: <b>%s</b>\nGroup ID: <code>%d</code>", esc(g.Title), esc(g.ApprovalStatus), admin, g.GroupID))
}

func (a *App) CmdAdmin(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }
	usersCount, _ := a.Users.CountDocuments(ctx, bson.M{})
	groupsCount, _ := a.Groups.CountDocuments(ctx, bson.M{})
	pendingOrders, _ := a.Orders.CountDocuments(ctx, bson.M{"status":"PENDING"})
	shop := "ON"; maint := "OFF"
	if !t.ShopEnabled { shop = "OFF" }
	if t.MaintenanceMode { maint = "ON" }
	a.ReplyHTML(m, fmt.Sprintf("🛡️ <b>BIKA • Pro Admin Dashboard</b>\n━━━━━━━━━━━━\n🏦 Treasury Balance: <b>%s</b> %s\n📦 Total Supply: <b>%s</b> %s\n👤 Users: <b>%d</b>\n👥 Groups: <b>%d</b>\n🧾 Pending Orders: <b>%d</b>\n🎯 VIP WR: <b>%d%%</b>\n🛒 Shop: <b>%s</b>\n🛠 Maintenance: <b>%s</b>\n🕒 %s (Yangon Time)",
		fmtInt(t.OwnerBalance), a.Coin, fmtInt(t.TotalSupply), a.Coin, usersCount, groupsCount, pendingOrders, t.VIPWinRate, shop, maint, esc(a.formatYangon(a.now()))))
}

func (a *App) CmdShop(m *tgbotapi.Message) {
	if !a.EnsureNotMaintenance(m) { return }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if t != nil && !t.ShopEnabled { a.ReplyHTML(m, "🛒 Shop is currently OFF."); return }
	u, _ := a.EnsureUser(ctx, m.From)
	if u == nil { a.ReplyHTML(m, "User error"); return }
	if strings.TrimSpace(m.Text) == "/shop on" || strings.TrimSpace(m.Text) == "/shop off" {
		if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }
		on := strings.HasSuffix(strings.TrimSpace(m.Text), "on")
		_, _ = a.Config.UpdateOne(ctx, bson.M{"key":"treasury"}, bson.M{"$set": bson.M{"shopEnabled": on, "updatedAt": a.now()}})
		if on { a.ReplyHTML(m, "✅ Shop ON") } else { a.ReplyHTML(m, "🛒 Shop OFF") }
		return
	}
	lines := []string{"🛒 <b>BIKA Shop</b>", "━━━━━━━━━━━━", fmt.Sprintf("💼 Your Balance: <b>%s</b> %s", fmtInt(u.Balance), a.Coin), "Select an item below:"}
	var rows [][]tgbotapi.InlineKeyboardButton
	for _, item := range shopItems {
		lines = append(lines, fmt.Sprintf("• <b>%s</b> — <b>%s</b> %s", esc(item.Name), fmtInt(item.Price), a.Coin))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%s — %s %s", item.Name, fmtInt(item.Price), a.Coin), "shop:"+item.ID),
		))
	}
	msg := tgbotapi.NewMessage(m.Chat.ID, strings.Join(lines, "\n"))
	msg.ParseMode = "HTML"
	msg.ReplyToMessageID = m.MessageID
	msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(rows...)
	_, _ = a.Bot.Send(msg)
}

func (a *App) HandleShopOrder(cb *tgbotapi.CallbackQuery, itemID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var item *ShopItem
	for _, it := range shopItems {
		if it.ID == itemID { tmp := it; item = &tmp; break }
	}
	if item == nil { a.AnswerCallback(cb.ID, "Unknown item", false); return }
	u, _ := a.EnsureUser(ctx, cb.From)
	if u == nil || u.Balance < item.Price { a.AnswerCallback(cb.ID, "Balance not enough", false); return }
	orderID, err := a.NextOrderID(ctx)
	if err != nil { a.AnswerCallback(cb.ID, "Order error", false); return }
	_, _ = a.Orders.InsertOne(ctx, bson.M{
		"orderId": orderID, "userId": cb.From.ID, "itemId": item.ID, "itemName": item.Name, "price": item.Price,
		"status": "PENDING", "createdAt": a.now(), "updatedAt": a.now(),
	})
	if a.OwnerID != 0 {
		_, _ = a.SendHTML(a.OwnerID, fmt.Sprintf("🧾 <b>New Shop Order</b>\n━━━━━━━━━━━━\nOrder ID: <code>%s</code>\nUser: %s\nItem: <b>%s</b>\nPrice: <b>%s</b> %s",
			orderID, a.MentionHTML(cb.From), esc(item.Name), fmtInt(item.Price), a.Coin))
	}
	a.AnswerCallback(cb.ID, "Order created.", false)
	_, _ = a.SendHTML(cb.Message.Chat.ID, fmt.Sprintf("🧾 <b>Order Created</b>\n━━━━━━━━━━━━\nOrder ID: <code>%s</code>\nItem: <b>%s</b>\nPrice: <b>%s</b> %s\nStatus: <b>PENDING</b>\n\nOwner ကို slip ပို့ပြီး confirm လုပ်ပါ။",
		orderID, esc(item.Name), fmtInt(item.Price), a.Coin))
}

func (a *App) CmdOrders(m *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t, _ := a.EnsureTreasury(ctx)
	if !a.IsOwner(m.From.ID, t) { a.ReplyHTML(m, "⛔ Owner only."); return }
	cur, err := a.Orders.Find(ctx, bson.M{}, options.Find().SetSort(bson.M{"createdAt": -1}).SetLimit(20))
	if err != nil { a.ReplyHTML(m, "No orders."); return }
	defer cur.Close(ctx)
	lines := []string{"🧾 <b>Recent Orders</b>", "━━━━━━━━━━━━"}
	i := 0
	for cur.Next(ctx) {
		var row struct {
			OrderID string `bson:"orderId"`
			ItemName string `bson:"itemName"`
			Status string `bson:"status"`
		}
		if cur.Decode(&row) == nil {
			i++
			lines = append(lines, fmt.Sprintf("• <code>%s</code> — <b>%s</b> — <b>%s</b>", row.OrderID, esc(row.ItemName), row.Status))
		}
	}
	if i == 0 { a.ReplyHTML(m, "No orders."); return }
	a.ReplyHTML(m, strings.Join(lines, "\n"))
}
