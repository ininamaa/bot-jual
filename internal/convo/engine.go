package convo

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"bot-jual/internal/atl"
	"bot-jual/internal/cache"
	"bot-jual/internal/metrics"
	"bot-jual/internal/nlu"
	"bot-jual/internal/repo"

	"github.com/google/uuid"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// WhatsAppGateway allows sending and downloading WhatsApp messages.
type WhatsAppGateway interface {
	SendText(ctx context.Context, to types.JID, text string) error
	DownloadMedia(ctx context.Context, msg *waProto.Message) ([]byte, string, error)
}

// Engine coordinates conversation logic with NLU and Atlantic client.
type Engine struct {
	repo    *repo.Repository
	nlu     *nlu.Client
	atl     *atl.Client
	gateway WhatsAppGateway
	cache   *cache.Redis
	metrics *metrics.Metrics
	logger  *slog.Logger
}

// New creates a conversation engine instance.
func New(repository *repo.Repository, nluClient *nlu.Client, atlClient *atl.Client, gateway WhatsAppGateway, cache *cache.Redis, metrics *metrics.Metrics, logger *slog.Logger) *Engine {
	return &Engine{
		repo:    repository,
		nlu:     nluClient,
		atl:     atlClient,
		gateway: gateway,
		cache:   cache,
		metrics: metrics,
		logger:  logger.With("component", "convo"),
	}
}

// ProcessMessage handles inbound WhatsApp events.
func (e *Engine) ProcessMessage(ctx context.Context, evt *events.Message) {
	if evt.Info.MessageSource.IsFromMe {
		return
	}

	msgType := detectMessageType(evt)
	e.metrics.WAIncomingMessages.WithLabelValues(msgType).Inc()

	senderJID := evt.Info.Sender
	text := extractText(evt)
	pushName := strings.TrimSpace(evt.Info.PushName)
	userProfile := repo.UserProfile{
		WAID:        senderJID.String(),
		WAJID:       toPtr(senderJID.String()),
		DisplayName: optionalString(pushName),
	}

	user, err := e.repo.UpsertUserByWA(ctx, userProfile)
	if err != nil {
		e.logger.Error("failed upserting user", "error", err)
		return
	}

	if err := e.repo.InsertMessage(ctx, repo.MessageRecord{
		UserID:    user.ID,
		Direction: "incoming",
		Type:      msgType,
		Content:   optionalString(text),
	}); err != nil {
		e.logger.Warn("failed logging incoming message", "error", err)
	}

	if text == "" {
		e.handleNonText(ctx, evt, user)
		return
	}

	intent, err := e.nlu.DetectIntent(ctx, nlu.IntentInput{
		UserMessage: text,
		Channel:     "whatsapp",
		UserLocale:  user.LanguagePreference,
	})
	if err != nil {
		e.logger.Error("nlu intent detection failed", "error", err)
		_ = e.respond(ctx, senderJID, "Maaf, sistem sedang sibuk. Coba lagi ya.")
		return
	}

	if err := e.routeIntent(ctx, evt, user, text, intent); err != nil {
		e.logger.Error("intent handling failed", "error", err, "intent", intent.Intent)
		_ = e.respond(ctx, senderJID, "Maaf, terjadi kesalahan memproses permintaan kamu.")
	}
}

func (e *Engine) routeIntent(ctx context.Context, evt *events.Message, user *repo.User, text string, intent *nlu.IntentResult) error {
	switch intent.Intent {
	case "smalltalk_greeting", "smalltalk":
		reply := intent.Reply
		if reply == "" {
			reply = "Halo! Ada yang bisa dibantu? Bisa cek harga produk, top up, bayar tagihan, deposit, atau transfer."
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "smalltalk")
	case "price_lookup":
		return e.handlePriceLookup(ctx, evt, user, intent)
	case "budget_filter":
		return e.handleBudgetFilter(ctx, evt, user, intent)
	case "create_prepaid":
		return e.handleCreatePrepaid(ctx, evt, user, intent)
	case "check_bill":
		return e.handleCheckBill(ctx, evt, user, intent)
	case "pay_bill":
		return e.handlePayBill(ctx, evt, user, intent)
	case "create_deposit":
		return e.handleCreateDeposit(ctx, evt, user, intent)
	case "create_transfer":
		return e.handleCreateTransfer(ctx, evt, user, intent)
	case "help":
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, helpMessage(), "help")
	default:
		if intent.Reply != "" {
			return e.respondAndLog(ctx, evt.Info.Sender, user.ID, intent.Reply, "nlu_reply")
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Maaf, aku belum paham permintaanmu. Bisa jelaskan lagi?", "fallback")
	}
}

func (e *Engine) handlePriceLookup(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	query := intent.Entities["product_query"]
	productType := intent.Entities["product_type"]
	if query == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Mau cari produk apa? Contoh: \"top up ML\" atau \"pulsa telkomsel 20k\".", "price_lookup_missing_query")
	}

	items, err := e.atl.PriceList(ctx, productType, false)
	if err != nil {
		return fmt.Errorf("fetch price list: %w", err)
	}
	matches := filterByQuery(items, query)
	if len(matches) == 0 {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Belum ketemu produk yang cocok. Coba sebutkan nama layanan lain ya.", "price_lookup_not_found")
	}
	reply := formatPriceList(matches)
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "price_lookup")
}

func (e *Engine) handleBudgetFilter(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	budgetStr := intent.Entities["budget"]
	productType := intent.Entities["product_type"]
	if budgetStr == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Budgetnya berapa? contoh: \"cuma punya 5000\".", "budget_missing_amount")
	}
	maxBudget, err := parseAmount(budgetStr)
	if err != nil || maxBudget <= 0 {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Angka budgetnya belum jelas. Tulis nominal seperti 5000 atau 20k ya.", "budget_invalid_amount")
	}

	items, err := e.atl.PriceList(ctx, productType, false)
	if err != nil {
		return fmt.Errorf("fetch price list: %w", err)
	}
	matches := filterByBudget(items, maxBudget)
	if len(matches) == 0 {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Belum ada produk dengan harga sesuai budget. Coba tambah sedikit nominalnya ya.", "budget_not_found")
	}
	reply := formatPriceList(matches)
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "budget_filter")
}

func (e *Engine) handleCreatePrepaid(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	productCode := intent.Entities["product_code"]
	customerID := intent.Entities["customer_id"]
	if productCode == "" || customerID == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Butuh kode produk dan nomor tujuan ya. Contoh: \"top up ML86 ke 123456\".", "prepaid_missing_fields")
	}
	refID := intent.Entities["ref_id"]
	if refID == "" {
		refID = generateRefID("trx")
	}

	resp, err := e.atl.CreatePrepaidTransaction(ctx, atl.CreatePrepaidRequest{
		ProductCode: productCode,
		CustomerID:  customerID,
		RefID:       refID,
	})
	if err != nil {
		return fmt.Errorf("create prepaid: %w", err)
	}

	if _, err := e.repo.InsertOrder(ctx, repo.Order{
		UserID:      user.ID,
		OrderRef:    refID,
		ProductCode: productCode,
		Status:      resp.Status,
		Metadata: map[string]any{
			"customer_id": customerID,
			"message":     resp.Message,
			"sn":          resp.SN,
		},
	}); err != nil {
		e.logger.Warn("failed storing order", "error", err)
	}

	reply := fmt.Sprintf("Oke, transaksi %s lagi diproses. Ref: %s. Nanti aku kabari kalau statusnya berubah ya.", productCode, refID)
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_prepaid")
}

func (e *Engine) handleCheckBill(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	productCode := intent.Entities["product_code"]
	customerID := intent.Entities["customer_id"]
	if productCode == "" || customerID == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Butuh kode produk dan ID pelanggan ya. Contoh: \"cek tagihan PLN 123456\".", "bill_missing_fields")
	}
	refID := generateRefID("bill")
	resp, err := e.atl.BillInquiry(ctx, atl.BillInquiryRequest{
		ProductCode: productCode,
		CustomerID:  customerID,
		RefID:       refID,
	})
	if err != nil {
		return fmt.Errorf("bill inquiry: %w", err)
	}

	if _, err := e.repo.InsertOrder(ctx, repo.Order{
		UserID:      user.ID,
		OrderRef:    resp.RefID,
		ProductCode: productCode,
		Status:      resp.Status,
		Metadata: map[string]any{
			"customer_id": customerID,
			"amount":      resp.Amount,
			"fee":         resp.Fee,
			"bill_info":   resp.BillInfo,
		},
	}); err != nil {
		e.logger.Warn("failed storing bill inquiry", "error", err)
	}

	reply := fmt.Sprintf("Tagihan %s atas %s total %.0f + fee %.0f. Kalau mau langsung bayar, ketik: bayar %s.", productCode, customerID, resp.Amount, resp.Fee, resp.RefID)
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "check_bill")
}

func (e *Engine) handlePayBill(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	refID := intent.Entities["ref_id"]
	if refID == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Butuh kode ref transaksi tagihan yang mau dibayar.", "pay_bill_missing_ref")
	}
	resp, err := e.atl.BillPayment(ctx, atl.BillPaymentRequest{
		RefID: refID,
	})
	if err != nil {
		return fmt.Errorf("bill payment: %w", err)
	}

	if err := e.repo.UpdateOrderStatus(ctx, refID, resp.Status, map[string]any{
		"message": resp.Message,
	}); err != nil {
		e.logger.Warn("failed update order status", "error", err)
	}

	reply := fmt.Sprintf("Pembayaran %s status: %s. Pesan: %s", refID, resp.Status, resp.Message)
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "pay_bill")
}

func (e *Engine) handleCreateDeposit(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	method := intent.Entities["method"]
	amountStr := intent.Entities["amount"]
	if method == "" || amountStr == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Sebutkan metode dan nominal deposit ya. Contoh: \"deposit qris 50000\".", "deposit_missing_fields")
	}
	amount, err := parseAmount(amountStr)
	if err != nil {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Nominal deposit belum jelas. Coba tulis angka seperti 50000.", "deposit_invalid_amount")
	}
	refID := generateRefID("dep")
	resp, err := e.atl.CreateDeposit(ctx, atl.DepositRequest{
		Method: method,
		Amount: float64(amount),
		RefID:  refID,
	})
	if err != nil {
		return fmt.Errorf("create deposit: %w", err)
	}

	if _, err := e.repo.InsertDeposit(ctx, repo.Deposit{
		UserID:     user.ID,
		DepositRef: refID,
		Method:     method,
		Amount:     amount,
		Status:     resp.Status,
		Metadata: map[string]any{
			"checkout": resp.Checkout,
			"message":  resp.Message,
		},
	}); err != nil {
		e.logger.Warn("failed store deposit", "error", err)
	}

	reply := fmt.Sprintf("Deposit %s via %s senilai %d dibuat. Ikuti instruksi pembayaran berikut: %v", refID, strings.ToUpper(method), amount, resp.Checkout)
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_deposit")
}

func (e *Engine) handleCreateTransfer(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	bank := intent.Entities["bank_code"]
	account := intent.Entities["account_no"]
	accountName := intent.Entities["account_name"]
	amountStr := intent.Entities["amount"]
	if bank == "" || account == "" || amountStr == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Format transfer belum lengkap. Contoh: \"transfer 100000 ke dana 08123 a.n Budi\".", "transfer_missing_fields")
	}
	amount, err := parseAmount(amountStr)
	if err != nil {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Nominal transfer belum jelas. Tulis angka seperti 100000 ya.", "transfer_invalid_amount")
	}
	refID := generateRefID("tf")

	resp, err := e.atl.CreateTransfer(ctx, atl.TransferRequest{
		BankCode:    bank,
		AccountNo:   account,
		AccountName: accountName,
		Amount:      float64(amount),
		RefID:       refID,
	})
	if err != nil {
		return fmt.Errorf("create transfer: %w", err)
	}

	if err := e.repo.UpdateOrderStatus(ctx, refID, resp.Status, map[string]any{
		"bank_code":    bank,
		"account_no":   account,
		"account_name": accountName,
		"amount":       amount,
		"message":      resp.Message,
	}); err != nil {
		e.logger.Warn("failed update transfer record", "error", err)
	}

	reply := fmt.Sprintf("Transfer %s status: %s. Pesan: %s", refID, resp.Status, resp.Message)
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_transfer")
}

func (e *Engine) handleNonText(ctx context.Context, evt *events.Message, user *repo.User) {
	switch detectMessageType(evt) {
	case "audio":
		e.handleAudioMessage(ctx, evt, user)
	case "image":
		e.handleImageMessage(ctx, evt, user)
	default:
		reply := "File-nya sudah kuterima. Saat ini aku lebih optimal kalau kamu jelaskan pakai teks ya."
		if err := e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "non_text"); err != nil {
			e.logger.Warn("failed respond non-text", "error", err)
		}
	}
}

func (e *Engine) handleAudioMessage(ctx context.Context, evt *events.Message, user *repo.User) {
	if !e.allowMediaRequest(ctx, user.ID, "audio") {
		_ = e.respond(ctx, evt.Info.Sender, "Permintaan voice note kamu lagi dibatasi sebentar ya. Coba lagi beberapa menit lagi.")
		return
	}

	data, mime, err := e.gateway.DownloadMedia(ctx, evt.Message)
	if err != nil {
		e.logger.Error("download audio failed", "error", err)
		_ = e.respond(ctx, evt.Info.Sender, "Maaf, voice note-nya gagal kuambil. Bisa kirim ulang atau ketik manual ya.")
		return
	}

	transcript, err := e.nlu.TranscribeAudio(ctx, data, mime)
	if err != nil {
		e.logger.Error("transcribe audio failed", "error", err)
		_ = e.respond(ctx, evt.Info.Sender, "Voice note-nya belum bisa kubaca. Coba ketik manual dulu ya.")
		return
	}

	if transcript == "" {
		_ = e.respond(ctx, evt.Info.Sender, "Voice note-nya kurang jelas. Ketik manual aja ya.")
		return
	}

	intent, err := e.nlu.DetectIntent(ctx, nlu.IntentInput{
		UserMessage: fmt.Sprintf("Hasil transkrip voice note: %s", transcript),
		Channel:     "whatsapp_audio",
		UserLocale:  user.LanguagePreference,
	})
	if err != nil {
		e.logger.Error("intent from audio failed", "error", err)
		_ = e.respond(ctx, evt.Info.Sender, "Maaf, aku belum paham isi voice note-nya. Coba jelaskan dengan teks ya.")
		return
	}

	if err := e.routeIntent(ctx, evt, user, transcript, intent); err != nil {
		e.logger.Error("audio intent handling failed", "error", err)
	}
}

func (e *Engine) handleImageMessage(ctx context.Context, evt *events.Message, user *repo.User) {
	if !e.allowMediaRequest(ctx, user.ID, "image") {
		_ = e.respond(ctx, evt.Info.Sender, "Analisa gambar lagi dibatasi sebentar ya. Coba kirim lagi nanti.")
		return
	}

	data, mime, err := e.gateway.DownloadMedia(ctx, evt.Message)
	if err != nil {
		e.logger.Error("download image failed", "error", err)
		_ = e.respond(ctx, evt.Info.Sender, "Gambarnya belum bisa kuambil. Boleh kirim ulang atau jelaskan dalam teks ya.")
		return
	}

	analysis, err := e.nlu.AnalyzeImage(ctx, data, mime)
	if err != nil {
		e.logger.Error("analyze image failed", "error", err)
		_ = e.respond(ctx, evt.Info.Sender, "Gambarnya belum bisa kubaca. Coba jelaskan manual ya.")
		return
	}

	intentInput := analysis.Summary
	if analysis.ExtractedText != "" {
		intentInput += "\nInfo penting: " + analysis.ExtractedText
	}

	intent, err := e.nlu.DetectIntent(ctx, nlu.IntentInput{
		UserMessage: fmt.Sprintf("Hasil analisa gambar: %s", intentInput),
		Channel:     "whatsapp_image",
		UserLocale:  user.LanguagePreference,
	})
	if err != nil {
		e.logger.Error("intent from image failed", "error", err)
		_ = e.respond(ctx, evt.Info.Sender, "Aku belum bisa memahami isi gambarnya. Boleh jelaskan lewat teks ya.")
		return
	}

	if err := e.routeIntent(ctx, evt, user, intentInput, intent); err != nil {
		e.logger.Error("image intent handling failed", "error", err)
	}
}

func (e *Engine) respondAndLog(ctx context.Context, to types.JID, userID string, text string, category string) error {
	if err := e.respond(ctx, to, text); err != nil {
		return err
	}
	if err := e.repo.InsertMessage(ctx, repo.MessageRecord{
		UserID:    userID,
		Direction: "outgoing",
		Type:      category,
		Content:   &text,
	}); err != nil {
		e.logger.Warn("failed logging outgoing message", "error", err)
	}
	return nil
}

func (e *Engine) respond(ctx context.Context, to types.JID, text string) error {
	return e.gateway.SendText(ctx, to, text)
}

func (e *Engine) allowMediaRequest(ctx context.Context, userID, mediaType string) bool {
	if e.cache == nil {
		return true
	}
	key := fmt.Sprintf("rl:media:%s:%s", mediaType, userID)
	client := e.cache.Client()
	res := client.Incr(ctx, key)
	if res.Err() != nil {
		e.logger.Warn("rate limit incr failed", "error", res.Err())
		return true
	}
	if res.Val() == 1 {
		client.Expire(ctx, key, 10*time.Minute)
	}
	return res.Val() <= 5
}

func detectMessageType(evt *events.Message) string {
	msg := evt.Message
	switch {
	case msg.GetConversation() != "":
		return "text"
	case msg.ExtendedTextMessage != nil:
		return "extended_text"
	case msg.ImageMessage != nil:
		return "image"
	case msg.VideoMessage != nil:
		return "video"
	case msg.AudioMessage != nil:
		return "audio"
	case msg.DocumentMessage != nil:
		return "document"
	default:
		return "unknown"
	}
}

func extractText(evt *events.Message) string {
	msg := evt.Message
	switch {
	case msg.GetConversation() != "":
		return strings.TrimSpace(msg.GetConversation())
	case msg.ExtendedTextMessage != nil:
		return strings.TrimSpace(msg.GetExtendedTextMessage().GetText())
	default:
		return ""
	}
}

func toPtr[T any](v T) *T {
	return &v
}

func optionalString(val string) *string {
	if strings.TrimSpace(val) == "" {
		return nil
	}
	ptr := val
	return &ptr
}

func generateRefID(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, strings.ReplaceAll(uuid.NewString(), "-", "")[:16])
}

func helpMessage() string {
	return "Aku bisa bantu cek harga, filter sesuai budget, top up prabayar, cek & bayar tagihan, buat deposit, dan transfer. Ketik contoh: \"pulsa telkomsel 20k\" atau \"budget 5000\"."
}
