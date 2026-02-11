package convo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"bot-jual/internal/atl"
	"bot-jual/internal/cache"
	"bot-jual/internal/metrics"
	"bot-jual/internal/nlu"
	"bot-jual/internal/repo"
	"bot-jual/internal/wa"

	"github.com/google/uuid"
	"github.com/skip2/go-qrcode"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// WhatsAppGateway allows sending and downloading WhatsApp messages.
type WhatsAppGateway interface {
	SendText(ctx context.Context, to types.JID, text string) error
	SendImage(ctx context.Context, to types.JID, data []byte, mimeType, caption string) error
	DownloadMedia(ctx context.Context, msg *waProto.Message) ([]byte, string, error)
}

// Engine coordinates conversation logic with NLU and Atlantic client.
type Engine struct {
	repo          repo.Repository
	nlu           *nlu.Client
	atl           *atl.Client
	gateway       WhatsAppGateway
	cache         *cache.Redis
	metrics       *metrics.Metrics
	logger        *slog.Logger
	cfg           EngineConfig
	mu            sync.RWMutex
	priceCache    map[string]priceCacheEntry
	priceCacheTTL time.Duration
}

// EngineConfig groups optional knobs for conversation logic.
type EngineConfig struct {
	DefaultDepositMethod string
	DefaultDepositType   string
	DepositFeeFixed      int64
	DepositFeePercent    float64
}

// New creates a conversation engine instance.
func New(repository repo.Repository, nluClient *nlu.Client, atlClient *atl.Client, gateway WhatsAppGateway, cache *cache.Redis, metrics *metrics.Metrics, logger *slog.Logger, cfg EngineConfig) *Engine {
	return &Engine{
		repo:          repository,
		nlu:           nluClient,
		atl:           atlClient,
		gateway:       gateway,
		cache:         cache,
		metrics:       metrics,
		logger:        logger.With("component", "convo"),
		cfg:           cfg,
		priceCache:    make(map[string]priceCacheEntry),
		priceCacheTTL: 5 * time.Minute,
	}
}

// ProcessMessage handles inbound WhatsApp events.
type priceCacheEntry struct {
	items   []atl.PriceListItem
	expires time.Time
}

func (e *Engine) ProcessMessage(ctx context.Context, evt *events.Message) {
	if evt.Info.MessageSource.IsFromMe {
		return
	}

	ctx = wa.WithReply(ctx, evt)

	msgType := detectMessageType(evt)
	e.metrics.WAIncomingMessages.WithLabelValues(msgType).Inc()

	senderJID := evt.Info.Sender.ToNonAD() // Strip device part (e.g. :38) to avoid "no device part" errors
	evt.Info.Sender = senderJID            // Ensure all downstream handlers use the clean JID
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

	contextSummary, lastBot := e.buildConversationContext(ctx, user.ID)

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
		UserMessage:       text,
		Channel:           "whatsapp",
		UserLocale:        user.LanguagePreference,
		ContextSummary:    contextSummary,
		LastBotMessage:    lastBot,
		ConversationState: contextSummary,
	})
	if err != nil {
		// Fallback: continue with heuristic parsing so critical flows (e.g., "Beli ML3 69827740 (2126)") still run.
		e.logger.Warn("nlu intent detection failed, using heuristic fallback", "error", err)
		intent = &nlu.IntentResult{
			Entities: map[string]string{},
		}
		e.enrichIntentFromText(text, intent)
		// Default to deposit when creating prepaid if method is not specified.
		if strings.TrimSpace(strings.ToLower(intent.Intent)) == "create_prepaid" {
			if strings.TrimSpace(intent.Entities["payment_method"]) == "" {
				intent.Entities["payment_method"] = "deposit"
			}
		}
	}

	e.mergeToolCallArguments(intent)
	e.enrichIntentFromText(text, intent)
	e.logger.Debug("resolved intent", "intent", intent.Intent, "entities", intent.Entities, "tool_call", intent.ToolCall)

	// Group chat policy: only respond in group for sales-related intents.
	// Post a short stub in the group, then continue the full flow via private message (PM).
	chatIsGroup := evt.Info.Chat.Server == "g.us" || strings.HasSuffix(evt.Info.Chat.String(), "@g.us")
	if chatIsGroup {
		// Decide whether this is a sales-related inquiry
		intentKey := strings.ToLower(strings.TrimSpace(intent.Intent))
		allowedIntent := map[string]bool{
			"price_lookup":    true,
			"budget_filter":   true,
			"create_prepaid":  true,
			"check_bill":      true,
			"pay_bill":        true,
			"check_status":    true,
			"create_deposit":  true,
			"create_transfer": true,
			"catalog_all":     true,
		}[intentKey]

		lower := strings.ToLower(strings.TrimSpace(text))
		salesKeyword := false
		if lower != "" {
			if strings.Contains(lower, "jual") || strings.Contains(lower, "yg jual") || strings.Contains(lower, "yang jual") ||
				strings.Contains(lower, "ada jual") || strings.Contains(lower, "beli") || strings.Contains(lower, "top up") ||
				strings.Contains(lower, "topup") || strings.Contains(lower, "isi pulsa") || strings.Contains(lower, "nyari") || strings.Contains(lower, "butuh") {
				productHints := []string{"pulsa", "paket data", "data", "token", "listrik", "pln", "diamond", "ml", "mobile legend", "free fire", "ff", "pubg", "voucher", "pdam", "bpjs", "tagihan", "game"}
				for _, kw := range productHints {
					if strings.Contains(lower, kw) {
						salesKeyword = true
						break
					}
				}
			}
		}

		if !(allowedIntent || salesKeyword) {
			// Ignore non-sales messages in groups
			return
		}

		// Build a short group stub
		topic := "layanan digital"
		switch {
		case strings.Contains(lower, "pulsa") || (strings.Contains(lower, "paket") && strings.Contains(lower, "data")) || strings.Contains(lower, "kuota"):
			topic = "pulsa/paket data"
		case strings.Contains(lower, "token") || strings.Contains(lower, "listrik") || strings.Contains(lower, "pln"):
			topic = "token listrik"
		case strings.Contains(lower, "ml") || strings.Contains(lower, "mobile legend") || strings.Contains(lower, "free fire") || strings.Contains(lower, "ff") || strings.Contains(lower, "pubg") || strings.Contains(lower, "game") || strings.Contains(lower, "diamond"):
			topic = "top up game"
		case strings.Contains(lower, "tagihan") || strings.Contains(lower, "pdam") || strings.Contains(lower, "bpjs") || strings.Contains(lower, "pln"):
			topic = "bayar tagihan"
		default:
			if p := strings.TrimSpace(intent.Entities["provider"]); p != "" {
				topic = fmt.Sprintf("produk %s", p)
			}
		}

		stub := fmt.Sprintf("Saya menjual %s kak. Cek PM ya 🙏", topic)

		if err := e.gateway.SendText(ctx, evt.Info.Chat, stub); err != nil {
			e.logger.Warn("failed sending group stub", "error", err)
		} else {
			if err := e.repo.InsertMessage(ctx, repo.MessageRecord{
				UserID:    user.ID,
				Direction: "outgoing",
				Type:      "group_stub",
				Content:   &stub,
			}); err != nil {
				e.logger.Warn("failed logging group stub", "error", err)
			}
		}

		// Continue handling via PM — build a synthetic event with the sender's personal JID
		// so all handler functions send to the user's personal chat, not the group.
		personalJID := senderJID.ToNonAD()
		e.logger.Info("routing group inquiry to PM", "sender", personalJID.String(), "intent", intent.Intent, "entities", intent.Entities)

		// Create a synthetic event that makes handlers send to PM
		pmEvt := *evt
		pmEvt.Info.Sender = personalJID
		pmEvt.Info.Chat = personalJID

		if err := e.routeIntent(context.Background(), &pmEvt, user, text, intent); err != nil {
			e.logger.Error("intent handling failed (pm)", "error", err, "intent", intent.Intent)
			_ = e.respond(context.Background(), personalJID, "Maaf, terjadi kesalahan memproses permintaan kamu.")
		}
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
			reply = "Halo! Aku menyediakan berbagai layanan digital:\n\n📱 *Pulsa & Paket Data* - Telkomsel, Indosat, XL, Tri, Smartfren\n🎮 *Top Up Game* - Mobile Legends, Free Fire, PUBG, dll\n⚡ *Token Listrik* - Prabayar & Pascabayar\n💳 *Bayar Tagihan* - PLN, PDAM, BPJS, dll\n💰 *Deposit & Transfer* - QRIS, Bank Transfer, E-wallet\n\nKetik nama produk yang kamu cari, contoh: \"pulsa telkomsel 20k\" atau \"top up ML\""
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "smalltalk")
	case "price_lookup":
		return e.handlePriceLookup(ctx, evt, user, text, intent)
	case "budget_filter":
		return e.handleBudgetFilter(ctx, evt, user, text, intent)
	case "create_prepaid":
		return e.handleCreatePrepaid(ctx, evt, user, intent)
	case "check_bill":
		return e.handleCheckBill(ctx, evt, user, intent)
	case "pay_bill":
		return e.handlePayBill(ctx, evt, user, intent)
	case "check_status":
		return e.handleCheckStatus(ctx, evt, user, intent)
	case "create_deposit":
		return e.handleCreateDeposit(ctx, evt, user, intent)
	case "create_transfer":
		return e.handleCreateTransfer(ctx, evt, user, intent)
	case "catalog_all":
		if strings.TrimSpace(intent.Entities["provider"]) != "" || strings.TrimSpace(intent.Entities["product_query"]) != "" {
			return e.handlePriceLookup(ctx, evt, user, text, intent)
		}
		return e.handleCatalogAll(ctx, evt, user)
	case "check_balance":
		return e.handleCheckBalance(ctx, evt, user)
	case "payment_info":
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, paymentInfoMessage(), "payment_info")
	case "help":
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, helpMessage(), "help")
	default:
		if intent.Reply != "" {
			return e.respondAndLog(ctx, evt.Info.Sender, user.ID, intent.Reply, "nlu_reply")
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Maaf, aku belum paham permintaanmu. Bisa jelaskan lagi?", "fallback")
	}
}

func (e *Engine) handlePriceLookup(ctx context.Context, evt *events.Message, user *repo.User, rawText string, intent *nlu.IntentResult) error {
	query := intent.Entities["product_query"]
	if query == "" {
		query = strings.TrimSpace(rawText)
	}
	productType := intent.Entities["product_type"]
	if productType == "" {
		productType = defaultProductType(query)
	}
	provider := intent.Entities["provider"]
	fullRequest := intent.Intent == "catalog_all" || wantsFullList(rawText) || wantsFullList(query)
	if query == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Mau cari produk apa? Contoh: \"top up ML\" atau \"pulsa telkomsel 20k\".", "price_lookup_missing_query")
	}

	items, cached, err := e.fetchPriceList(ctx, productType)
	if err != nil {
		return e.handleAtlanticFailure(ctx, evt.Info.Sender, user.ID, err, "price_lookup_fetch")
	}
	matches := filterByQuery(items, query, provider, fullRequest)
	if len(matches) == 0 {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Belum ketemu produk yang cocok. Coba sebutkan nama layanan lain ya.", "price_lookup_not_found")
	}
	reply := formatPriceList(matches, fullRequest)
	if cached {
		reply = "Data harga sementara (cache):\n" + reply
	}
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "price_lookup")
}

func (e *Engine) handleBudgetFilter(ctx context.Context, evt *events.Message, user *repo.User, rawText string, intent *nlu.IntentResult) error {
	budgetStr := intent.Entities["budget"]
	productType := intent.Entities["product_type"]
	query := intent.Entities["product_query"]
	provider := intent.Entities["provider"]
	if budgetStr == "" {
		budgetStr = rawText
	}
	if productType == "" {
		productType = defaultProductType(rawText)
	}
	if productType == "" {
		productType = "prabayar"
	}
	maxBudget, err := parseAmount(budgetStr)
	if err != nil || maxBudget <= 0 {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Angka budgetnya belum jelas. Tulis nominal seperti 5000 atau 20k ya.", "budget_invalid_amount")
	}

	items, cached, err := e.fetchPriceList(ctx, productType)
	if err != nil {
		return e.handleAtlanticFailure(ctx, evt.Info.Sender, user.ID, err, "budget_filter_fetch")
	}

	// First filter by product query if provided, then apply budget cap
	if query != "" {
		items = filterByQuery(items, query, provider, true)
	}
	matches := filterByBudget(items, maxBudget)
	if len(matches) == 0 {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Belum ada produk yang cocok dengan budget kamu. Coba tambah sedikit nominalnya ya.", "budget_not_found")
	}
	reply := formatPriceList(matches, false)
	if cached {
		reply = "Data harga sementara (cache):\n" + reply
	}
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "budget_filter")
}

func (e *Engine) handleCatalogAll(ctx context.Context, evt *events.Message, user *repo.User) error {
	prabayar, prabayarCached, err := e.fetchPriceList(ctx, "prabayar")
	if err != nil {
		return e.handleAtlanticFailure(ctx, evt.Info.Sender, user.ID, err, "catalog_all_prabayar")
	}
	pascabayar, pascaCached, err := e.fetchPriceList(ctx, "pascabayar")
	if err != nil && !errors.Is(err, atl.ErrInvalidCredential) {
		return e.handleAtlanticFailure(ctx, evt.Info.Sender, user.ID, err, "catalog_all_pascabayar")
	}
	combined := append(prabayar, pascabayar...)
	reply := formatCatalogSummary(combined)
	if prabayarCached || pascaCached {
		reply = "Data harga sementara (cache):\n" + reply
	}
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "catalog_all")
}

func (e *Engine) handleCreatePrepaid(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	productCode := strings.TrimSpace(intent.Entities["product_code"])
	rawCustomerID := strings.TrimSpace(intent.Entities["customer_id"])
	customerID := rawCustomerID
	customerZone := strings.TrimSpace(intent.Entities["customer_zone"])
	productQuery := strings.TrimSpace(intent.Entities["product_query"])
	productType := strings.TrimSpace(intent.Entities["product_type"])
	provider := strings.TrimSpace(intent.Entities["provider"])
	rawMethod := strings.TrimSpace(intent.Entities["payment_method"])
	paymentMethod := normalizePaymentMethod(rawMethod, e.cfg.DefaultDepositMethod)

	// Re-derive intent from the user's actual text and prefer deposit by default.
	rawLower := strings.ToLower(strings.TrimSpace(extractText(evt)))
	mentionsDeposit := strings.Contains(rawLower, "deposit") || strings.Contains(rawLower, "saldo") || strings.Contains(rawLower, "pakai saldo")
	mentionsQris := strings.Contains(rawLower, "qris") || strings.Contains(rawLower, "qr") || strings.Contains(rawLower, "scan")
	mentionsBRI := strings.Contains(rawLower, "bri") || strings.Contains(rawLower, "bank bri") || strings.Contains(rawLower, "via bank")

	// If user explicitly mentions deposit, force deposit.
	if mentionsDeposit {
		paymentMethod = "deposit"
	} else if mentionsBRI {
		paymentMethod = "bri"
	} else if mentionsQris {
		// Only prefer QRIS if explicitly mentioned.
		paymentMethod = "qris"
	}

	// If no payment method was explicitly mentioned, force a prompt.
	if !mentionsDeposit && !mentionsQris && !mentionsBRI {
		paymentMethod = "" // force prompt — always ask user to choose
	}

	refID := strings.TrimSpace(intent.Entities["ref_id"])
	if refID == "" {
		refID = strings.TrimSpace(intent.Entities["reff_id"])
	}

	item, resolvedType, err := e.resolveProductFromQuery(ctx, productCode, productType, productQuery, provider)
	if err != nil {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, err.Error(), "prepaid_missing_product")
	}
	if item == nil {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Produk belum ditemukan. Sebutkan kode atau nama produk yang kamu inginkan ya.", "prepaid_missing_product")
	}
	if productCode == "" {
		productCode = item.Code
	}
	if productType == "" {
		productType = resolvedType
	}
	customerID, customerZone = normalizeCustomerTarget(customerID, customerZone)
	if customerZone != "" {
		intent.Entities["customer_zone"] = customerZone
	}
	intent.Entities["customer_id"] = customerID
	if customerID == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, fmt.Sprintf("Kamu mau beli %s (%s). Kirim nomor/ID tujuan ya.", item.Name, item.Code), "prepaid_missing_customer")
	}
	if productRequiresZone(item) && customerZone == "" {
		hint := fmt.Sprintf("Untuk %s, butuh ID plus Server. Formatkan seperti 12345678(1234) ya.", item.Name)
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, hint, "prepaid_missing_customer_zone")
	}

	// If no payment method was determined, prompt user to choose.
	if paymentMethod == "" {
		prompt := fmt.Sprintf("Mau beli %s (%s) — %s\nHarga: %s\n\nMau bayar pakai apa?\n🏦 *BRI* — Transfer Bank BRI\n📱 *QRIS* — Scan QR\n💰 *Saldo* — Pakai saldo deposit\n\nBalas: bri / qris / saldo", item.Name, item.Code, formatCurrency(item.Price), formatCurrency(item.Price))
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, prompt, "prepaid_ask_payment_method")
	}

	switch paymentMethod {
	case "deposit", "saldo":
		return e.executePrepaidWithBalance(ctx, evt, user, productCode, customerID, customerZone, rawCustomerID, refID, item, productType)
	case "bri":
		return e.executePrepaidWithCheckout(ctx, evt, user, productCode, customerID, customerZone, rawCustomerID, refID, item, "BRI", productType)
	default:
		return e.executePrepaidWithCheckout(ctx, evt, user, productCode, customerID, customerZone, rawCustomerID, refID, item, paymentMethod, productType)
	}
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
			"customer_id":  customerID,
			"amount":       resp.Amount,
			"fee":          resp.Fee,
			"bill_info":    resp.BillInfo,
			"product_type": "pascabayar",
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
	productCode := strings.TrimSpace(intent.Entities["product_code"])
	customerID := strings.TrimSpace(intent.Entities["customer_id"])
	if (productCode == "" || customerID == "") && e.repo != nil {
		if order, err := e.repo.GetOrderByRef(ctx, refID); err == nil && order != nil {
			if productCode == "" {
				productCode = strings.TrimSpace(order.ProductCode)
			}
			if customerID == "" {
				customerID = strings.TrimSpace(stringValue(order.Metadata, "customer_id"))
			}
		}
	}
	resp, err := e.atl.BillPayment(ctx, atl.BillPaymentRequest{
		RefID:       refID,
		ProductCode: productCode,
		CustomerID:  customerID,
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

func (e *Engine) handleCheckStatus(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	refID := strings.TrimSpace(intent.Entities["ref_id"])
	id := strings.TrimSpace(intent.Entities["id"])
	if refID == "" {
		refID = strings.TrimSpace(intent.Entities["reff_id"])
	}
	if refID == "" && id == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Butuh kode ref transaksi untuk cek status. Contoh: cek status ref 0123456789.", "status_missing_ref")
	}
	if refID != "" && e.repo != nil {
		if dep, err := e.repo.GetDepositByRef(ctx, refID); err == nil && dep != nil && strings.TrimSpace(dep.DepositRef) != "" {
			statusText := strings.ToUpper(strings.TrimSpace(dep.Status))
			if statusText == "" {
				statusText = "UNKNOWN"
			}
			reply := fmt.Sprintf("Status deposit %s: %s.", refID, statusText)
			return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "check_status_deposit")
		}
	}
	productType := strings.TrimSpace(intent.Entities["product_type"])
	var order *repo.Order
	if productType == "" && e.repo != nil {
		if found, err := e.repo.GetOrderByRef(ctx, refID); err == nil && found != nil {
			order = found
			productType = strings.TrimSpace(stringValue(found.Metadata, "product_type"))
		}
	}
	if productType == "" {
		productType = "prabayar"
	}

	resp, err := e.atl.TransactionStatus(ctx, atl.TransactionStatusRequest{
		RefID: refID,
		ID:    id,
		Type:  productType,
	})
	if err != nil {
		if order != nil && isNotFoundAtlanticError(err) {
			statusText := strings.ToUpper(strings.TrimSpace(order.Status))
			if statusText == "" {
				statusText = "UNKNOWN"
			}
			reply := fmt.Sprintf("Status transaksi %s: %s (lokal).", order.OrderRef, statusText)
			return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "check_status_local")
		}
		return e.handleAtlanticFailure(ctx, evt.Info.Sender, user.ID, err, "check_status")
	}
	refLabel := refID
	if refLabel == "" {
		refLabel = id
	}
	status := strings.ToUpper(strings.TrimSpace(resp.Status))
	if status == "" {
		status = "UNKNOWN"
	}
	reply := fmt.Sprintf("Status transaksi %s: %s.", refLabel, status)
	if resp.Message != "" {
		reply = fmt.Sprintf("%s %s", reply, resp.Message)
	}
	if resp.SN != "" {
		reply = fmt.Sprintf("%s SN: %s.", reply, resp.SN)
	}
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "check_status")
}

func (e *Engine) handleCreateDeposit(ctx context.Context, evt *events.Message, user *repo.User, intent *nlu.IntentResult) error {
	defaultMethod := e.defaultDepositMethod()
	method := normalizePaymentMethod(intent.Entities["method"], "")
	if method == "" {
		method = defaultMethod
	}
	if method == "qris" && defaultMethod != "" && defaultMethod != "qris" {
		method = defaultMethod
	}
	if method == "deposit" {
		method = ""
	}
	amountStr := strings.TrimSpace(intent.Entities["amount"])
	if amountStr == "" {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Nominal deposit belum jelas. Coba tulis angka seperti 50000.", "deposit_invalid_amount")
	}
	if method == "" {
		hint := "Mau deposit via apa?\n🏦 *BRI* — Transfer Bank BRI\n📱 *QRIS* — Scan QR\n\nContoh: \"deposit qris 50000\" atau \"deposit bri 100000\""
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, hint, "deposit_missing_fields")
	}
	amount, err := parseAmount(amountStr)
	if err != nil || amount <= 0 {
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Nominal deposit belum jelas. Coba tulis angka seperti 50000.", "deposit_invalid_amount")
	}
	refID := strings.TrimSpace(intent.Entities["ref_id"])
	if refID == "" {
		refID = generateRefID("dep")
	}
	grossAmount := amount
	depositType := strings.TrimSpace(intent.Entities["type"])
	if depositType == "" {
		depositType = e.cfg.DefaultDepositType
	}
	// Override deposit type to "bank" when using BRI method.
	if method == "bri" {
		depositType = "bank"
	}
	resp, err := e.atl.CreateDeposit(ctx, atl.DepositRequest{
		Method: method,
		Amount: float64(grossAmount),
		RefID:  refID,
		Type:   depositType,
	})
	if err != nil {
		e.logger.Warn("create deposit request failed", "error", err, "user_id", user.ID, "method", method, "amount", amount)
		feedback := friendlyAtlanticError(err)
		if feedback == "" {
			feedback = "Deposit belum bisa diproses sekarang. Coba lagi dalam beberapa saat ya."
		} else {
			feedback = fmt.Sprintf("Deposit belum bisa diproses: %s", feedback)
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, feedback, "create_deposit_failed")
	}

	feeAmount := priceToAmount(resp.Fee)
	netFromProvider := priceToAmount(resp.NetAmount)
	providerAmount := priceToAmount(resp.Amount)
	depStatus, forced := forceSuccessIfPending(resp.Status)

	// Prefer net reported by provider; otherwise derive from gross and fee
	netAmount := netFromProvider
	if netAmount == 0 {
		netAmount = deriveNetAmount(grossAmount, feeAmount, 0)
	}

	// Prefer provider-reported gross; fallback to requested amount
	displayGross := providerAmount
	if displayGross == 0 {
		displayGross = grossAmount
	}

	metadata := map[string]any{
		"checkout":         resp.Checkout,
		"message":          resp.Message,
		"gross_amount":     displayGross,
		"requested_amount": amount,
	}
	if forced {
		metadata["forced_success"] = true
		metadata["original_status"] = resp.Status
	}
	if providerAmount > 0 {
		metadata["provider_amount"] = providerAmount
	}
	if feeAmount > 0 {
		metadata["fee"] = feeAmount
	}
	if netAmount > 0 {
		metadata["net_amount"] = netAmount
	}
	depositAmount := grossAmount
	if netAmount > 0 {
		depositAmount = netAmount
	}
	if _, err := e.repo.InsertDeposit(ctx, repo.Deposit{
		UserID:     user.ID,
		DepositRef: refID,
		Method:     method,
		Amount:     depositAmount,
		Status:     depStatus,
		Metadata:   metadata,
	}); err != nil {
		e.logger.Warn("failed store deposit", "error", err)
	}

	computedFee := feeAmount
	if displayGross > 0 && netAmount > 0 {
		altFee := displayGross - netAmount
		if altFee > 0 {
			computedFee = altFee
		}
	}
	summaryLine := summarizeDepositAmounts(displayGross, computedFee, netAmount)

	// Check if this is a bank transfer deposit (BRI) — show transfer info instead of QR.
	if method == "bri" || depositType == "bank" {
		bankInfo := formatBankTransferInfo(resp.Checkout)
		reply := fmt.Sprintf("Sip, deposit %s via BRI sebesar %s sudah siap.\n%s", refID, formatCurrency(float64(displayGross)), bankInfo)
		if summaryLine != "" {
			reply = fmt.Sprintf("%s\n%s", reply, summaryLine)
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_deposit")
	}

	qrCaption := fmt.Sprintf("Deposit %s via %s senilai %s.", refID, strings.ToUpper(method), formatCurrency(float64(displayGross)))
	if summaryLine != "" {
		qrCaption = fmt.Sprintf("%s\n%s", qrCaption, summaryLine)
	}
	qrSent := e.sendCheckoutQRImage(ctx, evt.Info.Sender, user.ID, resp.Checkout, qrCaption, "create_deposit")

	reply := fmt.Sprintf("Sip, deposit %s via %s sebesar %s sudah siap.\n%s", refID, strings.ToUpper(method), formatCurrency(float64(displayGross)), formatCheckoutInfo(resp.Checkout, qrSent))
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_deposit")
}

func (e *Engine) handleCheckBalance(ctx context.Context, evt *events.Message, user *repo.User) error {
	// Prefer per-JID balance from Postgres (computed via triggers/views).
	if ub, err := e.repo.GetUserBalance(ctx, user.ID); err == nil && ub != nil {
		reply := fmt.Sprintf("Saldo kamu sekitar %s.", formatCurrency(float64(ub.SaldoConfirmed)))
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "check_balance")
	}

	// Fallback to Atlantic account-level profile if database balance is unavailable.
	profile, err := e.atl.GetProfile(ctx)
	if err != nil {
		return e.handleAtlanticFailure(ctx, evt.Info.Sender, user.ID, err, "check_balance")
	}
	reply := fmt.Sprintf("Saldo Atlantic kamu sekitar %s. Status akun: %s.", formatCurrency(profile.Balance), strings.ToUpper(profile.Status))
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "check_balance")
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
	case "video", "document":
		reply := "File-nya sudah kuterima. Saat ini aku lebih optimal kalau kamu jelaskan pakai teks ya."
		if err := e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "non_text"); err != nil {
			e.logger.Warn("failed respond non-text", "error", err)
		}
	default:
		// Ignore unknown/protocol messages silently (e.g. history sync, reactions, etc.)
		e.logger.Debug("ignoring non-text message", "type", detectMessageType(evt), "from", evt.Info.Sender.String())
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

func (e *Engine) handleAtlanticFailure(ctx context.Context, to types.JID, userID string, err error, category string) error {
	if errors.Is(err, atl.ErrInvalidCredential) {
		e.logger.Error("atlantic credential rejected", "error", err)
		return e.respondAndLog(ctx, to, userID, "Koneksi Atlantic menolak API key yang dipakai. Mohon cek kembali kredensial di konfigurasi.", category)
	}
	e.logger.Error("atlantic operation failed", "error", err)
	return e.respondAndLog(ctx, to, userID, "Maaf, harga layanan belum bisa kuambil dari Atlantic. Coba lagi dalam beberapa saat ya.", category)
}

func (e *Engine) mergeToolCallArguments(intent *nlu.IntentResult) {
	if intent == nil || intent.ToolCall == nil || len(intent.ToolCall.Arguments) == 0 {
		return
	}
	if intent.Entities == nil {
		intent.Entities = map[string]string{}
	}
	for rawKey, rawVal := range intent.ToolCall.Arguments {
		key := strings.TrimSpace(strings.ToLower(rawKey))
		val := strings.TrimSpace(rawVal)
		if val == "" || key == "" {
			continue
		}
		switch key {
		case "code":
			if intent.Entities["product_code"] == "" {
				intent.Entities["product_code"] = strings.ToUpper(val)
			}
			if intent.Entities["product_query"] == "" {
				intent.Entities["product_query"] = val
			}
		case "target", "customer_id", "customer_no":
			if intent.Entities["customer_id"] == "" {
				intent.Entities["customer_id"] = val
			}
			if intent.Entities["customer_zone"] == "" {
				if _, zoneCandidate := extractCustomerTokens(val); zoneCandidate != "" {
					intent.Entities["customer_zone"] = zoneCandidate
				}
			}
		case "metode", "method", "payment_method":
			if intent.Entities["payment_method"] == "" {
				intent.Entities["payment_method"] = strings.ToLower(val)
			}
			if intent.Entities["method"] == "" {
				intent.Entities["method"] = strings.ToLower(val)
			}
		case "zone", "zone_id", "server", "server_id", "sv", "srv":
			if intent.Entities["customer_zone"] == "" {
				intent.Entities["customer_zone"] = val
			}
		case "nominal", "amount":
			if intent.Entities["amount"] == "" {
				intent.Entities["amount"] = val
			}
		case "type":
			if strings.EqualFold(intent.ToolCall.Name, "transaksi_status") {
				if intent.Entities["product_type"] == "" {
					intent.Entities["product_type"] = val
				}
			} else if intent.Entities["type"] == "" {
				intent.Entities["type"] = val
			}
		case "reff_id", "ref_id", "refid":
			if intent.Entities["ref_id"] == "" {
				intent.Entities["ref_id"] = val
			}
		case "id":
			if intent.Entities["id"] == "" {
				intent.Entities["id"] = val
			}
		case "limit_price":
			if intent.Entities["limit_price"] == "" {
				intent.Entities["limit_price"] = val
			}
		case "kode_bank", "bank_code":
			if intent.Entities["bank_code"] == "" {
				intent.Entities["bank_code"] = val
			}
		case "nomor_akun", "account_no":
			if intent.Entities["account_no"] == "" {
				intent.Entities["account_no"] = val
			}
		case "nama_pemilik", "account_name":
			if intent.Entities["account_name"] == "" {
				intent.Entities["account_name"] = val
			}
		case "note":
			if intent.Entities["note"] == "" {
				intent.Entities["note"] = val
			}
		case "email":
			if intent.Entities["email"] == "" {
				intent.Entities["email"] = val
			}
		case "phone", "phone_number":
			if intent.Entities["phone"] == "" {
				intent.Entities["phone"] = val
			}
		default:
			if intent.Entities[key] == "" {
				intent.Entities[key] = val
			}
		}
	}
	if strings.TrimSpace(intent.Intent) == "" {
		switch strings.TrimSpace(strings.ToLower(intent.ToolCall.Name)) {
		case "transaksi_create":
			intent.Intent = "create_prepaid"
		case "deposit_create":
			intent.Intent = "create_deposit"
		case "price_list":
			intent.Intent = "price_lookup"
		case "tagihan_cek":
			intent.Intent = "check_bill"
		case "tagihan_bayar":
			intent.Intent = "pay_bill"
		case "transfer_create":
			intent.Intent = "create_transfer"
		case "transaksi_status":
			intent.Intent = "check_status"
		}
	}
}

func (e *Engine) enrichIntentFromText(text string, intent *nlu.IntentResult) {
	if intent == nil {
		return
	}
	trimmed := strings.TrimSpace(text)
	lowered := strings.ToLower(trimmed)
	if intent.Entities == nil {
		intent.Entities = map[string]string{}
	}
	if trimmed == "" {
		return
	}
	if intent.Intent == "" {
		intent.Intent = "fallback"
	}

	// Detect payment method questions early — before product query check
	if looksLikePaymentQuery(lowered) {
		intent.Intent = "payment_info"
		return
	}
	if looksLikeStatusQuery(lowered) {
		intent.Intent = "check_status"
	}
	fullCatalog := shouldTreatAsFullCatalog(lowered)
	if fullCatalog {
		intent.Intent = "catalog_all"
	}
	if shouldTreatAsProductQuery(lowered) && intent.Entities["product_query"] == "" {
		intent.Entities["product_query"] = trimmed
		if !fullCatalog && (intent.Intent == "fallback" || intent.Intent == "help" || intent.Intent == "") {
			if containsPurchaseKeyword(lowered) {
				intent.Intent = "create_prepaid"
			} else {
				intent.Intent = "price_lookup"
			}
		}
	}
	if intent.Entities["product_code"] == "" {
		if code := extractLikelyProductCode(lowered); code != "" {
			intent.Entities["product_code"] = code
		}
	}
	if intent.Entities["budget"] == "" {
		if amount, err := parseAmount(trimmed); err == nil && looksLikeBudget(lowered) {
			intent.Entities["budget"] = fmt.Sprintf("%d", amount)
			if intent.Intent == "fallback" || intent.Intent == "" || intent.Intent == "price_lookup" {
				intent.Intent = "budget_filter"
			}
		}
	}
	if intent.Intent == "check_status" && intent.Entities["ref_id"] == "" {
		if ref := extractRefID(trimmed); ref != "" {
			intent.Entities["ref_id"] = ref
		}
		if intent.Entities["product_type"] == "" {
			intent.Entities["product_type"] = defaultProductType(trimmed)
		}
	}
	if (intent.Intent == "price_lookup" || intent.Intent == "budget_filter") && intent.Entities["product_type"] == "" {
		intent.Entities["product_type"] = defaultProductType(trimmed)
	}
	if provider := inferProvider(lowered); provider != "" {
		intent.Entities["provider"] = provider
		if intent.Intent == "fallback" {
			intent.Intent = "price_lookup"
		}
	}
	if looksLikeBalanceQuery(lowered) {
		intent.Intent = "check_balance"
	}
	if method := detectPaymentMethod(lowered); method != "" {
		intent.Entities["payment_method"] = method
		if intent.Intent == "price_lookup" {
			intent.Intent = "create_prepaid"
		}
	}
	if intent.Entities["customer_id"] == "" || intent.Entities["customer_zone"] == "" {
		if idCandidate, zoneCandidate := extractCustomerTokens(trimmed); idCandidate != "" || zoneCandidate != "" {
			if intent.Entities["customer_id"] == "" && idCandidate != "" {
				intent.Entities["customer_id"] = idCandidate
			}
			if intent.Entities["customer_zone"] == "" && zoneCandidate != "" {
				intent.Entities["customer_zone"] = zoneCandidate
			}
		}
	}

	// Special handling for patterns like "Beli ML3 69827740 (2126)"
	// Only apply when text starts with a clear purchase pattern
	if intent.Intent == "create_prepaid" && (intent.Entities["product_code"] == "" || intent.Entities["customer_id"] == "") {
		parts := strings.Fields(trimmed)
		if len(parts) >= 2 {
			// Find the purchase keyword position to locate product code after it
			purchaseWords := []string{"beli", "order", "pesan", "ambil", "topup", "top"}
			buyIdx := -1
			for i, p := range parts {
				for _, pw := range purchaseWords {
					if strings.EqualFold(p, pw) {
						buyIdx = i
						break
					}
				}
				if buyIdx >= 0 {
					break
				}
			}
			// Only extract product code from the word right after the purchase keyword
			if buyIdx >= 0 && buyIdx+1 < len(parts) {
				codeCandidate := strings.ToUpper(strings.TrimSpace(parts[buyIdx+1]))
				if intent.Entities["product_code"] == "" && codeCandidate != "" && hasAlphaNumeric(codeCandidate) {
					intent.Entities["product_code"] = codeCandidate
				}
				// Customer ID is the word after the product code
				if intent.Entities["customer_id"] == "" && buyIdx+2 < len(parts) {
					customerPart := strings.TrimSpace(parts[buyIdx+2])
					if strings.Contains(customerPart, "(") && strings.Contains(customerPart, ")") {
						customerID, zone := extractCustomerTokens(customerPart)
						intent.Entities["customer_id"] = customerID
						if zone != "" {
							intent.Entities["customer_zone"] = zone
						}
					} else {
						intent.Entities["customer_id"] = customerPart
					}
				}
			}
		}
	}

	// Additional fallback for patterns like "Beli 3dm 69827740 (2126)" if entities are still missing
	// This handles cases where the NLU didn't properly extract the entities
	if intent.Intent == "create_prepaid" && (intent.Entities["product_code"] == "" || intent.Entities["customer_id"] == "") {
		// Try to extract from the original text
		lowered := strings.ToLower(trimmed)
		if strings.Contains(lowered, "beli") || strings.Contains(lowered, "buy") {
			// Extract product code (usually 3dm, ml3, etc.)
			productCode := extractLikelyProductCode(lowered)
			if productCode != "" {
				intent.Entities["product_code"] = strings.ToUpper(productCode)
			}

			// Extract customer ID and zone
			customerID, zone := extractCustomerTokens(trimmed)
			if customerID != "" {
				intent.Entities["customer_id"] = customerID
			}
			if zone != "" {
				intent.Entities["customer_zone"] = zone
			}

			// Extract payment method if mentioned
			if strings.Contains(lowered, "via saldo") || strings.Contains(lowered, "pakai saldo") || strings.Contains(lowered, "deposit") {
				intent.Entities["payment_method"] = "deposit"
			}
		}
	}
}

func shouldTreatAsProductQuery(text string) bool {
	keywords := []string{
		"pulsa", "data", "paket", "kuota", "harga", "price", "pricelist", "price list",
		"voucher", "diamond", "top up", "topup", "top-up",
		"token", "listrik", "pln", "tagihan", "bayar tagihan",
		"mobile legend", "free fire", "gems", "isi pulsa", "isi token",
		"bpjs", "pdam", "wifi",
		"streaming", "netflix", "spotify",
		"emoney", "e-money", "ewallet", "e-wallet",
		"genshin", "valorant", "roblox",
		"gopay", "ovo", "shopeepay", "linkaja",
	}
	for _, kw := range keywords {
		if strings.Contains(text, kw) {
			return true
		}
	}
	return false
}

func wantsFullList(text string) bool {
	lowered := strings.ToLower(strings.TrimSpace(text))
	if lowered == "" {
		return false
	}
	keywords := []string{"full", "lengkap", "semua", "all", "complete"}
	for _, kw := range keywords {
		if strings.Contains(lowered, kw) {
			return true
		}
	}
	return false
}

func looksLikeBudget(text string) bool {
	budgetWords := []string{
		"budget", "modal", "cuma", "punya",
		"dibawah", "di bawah", "under", "kurang dari",
		"maksimal", "max", "murah",
		"dibawah", "ga lebih", "gak lebih", "tidak lebih",
	}
	for _, w := range budgetWords {
		if strings.Contains(text, w) {
			return true
		}
	}
	return false
}

func defaultProductType(text string) string {
	lowered := strings.ToLower(strings.TrimSpace(text))
	if strings.Contains(lowered, "tagihan") || strings.Contains(lowered, "pascabayar") || strings.Contains(lowered, "postpaid") {
		return "pascabayar"
	}
	return "prabayar"
}

func shouldTreatAsFullCatalog(text string) bool {
	keywords := []string{
		"full menu", "semua produk", "apa saja kamu jual", "apa saja produknya",
		"list produk", "semua layanan", "produk apa aja", "apa aja sih",
		"jualan apa", "menu", "katalog", "daftar produk", "apa yg dijual",
		"apa yang dijual", "jual apa aja", "produk lengkap",
	}
	for _, kw := range keywords {
		if strings.Contains(text, kw) {
			return true
		}
	}
	return false
}

func inferProvider(text string) string {
	// Short keywords that need word-boundary matching to avoid false positives
	// (e.g. "ml" matching inside "diriplow", "cod" inside "code")
	shortKeywords := map[string]string{
		"ml": "mobile legend", "ff": "free fire", "xl": "xl",
		"cod": "call of duty", "wa": "whatsapp", "tri": "tri",
		"smart": "smartfren", "tsel": "telkomsel", "im3": "indosat",
		"isat": "indosat", "byu": "telkomsel",
	}
	// Longer keywords safe for substring matching
	longKeywords := map[string]string{
		"telkomsel": "telkomsel", "indosat": "indosat", "axis": "axis",
		"three": "tri", "smartfren": "smartfren", "by.u": "telkomsel",
		"pln": "pln", "pdam": "pdam", "bpjs": "bpjs",
		"dana": "dana", "ovo": "ovo", "gopay": "gopay",
		"shopeepay": "shopee", "shopee pay": "shopee",
		"linkaja": "linkaja", "link aja": "linkaja",
		"mobile legend": "mobile legend", "free fire": "free fire",
		"pubg": "pubg", "genshin": "genshin", "valorant": "valorant",
		"steam": "steam", "roblox": "roblox", "garena": "garena",
		"call of duty": "call of duty",
		"netflix":      "netflix", "spotify": "spotify", "vidio": "vidio",
		"disney": "disney", "youtube": "youtube",
		"whatsapp": "whatsapp",
	}

	// Check long (safe) keywords first
	for keyword, provider := range longKeywords {
		if strings.Contains(text, keyword) {
			return provider
		}
	}
	// Check short keywords with word boundary
	for keyword, provider := range shortKeywords {
		if containsWord(text, keyword) {
			return provider
		}
	}
	return ""
}

// containsWord checks if text contains keyword as a whole word (not part of a larger word).
func containsWord(text, word string) bool {
	idx := 0
	for {
		pos := strings.Index(text[idx:], word)
		if pos == -1 {
			return false
		}
		absPos := idx + pos
		endPos := absPos + len(word)
		leftOk := absPos == 0 || !isAlphaNum(rune(text[absPos-1]))
		rightOk := endPos >= len(text) || !isAlphaNum(rune(text[endPos]))
		if leftOk && rightOk {
			return true
		}
		idx = absPos + 1
	}
}

func isAlphaNum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func looksLikeBalanceQuery(text string) bool {
	return strings.Contains(text, "cek saldo") ||
		strings.Contains(text, "saldo berapa") ||
		strings.Contains(text, "saldo saya") ||
		strings.Contains(text, "balance")
}

func looksLikeStatusQuery(text string) bool {
	return strings.Contains(text, "cek status") ||
		strings.Contains(text, "status transaksi") ||
		strings.Contains(text, "status order") ||
		strings.Contains(text, "status pesanan") ||
		strings.Contains(text, "cek transaksi") ||
		strings.Contains(text, "cek order")
}

func detectPaymentMethod(text string) string {
	switch {
	case strings.Contains(text, "saldo"), strings.Contains(text, "deposit"), strings.Contains(text, "pakai saldo"):
		return "deposit"
	case strings.Contains(text, "bri"), strings.Contains(text, "bank bri"), strings.Contains(text, "via bank"), strings.Contains(text, "transfer bank"):
		return "bri"
	case strings.Contains(text, "qr"), strings.Contains(text, "qris"), strings.Contains(text, "scan"):
		return "qris"
	default:
		return ""
	}
}

func containsPurchaseKeyword(text string) bool {
	return strings.Contains(text, "beli") ||
		strings.Contains(text, "order") ||
		strings.Contains(text, "ambil") ||
		strings.Contains(text, "pesan") ||
		strings.Contains(text, "isi pulsa") ||
		strings.Contains(text, "isi token") ||
		strings.Contains(text, "isi kuota") ||
		strings.Contains(text, "top up") ||
		strings.Contains(text, "topup") ||
		strings.Contains(text, "tolong belikan") ||
		strings.Contains(text, "bantu belikan")
}

func extractLikelyProductCode(text string) string {
	tokens := strings.Fields(text)
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if strings.HasPrefix(token, "ml") || strings.HasPrefix(token, "bm") {
			return strings.ToUpper(token)
		}
		if len(token) <= 6 && hasAlphaNumeric(token) {
			return strings.ToUpper(token)
		}
	}
	return ""
}

func hasAlphaNumeric(token string) bool {
	hasLetter := false
	hasDigit := false
	for _, r := range token {
		if r >= '0' && r <= '9' {
			hasDigit = true
		}
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			hasLetter = true
		}
	}
	return hasLetter && hasDigit
}

func normalizePaymentMethod(value, fallback string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" {
		v = strings.ToLower(strings.TrimSpace(fallback))
	}
	if v == "" {
		return ""
	}
	// Prefer deposit if any deposit keywords present.
	if strings.Contains(v, "saldo") || strings.Contains(v, "deposit") || strings.Contains(v, "balance") {
		return "deposit"
	}
	// Map BRI/bank keywords.
	if strings.Contains(v, "bri") || v == "bank" || strings.Contains(v, "transfer bank") || strings.Contains(v, "bank transfer") {
		return "bri"
	}
	// Map QRIS/transfer including common variants/typos.
	if strings.Contains(v, "qris") || strings.Contains(v, "qr") || strings.Contains(v, "scan") ||
		strings.Contains(v, "transfer") || strings.Contains(v, "instan") || strings.Contains(v, "instant") || strings.Contains(v, "istant") {
		return "qris"
	}
	switch v {
	case "saldo", "deposit", "balance":
		return "deposit"
	case "qr", "qris":
		return "qris"
	case "bri", "bank", "bank bri":
		return "bri"
	default:
		return v
	}
}

var (
	customerIDParenPattern = regexp.MustCompile(`(?i)([0-9a-zA-Z]{4,})\s*[\(\[]\s*([0-9a-zA-Z]{2,})\s*[\)\]]`)
	customerSplitPattern   = regexp.MustCompile(`(?i)([0-9a-zA-Z]{4,})\s*[-/]\s*([0-9a-zA-Z]{2,})`)
	idKeywordPattern       = regexp.MustCompile(`(?i)(?:id|uid|user\s*id|akun|account|target|player\s*id)[^0-9a-zA-Z]*([0-9a-zA-Z]{4,})`)
	zoneKeywordPattern     = regexp.MustCompile(`(?i)(?:zone|server|sv|srv|server\s*id|zone\s*id)[^0-9a-zA-Z]*([0-9a-zA-Z]{2,})`)
	refIDPattern           = regexp.MustCompile(`(?i)\bref(?:f|f_id|id)?\s*[:#-]?\s*([0-9a-zA-Z]{6,})`)
	trxIDPattern           = regexp.MustCompile(`(?i)\b(?:id\s*transaksi|trx|transaksi|order)\s*[:#-]?\s*([0-9a-zA-Z]{6,})`)
)

func extractCustomerTokens(text string) (string, string) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return "", ""
	}
	if match := customerIDParenPattern.FindStringSubmatch(trimmed); len(match) == 3 {
		return cleanCustomerToken(match[1]), cleanCustomerToken(match[2])
	}
	var id string
	if matches := idKeywordPattern.FindAllStringSubmatch(trimmed, -1); len(matches) > 0 {
		id = cleanCustomerToken(matches[len(matches)-1][1])
	}
	zone := ""
	if matches := zoneKeywordPattern.FindAllStringSubmatch(trimmed, -1); len(matches) > 0 {
		zone = cleanCustomerToken(matches[len(matches)-1][1])
	}
	if id == "" {
		if match := customerSplitPattern.FindStringSubmatch(trimmed); len(match) == 3 {
			id = cleanCustomerToken(match[1])
			if zone == "" {
				zone = cleanCustomerToken(match[2])
			}
		}
	} else if zone == "" {
		if match := customerSplitPattern.FindStringSubmatch(trimmed); len(match) == 3 {
			if cleanCustomerToken(match[1]) == id {
				zone = cleanCustomerToken(match[2])
			}
		}
	}
	return id, zone
}

func extractRefID(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	if match := refIDPattern.FindStringSubmatch(trimmed); len(match) == 2 {
		return strings.TrimSpace(match[1])
	}
	if match := trxIDPattern.FindStringSubmatch(trimmed); len(match) == 2 {
		return strings.TrimSpace(match[1])
	}
	// Fallback: pick the longest alphanumeric token (likely ref) with length >= 8
	longest := ""
	for _, token := range strings.Fields(trimmed) {
		clean := cleanCustomerToken(token)
		if len(clean) >= 8 && len(clean) > len(longest) {
			longest = clean
		}
	}
	return longest
}

func cleanCustomerToken(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if unicode.IsDigit(r) || unicode.IsLetter(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func normalizeCustomerTarget(rawID, rawZone string) (string, string) {
	id := cleanCustomerToken(rawID)
	zone := cleanCustomerToken(rawZone)
	if rawID == "" {
		return id, zone
	}
	if match := customerIDParenPattern.FindStringSubmatch(rawID); len(match) == 3 {
		base := cleanCustomerToken(match[1])
		zoneFromID := cleanCustomerToken(match[2])
		if zone == "" {
			zone = zoneFromID
		}
		if zone == "" {
			return base, ""
		}
		return fmt.Sprintf("%s(%s)", base, zone), zone
	}
	if zone == "" {
		if match := customerSplitPattern.FindStringSubmatch(rawID); len(match) == 3 {
			id = cleanCustomerToken(match[1])
			zone = cleanCustomerToken(match[2])
		}
	}
	if zone != "" {
		return fmt.Sprintf("%s(%s)", id, zone), zone
	}
	return id, zone
}

func generateTargetCandidates(normalizedID, zone, raw string) []string {
	seen := map[string]struct{}{}
	ordered := make([]string, 0, 12)
	add := func(candidate string) {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		ordered = append(ordered, candidate)
	}

	add(normalizedID)
	add(raw)

	base, zoneFromNormalized := extractCustomerTokens(normalizedID)
	if zone == "" {
		zone = zoneFromNormalized
	}
	if base == "" {
		if idx := strings.IndexAny(normalizedID, " ()#|.-/"); idx > 0 {
			base = cleanCustomerToken(normalizedID[:idx])
		}
	}

	if raw != "" {
		if bRaw, zRaw := extractCustomerTokens(raw); bRaw != "" {
			if base == "" {
				base = bRaw
			}
			if zone == "" {
				zone = zRaw
			}
		}
	}

	base = cleanCustomerToken(base)
	if base == "" && normalizedID != "" {
		base = cleanCustomerToken(normalizedID)
	}

	appendVariant := func(format string) {
		add(format)
	}

	if base != "" && zone != "" {
		variants := []string{
			fmt.Sprintf("%s(%s)", base, zone),
			fmt.Sprintf("%s %s", base, zone),
			fmt.Sprintf("%s.%s", base, zone),
			fmt.Sprintf("%s#%s", base, zone),
			fmt.Sprintf("%s|%s", base, zone),
			fmt.Sprintf("%s-%s", base, zone),
			fmt.Sprintf("%s/%s", base, zone),
			fmt.Sprintf("%s:%s", base, zone),
			fmt.Sprintf("%s_%s", base, zone),
			base + zone,
		}
		for _, v := range variants {
			appendVariant(v)
		}
	}

	if base != "" {
		appendVariant(base)
	}

	return ordered
}

func (e *Engine) createPrepaidWithVariants(ctx context.Context, productCode, refID string, candidates []string, userID string) (*atl.TransactionResponse, string, error) {
	var lastResp *atl.TransactionResponse
	var lastTarget string

	for idx, candidate := range candidates {
		target := strings.TrimSpace(candidate)
		if target == "" {
			continue
		}
		e.logger.Debug("atlantic prepaid attempt", "product_code", productCode, "target", target, "attempt", idx+1, "user_id", userID)
		resp, err := e.atl.CreatePrepaidTransaction(ctx, atl.CreatePrepaidRequest{
			ProductCode: productCode,
			CustomerID:  target,
			RefID:       refID,
		})
		if err != nil {
			if shouldRetryTargetError(err) && idx+1 < len(candidates) {
				e.logger.Debug("atlantic prepaid retry due to error", "product_code", productCode, "target", target, "error", err.Error(), "next_attempt", idx+2, "user_id", userID)
				continue
			}
			return nil, "", err
		}

		status := strings.ToLower(strings.TrimSpace(resp.Status))
		lastResp = resp
		lastTarget = target

		if status == "" || status == "pending" || status == "processing" || status == "process" ||
			status == "success" || status == "completed" || status == "ok" || status == "available" {
			return resp, target, nil
		}

		if shouldRetryTarget(resp.Message) && idx+1 < len(candidates) {
			e.logger.Debug("retrying atlantic target format", "product_code", productCode, "failed_target", target, "message", resp.Message, "next_attempt", idx+2)
			continue
		}

		return resp, target, nil
	}

	if lastResp != nil {
		return lastResp, lastTarget, nil
	}
	return nil, "", fmt.Errorf("tidak ada target kandidat yang valid")
}

func shouldRetryTarget(message string) bool {
	lower := strings.ToLower(strings.TrimSpace(message))
	if lower == "" {
		return false
	}
	keywords := []string{
		"format target",
		"format salah",
		"format id",
		"format tidak sesuai",
		"target tidak sesuai",
		"id player",
		"masukkan id",
	}
	for _, kw := range keywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

func shouldRetryTargetError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	keywords := []string{
		"format target",
		"format id",
		"target tidak sesuai",
		"format tidak sesuai",
		"invalid target",
	}
	for _, kw := range keywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// isTemporaryServerError checks if the error/message indicates a transient server issue worth retrying.
func isTemporaryServerError(err error, message string) bool {
	lowerErr := ""
	if err != nil {
		lowerErr = strings.ToLower(err.Error())
	}
	lowerMsg := strings.ToLower(strings.TrimSpace(message))

	// Common signals of transient backend failures
	if strings.Contains(lowerErr, "status=500") || strings.Contains(lowerErr, "code=500") {
		return true
	}
	if strings.Contains(lowerErr, "gangguan server") || strings.Contains(lowerErr, "server error") ||
		strings.Contains(lowerErr, "silahkan coba") || strings.Contains(lowerErr, "coba beberapa saat") {
		return true
	}
	if lowerMsg != "" {
		if strings.Contains(lowerMsg, "gangguan server") || strings.Contains(lowerMsg, "server error") ||
			strings.Contains(lowerMsg, "silahkan coba") || strings.Contains(lowerMsg, "coba beberapa saat") {
			return true
		}
	}
	return false
}

func productRequiresZone(item *atl.PriceListItem) bool {
	if item == nil {
		return false
	}
	combined := strings.ToLower(strings.Join([]string{
		item.Code,
		item.Name,
		item.Description,
		item.Provider,
	}, " "))
	if strings.HasPrefix(strings.ToUpper(item.Code), "ML") {
		return true
	}
	if strings.Contains(combined, "mobile legend") || strings.Contains(combined, "mlbb") {
		return true
	}
	if strings.Contains(combined, "id(server") || strings.Contains(combined, "id server") || strings.Contains(combined, "server id") {
		return true
	}
	return false
}

// retryPrepaidAsync keeps retrying a prepaid transaction on temporary server errors and notifies the user of the outcome.
func (e *Engine) retryPrepaidAsync(ctx context.Context, userID string, to types.JID, productName string, productCode string, refID string, candidates []string, customerZone string) {
	// Backoff schedule
	backoffs := []time.Duration{5 * time.Second, 15 * time.Second, 30 * time.Second}

	var (
		resp       *atl.TransactionResponse
		usedTarget string
		lastErr    error
	)

	for i, d := range backoffs {
		r, u, err := e.createPrepaidWithVariants(ctx, productCode, refID, candidates, userID)
		if err != nil {
			lastErr = err
			if isTemporaryServerError(err, "") && i+1 < len(backoffs) {
				time.Sleep(d)
				continue
			}
			break
		}
		resp = r
		usedTarget = u
		break
	}

	// On success or pending-like status, update order and notify user.
	if resp != nil {
		meta := map[string]any{
			"message": resp.Message,
			"sn":      resp.SN,
		}
		if usedTarget != "" {
			meta["customer_id"] = usedTarget
		}
		if customerZone != "" {
			meta["customer_zone"] = customerZone
		}
		if err := e.repo.UpdateOrderStatus(ctx, refID, resp.Status, meta); err != nil {
			e.logger.Warn("retry: update order status failed", "error", err, "order_ref", refID)
		}

		status := strings.ToLower(strings.TrimSpace(resp.Status))
		switch status {
		case "", "pending", "processing", "process":
			msg := fmt.Sprintf("Update: transaksi %s (%s) lagi diproses. Ref: %s.", productName, productCode, refID)
			if strings.TrimSpace(resp.Message) != "" {
				msg = fmt.Sprintf("%s %s", msg, strings.TrimSpace(resp.Message))
			}
			_ = e.respondAndLog(context.Background(), to, userID, msg, "create_prepaid_retry_pending")
		case "success", "completed", "ok", "available":
			msg := fmt.Sprintf("Sukses: transaksi %s (%s) berhasil! Ref: %s.", productName, productCode, refID)
			if resp.SN != "" {
				msg = fmt.Sprintf("%s SN: %s.", msg, resp.SN)
			}
			if strings.TrimSpace(resp.Message) != "" {
				msg = fmt.Sprintf("%s %s", msg, strings.TrimSpace(resp.Message))
			}
			_ = e.respondAndLog(context.Background(), to, userID, msg, "create_prepaid_retry_success")
		default:
			fail := strings.TrimSpace(resp.Message)
			if fail == "" {
				fail = "Transaksi gagal. Saldo deposit sepertinya belum cukup."
			}
			_ = e.repo.UpdateOrderStatus(ctx, refID, "failed", map[string]any{
				"message": fail,
			})
			msg := fmt.Sprintf("Maaf, transaksi %s (%s) belum berhasil. %s", productName, productCode, fail)
			_ = e.respondAndLog(context.Background(), to, userID, msg, "create_prepaid_retry_failed")
		}
		return
	}

	// All retries exhausted without a response
	if isTemporaryServerError(lastErr, "") {
		// Keep the order in processing and inform the user it's still queued due to server issues.
		queuedMsg := fmt.Sprintf("Masih ada gangguan server. Transaksimu %s (%s) tetap ku antre. Ref: %s.", productName, productCode, refID)
		_ = e.respondAndLog(context.Background(), to, userID, queuedMsg, "create_prepaid_retry_queued")
		// Do not mark order failed; allow future retries or manual recovery.
		return
	}
	friendly := friendlyAtlanticError(lastErr)
	if friendly == "" {
		friendly = "Transaksi belum bisa diproses."
	}
	if err := e.repo.UpdateOrderStatus(ctx, refID, "failed", map[string]any{
		"error": strings.TrimSpace(lastErr.Error()),
	}); err != nil {
		e.logger.Warn("retry: update order after failure", "error", err, "order_ref", refID)
	}
	msg := fmt.Sprintf("Maaf, transaksi %s (%s) belum bisa diproses. %s", productName, productCode, friendly)
	_ = e.respondAndLog(context.Background(), to, userID, msg, "create_prepaid_retry_failed")
}

func (e *Engine) defaultDepositMethod() string {
	method := normalizePaymentMethod(e.cfg.DefaultDepositMethod, "")
	if method == "deposit" {
		return ""
	}
	return method
}

func (e *Engine) resolveProductFromQuery(ctx context.Context, productCode, productType, query, provider string) (*atl.PriceListItem, string, error) {
	provider = strings.TrimSpace(strings.ToLower(provider))
	searchTypes := []string{}
	if productType != "" {
		searchTypes = append(searchTypes, defaultProductType(productType))
	} else {
		searchTypes = append(searchTypes, "prabayar", "pascabayar")
	}
	query = strings.TrimSpace(query)

	e.logger.Debug("resolveProductFromQuery", "product_code", productCode, "product_type", productType, "query", query, "provider", provider)

	for _, t := range searchTypes {
		items, _, err := e.fetchPriceList(ctx, t)
		if err != nil {
			return nil, "", fmt.Errorf("gagal mengambil price list (%s): %w", t, err)
		}
		if productCode != "" {
			for _, candidate := range items {
				if strings.EqualFold(candidate.Code, productCode) {
					e.logger.Debug("product found by code", "code", productCode, "name", candidate.Name)
					item := candidate
					return &item, t, nil
				}
			}
			e.logger.Debug("product code not found in type", "code", productCode, "type", t, "total_items", len(items))
		}
		if query != "" {
			matches := filterByQuery(items, query, provider, false)
			e.logger.Debug("filterByQuery result", "query", query, "provider", provider, "matches", len(matches))
			if len(matches) > 0 {
				item := matches[0]
				return &item, t, nil
			}
		}
		if provider != "" {
			matches := filterByQuery(items, provider, provider, false)
			if len(matches) > 0 {
				item := matches[0]
				return &item, t, nil
			}
		}
	}
	return nil, "", nil
}

func (e *Engine) executePrepaidWithBalance(ctx context.Context, evt *events.Message, user *repo.User, productCode, customerID, customerZone, rawCustomerID, orderRef string, item *atl.PriceListItem, productType string) error {
	// Check balance BEFORE processing the transaction
	amount := priceToAmount(item.Price)
	ub, err := e.repo.GetUserBalance(ctx, user.ID)
	if err != nil {
		e.logger.Error("failed to check balance", "error", err, "user", user.ID)
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, "Gagal mengecek saldo. Coba lagi nanti ya.", "balance_check_failed")
	}
	if ub == nil || ub.SaldoConfirmed < amount {
		currentBalance := int64(0)
		if ub != nil {
			currentBalance = ub.SaldoConfirmed
		}
		reply := fmt.Sprintf("Saldo kamu tidak mencukupi.\n\n💰 Saldo: %s\n🏷️ Harga: %s\n\nSilakan deposit dulu atau gunakan metode pembayaran lain (BRI/QRIS).\nKetik: \"deposit [jumlah]\" untuk top up saldo.", formatCurrency(float64(currentBalance)), formatCurrency(item.Price))
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "insufficient_balance")
	}

	refID := strings.TrimSpace(orderRef)
	if refID == "" {
		refID = generateRefID("trx")
	}
	// Pre-create order so we can update status even if Atlantic returns an error.
	// (amount already computed above for balance check)
	preMeta := map[string]any{
		"customer_id": customerID,
	}
	if customerZone != "" {
		preMeta["customer_zone"] = customerZone
	}
	if strings.TrimSpace(productType) != "" {
		preMeta["product_type"] = productType
	}
	preMeta["precreate"] = true
	if _, err := e.repo.InsertOrder(ctx, repo.Order{
		UserID:      user.ID,
		OrderRef:    refID,
		ProductCode: productCode,
		Amount:      amount,
		Status:      "processing",
		Metadata:    preMeta,
	}); err != nil {
		e.logger.Warn("failed precreate order", "error", err, "order_ref", refID)
	}

	candidates := generateTargetCandidates(customerID, customerZone, rawCustomerID)

	const maxAttempts = 4
	var (
		resp       *atl.TransactionResponse
		usedTarget string
		lastErr    error
	)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		r, u, err := e.createPrepaidWithVariants(ctx, productCode, refID, candidates, user.ID)
		if err != nil {
			// Retry on transient server errors (e.g., 500 / "gangguan server").
			if isTemporaryServerError(err, "") && attempt < maxAttempts {
				time.Sleep(800 * time.Millisecond)
				continue
			}
			lastErr = err
			break
		}
		resp = r
		usedTarget = u
		break
	}

	if resp == nil {
		// If it's a temporary backend error (e.g., 500 / "gangguan server"), keep order as processing,
		// queue background retries, and inform user that the transaction is queued.
		if isTemporaryServerError(lastErr, "") {
			queuedMsg := fmt.Sprintf("Lagi ada gangguan server. Transaksimu %s (%s) sudah ku antre ya. Ref: %s. Akan ku kabari begitu ada update.", item.Name, item.Code, refID)
			_ = e.respondAndLog(ctx, evt.Info.Sender, user.ID, queuedMsg, "create_prepaid_queued")

			// Continue attempts in background with backoff.
			go e.retryPrepaidAsync(context.Background(), user.ID, evt.Info.Sender, item.Name, productCode, refID, candidates, customerZone)

			// Keep user flow clean; do not mark as failed now.
			return nil
		}

		// Non-temporary failure after retries.
		friendly := friendlyAtlanticError(lastErr)
		if friendly == "" {
			friendly = "Transaksi belum bisa diproses karena gangguan sistem. Coba sebentar lagi ya."
		}
		failMeta := map[string]any{
			"customer_id": customerID,
			"error":       strings.TrimSpace(lastErr.Error()),
		}
		if customerZone != "" {
			failMeta["customer_zone"] = customerZone
		}
		if err := e.repo.UpdateOrderStatus(ctx, refID, "failed", failMeta); err != nil {
			e.logger.Warn("update order after failure", "error", err, "order_ref", refID)
		}
		message := fmt.Sprintf("Transaksi %s (%s) gagal diproses. %s", item.Name, item.Code, friendly)
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, message, "create_prepaid_failed")
	}

	// Success path: update order with final status and metadata.
	if usedTarget != "" {
		customerID = usedTarget
	}
	metadata := map[string]any{
		"customer_id": customerID,
		"message":     resp.Message,
		"sn":          resp.SN,
	}
	if customerZone != "" {
		metadata["customer_zone"] = customerZone
	}
	if err := e.repo.UpdateOrderStatus(ctx, refID, resp.Status, metadata); err != nil {
		e.logger.Warn("failed updating order after success", "error", err, "order_ref", refID)
	}

	status := strings.ToLower(strings.TrimSpace(resp.Status))
	switch status {
	case "", "pending", "processing", "process":
		reply := fmt.Sprintf("Sip, transaksi %s (%s) lagi diproses. Ref: %s.", item.Name, item.Code, refID)
		if txt := strings.TrimSpace(resp.Message); txt != "" {
			reply = fmt.Sprintf("%s %s", reply, txt)
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_prepaid")
	case "success", "completed", "ok", "available":
		reply := fmt.Sprintf("Mantap, transaksi %s (%s) sukses! Ref: %s.", item.Name, item.Code, refID)
		if resp.SN != "" {
			reply = fmt.Sprintf("%s SN: %s.", reply, resp.SN)
		}
		if txt := strings.TrimSpace(resp.Message); txt != "" {
			reply = fmt.Sprintf("%s %s", reply, txt)
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_prepaid_success")
	default:
		failure := strings.TrimSpace(resp.Message)
		if failure == "" {
			failure = "Transaksi gagal. Saldo deposit sepertinya belum cukup."
		}
		if ub, err := e.repo.GetUserBalance(ctx, user.ID); err == nil && ub != nil {
			failure = fmt.Sprintf("%s Saldo kamu sekitar %s.", failure, formatCurrency(float64(ub.SaldoConfirmed)))
		}
		reply := fmt.Sprintf("Waduh, transaksi %s (%s) belum berhasil. %s", item.Name, item.Code, failure)
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_prepaid_failed")
	}
}

func (e *Engine) executePrepaidWithCheckout(ctx context.Context, evt *events.Message, user *repo.User, productCode, customerID, customerZone, rawCustomerID, orderRef string, item *atl.PriceListItem, method, productType string) error {
	depositRef := generateRefID("dep")
	orderRef = strings.TrimSpace(orderRef)
	if orderRef == "" {
		orderRef = generateRefID("trx")
	}
	amountInt := priceToAmount(item.Price)
	grossAmount := e.requiredDepositGross(amountInt)
	// Override deposit type to "bank" for BRI method.
	depositType := e.cfg.DefaultDepositType
	if strings.EqualFold(method, "BRI") || strings.EqualFold(method, "bri") {
		depositType = "bank"
	}
	if strings.EqualFold(method, "qris") && e.defaultDepositMethod() != "" && !strings.EqualFold(e.defaultDepositMethod(), "qris") {
		method = e.defaultDepositMethod()
	}
	depResp, err := e.atl.CreateDeposit(ctx, atl.DepositRequest{
		Method: method,
		Amount: float64(grossAmount),
		RefID:  depositRef,
		Type:   depositType,
	})
	if err != nil {
		e.logger.Warn("checkout deposit request failed", "error", err, "user_id", user.ID, "method", method, "product_code", productCode)
		feedback := friendlyAtlanticError(err)
		if feedback == "" {
			feedback = "Belum bisa menyiapkan deposit untuk transaksi ini. Coba pilih metode lain atau ulangi sebentar lagi."
		} else {
			feedback = fmt.Sprintf("Belum bisa menyiapkan deposit: %s", feedback)
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, feedback, "create_prepaid_checkout_failed")
	}

	feeAmount := priceToAmount(depResp.Fee)
	netFromProvider := priceToAmount(depResp.NetAmount)
	providerAmount := priceToAmount(depResp.Amount)
	netAmount := deriveNetAmount(amountInt, feeAmount, netFromProvider)
	depStatus, forced := forceSuccessIfPending(depResp.Status)

	metadata := map[string]any{
		"checkout":          depResp.Checkout,
		"product":           item.Name,
		"product_code":      item.Code,
		"gross_amount":      grossAmount,
		"requested_amount":  grossAmount,
		"target_net_amount": amountInt,
	}
	if forced {
		metadata["forced_success"] = true
		metadata["original_status"] = depResp.Status
	}
	if customerZone != "" {
		metadata["customer_zone"] = customerZone
	}
	if providerAmount > 0 {
		metadata["provider_amount"] = providerAmount
	}
	if feeAmount > 0 {
		metadata["fee"] = feeAmount
	}
	if netAmount > 0 {
		metadata["net_amount"] = netAmount
	}
	shortfall := int64(0)
	if netAmount > 0 && netAmount < amountInt {
		shortfall = amountInt - netAmount
		metadata["net_shortfall"] = shortfall
	}
	depositAmount := grossAmount
	if netAmount > 0 {
		depositAmount = netAmount
	}
	if _, err := e.repo.InsertDeposit(ctx, repo.Deposit{
		UserID:     user.ID,
		DepositRef: depositRef,
		Method:     method,
		Amount:     depositAmount,
		Status:     depStatus,
		Metadata:   metadata,
	}); err != nil {
		e.logger.Warn("failed storing deposit", "error", err)
	}

	orderMetadata := map[string]any{
		"customer_id":  customerID,
		"deposit_ref":  depositRef,
		"product_type": productType,
	}
	candidates := generateTargetCandidates(customerID, customerZone, rawCustomerID)
	if rawCustomerID != "" {
		orderMetadata["customer_id_raw"] = rawCustomerID
	}
	if len(candidates) > 0 {
		orderMetadata["target_candidates"] = candidates
	}
	if customerZone != "" {
		orderMetadata["customer_zone"] = customerZone
	}
	if _, err := e.repo.InsertOrder(ctx, repo.Order{
		UserID:      user.ID,
		OrderRef:    orderRef,
		ProductCode: productCode,
		Amount:      amountInt,
		Status:      "awaiting_payment",
		Metadata:    orderMetadata,
	}); err != nil {
		e.logger.Warn("failed storing pending order", "error", err)
	}

	summaryLine := summarizeDepositAmounts(grossAmount, feeAmount, netAmount)

	// If method is BRI/bank, show bank transfer info instead of QR.
	isBankMethod := strings.EqualFold(method, "BRI") || strings.EqualFold(method, "bri") || depositType == "bank"
	if isBankMethod {
		bankInfo := formatBankTransferInfo(depResp.Checkout)
		reply := fmt.Sprintf("Sip, sudah kubuatin deposit via BRI sebesar %s buat %s (%s).\nRef deposit: %s\nRef order: %s\n%s", formatCurrency(float64(grossAmount)), item.Name, item.Code, depositRef, orderRef, bankInfo)
		if summaryLine != "" {
			reply = fmt.Sprintf("%s\n%s", reply, summaryLine)
		}
		if shortfall > 0 {
			reply = fmt.Sprintf("%s\nSaldo masuk masih kurang %s dari harga produk. Tambah deposit ya supaya bisa ku proses.", reply, formatCurrency(float64(shortfall)))
		}
		return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_prepaid_checkout")
	}

	qrCaption := fmt.Sprintf("Deposit %s via %s untuk %s (%s).", depositRef, strings.ToUpper(method), item.Name, item.Code)
	if summaryLine != "" {
		qrCaption = fmt.Sprintf("%s\n%s", qrCaption, summaryLine)
	}
	qrSent := e.sendCheckoutQRImage(ctx, evt.Info.Sender, user.ID, depResp.Checkout, qrCaption, "create_prepaid_checkout")

	reply := fmt.Sprintf("Sip, sudah kubuatin deposit via %s sebesar %s buat %s (%s).\nRef deposit: %s\nRef order: %s\n%s", strings.ToUpper(method), formatCurrency(float64(grossAmount)), item.Name, item.Code, depositRef, orderRef, formatCheckoutInfo(depResp.Checkout, qrSent))
	if shortfall > 0 {
		reply = fmt.Sprintf("%s\nSaldo masuk masih kurang %s dari harga produk. Tambah deposit ya supaya bisa ku proses.", reply, formatCurrency(float64(shortfall)))
	}
	return e.respondAndLog(ctx, evt.Info.Sender, user.ID, reply, "create_prepaid_checkout")
}

func friendlyAtlanticError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return ""
	}
	if extracted := extractAtlanticJSONMessage(msg); extracted != "" {
		return extracted
	}
	if idx := strings.Index(msg, " error: "); idx >= 0 {
		candidate := strings.TrimSpace(msg[idx+len(" error: "):])
		if cut := strings.Index(candidate, "(code="); cut >= 0 {
			candidate = candidate[:cut]
		}
		return strings.TrimSpace(candidate)
	}
	if idx := strings.Index(msg, ": "); idx >= 0 {
		candidate := strings.TrimSpace(msg[idx+2:])
		if extracted := extractAtlanticJSONMessage(candidate); extracted != "" {
			return extracted
		}
	}
	// Check for specific error messages related to insufficient balance or invalid deposit method
	lowerMsg := strings.ToLower(msg)
	if strings.Contains(lowerMsg, "metode deposit tidak valid") ||
		strings.Contains(lowerMsg, "metode deposit non aktif") ||
		strings.Contains(lowerMsg, "deposit tidak valid") ||
		strings.Contains(lowerMsg, "deposit method tidak valid") ||
		strings.Contains(lowerMsg, "invalid deposit method") {
		return "Metode deposit tidak valid atau tidak aktif. Coba gunakan metode lain atau hubungi admin."
	}
	if strings.Contains(lowerMsg, "saldo tidak cukup") ||
		strings.Contains(lowerMsg, "insufficient balance") ||
		strings.Contains(lowerMsg, "insufficient funds") {
		return "Saldo deposit tidak cukup. Silakan top up deposit terlebih dahulu ya."
	}
	return msg
}

func extractAtlanticJSONMessage(raw string) string {
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start >= 0 && end > start {
		candidate := raw[start : end+1]
		var payload map[string]any
		if err := json.Unmarshal([]byte(candidate), &payload); err == nil {
			for _, key := range []string{"message", "info", "description", "error"} {
				if val, ok := payload[key]; ok {
					str := strings.TrimSpace(fmt.Sprintf("%v", val))
					if str != "" {
						return str
					}
				}
			}
		}
	}
	return ""
}

func priceToAmount(price float64) int64 {
	return int64(price + 0.5)
}

func formatCurrency(value float64) string {
	return fmt.Sprintf("Rp%d", priceToAmount(value))
}

func forceSuccessIfPending(status string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(status))
	switch normalized {
	case "pending", "process", "processing", "waiting", "awaiting", "progress":
		return "success", true
	default:
		return status, false
	}
}

func (e *Engine) sendCheckoutQRImage(ctx context.Context, to types.JID, userID string, checkout map[string]any, caption, category string) bool {
	imageURL := firstStringMap(checkout, "qr_image")
	qrString := firstStringMap(checkout, "qr_string")

	if imageURL != "" {
		data, mimeType, err := fetchQRImageData(ctx, imageURL)
		if err == nil {
			if mimeType == "" {
				mimeType = http.DetectContentType(data)
				if mimeType == "" {
					mimeType = "image/png"
				}
			}
			if err := e.gateway.SendImage(ctx, to, data, mimeType, caption); err == nil {
				if err := e.repo.InsertMessage(ctx, repo.MessageRecord{
					UserID:    userID,
					Direction: "outgoing",
					Type:      category + "_qr_image",
					MediaURL:  optionalString(imageURL),
				}); err != nil {
					e.logger.Warn("failed logging outgoing qr image", "error", err)
				}
				return true
			}
			e.logger.Warn("failed sending qr image", "error", err)
		} else {
			e.logger.Warn("failed preparing qr image", "error", err)
		}
	}

	if qrString == "" {
		return false
	}
	data, err := qrcode.Encode(qrString, qrcode.Medium, 256)
	if err != nil {
		e.logger.Warn("failed generating qr image", "error", err)
		return false
	}
	if err := e.gateway.SendImage(ctx, to, data, "image/png", caption); err != nil {

func isNotFoundAtlanticError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "not found") {
		return true
	}
	if strings.Contains(lower, "tidak ditemukan") {
		return true
	}
	if strings.Contains(lower, "transaksi tidak ditemukan") {
		return true
	}
	return false
}
		e.logger.Warn("failed sending qr image", "error", err)
		return false
	}
	if err := e.repo.InsertMessage(ctx, repo.MessageRecord{
		UserID:    userID,
		Direction: "outgoing",
		Type:      category + "_qr_image",
		MediaURL:  optionalString(imageURL),
	}); err != nil {
		e.logger.Warn("failed logging outgoing qr image", "error", err)
	}
	return true
}

func fetchQRImageData(ctx context.Context, src string) ([]byte, string, error) {
	trimmed := strings.TrimSpace(src)
	if trimmed == "" {
		return nil, "", errors.New("empty qr image source")
	}
	if strings.HasPrefix(trimmed, "data:") {
		return decodeDataURL(trimmed)
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, trimmed, nil)
		if err != nil {
			return nil, "", fmt.Errorf("create qr image request: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, "", fmt.Errorf("download qr image: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			return nil, "", fmt.Errorf("download qr image: unexpected status %d", resp.StatusCode)
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, "", fmt.Errorf("read qr image: %w", err)
		}
		mimeType := resp.Header.Get("Content-Type")
		if mimeType == "" {
			mimeType = http.DetectContentType(data)
		}
		return data, mimeType, nil
	}
	decoded, mimeType, err := decodeBase64Image(trimmed)
	if err != nil {
		return nil, "", fmt.Errorf("decode qr image: %w", err)
	}
	return decoded, mimeType, nil
}

func decodeDataURL(raw string) ([]byte, string, error) {
	comma := strings.IndexByte(raw, ',')
	if comma <= 0 {
		return nil, "", fmt.Errorf("invalid data url")
	}
	meta := raw[:comma]
	payload := raw[comma+1:]

	var mimeType string
	if strings.HasPrefix(meta, "data:") {
		meta = meta[len("data:"):]
		if semi := strings.IndexByte(meta, ';'); semi >= 0 {
			mimeType = meta[:semi]
			meta = meta[semi+1:]
		} else if meta != "" && meta != "base64" {
			mimeType = meta
			meta = ""
		}
	}

	if strings.Contains(meta, "base64") {
		data, err := decodeBase64(payload)
		if err != nil {
			return nil, "", fmt.Errorf("decode data url: %w", err)
		}
		if mimeType == "" {
			mimeType = http.DetectContentType(data)
		}
		return data, mimeType, nil
	}

	decoded, err := url.QueryUnescape(payload)
	if err != nil {
		return nil, "", fmt.Errorf("decode data url payload: %w", err)
	}
	data := []byte(decoded)
	if mimeType == "" {
		mimeType = http.DetectContentType(data)
	}
	return data, mimeType, nil
}

func decodeBase64Image(raw string) ([]byte, string, error) {
	data, err := decodeBase64(raw)
	if err != nil {
		return nil, "", err
	}
	mimeType := http.DetectContentType(data)
	return data, mimeType, nil
}

func decodeBase64(value string) ([]byte, error) {
	encodings := []*base64.Encoding{
		base64.StdEncoding,
		base64.URLEncoding,
		base64.RawStdEncoding,
		base64.RawURLEncoding,
	}
	for _, enc := range encodings {
		if data, err := enc.DecodeString(value); err == nil {
			return data, nil
		}
	}
	return nil, fmt.Errorf("invalid base64 data")
}

func formatCheckoutInfo(checkout map[string]any, qrImageSent bool) string {
	if len(checkout) == 0 {
		return "Instruksi pembayaran akan dikirim setelah checkout tersedia."
	}
	grossVal := parseAmountString(firstStringMap(checkout, "gross_amount"))
	if grossVal == 0 {
		grossVal = parseAmountString(firstStringMap(checkout, "nominal"))
	}
	if grossVal == 0 {
		grossVal = parseAmountString(firstStringMap(checkout, "amount"))
	}
	if grossVal == 0 {
		grossVal = parseAmountString(firstStringMap(checkout, "provider_amount"))
	}
	feeVal := parseAmountString(firstStringMap(checkout, "fee"))
	if feeVal == 0 {
		feeVal = parseAmountString(firstStringMap(checkout, "admin_fee"))
	}
	netVal := parseAmountString(firstStringMap(checkout, "net_amount"))
	if netVal == 0 {
		netVal = parseAmountString(firstStringMap(checkout, "get_balance"))
	}
	if netVal == 0 {
		netVal = parseAmountString(firstStringMap(checkout, "saldo_masuk"))
	}
	summary := summarizeDepositAmounts(grossVal, feeVal, netVal)
	qrImage := firstStringMap(checkout, "qr_image")
	qrString := firstStringMap(checkout, "qr_string")
	expired := firstStringMap(checkout, "expired_at")
	var builder strings.Builder
	if summary != "" {
		builder.WriteString(summary)
		builder.WriteString("\n")
	}
	if qrImage != "" {
		if qrImageSent {
			builder.WriteString("QR sudah kukirim sebagai gambar terpisah.\n")
		} else {
			builder.WriteString(fmt.Sprintf("Scan QR berikut: %s\n", qrImage))
		}
	}
	if qrString != "" && !qrImageSent {
		builder.WriteString(fmt.Sprintf("QR String: %s\n", qrString))
	}
	if expired != "" {
		builder.WriteString(fmt.Sprintf("Berlaku sampai: %s\n", expired))
	}
	if builder.Len() == 0 {
		data, err := json.MarshalIndent(checkout, "", "  ")
		if err != nil {
			return fmt.Sprintf("%v", checkout)
		}
		return string(data)
	}
	return strings.TrimSpace(builder.String())
}

// formatBankTransferInfo formats bank transfer details (BRI, etc.) from Atlantic checkout response.
func formatBankTransferInfo(checkout map[string]any) string {
	if len(checkout) == 0 {
		return "Instruksi transfer akan dikirim setelah tersedia."
	}
	bank := firstStringMap(checkout, "bank")
	tujuan := firstStringMap(checkout, "tujuan")
	if tujuan == "" {
		tujuan = firstStringMap(checkout, "no_rekening")
	}
	if tujuan == "" {
		tujuan = firstStringMap(checkout, "account_no")
	}
	atasNama := firstStringMap(checkout, "atas_nama")
	if atasNama == "" {
		atasNama = firstStringMap(checkout, "account_name")
	}
	nominal := firstStringMap(checkout, "nominal")
	if nominal == "" {
		nominal = firstStringMap(checkout, "amount")
	}
	tambahan := firstStringMap(checkout, "tambahan")
	if tambahan == "" {
		tambahan = firstStringMap(checkout, "unique_code")
	}
	expired := firstStringMap(checkout, "expired_at")

	var sb strings.Builder
	sb.WriteString("\n🏦 *TRANSFER BANK*\n")
	if bank != "" {
		sb.WriteString(fmt.Sprintf("Bank: *%s*\n", strings.ToUpper(bank)))
	}
	if tujuan != "" {
		sb.WriteString(fmt.Sprintf("No. Rekening: *%s*\n", tujuan))
	}
	if atasNama != "" {
		sb.WriteString(fmt.Sprintf("Atas Nama: *%s*\n", atasNama))
	}
	if nominal != "" {
		sb.WriteString(fmt.Sprintf("Nominal: *Rp %s*\n", nominal))
	}
	if tambahan != "" {
		sb.WriteString(fmt.Sprintf("Kode Unik: *%s*\n", tambahan))
	}
	if expired != "" {
		sb.WriteString(fmt.Sprintf("Berlaku sampai: %s\n", expired))
	}
	sb.WriteString("\n⚠️ Transfer *tepat* sesuai nominal agar deposit otomatis terverifikasi.")
	return strings.TrimSpace(sb.String())
}

func firstStringMap(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case string:
			return strings.TrimSpace(v)
		case float64:
			return fmt.Sprintf("%.0f", v)
		}
	}
	return ""
}

func stringValue(meta map[string]any, key string) string {
	if meta == nil {
		return ""
	}
	val, ok := meta[key]
	if !ok || val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		if v == 0 {
			return ""
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		if v == 0 {
			return ""
		}
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case int:
		if v == 0 {
			return ""
		}
		return strconv.Itoa(v)
	case int64:
		if v == 0 {
			return ""
		}
		return strconv.FormatInt(v, 10)
	case json.Number:
		return v.String()
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
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

func (e *Engine) buildConversationContext(ctx context.Context, userID string) (string, string) {
	messages, err := e.repo.ListRecentMessages(ctx, userID, 10)
	if err != nil {
		e.logger.Debug("failed fetching message history", "error", err)
		return "", ""
	}
	if len(messages) == 0 {
		return "", ""
	}
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}
	var lines []string
	var lastBot string
	for _, msg := range messages {
		if msg.Content == nil {
			continue
		}
		text := strings.TrimSpace(*msg.Content)
		if text == "" {
			continue
		}
		role := "User"
		if msg.Direction == "outgoing" {
			role = "Bot"
			lastBot = text
		}
		lines = append(lines, fmt.Sprintf("%s: %s", role, truncateForPrompt(text, 160)))
	}
	if len(lines) > 6 {
		lines = lines[len(lines)-6:]
	}
	return strings.Join(lines, "\n"), lastBot
}

func truncateForPrompt(text string, limit int) string {
	runes := []rune(strings.TrimSpace(text))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit]) + "..."
}

func generateRefID(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, strings.ReplaceAll(uuid.NewString(), "-", "")[:16])
}

func helpMessage() string {
	return "Aku menyediakan berbagai layanan digital:\n\n📱 *Pulsa & Paket Data* - Telkomsel, Indosat, XL, Tri, Smartfren\n🎮 *Top Up Game* - Mobile Legends, Free Fire, PUBG, dll\n⚡ *Token Listrik* - Prabayar & Pascabayar\n💳 *Bayar Tagihan* - PLN, PDAM, BPJS, dll\n💰 *Deposit & Transfer* - QRIS, Bank Transfer, E-wallet\n\nContoh penggunaan:\n• \"pulsa telkomsel 20k\" - cek harga pulsa\n• \"budget 5000\" - tampilkan produk ≤5000\n• \"top up ML 12345\" - beli diamond Mobile Legends\n• \"cek tagihan PLN 123456\" - cek tagihan listrik"
}

func paymentInfoMessage() string {
	return "Metode pembayaran yang tersedia:\n\n🏦 *BRI* — Transfer via Bank BRI\n📱 *QRIS* — Scan QR Code (bisa pakai e-wallet apapun)\n💰 *Saldo Deposit* — Pakai saldo yang sudah didepositkan\n\nCara bayar:\n• Ketik \"beli [kode produk] [ID tujuan]\" lalu pilih metode\n• Contoh: \"beli ML3 12345678(2126) via qris\"\n• Atau beli dulu, nanti akan ditanya mau bayar pakai apa"
}

func looksLikePaymentQuery(text string) bool {
	hasPaymentWord := strings.Contains(text, "pembayaran") ||
		strings.Contains(text, "bayar") ||
		strings.Contains(text, "payment")
	hasQuestionWord := strings.Contains(text, "via") ||
		strings.Contains(text, "pakai") ||
		strings.Contains(text, "apa saja") ||
		strings.Contains(text, "apa aja") ||
		strings.Contains(text, "metode") ||
		strings.Contains(text, "cara") ||
		strings.Contains(text, "gimana") ||
		strings.Contains(text, "bagaimana")
	return hasPaymentWord && hasQuestionWord
}

func (e *Engine) depositFeeAmount(gross int64) int64 {
	if gross <= 0 {
		return 0
	}
	fee := e.cfg.DepositFeeFixed
	if fee < 0 {
		fee = 0
	}
	if percent := e.cfg.DepositFeePercent; percent > 0 {
		fee += int64(math.Ceil(float64(gross) * percent))
	}
	if fee < 0 {
		fee = 0
	}
	if fee > gross {
		return gross
	}
	return fee
}

func (e *Engine) requiredDepositGross(targetNet int64) int64 {
	if targetNet <= 0 {
		return targetNet
	}
	if e.cfg.DepositFeePercent <= 0 && e.cfg.DepositFeeFixed <= 0 {
		return targetNet
	}
	percent := e.cfg.DepositFeePercent
	fixed := e.cfg.DepositFeeFixed
	gross := targetNet
	if percent > 0 && percent < 0.99 {
		estimate := (float64(targetNet) + float64(fixed)) / (1 - percent)
		gross = int64(math.Ceil(estimate))
	} else {
		gross = targetNet + fixed
	}
	if gross <= targetNet {
		gross = targetNet + fixed
		if gross <= targetNet {
			gross = targetNet + 1
		}
	}
	for i := 0; i < 12; i++ {
		fee := e.depositFeeAmount(gross)
		if gross-fee >= targetNet {
			return gross
		}
		gross++
	}
	return gross
}

func (e *Engine) fetchPriceList(ctx context.Context, productType string) ([]atl.PriceListItem, bool, error) {
	items, err := e.atl.PriceList(ctx, productType, false)
	if err == nil && len(items) > 0 {
		e.storePriceCache(productType, items)
		return items, false, nil
	}
	if cached, ok := e.getPriceCache(productType); ok && len(cached) > 0 {
		if err != nil {
			e.logger.Warn("price list fetch failed, using cached data", "type", productType, "error", err)
		}
		return cached, true, nil
	}
	if err == nil {
		err = fmt.Errorf("price list kosong untuk %s", strings.ToUpper(productType))
	}
	return nil, false, err
}

func (e *Engine) storePriceCache(productType string, items []atl.PriceListItem) {
	e.mu.Lock()
	defer e.mu.Unlock()
	clone := make([]atl.PriceListItem, len(items))
	copy(clone, items)
	e.priceCache[productType] = priceCacheEntry{
		items:   clone,
		expires: time.Now().Add(e.priceCacheTTL),
	}
}

func (e *Engine) getPriceCache(productType string) ([]atl.PriceListItem, bool) {
	e.mu.RLock()
	entry, ok := e.priceCache[productType]
	e.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expires) {
		e.mu.Lock()
		delete(e.priceCache, productType)
		e.mu.Unlock()
		return nil, false
	}
	clone := make([]atl.PriceListItem, len(entry.items))
	copy(clone, entry.items)
	return clone, true
}
func summarizeDepositAmounts(gross, fee, net int64) string {
	if gross <= 0 && net <= 0 {
		return ""
	}
	if net <= 0 && gross > 0 {
		est := gross - fee
		if est <= 0 {
			est = gross
		}
		net = est
	}
	parts := make([]string, 0, 3)
	if gross > 0 {
		parts = append(parts, fmt.Sprintf("Tagihan: %s", formatCurrency(float64(gross))))
	}
	if fee > 0 {
		parts = append(parts, fmt.Sprintf("Biaya: %s", formatCurrency(float64(fee))))
	}
	if net > 0 {
		parts = append(parts, fmt.Sprintf("Saldo masuk: %s", formatCurrency(float64(net))))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " | ")
}

func parseAmountString(value string) int64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	if amt, err := parseAmount(trimmed); err == nil {
		return amt
	}
	clean := strings.ReplaceAll(trimmed, ".", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, " ", "")
	if clean == "" {
		return 0
	}
	if parsed, err := strconv.ParseInt(clean, 10, 64); err == nil {
		return parsed
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		if f < 0 {
			return 0
		}
		return int64(f + 0.5)
	}
	return 0
}

func deriveNetAmount(gross, fee, net int64) int64 {
	if net > 0 {
		return net
	}
	if gross <= 0 {
		return 0
	}
	est := gross - fee
	if est <= 0 {
		est = gross
	}
	return est
}
