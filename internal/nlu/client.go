package nlu

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"bot-jual/internal/metrics"
	"bot-jual/internal/repo"

	"log/slog"
)

const (
	geminiAPIBase = "https://generativelanguage.googleapis.com/v1beta"
	providerName  = "gemini"
)

// Client communicates with Gemini API to perform intent extraction and response generation.
type Client struct {
	repo        *repo.Repository
	logger      *slog.Logger
	metrics     *metrics.Metrics
	httpClient  *http.Client
	model       string
	timeout     time.Duration
	cooldown    time.Duration
	keyCacheTTL time.Duration

	mu       sync.Mutex
	cachedAt time.Time
	cached   []repo.APIKey
}

type callResult struct {
	text string
	key  string
	err  error
}

// Config holds NLU client configuration.
type Config struct {
	Model    string
	Timeout  time.Duration
	Cooldown time.Duration
}

// New creates a Gemini client.
func New(repository *repo.Repository, logger *slog.Logger, metrics *metrics.Metrics, cfg Config) *Client {
	return &Client{
		repo:        repository,
		logger:      logger.With("component", "nlu"),
		metrics:     metrics,
		httpClient:  &http.Client{Timeout: cfg.Timeout},
		model:       cfg.Model,
		timeout:     cfg.Timeout,
		cooldown:    cfg.Cooldown,
		keyCacheTTL: 1 * time.Minute,
	}
}

// IntentInput represents the information sent to Gemini to infer user intent.
type IntentInput struct {
	UserMessage       string
	ConversationState string
	LastBotMessage    string
	ContextSummary    string
	Channel           string
	UserLocale        string
}

// IntentResult contains the structured response from Gemini.
type IntentResult struct {
	Intent          string            `json:"intent"`
	Confidence      float64           `json:"confidence"`
	Reply           string            `json:"reply"`
	Entities        map[string]string `json:"entities"`
	RequiresConfirm bool              `json:"requires_confirmation"`
	ToolCall        *ToolCall         `json:"tool_call,omitempty"`
}

// ToolCall indicates that Gemini wants to invoke a backend action.
type ToolCall struct {
	Name      string            `json:"name"`
	Arguments map[string]string `json:"arguments"`
}

// ImageAnalysis contains structured data extracted from an image.
type ImageAnalysis struct {
	Summary       string            `json:"summary"`
	ExtractedText string            `json:"extracted_text"`
	Entities      map[string]string `json:"entities"`
}

// DetectIntent analyses a WhatsApp message with Gemini and returns structured intent data.
func (c *Client) DetectIntent(ctx context.Context, input IntentInput) (*IntentResult, error) {
	payload := buildIntentPrompt(input)

	res, keyUsed, err := c.callGemini(ctx, payload)
	if err != nil {
		return nil, err
	}
	c.metrics.GeminiRequests.WithLabelValues("success").Inc()

	normalised := normaliseJSON(res)

	var result IntentResult
	if err := json.Unmarshal([]byte(normalised), &result); err != nil {
		// Try to salvage partially truncated JSON first from the normalised text, then raw response.
		if partial, perr := fallbackParseIntent(normalised); perr == nil && partial != nil {
			if partial.Entities == nil {
				partial.Entities = map[string]string{}
			}
			if partial.ToolCall != nil && partial.ToolCall.Arguments == nil {
				partial.ToolCall.Arguments = map[string]string{}
			}
			c.logger.Debug("intent detected via fallback(normalised)", "intent", partial.Intent, "confidence", partial.Confidence, "key", keyUsed)
			return partial, nil
		}
		if partial, perr := fallbackParseIntent(res); perr == nil && partial != nil {
			if partial.Entities == nil {
				partial.Entities = map[string]string{}
			}
			if partial.ToolCall != nil && partial.ToolCall.Arguments == nil {
				partial.ToolCall.Arguments = map[string]string{}
			}
			c.logger.Debug("intent detected via fallback(raw)", "intent", partial.Intent, "confidence", partial.Confidence, "key", keyUsed)
			return partial, nil
		}
		// If fallback fails, return original parse error with snippet for debugging.
		c.metrics.Errors.WithLabelValues("nlu").Inc()
		snippet := res
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, fmt.Errorf("parse intent json: %w (snippet=%q)", err, snippet)
	}

	if result.Entities == nil {
		result.Entities = map[string]string{}
	}
	if result.ToolCall != nil && result.ToolCall.Arguments == nil {
		result.ToolCall.Arguments = map[string]string{}
	}

	c.logger.Debug("intent detected", "intent", result.Intent, "confidence", result.Confidence, "key", keyUsed)
	return &result, nil
}

// TranscribeAudio converts audio bytes into text using Gemini.
func (c *Client) TranscribeAudio(ctx context.Context, audio []byte, mimeType string) (string, error) {
	if len(audio) == 0 {
		return "", fmt.Errorf("audio payload empty")
	}
	if mimeType == "" {
		mimeType = "audio/ogg"
	}

	payload := geminiRequest{
		Contents: []geminiContent{
			{
				Role: "user",
				Parts: []geminiPart{
					{Text: "Transkripsikan voice note berikut ke teks bahasa Indonesia yang mudah dibaca. Balas hanya isi transkrip tanpa tambahan lain."},
					{InlineData: &inlineData{
						MimeType: mimeType,
						Data:     base64.StdEncoding.EncodeToString(audio),
					}},
				},
			},
		},
		GenerationConfig: generationConfig{
			Temperature:     0.2,
			MaxOutputTokens: 256,
		},
	}

	text, _, err := c.callGemini(ctx, payload)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(text), nil
}

// AnalyzeImage extracts structured info from an image.
func (c *Client) AnalyzeImage(ctx context.Context, image []byte, mimeType string) (*ImageAnalysis, error) {
	if len(image) == 0 {
		return nil, fmt.Errorf("image payload empty")
	}
	if mimeType == "" {
		mimeType = "image/jpeg"
	}

	payload := geminiRequest{
		Contents: []geminiContent{
			{
				Role: "user",
				Parts: []geminiPart{
					{Text: "Analisa gambar berikut. Jelaskan ringkas apa isinya, ambil informasi penting seperti ID pelanggan, nomor akun, jumlah tagihan, atau paket produk. Balas dalam JSON dengan format {\"summary\":\"...\",\"extracted_text\":\"...\",\"entities\":{\"key\":\"value\"}} tanpa teks tambahan."},
					{InlineData: &inlineData{
						MimeType: mimeType,
						Data:     base64.StdEncoding.EncodeToString(image),
					}},
				},
			},
		},
		GenerationConfig: generationConfig{
			Temperature:     0.3,
			MaxOutputTokens: 512,
		},
	}

	res, _, err := c.callGemini(ctx, payload)
	if err != nil {
		return nil, err
	}

	var analysis ImageAnalysis
	if err := json.Unmarshal([]byte(res), &analysis); err != nil {
		return nil, fmt.Errorf("parse image analysis: %w", err)
	}
	if analysis.Entities == nil {
		analysis.Entities = map[string]string{}
	}
	return &analysis, nil
}

func buildIntentPrompt(input IntentInput) geminiRequest {
	var sb strings.Builder
	sb.WriteString("Anda adalah AI asisten customer service PPOB untuk WhatsApp. ")
	sb.WriteString("Tugas Anda adalah mengklasifikasikan niat user dan menyiapkan respon singkat dengan gaya santai. ")
	sb.WriteString("Balasan wajib berupa JSON valid satu objek tanpa teks tambahan.\n")
	sb.WriteString("Field \"reply\" bila diisi harus terdengar ramah (contoh: \"Sip, aku bantu cek dulu ya.\").\n\n")
	sb.WriteString("Format JSON:\n")
	sb.WriteString(`{"intent":"string","confidence":0.0,"reply":"string","requires_confirmation":false,"entities":{"key":"value"},"tool_call":{"name":"tool_name","arguments":{"arg":"value"}}}` + "\n\n")
	sb.WriteString("Daftar intent utama: smalltalk_greeting, price_lookup, budget_filter, create_prepaid, check_bill, pay_bill, create_deposit, create_transfer, catalog_all, check_balance, help, fallback.\n")
	sb.WriteString("Jika tidak yakin gunakan intent \"fallback\".\n\n")
	sb.WriteString("Aturan entitas per intent:\n")
	sb.WriteString("- price_lookup: entities.product_query wajib, isi nama/keyword produk; entities.product_type boleh \"prabayar\" atau \"pascabayar\" (default \"prabayar\"), entities.provider opsional.\n")
	sb.WriteString("- budget_filter: entities.budget wajib (nominal), entities.product_type opsional.\n")
	sb.WriteString("- create_prepaid: entities.product_code, entities.customer_id (format akhir target; gabungkan ID dan server bila ada, contoh \"12345678(1234)\"), entities.payment_method (deposit/saldo/qris), opsional entities.customer_zone, entities.ref_id, dan entities.limit_price.\n")
	sb.WriteString("- check_bill/pay_bill: gunakan entities.product_code dan entities.customer_id (check) atau entities.ref_id (pay).\n")
	sb.WriteString("- create_deposit: entities.method/metode dan entities.amount/nominal wajib, entities.type opsional.\n")
	sb.WriteString("- create_transfer: entities.bank_code, entities.account_no, entities.account_name, entities.amount.\n")
	sb.WriteString("- catalog_all: tidak butuh entitas; gunakan saat user minta semua produk/menu.\n")
	sb.WriteString("- check_balance: tidak butuh entitas; gunakan saat user menanyakan saldo/akun atlantic.\n\n")
	sb.WriteString("Saat seluruh slot untuk sebuah aksi sudah lengkap, isi field \"tool_call\" untuk memicu backend. ")
	sb.WriteString("Nama tool harus diambil dari daftar berikut dan argument wajib dalam lowercase key:\n")
	sb.WriteString("- price_list(type, code?)\n")
	sb.WriteString("- transaksi_create(code, target, metode?, limit_price?, reff_id?, server?)\n")
	sb.WriteString("- tagihan_cek(code, customer_no, reff_id?)\n")
	sb.WriteString("- tagihan_bayar(code, customer_no, reff_id)\n")
	sb.WriteString("- deposit_create(metode, nominal, type?, reff_id?)\n")
	sb.WriteString("- transfer_create(kode_bank, nomor_akun, nama_pemilik, nominal, note?, email?, phone?)\n")
	sb.WriteString("Jika ada tool_call, tetap isi intent & entities agar router punya cadangan. ")
	sb.WriteString("Kosongkan tool_call atau set null bila belum lengkap.\n")
	sb.WriteString("Isi argument target dengan format akhir yang siap dikirim ke Atlantic (misal \"12345678(1234)\").\n\n")
	sb.WriteString("Tips parsing ID game:\n")
	sb.WriteString("- Tangkap kata kunci ID seperti \"id\", \"uid\", \"akun\", \"player id\", \"target\".\n")
	sb.WriteString("- Tangkap kata kunci server/zone seperti \"server\", \"sv\", \"srv\", \"zone\"; simpan ke entities.customer_zone bila disebut.\n")
	sb.WriteString("- Jika user menyebut dua nomor berurutan (contoh: \"69827740 2126\" atau \"69827740-2126\"), gabungkan sebagai customer_id \"69827740(2126)\".\n")
	sb.WriteString("- Perhatikan pattern \"Beli X Y (Z)\" dimana X=product_code, Y=customer_id, Z=zone. Contoh: \"Beli 3dm 69827740 (2126)\" harus menghasilkan product_code=\"3DM\", customer_id=\"69827740(2126)\".\n")
	sb.WriteString("- Product code bisa dalam format lowercase (3dm) atau uppercase (3DM), selalu konversi ke uppercase.\n")
	sb.WriteString("- Jika user menyebut \"via saldo ya mas\" atau \"pakai saldo\", anggap sebagai payment_method=\"deposit\".\n\n")
	sb.WriteString("Contoh:\n")
	sb.WriteString("User: \"pulsa telkomsel 20k\"\n")
	sb.WriteString(`Output: {"intent":"price_lookup","confidence":0.9,"reply":"","requires_confirmation":false,"entities":{"product_query":"pulsa telkomsel 20k","product_type":"prabayar"}}` + "\n")
	sb.WriteString("User: \"aku cuma punya 5000 buat topup\"\n")
	sb.WriteString(`Output: {"intent":"budget_filter","confidence":0.85,"reply":"","requires_confirmation":false,"entities":{"budget":"5000","product_type":"prabayar"}}` + "\n")
	sb.WriteString("User: \"beli ML3 69827740 deposit\"\n")
	sb.WriteString(`Output: {"intent":"create_prepaid","confidence":0.92,"reply":"Sip, aku siapin transaksinya ya.","requires_confirmation":false,"entities":{"product_code":"ML3","customer_id":"69827740","payment_method":"deposit"},"tool_call":{"name":"transaksi_create","arguments":{"code":"ML3","target":"69827740","metode":"deposit"}}}` + "\n")
	sb.WriteString("User: \"bantu deposit 150k via qris dong\"\n")
	sb.WriteString(`Output: {"intent":"create_deposit","confidence":0.9,"reply":"Oke, aku buatin deposit QRIS-nya.","requires_confirmation":false,"entities":{"method":"qris","amount":"150000"},"tool_call":{"name":"deposit_create","arguments":{"metode":"qris","nominal":"150000"}}}` + "\n")
	sb.WriteString("User: \"tolong isi ML3 ke id 69827740 server 2126 pakai saldo\"\n")
	sb.WriteString(`Output: {"intent":"create_prepaid","confidence":0.94,"reply":"Siap, aku proses dengan saldo ya.","requires_confirmation":false,"entities":{"product_code":"ML3","customer_id":"69827740(2126)","customer_zone":"2126","payment_method":"deposit"},"tool_call":{"name":"transaksi_create","arguments":{"code":"ML3","target":"69827740(2126)","metode":"deposit","server":"2126"}}}` + "\n")
	sb.WriteString("User: \"Beli 3dm 69827740 (2126)\"\n")
	sb.WriteString(`Output: {"intent":"create_prepaid","confidence":0.95,"reply":"Sip, aku proses transaksinya ya.","requires_confirmation":false,"entities":{"product_code":"3DM","customer_id":"69827740(2126)","customer_zone":"2126","payment_method":"deposit"},"tool_call":{"name":"transaksi_create","arguments":{"code":"3DM","target":"69827740(2126)","metode":"deposit","server":"2126"}}}` + "\n")
	sb.WriteString("User: \"Beli 3dm 69827740 (2126) via saldo ya mas\"\n")
	sb.WriteString(`Output: {"intent":"create_prepaid","confidence":0.95,"reply":"Sip, aku proses transaksinya ya.","requires_confirmation":false,"entities":{"product_code":"3DM","customer_id":"69827740(2126)","customer_zone":"2126","payment_method":"deposit"},"tool_call":{"name":"transaksi_create","arguments":{"code":"3DM","target":"69827740(2126)","metode":"deposit","server":"2126"}}}` + "\n")
	sb.WriteString("User: \"halo\"\n")
	sb.WriteString(`Output: {"intent":"smalltalk_greeting","confidence":0.95,"reply":"Halo! Aku menyediakan berbagai layanan digital:\n\n📱 Pulsa & Paket Data - Telkomsel, Indosat, XL, Tri, Smartfren\n🎮 Top Up Game - Mobile Legends, Free Fire, PUBG, dll\n⚡ Token Listrik - Prabayar & Pascabayar\n💳 Bayar Tagihan - PLN, PDAM, BPJS, dll\n💰 Deposit & Transfer - QRIS, Bank Transfer, E-wallet\n\nKetik nama produk yang kamu cari, contoh: \"pulsa telkomsel 20k\" atau \"top up ML\"","requires_confirmation":false,"entities":{}}` + "\n\n")
	sb.WriteString("User: \"hai\"\n")
	sb.WriteString(`Output: {"intent":"smalltalk_greeting","confidence":0.95,"reply":"Halo! Aku menyediakan berbagai layanan digital:\n\n📱 Pulsa & Paket Data - Telkomsel, Indosat, XL, Tri, Smartfren\n🎮 Top Up Game - Mobile Legends, Free Fire, PUBG, dll\n⚡ Token Listrik - Prabayar & Pascabayar\n💳 Bayar Tagihan - PLN, PDAM, BPJS, dll\n💰 Deposit & Transfer - QRIS, Bank Transfer, E-wallet\n\nKetik nama produk yang kamu cari, contoh: \"pulsa telkomsel 20k\" atau \"top up ML\"","requires_confirmation":false,"entities":{}}` + "\n\n")
	sb.WriteString("User: \"pagi\"\n")
	sb.WriteString(`Output: {"intent":"smalltalk_greeting","confidence":0.95,"reply":"Selamat pagi! Aku menyediakan berbagai layanan digital:\n\n📱 Pulsa & Paket Data - Telkomsel, Indosat, XL, Tri, Smartfren\n🎮 Top Up Game - Mobile Legends, Free Fire, PUBG, dll\n⚡ Token Listrik - Prabayar & Pascabayar\n💳 Bayar Tagihan - PLN, PDAM, BPJS, dll\n💰 Deposit & Transfer - QRIS, Bank Transfer, E-wallet\n\nKetik nama produk yang kamu cari, contoh: \"pulsa telkomsel 20k\" atau \"top up ML\"","requires_confirmation":false,"entities":{}}` + "\n\n")
	sb.WriteString("User: \"assalamualaikum\"\n")
	sb.WriteString(`Output: {"intent":"smalltalk_greeting","confidence":0.95,"reply":"Waalaikumsalam! Aku menyediakan berbagai layanan digital:\n\n📱 Pulsa & Paket Data - Telkomsel, Indosat, XL, Tri, Smartfren\n🎮 Top Up Game - Mobile Legends, Free Fire, PUBG, dll\n⚡ Token Listrik - Prabayar & Pascabayar\n💳 Bayar Tagihan - PLN, PDAM, BPJS, dll\n💰 Deposit & Transfer - QRIS, Bank Transfer, E-wallet\n\nKetik nama produk yang kamu cari, contoh: \"pulsa telkomsel 20k\" atau \"top up ML\"","requires_confirmation":false,"entities":{}}` + "\n\n")
	sb.WriteString("Konteks percakapan:\n")

	if input.ContextSummary != "" {
		sb.WriteString("- Ringkasan: " + input.ContextSummary + "\n")
	}
	if input.LastBotMessage != "" {
		sb.WriteString("- Pesan bot terakhir: " + input.LastBotMessage + "\n")
	}
	sb.WriteString("- Kanal: " + nonEmpty(input.Channel, "whatsapp") + "\n")
	if input.UserLocale != "" {
		sb.WriteString("- Bahasa user: " + input.UserLocale + "\n")
	}
	sb.WriteString("\nPesan user:\n")
	sb.WriteString(input.UserMessage)

	return geminiRequest{
		Contents: []geminiContent{
			{
				Role: "user",
				Parts: []geminiPart{
					{Text: sb.String()},
				},
			},
		},
		GenerationConfig: generationConfig{
			Temperature:     0.3,
			MaxOutputTokens: 512,
			TopP:            0.8,
		},
	}
}

func (c *Client) callGemini(ctx context.Context, payload geminiRequest) (string, string, error) {
	var lastErr error

	keys, err := c.fetchKeys(ctx)
	if err != nil {
		return "", "", err
	}

	for _, k := range keys {
		if k.CooldownUntil != nil && time.Now().Before(*k.CooldownUntil) {
			continue
		}

		res := c.invokeWithKey(ctx, k, payload)
		if res.err == nil {
			return res.text, res.key, nil
		}
		lastErr = res.err

		if errors.Is(res.err, errQuotaExceeded) || errors.Is(res.err, errUnauthorised) {
			if err := c.repo.SetCooldownUntil(ctx, k.ID, time.Now().Add(c.cooldown)); err != nil {
				c.logger.Error("set cooldown failed", "error", err, "key", k.ID)
			}
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no available gemini keys")
	}
	c.metrics.GeminiRequests.WithLabelValues("failed").Inc()
	return "", "", lastErr
}

func (c *Client) invokeWithKey(ctx context.Context, key repo.APIKey, payload geminiRequest) callResult {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return callResult{err: fmt.Errorf("marshal payload: %w", err)}
	}
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	url := fmt.Sprintf("%s/models/%s:generateContent?key=%s", geminiAPIBase, c.model, key.Value)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return callResult{err: fmt.Errorf("new request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.metrics.GeminiRequests.WithLabelValues("error").Inc()
		return callResult{err: fmt.Errorf("gemini http: %w", err)}
	}
	defer resp.Body.Close()

	latency := time.Since(start).Seconds()
	statusLabel := fmt.Sprintf("%d", resp.StatusCode)
	c.metrics.GeminiLatency.WithLabelValues(statusLabel).Observe(latency)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return callResult{err: fmt.Errorf("read body: %w", err)}
	}

	if resp.StatusCode == http.StatusOK {
		text, err := extractCandidateText(body)
		if err != nil {
			return callResult{err: err}
		}
		return callResult{text: text, key: key.ID}
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		return callResult{err: errQuotaExceeded}
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return callResult{err: errUnauthorised}
	}

	return callResult{err: fmt.Errorf("gemini request failed: status=%d body=%s", resp.StatusCode, string(body))}
}

func (c *Client) fetchKeys(ctx context.Context) ([]repo.APIKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cached) > 0 && time.Since(c.cachedAt) < c.keyCacheTTL {
		return c.cached, nil
	}

	keys, err := c.repo.ListActiveGeminiKeys(ctx)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("gemini keys not found")
	}

	c.cached = keys
	c.cachedAt = time.Now()
	return keys, nil
}

func extractCandidateText(body []byte) (string, error) {
	var resp geminiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("decode gemini response: %w", err)
	}
	for _, cand := range resp.Candidates {
		for _, part := range cand.Content.Parts {
			if part.Text != "" {
				return part.Text, nil
			}
		}
	}
	return "", fmt.Errorf("no candidate text found")
}

func nonEmpty(val, fallback string) string {
	if strings.TrimSpace(val) == "" {
		return fallback
	}
	return val
}

var (
	errQuotaExceeded = errors.New("gemini quota exceeded")
	errUnauthorised  = errors.New("gemini unauthorised")
)

type geminiRequest struct {
	Contents         []geminiContent  `json:"contents"`
	GenerationConfig generationConfig `json:"generationConfig,omitempty"`
}

type generationConfig struct {
	Temperature     float64 `json:"temperature,omitempty"`
	TopP            float64 `json:"topP,omitempty"`
	TopK            float64 `json:"topK,omitempty"`
	MaxOutputTokens int32   `json:"maxOutputTokens,omitempty"`
}

type geminiContent struct {
	Role  string       `json:"role"`
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text       string      `json:"text,omitempty"`
	InlineData *inlineData `json:"inlineData,omitempty"`
}

type inlineData struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Role  string       `json:"role"`
			Parts []geminiPart `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
}

func normaliseJSON(text string) string {
	s := strings.TrimSpace(text)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```")
		s = strings.TrimSpace(s)
		if strings.HasPrefix(strings.ToLower(s), "json") {
			if idx := strings.IndexByte(s, '\n'); idx >= 0 {
				s = s[idx+1:]
			} else {
				s = ""
			}
		}
		if idx := strings.LastIndex(s, "```"); idx >= 0 {
			s = s[:idx]
		}
		s = strings.TrimSpace(s)
	}
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		if start := strings.Index(s, "{"); start >= 0 {
			if end := strings.LastIndex(s, "}"); end >= start {
				s = s[start : end+1]
			}
		}
	}
	// Handle incomplete JSON like {"intent":"create_prepaid","confidence":0.8
	if strings.HasSuffix(s, "\"") && !strings.HasSuffix(s, "}") {
		// Try to close the JSON if it's incomplete
		s = s + "\"}"
	}
	// Attempt to balance braces if still unbalanced
	openBraces := strings.Count(s, "{")
	closeBraces := strings.Count(s, "}")
	if openBraces > closeBraces {
		s = s + strings.Repeat("}", openBraces-closeBraces)
	}
	return strings.TrimSpace(s)
}

// fallbackParseIntent attempts to extract key fields using regex from a malformed or truncated JSON-like string.
func fallbackParseIntent(raw string) (*IntentResult, error) {
	r := &IntentResult{
		Entities: map[string]string{},
	}

	// Regex extractor (works when closing quotes exist)
	extract := func(pattern string) string {
		re := regexp.MustCompile(pattern)
		m := re.FindStringSubmatch(raw)
		if len(m) >= 2 {
			return strings.TrimSpace(m[1])
		}
		return ""
	}

	// Truncated-safe scanner: finds `"key":"value...` even when closing quote is missing
	scan := func(src, key string) string {
		keyMark := `"` + key + `"`
		idx := strings.Index(src, keyMark)
		if idx < 0 {
			return ""
		}
		colon := strings.Index(src[idx+len(keyMark):], ":")
		if colon < 0 {
			return ""
		}
		afterColon := src[idx+len(keyMark)+colon+1:]
		// Expect opening quote
		open := strings.Index(afterColon, `"`)
		if open < 0 {
			return ""
		}
		valStart := open + 1
		rest := afterColon[valStart:]
		// Find closing quote; if missing, take up to first control char or end
		close := strings.Index(rest, `"`)
		val := ""
		if close >= 0 {
			val = rest[:close]
		} else {
			// Trim at newline or comma or brace to avoid bleeding into next fields
			end := len(rest)
			for i, ch := range rest {
				if ch == '\n' || ch == '\r' || ch == ',' || ch == '}' {
					end = i
					break
				}
			}
			val = rest[:end]
		}
		return strings.TrimSpace(val)
	}

	// Core fields
	r.Intent = extract(`"intent"\s*:\s*"([^"]+)"`)
	if r.Intent == "" {
		r.Intent = scan(raw, "intent")
	}

	confStr := extract(`"confidence"\s*:\s*([0-9\.]+)`)
	if confStr == "" {
		confStr = scan(raw, "confidence")
	}
	if confStr != "" {
		if v, err := strconv.ParseFloat(confStr, 64); err == nil {
			r.Confidence = v
		}
	}

	r.Reply = extract(`"reply"\s*:\s*"([^"]*)"`)
	if r.Reply == "" {
		r.Reply = scan(raw, "reply")
	}

	// Entities commonly needed for sales flows
	if v := extract(`"product_code"\s*:\s*"([^"]+)"`); v != "" {
		r.Entities["product_code"] = strings.ToUpper(v)
	} else if v := scan(raw, "product_code"); v != "" {
		r.Entities["product_code"] = strings.ToUpper(v)
	}

	if v := extract(`"customer_id"\s*:\s*"([^"]+)"`); v != "" {
		r.Entities["customer_id"] = v
	} else if v := scan(raw, "customer_id"); v != "" {
		r.Entities["customer_id"] = v
	}

	if v := extract(`"customer_zone"\s*:\s*"([^"]+)"`); v != "" {
		r.Entities["customer_zone"] = v
	} else if v := scan(raw, "customer_zone"); v != "" {
		r.Entities["customer_zone"] = v
	}

	if v := extract(`"payment_method"\s*:\s*"([^"]+)"`); v != "" {
		r.Entities["payment_method"] = strings.ToLower(v)
	} else if v := scan(raw, "payment_method"); v != "" {
		r.Entities["payment_method"] = strings.ToLower(v)
	}

	if v := extract(`"provider"\s*:\s*"([^"]+)"`); v != "" {
		r.Entities["provider"] = strings.ToLower(v)
	} else if v := scan(raw, "provider"); v != "" {
		r.Entities["provider"] = strings.ToLower(v)
	}

	if v := extract(`"product_query"\s*:\s*"([^"]+)"`); v != "" {
		r.Entities["product_query"] = v
	} else if v := scan(raw, "product_query"); v != "" {
		r.Entities["product_query"] = v
	}

	if v := extract(`"ref_id"\s*:\s*"([^"]+)"`); v != "" {
		r.Entities["ref_id"] = v
	} else if v := scan(raw, "ref_id"); v != "" {
		r.Entities["ref_id"] = v
	}

	// Basic validation: require at least an intent to proceed
	if strings.TrimSpace(r.Intent) == "" {
		return nil, fmt.Errorf("fallback parse: intent not found")
	}

	return r, nil
}
