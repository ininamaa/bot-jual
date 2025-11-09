package atl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"bot-jual/internal/cache"
	"bot-jual/internal/metrics"

	"log/slog"
)

const (
	defaultPriceCacheTTL = 5 * time.Minute
	formContentType      = "application/x-www-form-urlencoded"
)

var (
	// ErrInvalidCredential indicates Atlantic rejected the provided credentials.
	ErrInvalidCredential = errors.New("atlantic invalid credential")
)

// Client provides typed access to Atlantic H2H API.
type Client struct {
	logger   *slog.Logger
	baseURL  string
	apiKey   string
	timeout  time.Duration
	http     *http.Client
	metrics  *metrics.Metrics
	cache    *cache.Redis
	priceTTL time.Duration
}

// Config holds Atlantic client configuration.
type Config struct {
	BaseURL string
	APIKey  string
	Timeout time.Duration
}

// responseEnvelope mirrors Atlantic's standard response shape.
type responseEnvelope struct {
	Status  bool
	Message string
	Code    int
	Data    json.RawMessage
}

func (r *responseEnvelope) UnmarshalJSON(data []byte) error {
	type alias struct {
		Status  json.RawMessage `json:"status"`
		Message json.RawMessage `json:"message"`
		Code    json.RawMessage `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	r.Message = strings.TrimSpace(stringTrimQuotes(a.Message))
	r.Data = a.Data
	if len(a.Status) != 0 {
		var boolVal bool
		if err := json.Unmarshal(a.Status, &boolVal); err == nil {
			r.Status = boolVal
		} else {
			str := strings.TrimSpace(stringTrimQuotes(a.Status))
			r.Status = strings.EqualFold(str, "true") || strings.EqualFold(str, "success") || str == "1"
		}
	}
	if len(a.Code) != 0 {
		var intVal int
		if err := json.Unmarshal(a.Code, &intVal); err == nil {
			r.Code = intVal
		} else {
			str := strings.TrimSpace(stringTrimQuotes(a.Code))
			if parsed, err := strconv.Atoi(str); err == nil {
				r.Code = parsed
			}
		}
	}
	return nil
}

// New creates a new Atlantic client.
func New(cfg Config, logger *slog.Logger, metrics *metrics.Metrics, redis *cache.Redis) *Client {
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		base = "https://atlantich2h.com"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &Client{
		logger:   logger.With("component", "atlantic"),
		baseURL:  base,
		apiKey:   cfg.APIKey,
		timeout:  timeout,
		http:     &http.Client{Timeout: timeout},
		metrics:  metrics,
		cache:    redis,
		priceTTL: defaultPriceCacheTTL,
	}
}

// PriceListItem represents a product price entry.
type PriceListItem struct {
	Code        string         `json:"code"`
	Name        string         `json:"name"`
	Category    string         `json:"category"`
	Provider    string         `json:"provider"`
	Nominal     string         `json:"nominal"`
	Price       float64        `json:"price"`
	Status      string         `json:"status"`
	Description string         `json:"description"`
	Raw         map[string]any `json:"-"`
}

// UnmarshalJSON supports flexible Atlantic payloads.
func (p *PriceListItem) UnmarshalJSON(data []byte) error {
	type alias PriceListItem
	tmp := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	p.Raw = make(map[string]any, len(tmp))

	for key, val := range tmp {
		var anyVal any
		if err := json.Unmarshal(val, &anyVal); err == nil {
			p.Raw[key] = anyVal
		} else {
			p.Raw[key] = string(val)
		}
	}

	p.Code = readStringRaw(tmp, "code", "kode", "product_code")
	p.Name = readStringRaw(tmp, "name", "layanan", "product_name", "description")
	p.Category = readStringRaw(tmp, "category", "kategori")
	p.Provider = readStringRaw(tmp, "provider", "operator")
	p.Nominal = readStringRaw(tmp, "nominal", "nilai")
	if p.Nominal == "" {
		p.Nominal = readStringRaw(tmp, "denom")
	}

	price := readFloatRaw(tmp, "price", "harga", "sell_price", "amount")
	p.Price = price

	status := readStringRaw(tmp, "status", "status_text")
	if status == "" {
		status = normalizeStatus(readFloatRaw(tmp, "status_id", "status_code"))
	}
	p.Status = normalizeAvailabilityStatus(status)

	p.Description = readStringRaw(tmp, "description", "keterangan", "note")
	return nil
}

// PriceList retrieves price list (cached if redis configured).
func (c *Client) PriceList(ctx context.Context, productType string, forceRefresh bool) ([]PriceListItem, error) {
	productType = normalizeProductType(productType)
	cacheKey := fmt.Sprintf("atlantic:pricelist:%s", productType)
	if c.cache != nil && !forceRefresh {
		var cached []PriceListItem
		ok, err := c.cache.GetJSON(ctx, cacheKey, &cached)
		if err != nil {
			c.logger.Warn("read price list cache failed", "error", err)
		} else if ok {
			return cached, nil
		}
	}

	form := url.Values{}
	if productType != "" {
		form.Set("type", productType)
	}
	env, err := c.postForm(ctx, "/layanan/price_list", form)
	if err != nil {
		return nil, err
	}

	items, err := parsePriceList(env.Data)
	if err != nil {
		return nil, fmt.Errorf("parse price list: %w", err)
	}

	if c.cache != nil {
		if err := c.cache.SetJSON(ctx, cacheKey, items, c.priceTTL); err != nil {
			c.logger.Warn("set price list cache failed", "error", err)
		}
	}
	return items, nil
}

func normalizeProductType(productType string) string {
	p := strings.TrimSpace(strings.ToLower(productType))
	if p == "" {
		return "prabayar"
	}
	switch p {
	case "prepaid":
		return "prabayar"
	case "postpaid":
		return "pascabayar"
	default:
		return p
	}
}

// ProfileResponse contains account profile & balance data.
type ProfileResponse struct {
	Name     string         `json:"name"`
	Username string         `json:"username"`
	Email    string         `json:"email"`
	Phone    string         `json:"phone"`
	Balance  float64        `json:"balance"`
	Status   string         `json:"status"`
	Raw      map[string]any `json:"raw"`
}

// GetProfile retrieves Atlantic account profile/balance.
func (c *Client) GetProfile(ctx context.Context) (*ProfileResponse, error) {
	env, err := c.postForm(ctx, "/get_profile", url.Values{})
	if err != nil {
		return nil, err
	}
	data, err := decodeMap(env.Data)
	if err != nil {
		return nil, err
	}
	resp := &ProfileResponse{
		Name:     firstString(data, "name"),
		Username: firstString(data, "username"),
		Email:    firstString(data, "email"),
		Phone:    firstString(data, "phone"),
		Status:   firstString(data, "status"),
		Balance:  toFloat(data["balance"]),
		Raw:      data,
	}
	if resp.Balance == 0 {
		if balStr := firstString(data, "balance"); balStr != "" {
			if parsed, err := strconv.ParseFloat(strings.ReplaceAll(balStr, ",", ""), 64); err == nil {
				resp.Balance = parsed
			}
		}
	}
	return resp, nil
}

// CreatePrepaidRequest holds parameters to create top-up transaction.
type CreatePrepaidRequest struct {
	ProductCode string `json:"product_code"`
	CustomerID  string `json:"customer_id"`
	RefID       string `json:"ref_id"`
	Amount      int64  `json:"amount,omitempty"`
	PhoneNumber string `json:"phone_number,omitempty"`
	Note        string `json:"note,omitempty"`
}

// TransactionResponse captures Atlantic transaction response.
type TransactionResponse struct {
	RefID   string         `json:"ref_id"`
	Status  string         `json:"status"`
	Message string         `json:"message"`
	SN      string         `json:"sn,omitempty"`
	Raw     map[string]any `json:"raw"`
}

// CreatePrepaidTransaction triggers Atlantic transaction creation.
func (c *Client) CreatePrepaidTransaction(ctx context.Context, req CreatePrepaidRequest) (*TransactionResponse, error) {
	form := url.Values{}
	form.Set("code", req.ProductCode)
	form.Set("target", req.CustomerID)
	if req.RefID != "" {
		form.Set("reff_id", req.RefID)
	}
	if req.Amount > 0 {
		form.Set("amount", strconv.FormatInt(req.Amount, 10))
	}
	if req.PhoneNumber != "" {
		form.Set("phone", req.PhoneNumber)
	}
	if req.Note != "" {
		form.Set("note", req.Note)
	}

	env, err := c.postForm(ctx, "/transaksi/create", form)
	if err != nil {
		return nil, err
	}
	return parseTransactionResponse(env)
}

// TransactionStatusRequest holds parameters to check Atlantic transaction.
type TransactionStatusRequest struct {
	RefID string `json:"ref_id"`
}

// TransactionStatusResponse details transaction status.
type TransactionStatusResponse struct {
	RefID        string         `json:"ref_id"`
	Status       string         `json:"status"`
	Message      string         `json:"message"`
	ResponseCode string         `json:"response_code"`
	SN           string         `json:"sn,omitempty"`
	Raw          map[string]any `json:"raw"`
}

// TransactionStatus fetches status of a transaction.
func (c *Client) TransactionStatus(ctx context.Context, req TransactionStatusRequest) (*TransactionStatusResponse, error) {
	form := url.Values{}
	form.Set("reff_id", req.RefID)

	env, err := c.postForm(ctx, "/transaksi/status", form)
	if err != nil {
		return nil, err
	}
	data, err := decodeMap(env.Data)
	if err != nil {
		return nil, err
	}
	resp := &TransactionStatusResponse{
		RefID:        firstString(data, "reff_id", "ref_id", "reference"),
		Status:       normalizeTransactionStatus(firstString(data, "status", "state")),
		Message:      firstString(data, "message", "info", "description"),
		ResponseCode: firstString(data, "response_code", "code"),
		SN:           firstString(data, "sn", "serial_number"),
		Raw:          data,
	}
	if resp.Message == "" {
		resp.Message = strings.TrimSpace(env.Message)
	}
	return resp, nil
}

// BillInquiryRequest holds data to check a bill.
type BillInquiryRequest struct {
	ProductCode string `json:"product_code"`
	CustomerID  string `json:"customer_id"`
	RefID       string `json:"ref_id"`
}

// BillInquiryResponse holds bill details.
type BillInquiryResponse struct {
	RefID    string         `json:"ref_id"`
	Status   string         `json:"status"`
	Message  string         `json:"message"`
	Amount   float64        `json:"amount"`
	Fee      float64        `json:"fee"`
	BillInfo map[string]any `json:"bill_info"`
	Raw      map[string]any `json:"raw"`
}

// BillInquiry checks outstanding bill.
func (c *Client) BillInquiry(ctx context.Context, req BillInquiryRequest) (*BillInquiryResponse, error) {
	form := url.Values{}
	form.Set("code", req.ProductCode)
	form.Set("customer_no", req.CustomerID)
	if req.RefID != "" {
		form.Set("reff_id", req.RefID)
	}

	env, err := c.postForm(ctx, "/transaksi/tagihan", form)
	if err != nil {
		return nil, err
	}
	data, err := decodeMap(env.Data)
	if err != nil {
		return nil, err
	}

	resp := &BillInquiryResponse{
		RefID:    firstString(data, "reff_id", "ref_id", "reference"),
		Status:   normalizeTransactionStatus(firstString(data, "status", "state")),
		Message:  firstString(data, "message", "info", "description"),
		Amount:   firstFloat(data, "amount", "total", "tagihan"),
		Fee:      firstFloat(data, "fee", "admin"),
		BillInfo: extractNested(data, "bill_info", "detail", "data"),
		Raw:      data,
	}
	if resp.Message == "" {
		resp.Message = strings.TrimSpace(env.Message)
	}
	return resp, nil
}

// BillPaymentRequest triggers bill payment.
type BillPaymentRequest struct {
	RefID string `json:"ref_id"`
	PIN   string `json:"pin,omitempty"`
}

// BillPaymentResponse describes bill payment outcome.
type BillPaymentResponse struct {
	RefID   string         `json:"ref_id"`
	Status  string         `json:"status"`
	Message string         `json:"message"`
	Raw     map[string]any `json:"raw"`
}

// BillPayment pays a bill previously inquired.
func (c *Client) BillPayment(ctx context.Context, req BillPaymentRequest) (*BillPaymentResponse, error) {
	form := url.Values{}
	form.Set("reff_id", req.RefID)
	if req.PIN != "" {
		form.Set("pin", req.PIN)
	}
	env, err := c.postForm(ctx, "/transaksi/tagihan/bayar", form)
	if err != nil {
		return nil, err
	}
	data, err := decodeMap(env.Data)
	if err != nil {
		return nil, err
	}

	resp := &BillPaymentResponse{
		RefID:   firstString(data, "reff_id", "ref_id", "reference"),
		Status:  normalizeTransactionStatus(firstString(data, "status", "state")),
		Message: firstString(data, "message", "info", "description"),
		Raw:     data,
	}
	if resp.Message == "" {
		resp.Message = strings.TrimSpace(env.Message)
	}
	return resp, nil
}

// DepositRequest holds deposit parameters.
type DepositRequest struct {
	Method string  `json:"method"`
	Amount float64 `json:"amount"`
	RefID  string  `json:"ref_id"`
	Type   string  `json:"type,omitempty"`
}

// DepositResponse contains deposit status.
type DepositResponse struct {
	RefID     string         `json:"ref_id"`
	Status    string         `json:"status"`
	Message   string         `json:"message"`
	Checkout  map[string]any `json:"checkout"`
	QRString  string         `json:"qr_string"`
	QRImage   string         `json:"qr_image"`
	ExpiredAt string         `json:"expired_at"`
	Amount    float64        `json:"amount"`
	Fee       float64        `json:"fee"`
	NetAmount float64        `json:"net_amount"`
	Raw       map[string]any `json:"raw"`
}

// CreateDeposit starts a deposit.
func (c *Client) CreateDeposit(ctx context.Context, req DepositRequest) (*DepositResponse, error) {
	form := url.Values{}
	form.Set("reff_id", req.RefID)
	form.Set("nominal", strconv.FormatFloat(req.Amount, 'f', 0, 64))
	form.Set("metode", req.Method)
	if req.Type != "" {
		form.Set("type", req.Type)
	}
	env, err := c.postForm(ctx, "/deposit/create", form)
	if err != nil {
		return nil, err
	}
	data, err := decodeMap(env.Data)
	if err != nil {
		return nil, err
	}
	fee := firstFloat(data, "fee", "admin_fee", "admin")
	net := firstFloat(data, "get_balance", "net_amount", "saldo_masuk", "balance_masuk")
	resp := &DepositResponse{
		RefID:     firstString(data, "reff_id", "ref_id", "reference"),
		Status:    normalizeTransactionStatus(firstString(data, "status", "state")),
		Message:   firstString(data, "message", "info", "description"),
		QRString:  firstString(data, "qr_string", "qr"),
		QRImage:   firstString(data, "qr_image", "image"),
		ExpiredAt: firstString(data, "expired_at", "expire_at"),
		Amount:    firstFloat(data, "nominal", "amount"),
		Checkout:  extractNested(data, "checkout"),
		Raw:       data,
	}
	resp.Fee = fee
	if net == 0 && resp.Amount > 0 && fee > 0 {
		net = resp.Amount - fee
	}
	resp.NetAmount = net
	if resp.Message == "" {
		resp.Message = strings.TrimSpace(env.Message)
	}
	if resp.Checkout == nil {
		resp.Checkout = map[string]any{}
	}
	if resp.QRString != "" {
		resp.Checkout["qr_string"] = resp.QRString
	}
	if resp.QRImage != "" {
		resp.Checkout["qr_image"] = resp.QRImage
	}
	if resp.ExpiredAt != "" {
		resp.Checkout["expired_at"] = resp.ExpiredAt
	}
	if resp.Amount > 0 {
		resp.Checkout["nominal"] = resp.Amount
	}
	if resp.Fee > 0 {
		resp.Checkout["fee"] = resp.Fee
	}
	if resp.NetAmount > 0 {
		resp.Checkout["net_amount"] = resp.NetAmount
	}
	return resp, nil
}

// TransferRequest holds transfer parameters.
type TransferRequest struct {
	BankCode    string  `json:"bank_code"`
	AccountName string  `json:"account_name"`
	AccountNo   string  `json:"account_no"`
	Amount      float64 `json:"amount"`
	RefID       string  `json:"ref_id"`
	Description string  `json:"description,omitempty"`
	Email       string  `json:"email,omitempty"`
	Phone       string  `json:"phone,omitempty"`
}

// TransferResponse contains transfer status.
type TransferResponse struct {
	RefID   string         `json:"ref_id"`
	Status  string         `json:"status"`
	Message string         `json:"message"`
	Raw     map[string]any `json:"raw"`
}

// CreateTransfer initiates fund transfer.
func (c *Client) CreateTransfer(ctx context.Context, req TransferRequest) (*TransferResponse, error) {
	form := url.Values{}
	form.Set("reff_id", req.RefID)
	form.Set("kode_bank", req.BankCode)
	form.Set("nomor_akun", req.AccountNo)
	form.Set("nama_penerima", req.AccountName)
	form.Set("nominal", strconv.FormatFloat(req.Amount, 'f', -1, 64))
	if req.Description != "" {
		form.Set("catatan", req.Description)
	}
	if req.Email != "" {
		form.Set("email", req.Email)
	}
	if req.Phone != "" {
		form.Set("phone", req.Phone)
	}

	env, err := c.postForm(ctx, "/transfer/create", form)
	if err != nil {
		return nil, err
	}
	data, err := decodeMap(env.Data)
	if err != nil {
		return nil, err
	}
	resp := &TransferResponse{
		RefID:   firstString(data, "reff_id", "ref_id", "reference"),
		Status:  normalizeTransactionStatus(firstString(data, "status", "state")),
		Message: firstString(data, "message", "info", "description"),
		Raw:     data,
	}
	if resp.Message == "" {
		resp.Message = strings.TrimSpace(env.Message)
	}
	return resp, nil
}

func (c *Client) postForm(ctx context.Context, endpoint string, values url.Values) (*responseEnvelope, error) {
	if c.apiKey != "" && values.Get("api_key") == "" {
		values.Set("api_key", c.apiKey)
	}
	body := strings.NewReader(values.Encode())
	return c.call(ctx, http.MethodPost, endpoint, body, formContentType)
}

func (c *Client) call(ctx context.Context, method, endpoint string, body io.Reader, contentType string) (*responseEnvelope, error) {
	var env responseEnvelope
	if err := c.do(ctx, method, endpoint, body, contentType, &env); err != nil {
		return nil, err
	}
	if !env.Status {
		message := strings.TrimSpace(env.Message)
		if message == "" {
			message = "atlantic operation failed"
		}
		if env.Code != 0 {
			return nil, fmt.Errorf("atlantic %s error: %s (code=%d)", endpoint, message, env.Code)
		}
		return nil, fmt.Errorf("atlantic %s error: %s", endpoint, message)
	}
	return &env, nil
}

func (c *Client) do(ctx context.Context, method, endpoint string, body io.Reader, contentType string, dest any) error {
	reqURL := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}

	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")

	// Additional hardened headers to make QRIS generation safer on VPS deployments.
	// - Set a stable User-Agent instead of default Go http client UA
	// - Mark X-Requested-With for server-side request identification
	// - Provide Origin matching baseURL so upstream can apply origin policies
	// - Keep-alive connection for better performance
	req.Header.Set("User-Agent", "bot-jual/atlantic-client")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Connection", "keep-alive")
	if c.baseURL != "" {
		req.Header.Set("Origin", c.baseURL)
	}

	// For deposit (QRIS) endpoints, include an explicit client-intent header
	if strings.Contains(endpoint, "/deposit/") {
		req.Header.Set("X-Client-Action", "create_deposit_qris")
	}

	start := time.Now()
	res, err := c.http.Do(req)
	if err != nil {
		if c.metrics != nil {
			c.metrics.AtlanticRequests.WithLabelValues(endpoint, "error").Inc()
		}
		return fmt.Errorf("atlantic request: %w", err)
	}
	defer res.Body.Close()

	duration := time.Since(start).Seconds()
	statusLabel := fmt.Sprintf("%d", res.StatusCode)
	if c.metrics != nil {
		c.metrics.AtlanticRequests.WithLabelValues(endpoint, statusLabel).Inc()
		c.metrics.AtlanticLatency.WithLabelValues(endpoint, statusLabel).Observe(duration)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if res.StatusCode >= 400 {
		return classifyHTTPError(res.StatusCode, string(bodyBytes))
	}

	if dest == nil {
		return nil
	}

	decoder := json.NewDecoder(strings.NewReader(string(bodyBytes)))
	if err := decoder.Decode(dest); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func classifyHTTPError(status int, body string) error {
	snippet := strings.TrimSpace(body)
	lower := strings.ToLower(snippet)
	if status == http.StatusUnauthorized ||
		strings.Contains(lower, "invalid credential") ||
		strings.Contains(lower, "credential invalid") ||
		strings.Contains(lower, "invalid api key") ||
		strings.Contains(lower, "api key invalid") ||
		strings.Contains(lower, "kredensial tidak") {
		return fmt.Errorf("%w: %s", ErrInvalidCredential, snippet)
	}
	// Check for specific error messages related to insufficient balance or invalid deposit method
	if strings.Contains(lower, "metode deposit tidak valid") ||
		strings.Contains(lower, "metode deposit non aktif") ||
		strings.Contains(lower, "deposit tidak valid") ||
		strings.Contains(lower, "deposit method tidak valid") ||
		strings.Contains(lower, "invalid deposit method") ||
		strings.Contains(lower, "saldo tidak cukup") ||
		strings.Contains(lower, "insufficient balance") ||
		strings.Contains(lower, "insufficient funds") {
		return fmt.Errorf("insufficient balance: %s", snippet)
	}
	return fmt.Errorf("atlantic error: status=%d body=%s", status, snippet)
}

// parsePriceList normalizes price list payloads that may be grouped.
func parsePriceList(data json.RawMessage) ([]PriceListItem, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}

	var direct []PriceListItem
	if err := json.Unmarshal(data, &direct); err == nil {
		return direct, nil
	}

	grouped := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &grouped); err != nil {
		return nil, err
	}

	items := make([]PriceListItem, 0, len(grouped))
	for _, raw := range grouped {
		var subset []PriceListItem
		if err := json.Unmarshal(raw, &subset); err != nil {
			return nil, err
		}
		items = append(items, subset...)
	}
	return items, nil
}

func parseTransactionResponse(env *responseEnvelope) (*TransactionResponse, error) {
	data, err := decodeMap(env.Data)
	if err != nil {
		return nil, err
	}
	resp := &TransactionResponse{
		RefID:   firstString(data, "reff_id", "ref_id", "reference"),
		Status:  normalizeTransactionStatus(firstString(data, "status", "state")),
		Message: firstString(data, "message", "info", "description"),
		SN:      firstString(data, "sn", "serial_number"),
		Raw:     data,
	}
	if resp.Message == "" {
		resp.Message = strings.TrimSpace(env.Message)
	}
	return resp, nil
}

func decodeMap(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err == nil {
		return out, nil
	}
	var withNumbers map[string]json.RawMessage
	if err := json.Unmarshal(raw, &withNumbers); err != nil {
		return nil, err
	}
	out = make(map[string]any, len(withNumbers))
	for key, val := range withNumbers {
		var anyVal any
		if err := json.Unmarshal(val, &anyVal); err == nil {
			out[key] = anyVal
		} else {
			out[key] = string(val)
		}
	}
	return out, nil
}

func extractNested(data map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		if val, ok := data[key]; ok {
			if nested, ok := val.(map[string]any); ok {
				return nested
			}
		}
	}
	return nil
}

func firstString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if val, ok := data[key]; ok {
			if str := toString(val); str != "" {
				return str
			}
		}
	}
	return ""
}

func firstFloat(data map[string]any, keys ...string) float64 {
	for _, key := range keys {
		if val, ok := data[key]; ok {
			if f := toFloat(val); f != 0 {
				return f
			}
		}
	}
	return 0
}

func readStringRaw(raw map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if val, ok := raw[key]; ok {
			if str := strings.TrimSpace(stringTrimQuotes(val)); str != "" {
				return str
			}
			var decoded string
			if err := json.Unmarshal(val, &decoded); err == nil {
				decoded = strings.TrimSpace(decoded)
				if decoded != "" {
					return decoded
				}
			}
			var number float64
			if err := json.Unmarshal(val, &number); err == nil && number != 0 {
				return strconv.FormatFloat(number, 'f', -1, 64)
			}
		}
	}
	return ""
}

func readFloatRaw(raw map[string]json.RawMessage, keys ...string) float64 {
	for _, key := range keys {
		if val, ok := raw[key]; ok {
			var decoded float64
			if err := json.Unmarshal(val, &decoded); err == nil {
				return decoded
			}
			var str string
			if err := json.Unmarshal(val, &str); err == nil {
				if parsed, err := strconv.ParseFloat(strings.TrimSpace(str), 64); err == nil {
					return parsed
				}
			}
		}
	}
	return 0
}

func normalizeStatus(value float64) string {
	switch int(value) {
	case 1:
		return "available"
	case 2:
		return "unavailable"
	default:
		return ""
	}
}

func normalizeAvailabilityStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "", "null":
		return ""
	case "available", "aktif", "active", "success", "sukses", "ok", "ready":
		return "available"
	case "pending", "process", "diproses", "processing":
		return "processing"
	case "failed", "gagal", "unavailable", "off", "soldout":
		return "unavailable"
	default:
		return strings.ToLower(strings.TrimSpace(status))
	}
}

func normalizeTransactionStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "", "null":
		return "unknown"
	case "success", "sukses", "ok", "completed", "complete", "done", "paid", "berhasil", "available":
		return "success"
	case "pending", "process", "processing", "diproses", "waiting", "awaiting", "progress", "menunggu":
		return "pending"
	case "failed", "gagal", "unavailable", "cancel", "cancelled", "expired", "timeout", "void", "rejected":
		return "failed"
	default:
		return strings.ToLower(strings.TrimSpace(status))
	}
}

// NormalizeTransactionStatus exposes the transaction status normalizer for other packages.
func NormalizeTransactionStatus(status string) string {
	return normalizeTransactionStatus(status)
}

func toString(val any) string {
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	case float64:
		if v == 0 {
			return ""
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
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
	default:
		return ""
	}
}

func toFloat(val any) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err == nil {
			return parsed
		}
		return 0
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		parsed, err := v.Float64()
		if err == nil {
			return parsed
		}
		return 0
	default:
		return 0
	}
}

func stringTrimQuotes(raw json.RawMessage) string {
	str := strings.TrimSpace(string(raw))
	str = strings.Trim(str, `"`)
	return str
}
