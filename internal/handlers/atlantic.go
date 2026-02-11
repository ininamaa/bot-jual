package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"bot-jual/internal/atl"
	"bot-jual/internal/metrics"
	"bot-jual/internal/repo"

	"log/slog"

	"go.mau.fi/whatsmeow/types"
)

// Notifier sends WhatsApp notifications to users.
type Notifier interface {
	SendText(ctx context.Context, to types.JID, text string) error
}

// AtlanticWebhookProcessor processes Atlantic webhook callbacks.
type AtlanticWebhookProcessor struct {
	repo     repo.Repository
	logger   *slog.Logger
	metrics  *metrics.Metrics
	notifier Notifier
	atl      *atl.Client
}

// NewAtlanticWebhookProcessor constructs processor.
func NewAtlanticWebhookProcessor(repository repo.Repository, notifier Notifier, metrics *metrics.Metrics, logger *slog.Logger, atlClient *atl.Client) *AtlanticWebhookProcessor {
	return &AtlanticWebhookProcessor{
		repo:     repository,
		notifier: notifier,
		metrics:  metrics,
		logger:   logger.With("component", "atlantic_processor"),
		atl:      atlClient,
	}
}

// HandleAtlanticEvent satisfies atl.WebhookProcessor.
func (p *AtlanticWebhookProcessor) HandleAtlanticEvent(ctx context.Context, event atl.WebhookEvent) error {
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		p.metrics.Errors.WithLabelValues("atlantic_webhook_decode").Inc()
		return fmt.Errorf("decode payload: %w", err)
	}

	flattened := flattenPayload(payload)
	ref := firstString(flattened, "ref_id", "reff_id", "reference", "transaction_ref", "order_ref", "deposit_ref")
	if ref == "" {
		p.logger.Error("atlantic webhook missing ref", "event", event.Type, "payload", payload)
		return fmt.Errorf("missing ref_id in payload")
	}
	rawStatus := firstString(flattened, "status", "state")
	status := atl.NormalizeTransactionStatus(rawStatus)
	message := firstString(flattened, "message", "info", "description")
	sn := firstString(flattened, "sn", "serial_number")
	originalStatus := status

	meta := map[string]any{
		"payload": payload,
		"headers": event.Headers,
		"event":   event.Type,
		"status": map[string]any{
			"raw":        rawStatus,
			"normalized": status,
		},
	}

	if shouldForceSuccess(event.Type, status) {
		status = "success"
		meta["forced_success"] = true
		meta["original_status"] = originalStatus
	}

	if strings.Contains(strings.ToLower(event.Type), "deposit") {
		if err := p.repo.UpdateDepositStatus(ctx, ref, status, meta); err != nil {
			return err
		}
		dep, err := p.repo.GetDepositByRef(ctx, ref)
		if err != nil {
			return fmt.Errorf("lookup deposit %s: %w", ref, err)
		}

		handled := false
		switch status {
		case "success":
			handled = p.handleDepositSuccess(ctx, dep, message)
		case "failed":
			handled = p.handleDepositFailure(ctx, dep, message)
		}

		if !handled {
			p.notifyUser(ctx, dep.UserID, formatDepositStatusMessage(dep, status, message))
		}
		return nil
	}

	if err := p.repo.UpdateOrderStatus(ctx, ref, status, meta); err != nil {
		return err
	}

	order, err := p.repo.GetOrderByRef(ctx, ref)
	if err == nil {
		info := strings.Builder{}
		info.WriteString(fmt.Sprintf("Update transaksi %s: %s", ref, strings.ToUpper(status)))
		if message != "" {
			info.WriteString(". ")
			info.WriteString(message)
		}
		if sn != "" {
			info.WriteString(". SN: ")
			info.WriteString(sn)
		}
		p.notifyUser(ctx, order.UserID, info.String())
	}

	return nil
}

func shouldForceSuccess(eventType, status string) bool {
	etype := strings.ToLower(eventType)
	if !strings.Contains(etype, "deposit") && !strings.Contains(etype, "transfer") {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(status))
	switch normalized {
	case "pending", "process", "processing", "waiting", "awaiting", "progress":
		return true
	default:
		return false
	}
}

func (p *AtlanticWebhookProcessor) notifyUser(ctx context.Context, userID string, text string) {
	if p.notifier == nil {
		return
	}
	user, err := p.repo.GetUserByID(ctx, userID)
	if err != nil {
		p.logger.Warn("failed fetching user for notification", "error", err, "user_id", userID)
		return
	}
	if user.WAJID == nil || *user.WAJID == "" {
		return
	}
	jid, err := types.ParseJID(*user.WAJID)
	if err != nil {
		p.logger.Warn("invalid user jid", "error", err, "jid", *user.WAJID)
		return
	}
	if err := p.notifier.SendText(ctx, jid, text); err != nil {
		p.logger.Warn("failed sending notification", "error", err)
	}
}

func flattenPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	flat := make(map[string]any, len(payload)+8)
	for k, v := range payload {
		flat[k] = v
	}
	dataRaw, ok := payload["data"]
	if !ok || dataRaw == nil {
		return flat
	}
	switch data := dataRaw.(type) {
	case map[string]any:
		for k, v := range data {
			if _, exists := flat[k]; !exists {
				flat[k] = v
			}
		}
	case string:
		var nested map[string]any
		if err := json.Unmarshal([]byte(data), &nested); err == nil {
			for k, v := range nested {
				if _, exists := flat[k]; !exists {
					flat[k] = v
				}
			}
		}
	}
	return flat
}

func firstString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if val, ok := payload[key]; ok {
			switch v := val.(type) {
			case string:
				if strings.TrimSpace(v) != "" {
					return v
				}
			case float64:
				return fmt.Sprintf("%.0f", v)
			case json.Number:
				return v.String()
			}
		}
	}
	return ""
}

func formatDepositStatusMessage(dep *repo.Deposit, status, message string) string {
	var ref string
	if dep != nil && strings.TrimSpace(dep.DepositRef) != "" {
		ref = dep.DepositRef
	} else {
		ref = "-"
	}
	stat := strings.ToUpper(strings.TrimSpace(status))
	if stat == "" || stat == "UNKNOWN" {
		stat = "STATUS TIDAK DIKETAHUI"
	}
	base := fmt.Sprintf("Update deposit %s: %s", ref, stat)
	if strings.TrimSpace(message) != "" {
		base = fmt.Sprintf("%s. %s", base, message)
	} else {
		base += "."
	}
	if summary := depositSummary(dep); summary != "" {
		return fmt.Sprintf("%s\n%s", base, summary)
	}
	return base
}

func (p *AtlanticWebhookProcessor) handleDepositSuccess(ctx context.Context, dep *repo.Deposit, depositMessage string) bool {
	if p.atl == nil {
		p.logger.Warn("atlantic client unavailable for auto-fulfill", "deposit_ref", dep.DepositRef)
		return false
	}
	orders, err := p.repo.ListOrdersAwaitingDeposit(ctx, dep.DepositRef)
	if err != nil {
		p.logger.Error("list orders awaiting deposit failed", "error", err, "deposit_ref", dep.DepositRef)
		return false
	}
	if len(orders) == 0 {
		return false
	}
	handled := false
	for _, order := range orders {
		if p.autoFulfillOrderAfterDeposit(ctx, dep, order, depositMessage) {
			handled = true
		}
	}
	return handled
}

func (p *AtlanticWebhookProcessor) handleDepositFailure(ctx context.Context, dep *repo.Deposit, failureMessage string) bool {
	orders, err := p.repo.ListOrdersAwaitingDeposit(ctx, dep.DepositRef)
	if err != nil {
		p.logger.Error("list orders awaiting deposit failed", "error", err, "deposit_ref", dep.DepositRef)
		return false
	}
	if len(orders) == 0 {
		return false
	}
	statusText := formatDepositStatusMessage(dep, "failed", failureMessage)
	for _, order := range orders {
		meta := cloneMetadata(order.Metadata)
		meta["deposit_ref"] = dep.DepositRef
		if strings.TrimSpace(failureMessage) != "" {
			meta["deposit_failure_message"] = failureMessage
		}
		meta["auto_fulfilled"] = false
		if err := p.repo.UpdateOrderStatus(ctx, order.OrderRef, "failed", meta); err != nil {
			p.logger.Error("update order after deposit failure", "error", err, "order_ref", order.OrderRef, "deposit_ref", dep.DepositRef)
		}
		msg := fmt.Sprintf("%s\nPesanan %s dibatalkan. Silakan buat ulang jika masih ingin melanjutkan.", statusText, order.OrderRef)
		p.notifyUser(ctx, order.UserID, msg)
	}
	return true
}

func (p *AtlanticWebhookProcessor) autoFulfillOrderAfterDeposit(ctx context.Context, dep *repo.Deposit, order repo.Order, depositMessage string) bool {
	customerID := stringValue(order.Metadata, "customer_id")
	if customerID == "" {
		p.logger.Warn("order missing customer id for auto-fulfill", "order_ref", order.OrderRef, "deposit_ref", dep.DepositRef)
		p.notifyUser(ctx, order.UserID, fmt.Sprintf("Deposit %s diterima, tapi data tujuan untuk pesanan %s belum lengkap. Hubungi admin ya.", dep.DepositRef, order.OrderRef))
		return true
	}
	availableNet := numberFromMetadata(dep.Metadata, "net_amount")
	expected := order.Amount
	if availableNet > 0 && expected > 0 && availableNet < expected {
		diff := expected - availableNet
		meta := cloneMetadata(order.Metadata)
		meta["deposit_ref"] = dep.DepositRef
		meta["auto_fulfilled"] = false
		meta["auto_fulfill_error"] = "insufficient_net_amount"
		meta["net_amount_available"] = availableNet
		meta["required_amount"] = expected
		if err := p.repo.UpdateOrderStatus(ctx, order.OrderRef, "awaiting_payment", meta); err != nil {
			p.logger.Error("update order insufficient deposit", "error", err, "order_ref", order.OrderRef, "deposit_ref", dep.DepositRef)
		}
		msg := fmt.Sprintf("Deposit %s sudah masuk %s, tapi masih kurang %s untuk transaksi %s. Tambah deposit ya supaya bisa ku proses.", dep.DepositRef, formatIDR(availableNet), formatIDR(diff), order.OrderRef)
		p.notifyUser(ctx, order.UserID, msg)
		return true
	}
	resp, err := p.atl.CreatePrepaidTransaction(ctx, atl.CreatePrepaidRequest{
		ProductCode: order.ProductCode,
		CustomerID:  customerID,
		RefID:       order.OrderRef,
	})
	if err != nil {
		p.logger.Error("auto create prepaid failed", "error", err, "order_ref", order.OrderRef, "deposit_ref", dep.DepositRef)
		meta := cloneMetadata(order.Metadata)
		meta["deposit_ref"] = dep.DepositRef
		meta["auto_fulfilled"] = false
		meta["auto_fulfill_error"] = err.Error()
		meta["auto_fulfilled_at"] = time.Now().UTC().Format(time.RFC3339)
		if strings.TrimSpace(depositMessage) != "" {
			meta["deposit_message"] = depositMessage
		}
		if err := p.repo.UpdateOrderStatus(ctx, order.OrderRef, "failed", meta); err != nil {
			p.logger.Error("update order after auto-fulfill failure", "error", err, "order_ref", order.OrderRef, "deposit_ref", dep.DepositRef)
		}
		p.notifyUser(ctx, order.UserID, fmt.Sprintf("Deposit %s sudah diterima, tapi transaksi %s gagal dibuat: %v. Tolong hubungi admin ya.", dep.DepositRef, order.OrderRef, err))
		return true
	}

	meta := cloneMetadata(order.Metadata)
	meta["deposit_ref"] = dep.DepositRef
	meta["auto_fulfilled"] = true
	meta["auto_fulfilled_at"] = time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(depositMessage) != "" {
		meta["deposit_message"] = depositMessage
	}
	if resp.Message != "" {
		meta["message"] = resp.Message
	}
	if resp.SN != "" {
		meta["sn"] = resp.SN
	}
	if resp.Raw != nil {
		meta["transaction_raw"] = resp.Raw
	}
	if err := p.repo.UpdateOrderStatus(ctx, order.OrderRef, resp.Status, meta); err != nil {
		p.logger.Error("update order after auto-fulfill success", "error", err, "order_ref", order.OrderRef, "deposit_ref", dep.DepositRef)
	}

	var lines []string
	lines = append(lines, fmt.Sprintf("Deposit %s sudah diterima.", dep.DepositRef))
	if summary := depositSummary(dep); summary != "" {
		lines = append(lines, summary)
	}
	product := productLabel(dep, order)
	statusLine := fmt.Sprintf("Transaksi %s untuk %s status: %s", order.OrderRef, product, strings.ToUpper(resp.Status))
	if resp.Message != "" {
		statusLine = fmt.Sprintf("%s. %s", statusLine, resp.Message)
	} else {
		statusLine += "."
	}
	lines = append(lines, statusLine)
	lines = append(lines, fmt.Sprintf("Tujuan: %s", customerID))
	if resp.SN != "" {
		lines = append(lines, fmt.Sprintf("SN: %s", resp.SN))
	}
	p.notifyUser(ctx, order.UserID, strings.Join(lines, "\n"))
	return true
}

func cloneMetadata(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	dst := make(map[string]any, len(src)+4)
	for k, v := range src {
		dst[k] = v
	}
	return dst
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
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func productLabel(dep *repo.Deposit, order repo.Order) string {
	if dep != nil && dep.Metadata != nil {
		if name := stringValue(dep.Metadata, "product"); name != "" {
			return name
		}
	}
	if name := stringValue(order.Metadata, "product"); name != "" {
		return name
	}
	return order.ProductCode
}

func depositSummary(dep *repo.Deposit) string {
	if dep == nil {
		return ""
	}
	gross := dep.Amount
	if dep.Metadata != nil {
		if v := numberFromMetadata(dep.Metadata, "gross_amount"); v > 0 {
			gross = v
		} else if v := numberFromMetadata(dep.Metadata, "requested_amount"); v > 0 {
			gross = v
		} else if v := numberFromMetadata(dep.Metadata, "amount"); v > 0 {
			gross = v
		}
	}
	fee := numberFromMetadata(dep.Metadata, "fee")
	if fee == 0 {
		fee = numberFromMetadata(dep.Metadata, "admin_fee")
	}
	net := numberFromMetadata(dep.Metadata, "net_amount")
	if net == 0 {
		net = numberFromMetadata(dep.Metadata, "get_balance")
	}
	if net == 0 {
		net = numberFromMetadata(dep.Metadata, "saldo_masuk")
	}
	summary := summarizeAmounts(gross, fee, net)
	if summary == "" {
		return summary
	}
	if shortfall := numberFromMetadata(dep.Metadata, "net_shortfall"); shortfall > 0 {
		summary = fmt.Sprintf("%s | Kekurangan: %s", summary, formatIDR(shortfall))
	}
	return summary
}

func summarizeAmounts(gross, fee, net int64) string {
	if gross <= 0 && net <= 0 {
		return ""
	}
	if net <= 0 && gross > 0 {
		net = gross - fee
		if net <= 0 {
			net = gross
		}
	}
	parts := make([]string, 0, 3)
	if gross > 0 {
		parts = append(parts, fmt.Sprintf("Tagihan: %s", formatIDR(gross)))
	}
	if fee > 0 {
		parts = append(parts, fmt.Sprintf("Biaya: %s", formatIDR(fee)))
	}
	if net > 0 {
		parts = append(parts, fmt.Sprintf("Saldo masuk: %s", formatIDR(net)))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " | ")
}

func numberFromMetadata(meta map[string]any, key string) int64 {
	if meta == nil {
		return 0
	}
	val, ok := meta[key]
	if !ok || val == nil {
		return 0
	}
	switch v := val.(type) {
	case float64:
		if v < 0 {
			return 0
		}
		return int64(v + 0.5)
	case float32:
		if v < 0 {
			return 0
		}
		return int64(v + 0.5)
	case int:
		return int64(v)
	case int64:
		return v
	case json.Number:
		if f, err := v.Float64(); err == nil {
			if f < 0 {
				return 0
			}
			return int64(f + 0.5)
		}
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0
		}
		clean := strings.ReplaceAll(trimmed, ".", "")
		clean = strings.ReplaceAll(clean, ",", "")
		if parsed, err := strconv.ParseInt(clean, 10, 64); err == nil {
			return parsed
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			if f < 0 {
				return 0
			}
			return int64(f + 0.5)
		}
	}
	return 0
}

func formatIDR(value int64) string {
	if value <= 0 {
		return "Rp0"
	}
	return fmt.Sprintf("Rp%d", value)
}
