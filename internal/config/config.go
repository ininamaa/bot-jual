package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the application configuration loaded from environment variables.
type Config struct {
	AppEnv                           string
	LogLevel                         string
	HTTPListenAddr                   string
	DatabaseURL                      string
	SupabaseSchema                   string
	WhatsAppStorePath                string
	WhatsAppDeviceJID                string
	WhatsAppLogLevel                 string
	AtlanticAPIKey                   string
	AtlanticBaseURL                  string
	AtlanticTimeout                  time.Duration
	AtlanticWebhookSecretMD5Username string
	AtlanticWebhookSecretMD5Password string
	GeminiAPIKeys                    []string
	GeminiModel                      string
	GeminiTimeout                    time.Duration
	MetricsNamespace                 string
	GeminiCooldown                   time.Duration
	RedisAddr                        string
	RedisPassword                    string
	RedisDB                          int
	RedisTLS                         bool
	PublicBaseURL                    string
	PublicBasePath                   string
	AtlanticDepositType              string
	AtlanticDepositMethod            string
	AtlanticDepositFeeFixed          int64
	AtlanticDepositFeePercent        float64
}

// Load returns configuration populated from environment variables with fallbacks.
func Load() (*Config, error) {
	cfg := &Config{
		AppEnv:                           getenvDefault("APP_ENV", "development"),
		LogLevel:                         getenvDefault("LOG_LEVEL", "info"),
		HTTPListenAddr:                   getenvDefault("HTTP_LISTEN_ADDR", ":8080"),
		DatabaseURL:                      trimmedEnv("DATABASE_URL"),
		SupabaseSchema:                   getenvDefault("SUPABASE_SCHEMA", "public"),
		WhatsAppStorePath:                getenvDefault("WHATSAPP_STORE_PATH", "data/wa-store.db"),
		WhatsAppDeviceJID:                trimmedEnv("WHATSAPP_DEVICE_JID"),
		WhatsAppLogLevel:                 getenvDefault("WHATSAPP_LOG_LEVEL", "INFO"),
		AtlanticAPIKey:                   trimmedEnv("ATL_API_KEY"),
		AtlanticBaseURL:                  getenvDefault("ATL_BASE_URL", "https://atlantich2h.com"),
		AtlanticWebhookSecretMD5Username: trimmedEnv("ATL_WEBHOOK_SECRET_MD5_USERNAME"),
		AtlanticWebhookSecretMD5Password: trimmedEnv("ATL_WEBHOOK_SECRET_MD5_PASSWORD"),
		GeminiAPIKeys:                    splitAndTrim(trimmedEnv("GEMINI_KEYS")),
		GeminiModel:                      getenvDefault("GEMINI_MODEL_FLASH_LITE", "gemini-2.5-flash-lite"),
		MetricsNamespace:                 getenvDefault("METRICS_NAMESPACE", "bot_jual"),
		RedisAddr:                        getenvDefault("REDIS_ADDR", "localhost:6379"),
		RedisPassword:                    trimmedEnv("REDIS_PASSWORD"),
		PublicBaseURL:                    getenvDefault("PUBLIC_BASE_URL", ""),
		AtlanticDepositType:              getenvDefault("ATL_DEPOSIT_TYPE", "ewallet"),
		AtlanticDepositMethod:            getenvDefault("ATL_DEPOSIT_METHOD", "qris"),
	}

	cooldown := getenvDefault("GEMINI_COOLDOWN", "24h")
	dur, err := time.ParseDuration(cooldown)
	if err != nil {
		return nil, fmt.Errorf("invalid GEMINI_COOLDOWN duration: %w", err)
	}
	cfg.GeminiCooldown = dur

	atlTimeoutStr := getenvDefault("ATL_TIMEOUT", "15s")
	if cfg.AtlanticTimeout, err = time.ParseDuration(atlTimeoutStr); err != nil {
		return nil, fmt.Errorf("invalid ATL_TIMEOUT duration: %w", err)
	}

	geminiTimeoutStr := getenvDefault("GEMINI_TIMEOUT", "20s")
	if cfg.GeminiTimeout, err = time.ParseDuration(geminiTimeoutStr); err != nil {
		return nil, fmt.Errorf("invalid GEMINI_TIMEOUT duration: %w", err)
	}

	if fixedStr := getenvDefault("ATL_DEPOSIT_FEE_FIXED", "0"); fixedStr != "" {
		fixedVal, convErr := strconv.ParseInt(strings.TrimSpace(fixedStr), 10, 64)
		if convErr != nil {
			return nil, fmt.Errorf("invalid ATL_DEPOSIT_FEE_FIXED value: %w", convErr)
		}
		if fixedVal < 0 {
			fixedVal = 0
		}
		cfg.AtlanticDepositFeeFixed = fixedVal
	}

	if percentStr := getenvDefault("ATL_DEPOSIT_FEE_PERCENT", "0"); percentStr != "" {
		percentVal, convErr := strconv.ParseFloat(strings.TrimSpace(percentStr), 64)
		if convErr != nil {
			return nil, fmt.Errorf("invalid ATL_DEPOSIT_FEE_PERCENT value: %w", convErr)
		}
		if percentVal < 0 {
			percentVal = 0
		}
		cfg.AtlanticDepositFeePercent = percentVal
	}

	if redisDBStr := getenvDefault("REDIS_DB", "0"); redisDBStr != "" {
		db, convErr := strconv.Atoi(redisDBStr)
		if convErr != nil {
			return nil, fmt.Errorf("invalid REDIS_DB value: %w", convErr)
		}
		cfg.RedisDB = db
	}

	cfg.RedisTLS = strings.EqualFold(getenvDefault("REDIS_TLS", "false"), "true")

	if cfg.PublicBaseURL != "" {
		parsed, err := url.Parse(cfg.PublicBaseURL)
		if err != nil {
			return nil, fmt.Errorf("invalid PUBLIC_BASE_URL: %w", err)
		}
		basePath := strings.TrimSuffix(parsed.Path, "/")
		if basePath == "/" {
			basePath = ""
		}
		cfg.PublicBasePath = basePath
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if len(cfg.GeminiAPIKeys) == 0 {
		return nil, fmt.Errorf("GEMINI_KEYS cannot be empty")
	}
	if cfg.AtlanticAPIKey == "" {
		return nil, fmt.Errorf("ATL_API_KEY is required")
	}
	if cfg.AtlanticWebhookSecretMD5Username == "" || cfg.AtlanticWebhookSecretMD5Password == "" {
		return nil, fmt.Errorf("ATL_WEBHOOK_SECRET_MD5_USERNAME and ATL_WEBHOOK_SECRET_MD5_PASSWORD are required")
	}

	cfg.AtlanticBaseURL = strings.TrimRight(cfg.AtlanticBaseURL, "/")

	return cfg, nil
}

func getenvDefault(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return fallback
}

func splitAndTrim(val string) []string {
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			res = append(res, trimmed)
		}
	}
	return res
}

func trimmedEnv(key string) string {
	if val, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(val)
	}
	return ""
}
