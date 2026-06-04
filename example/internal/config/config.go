package config

import (
	"os"
)

// Config holds all configuration loaded from environment variables.
type Config struct {
	PostgresDSN string
	RedisAddr   string
	RedisPass   string
	TopAddr     string
	AzAddr      string
	AzURL       string
	AccessKey   string
	SecretKey   string
	LogMode     string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		PostgresDSN: getEnv("POSTGRES_DSN", "postgres://nsp:nsp123@localhost:5432/nsp?sslmode=disable"),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPass:   getEnv("REDIS_PASSWORD", ""),
		TopAddr:     getEnv("TOP_ADDR", ":8080"),
		AzAddr:      getEnv("AZ_ADDR", ":8081"),
		AzURL:       getEnv("AZ_URL", "http://localhost:8081"),
		AccessKey:   getEnv("ACCESS_KEY", "example-ak"),
		SecretKey:   getEnv("SECRET_KEY", "example-sk-1234567890abcdef"),
		LogMode:     getEnv("LOG_MODE", "dev"),
	}
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
