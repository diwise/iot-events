package storage

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/diwise/service-chassis/pkg/infrastructure/env"
)

type Config struct {
	host              string
	user              string
	password          string
	port              string
	dbname            string
	sslmode           string
	maxConns          int
	minConns          int
	maxConnIdleTime   time.Duration
	maxConnLifetime   time.Duration
	healthCheckPeriod time.Duration
}

func NewConfig(host, port, dbname, user, password, sslmode string) Config {
	return Config{
		host:              host,
		user:              user,
		password:          password,
		port:              port,
		dbname:            dbname,
		sslmode:           sslmode,
		maxConns:          4,
		minConns:          0,
		maxConnIdleTime:   5 * time.Minute,
		maxConnLifetime:   1 * time.Hour,
		healthCheckPeriod: 1 * time.Minute,
	}
}

func LoadConfiguration(ctx context.Context) Config {
	return Config{
		host:              env.GetVariableOrDefault(ctx, "POSTGRES_HOST", ""),
		user:              env.GetVariableOrDefault(ctx, "POSTGRES_USER", ""),
		password:          env.GetVariableOrDefault(ctx, "POSTGRES_PASSWORD", ""),
		port:              env.GetVariableOrDefault(ctx, "POSTGRES_PORT", "5432"),
		dbname:            env.GetVariableOrDefault(ctx, "POSTGRES_DBNAME", "diwise"),
		sslmode:           env.GetVariableOrDefault(ctx, "POSTGRES_SSLMODE", "disable"),
		maxConns:          getIntOrDefault(ctx, "POSTGRES_MAX_CONNS", 4),
		minConns:          getIntOrDefault(ctx, "POSTGRES_MIN_CONNS", 0),
		maxConnIdleTime:   getDurationOrDefault(ctx, "POSTGRES_MAX_CONN_IDLE_TIME", 5*time.Minute),
		maxConnLifetime:   getDurationOrDefault(ctx, "POSTGRES_MAX_CONN_LIFETIME", 1*time.Hour),
		healthCheckPeriod: getDurationOrDefault(ctx, "POSTGRES_HEALTH_CHECK_PERIOD", 1*time.Minute),
	}
}

func getIntOrDefault(ctx context.Context, envVar string, defaultValue int) int {
	strVal := env.GetVariableOrDefault(ctx, envVar, "")
	if strVal == "" {
		return defaultValue
	}
	val, err := strconv.Atoi(strVal)
	if err != nil {
		return defaultValue
	}
	return val
}

func getDurationOrDefault(ctx context.Context, envVar string, defaultValue time.Duration) time.Duration {
	strVal := env.GetVariableOrDefault(ctx, envVar, "")
	if strVal == "" {
		return defaultValue
	}
	val, err := time.ParseDuration(strVal)
	if err != nil {
		return defaultValue
	}
	return val
}

func (c Config) ConnStr() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", c.user, c.password, c.host, c.port, c.dbname, c.sslmode)
}

func (c Config) MaxConns() int {
	return c.maxConns
}

func (c Config) MinConns() int {
	return c.minConns
}

func (c Config) MaxConnIdleTime() time.Duration {
	return c.maxConnIdleTime
}

func (c Config) MaxConnLifetime() time.Duration {
	return c.maxConnLifetime
}

func (c Config) HealthCheckPeriod() time.Duration {
	return c.healthCheckPeriod
}
