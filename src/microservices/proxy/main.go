// Package main реализует прокси-сервис (API Gateway) для паттерна Strangler Fig:
// единая точка входа, которая постепенно переводит трафик с монолита на микросервисы
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	upstreamMonolith = "monolith"
	upstreamMovies   = "movies-service"
	upstreamEvents   = "events-service"
)

type Config struct {
	Port             string
	MonolithURL      string
	MoviesServiceURL string
	EventsServiceURL string
	// GradualMigration включает процентную маршрутизацию домена movies
	// Если флаг выключен, весь домен movies идёт в микросервис (см. README)
	GradualMigration bool
	// Доля запросов к movies (0–100), которая идёт в микросервис
	MoviesMigrationPercent int
}

func loadConfig() Config {
	return Config{
		Port:                   getEnv("PORT", "8000"),
		MonolithURL:            getEnv("MONOLITH_URL", "http://monolith:8080"),
		MoviesServiceURL:       getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"),
		EventsServiceURL:       getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"),
		GradualMigration:       parseBool(os.Getenv("GRADUAL_MIGRATION"), false),
		MoviesMigrationPercent: parsePercent(os.Getenv("MOVIES_MIGRATION_PERCENT")),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseBool(raw string, fallback bool) bool {
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

// parsePercent при некорректном значении возвращает 0: трафик остаётся на монолите, то есть на проверенном пути
func parsePercent(raw string) int {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		if raw != "" {
			log.Printf("invalid MOVIES_MIGRATION_PERCENT=%q, using 0", raw)
		}
		return 0
	}
	return min(max(v, 0), 100)
}

type Router struct {
	gradual bool
	percent int
	roll    func() int
}

func hasPathPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// Route возвращает имя сервиса, который должен обработать путь
func (r Router) Route(path string) string {
	switch {
	case hasPathPrefix(path, "/api/events"):
		return upstreamEvents
	case hasPathPrefix(path, "/api/movies/health"):
		return upstreamMovies
	case hasPathPrefix(path, "/api/movies"):
		if !r.gradual || r.roll() < r.percent {
			return upstreamMovies
		}
		return upstreamMonolith
	default:
		// Всё, что ещё не выделено из монолита, остаётся в нём
		return upstreamMonolith
	}
}

func newReverseProxy(name, rawURL string) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse %s url: %w", name, err)
	}
	if target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("parse %s url: %q must be absolute", name, rawURL)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Printf("upstream %s error: %s %s: %v", name, r.Method, r.URL.Path, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream " + name + " is unavailable"})
	}
	return proxy, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write response: %v", err)
	}
}

func NewHandler(cfg Config) (http.Handler, error) {
	upstreams := map[string]string{
		upstreamMonolith: cfg.MonolithURL,
		upstreamMovies:   cfg.MoviesServiceURL,
		upstreamEvents:   cfg.EventsServiceURL,
	}
	proxies := make(map[string]*httputil.ReverseProxy, len(upstreams))
	for name, rawURL := range upstreams {
		proxy, err := newReverseProxy(name, rawURL)
		if err != nil {
			return nil, err
		}
		proxies[name] = proxy
	}

	router := Router{
		gradual: cfg.GradualMigration,
		percent: cfg.MoviesMigrationPercent,
		roll:    func() int { return rand.IntN(100) },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"status": true})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := router.Route(r.URL.Path)
		// Заголовок показывает, куда ушёл запрос: по нему удобно проверять долю миграции
		w.Header().Set("X-Upstream", name)
		log.Printf("%s %s -> %s", r.Method, r.URL.RequestURI(), name)
		proxies[name].ServeHTTP(w, r)
	})
	return mux, nil
}

func main() {
	cfg := loadConfig()
	handler, err := NewHandler(cfg)
	if err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("proxy started on :%s, gradual_migration=%t, movies_migration_percent=%d",
			cfg.Port, cfg.GradualMigration, cfg.MoviesMigrationPercent)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
