package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouterRoute(t *testing.T) {
	tests := []struct {
		name    string
		gradual bool
		percent int
		roll    int
		path    string
		want    string
	}{
		{"events always go to events service", true, 0, 0, "/api/events/movie", upstreamEvents},
		{"users stay in monolith", true, 100, 0, "/api/users", upstreamMonolith},
		{"unknown path stays in monolith", true, 100, 0, "/api/payments", upstreamMonolith},
		{"similar prefix is not movies", true, 100, 0, "/api/moviesfoo", upstreamMonolith},
		{"movies health is microservice only", true, 0, 99, "/api/movies/health", upstreamMovies},
		{"percent 0 keeps monolith", true, 0, 0, "/api/movies", upstreamMonolith},
		{"percent 100 moves everything", true, 100, 99, "/api/movies", upstreamMovies},
		{"roll below percent goes to movies", true, 50, 49, "/api/movies", upstreamMovies},
		{"roll at percent goes to monolith", true, 50, 50, "/api/movies", upstreamMonolith},
		{"flag off sends domain to microservice", false, 0, 99, "/api/movies", upstreamMovies},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Router{gradual: tt.gradual, percent: tt.percent, roll: func() int { return tt.roll }}
			if got := r.Route(tt.path); got != tt.want {
				t.Fatalf("Route(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestParsePercent(t *testing.T) {
	for raw, want := range map[string]int{"": 0, "abc": 0, "-5": 0, "42": 42, " 50 ": 50, "100": 100, "150": 100} {
		if got := parsePercent(raw); got != want {
			t.Errorf("parsePercent(%q) = %d, want %d", raw, got, want)
		}
	}
}

func upstream(t *testing.T, name string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, name+" "+r.URL.RequestURI())
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestHandlerProxiesToUpstream(t *testing.T) {
	handler, err := NewHandler(Config{
		MonolithURL:            upstream(t, "monolith").URL,
		MoviesServiceURL:       upstream(t, "movies").URL,
		EventsServiceURL:       upstream(t, "events").URL,
		GradualMigration:       true,
		MoviesMigrationPercent: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(handler)
	defer gateway.Close()

	tests := []struct{ path, wantBody, wantUpstream string }{
		{"/api/movies?id=7", "movies /api/movies?id=7", upstreamMovies},
		{"/api/users", "monolith /api/users", upstreamMonolith},
		{"/api/events/user", "events /api/events/user", upstreamEvents},
	}
	for _, tt := range tests {
		resp, err := http.Get(gateway.URL + tt.path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != tt.wantBody {
			t.Errorf("%s: body = %q, want %q", tt.path, body, tt.wantBody)
		}
		if got := resp.Header.Get("X-Upstream"); got != tt.wantUpstream {
			t.Errorf("%s: X-Upstream = %q, want %q", tt.path, got, tt.wantUpstream)
		}
	}
}

func TestHandlerHealthAndBadGateway(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	handler, err := NewHandler(Config{MonolithURL: deadURL, MoviesServiceURL: deadURL, EventsServiceURL: deadURL})
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/health status = %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/users", nil))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("dead upstream status = %d, want 502", rec.Code)
	}
}

func TestNewHandlerRejectsRelativeURL(t *testing.T) {
	if _, err := NewHandler(Config{MonolithURL: "monolith:8080", MoviesServiceURL: "http://m", EventsServiceURL: "http://e"}); err == nil {
		t.Fatal("expected error for url without scheme")
	}
}
