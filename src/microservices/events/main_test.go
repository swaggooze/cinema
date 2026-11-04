package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakePublisher struct {
	err   error
	topic string
	key   string
	value []byte
}

func (f *fakePublisher) Publish(topic, key string, value []byte) (int32, int64, error) {
	f.topic, f.key, f.value = topic, key, value
	return 0, 42, f.err
}

func post(h http.Handler, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	return rec
}

func alwaysReady() bool { return true }

func TestCreateEvents(t *testing.T) {
	tests := []struct {
		name, path, body, wantTopic, wantID, wantType string
	}{
		{"movie", "/api/events/movie", `{"movie_id":1,"title":"Inception","action":"viewed","user_id":2}`, topicMovie, "movie-1-viewed", "movie"},
		{"user", "/api/events/user", `{"user_id":1,"action":"logged_in","timestamp":"2023-01-15T14:30:00Z"}`, topicUser, "user-1-logged_in", "user"},
		{"payment", "/api/events/payment", `{"payment_id":1,"user_id":2,"amount":9.99,"status":"completed","timestamp":"2023-01-15T14:30:00Z"}`, topicPayment, "payment-1-completed", "payment"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			rec := post(NewHandler(pub, alwaysReady), tt.path, tt.body)
			if rec.Code != http.StatusCreated {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
			}
			var resp EventResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if resp.Status != "success" || resp.Offset != 42 || resp.Event.ID != tt.wantID || resp.Event.Type != tt.wantType {
				t.Errorf("unexpected response: %+v", resp)
			}
			if pub.topic != tt.wantTopic || pub.key != tt.wantID {
				t.Errorf("published to %q with key %q", pub.topic, pub.key)
			}
			var sent Event
			if err := json.Unmarshal(pub.value, &sent); err != nil {
				t.Errorf("published value is not an event: %v", err)
			}
		})
	}
}

func TestCreateEventBadRequest(t *testing.T) {
	tests := []struct{ name, path, body string }{
		{"invalid json", "/api/events/movie", `{`},
		{"movie without title", "/api/events/movie", `{"movie_id":1,"action":"viewed"}`},
		{"user without timestamp", "/api/events/user", `{"user_id":1,"action":"logged_in"}`},
		{"payment without amount", "/api/events/payment", `{"payment_id":1,"user_id":2,"status":"completed","timestamp":"2023-01-15T14:30:00Z"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			rec := post(NewHandler(pub, alwaysReady), tt.path, tt.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if pub.topic != "" {
				t.Error("invalid event must not be published")
			}
		})
	}
}

func TestZeroIDsAreAccepted(t *testing.T) {
	rec := post(NewHandler(&fakePublisher{}, alwaysReady), "/api/events/movie", `{"movie_id":0,"title":"t","action":"a"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
}

func TestPublishFailure(t *testing.T) {
	pub := &fakePublisher{err: errors.New("kafka down")}
	rec := post(NewHandler(pub, alwaysReady), "/api/events/user", `{"user_id":1,"action":"a","timestamp":"2023-01-15T14:30:00Z"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	rec := httptest.NewRecorder()
	NewHandler(&fakePublisher{}, alwaysReady).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/events/movie", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestHealth(t *testing.T) {
	for ready, want := range map[bool]int{true: http.StatusOK, false: http.StatusServiceUnavailable} {
		rec := httptest.NewRecorder()
		NewHandler(&fakePublisher{}, func() bool { return ready }).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/events/health", nil))
		if rec.Code != want {
			t.Errorf("ready=%t: status = %d, want %d", ready, rec.Code, want)
		}
		var body map[string]bool
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["status"] != ready {
			t.Errorf("ready=%t: body = %s", ready, rec.Body)
		}
	}
}

func TestParseBrokers(t *testing.T) {
	got := parseBrokers(" kafka-0:9092, kafka-1:9092 ,,")
	if len(got) != 2 || got[0] != "kafka-0:9092" || got[1] != "kafka-1:9092" {
		t.Fatalf("parseBrokers = %v", got)
	}
}

func TestMissingFieldsAreReported(t *testing.T) {
	err := PaymentEvent{}.validate()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, field := range []string{"payment_id", "user_id", "amount", "status", "timestamp"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("%q not reported in %q", field, err)
		}
	}
}
