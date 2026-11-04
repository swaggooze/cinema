// MVP сервис событий
// API публикует события в Kafka, а consumer этого же сервиса читает их из топиков и пишет в лог
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	topicMovie   = "movie-events"
	topicUser    = "user-events"
	topicPayment = "payment-events"

	consumerGroup = "events-service"
	maxBodyBytes  = 1 << 20
	retryInterval = 3 * time.Second
)

var topics = []string{topicMovie, topicUser, topicPayment}

type MovieEvent struct {
	MovieID     *int     `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    *int       `json:"user_id"`
	Username  string     `json:"username,omitempty"`
	Email     string     `json:"email,omitempty"`
	Action    string     `json:"action"`
	Timestamp *time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  *int       `json:"payment_id"`
	UserID     *int       `json:"user_id"`
	Amount     *float64   `json:"amount"`
	Status     string     `json:"status"`
	Timestamp  *time.Time `json:"timestamp"`
	MethodType string     `json:"method_type,omitempty"`
}

func (e MovieEvent) validate() error {
	return firstMissing(map[string]bool{"movie_id": e.MovieID != nil, "title": e.Title != "", "action": e.Action != ""})
}

func (e MovieEvent) eventID() string {
	return fmt.Sprintf("movie-%d-%s", *e.MovieID, e.Action)
}

func (e UserEvent) validate() error {
	return firstMissing(map[string]bool{"user_id": e.UserID != nil, "action": e.Action != "", "timestamp": e.Timestamp != nil})
}

func (e UserEvent) eventID() string {
	return fmt.Sprintf("user-%d-%s", *e.UserID, e.Action)
}

func (e PaymentEvent) validate() error {
	return firstMissing(map[string]bool{
		"payment_id": e.PaymentID != nil,
		"user_id":    e.UserID != nil,
		"amount":     e.Amount != nil,
		"status":     e.Status != "",
		"timestamp":  e.Timestamp != nil,
	})
}

func (e PaymentEvent) eventID() string {
	return fmt.Sprintf("payment-%d-%s", *e.PaymentID, e.Status)
}

func firstMissing(fields map[string]bool) error {
	var missing []string
	for name, present := range fields {
		if !present {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("missing required fields: %s", strings.Join(missing, ", "))
}

// Event конверт, который уходит в Kafka и возвращается клиенту
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Payload   any       `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type Publisher interface {
	Publish(topic, key string, value []byte) (partition int32, offset int64, err error)
}

type Broker struct {
	mu       sync.RWMutex
	producer sarama.SyncProducer
	consumer bool
}

var errBrokerNotReady = errors.New("kafka is not ready")

func (b *Broker) setProducer(p sarama.SyncProducer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.producer = p
}

func (b *Broker) setConsumerReady(ready bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consumer = ready
}

func (b *Broker) Ready() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.producer != nil && b.consumer
}

func (b *Broker) Publish(topic, key string, value []byte) (int32, int64, error) {
	b.mu.RLock()
	producer := b.producer
	b.mu.RUnlock()
	if producer == nil {
		return 0, 0, errBrokerNotReady
	}
	return producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	})
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.producer != nil {
		if err := b.producer.Close(); err != nil {
			log.Printf("close producer: %v", err)
		}
		b.producer = nil
	}
}

func newSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = "events-service"
	cfg.Version = sarama.V2_7_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return cfg
}

func connectProducer(ctx context.Context, brokers []string, broker *Broker) {
	for ctx.Err() == nil {
		producer, err := sarama.NewSyncProducer(brokers, newSaramaConfig())
		if err == nil {
			broker.setProducer(producer)
			log.Printf("producer connected to %v", brokers)
			return
		}
		log.Printf("producer: kafka is not available yet: %v", err)
		sleep(ctx, retryInterval)
	}
}

type groupHandler struct{ broker *Broker }

func (h groupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.broker.setConsumerReady(true)
	return nil
}

func (h groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			log.Printf("event processed: topic=%s partition=%d offset=%d key=%s value=%s",
				msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			session.MarkMessage(msg, "")
		}
	}
}

func runConsumer(ctx context.Context, brokers []string, broker *Broker) {
	var group sarama.ConsumerGroup
	for ctx.Err() == nil {
		var err error
		group, err = sarama.NewConsumerGroup(brokers, consumerGroup, newSaramaConfig())
		if err == nil {
			break
		}
		log.Printf("consumer: kafka is not available yet: %v", err)
		sleep(ctx, retryInterval)
	}
	if group == nil {
		return
	}
	defer group.Close()
	log.Printf("consumer group %q subscribed to %v", consumerGroup, topics)

	go func() {
		for err := range group.Errors() {
			log.Printf("consumer error: %v", err)
		}
	}()

	for ctx.Err() == nil {
		if err := group.Consume(ctx, topics, groupHandler{broker: broker}); err != nil && ctx.Err() == nil {
			broker.setConsumerReady(false)
			log.Printf("consume: %v", err)
			sleep(ctx, retryInterval)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

type validatable interface {
	validate() error
	eventID() string
}

func eventHandler[T validatable](pub Publisher, eventType, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var payload T
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		if err := payload.validate(); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		event := Event{ID: payload.eventID(), Type: eventType, Timestamp: time.Now().UTC(), Payload: payload}
		value, err := json.Marshal(event)
		if err != nil {
			log.Printf("marshal event: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to encode event")
			return
		}

		partition, offset, err := pub.Publish(topic, event.ID, value)
		if err != nil {
			log.Printf("publish to %s: %v", topic, err)
			writeError(w, http.StatusInternalServerError, "failed to publish event")
			return
		}
		log.Printf("event published: topic=%s partition=%d offset=%d id=%s", topic, partition, offset, event.ID)
		writeJSON(w, http.StatusCreated, EventResponse{Status: "success", Partition: partition, Offset: offset, Event: event})
	}
}

func NewHandler(pub Publisher, ready func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", func(w http.ResponseWriter, _ *http.Request) {
		status := http.StatusOK
		if !ready() {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]bool{"status": status == http.StatusOK})
	})
	mux.HandleFunc("/api/events/movie", eventHandler[MovieEvent](pub, "movie", topicMovie))
	mux.HandleFunc("/api/events/user", eventHandler[UserEvent](pub, "user", topicUser))
	mux.HandleFunc("/api/events/payment", eventHandler[PaymentEvent](pub, "payment", topicPayment))
	return mux
}

func parseBrokers(raw string) []string {
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	rawBrokers := os.Getenv("KAFKA_BROKERS")
	if rawBrokers == "" {
		rawBrokers = "kafka:9092"
	}
	brokers := parseBrokers(rawBrokers)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := &Broker{}
	defer broker.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); connectProducer(ctx, brokers, broker) }()
	go func() { defer wg.Done(); runConsumer(ctx, brokers, broker) }()

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           NewHandler(broker, broker.Ready),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("events service started on :%s, brokers=%v", port, brokers)
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
	wg.Wait()
}
