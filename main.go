package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/pubsub"
)

// Order represents the structure of the Pub/Sub message
type Order struct {
	OrderID   string `json:"orderId"`
	UserEmail string `json:"userEmail"`
}

// Worker processes messages from multiple Pub/Sub topics
type Worker struct {
	PubSubClient   *pubsub.Client
	OrderSub       *pubsub.Subscription
	CancelSub      *pubsub.Subscription
	WaitGroup      *sync.WaitGroup
}

// NewWorker initializes the Worker with subscriptions
func NewWorker(projectID string) (*Worker, error) {
	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %v", err)
	}

	return &Worker{
		PubSubClient:   client,
		OrderSub:       client.Subscription("order-placed-sub"),
		CancelSub:      client.Subscription("cancel-order-sub"),
		WaitGroup:      &sync.WaitGroup{},
	}, nil
}

// HandleOrder processes messages from the "order-placed-topic"
func (w *Worker) HandleOrder(order Order) error {
	fmt.Printf("Handling order: ID=%s, Email=%s\n", order.OrderID, order.UserEmail)

	// Simulate order handling logic
	// After handling, generate an invoice
	if err := w.GenerateInvoice(order); err != nil {
		return fmt.Errorf("failed to generate invoice: %v", err)
	}

	return nil
}

// CancelOrder processes messages from the "auto-cancel-order-topic"
func (w *Worker) CancelOrder(order Order) error {
	fmt.Printf("Canceling order: ID=%s, Email=%s\n", order.OrderID, order.UserEmail)

	// Simulate order cancellation logic
	// (e.g., updating the database, sending notifications)

	return errors.New("something hapenned!")
}

// GenerateInvoice generates an invoice for a processed order
func (w *Worker) GenerateInvoice(order Order) error {
	fmt.Printf("Generating invoice for order: ID=%s, Email=%s\n", order.OrderID, order.UserEmail)

	// Simulate invoice generation logic
	// (e.g., saving to a database, sending an email)

	return nil
}

// StartListening starts listening to messages from both subscriptions
func (w *Worker) StartListening(ctx context.Context) {
	// Listen to "order-placed-topic"
	w.WaitGroup.Add(1)
	go func() {
		defer w.WaitGroup.Done()
		err := w.OrderSub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			var order Order
			if err := json.Unmarshal(msg.Data, &order); err != nil {
				log.Printf("Failed to parse order message: %v", err)
				msg.Nack()
				return
			}

			if err := w.HandleOrder(order); err != nil {
				log.Printf("Failed to handle order: %v", err)
				msg.Nack()
				return
			}

			_, err := msg.AckWithResult().Get(ctx)
			if err != nil {
				log.Printf("Failed ack: %s", err)
				msg.Ack()
				return	
			}
			log.Printf("Success ack")
		})
		if err != nil {
			log.Printf("Error receiving messages from order-placed-subscription: %v", err)
		}
	}()
	log.Print("Listening order started...")

	// Listen to "auto-cancel-order-topic"
	w.WaitGroup.Add(1)
	go func() {
		defer w.WaitGroup.Done()
		err := w.CancelSub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			var order Order
			if err := json.Unmarshal(msg.Data, &order); err != nil {
				log.Printf("Failed to parse cancel order message: %v", err)
				msg.Nack()
				return
			}

			if err := w.CancelOrder(order); err != nil {
				log.Printf("Failed to cancel order: %v", err)

				msg.Ack()
				return
			}

			msg.Ack()
		})
		if err != nil {
			log.Printf("Error receiving messages from auto-cancel-order-subscription: %v", err)
		}
	}()
	log.Print("Listening cancel started...")
}

func main() {
	projectID := "golang-practice-447504"

	// Initialize the worker
	worker, err := NewWorker(projectID)
	if err != nil {
		log.Fatalf("Failed to initialize worker: %v", err)
	}
	defer worker.PubSubClient.Close()

	ctx := context.Background()

	// Start listening to topics
	worker.StartListening(ctx)

	// Wait for all goroutines to finish
	worker.WaitGroup.Wait()
}