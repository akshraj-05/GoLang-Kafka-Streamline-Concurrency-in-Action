// main.go

package main

import (
	"Go_Project/config"
	"Go_Project/consumer"
	"Go_Project/model"
	"Go_Project/validate"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
)

func main() {
	// Apply configuration
	Broker, Topic, GroupID, err := config.ApplyConfig()
	if err != nil {
		fmt.Println("Error in configuration:", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize MongoDB client
	connectionString := "mongodb://localhost:27017"
	client, err := model.Initialize(ctx, connectionString)
	if err != nil {
		fmt.Println("Error initializing MongoDB client:", err)
		return
	}

	// Channels for cross-goroutine communication
	authCh := make(chan *sarama.ConsumerMessage)
	dataCh := make(chan *sarama.ConsumerMessage)

	// Initiate consumer, validation, and database storing goroutines
	wg := new(sync.WaitGroup)
	wg.Add(3)
	go consumer.Consumer(ctx, Broker, Topic, GroupID, authCh, wg)
	go validate.ValidateUser(ctx, authCh, dataCh, wg)
	go model.Start(ctx, client, dataCh, wg)

	// Graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	select {
	case <-signals:
		fmt.Println("Interrupt signal received, shutting down...")
		cancel()
	}

	wg.Wait()
	fmt.Println("Clossing channel ...")
	close(authCh)
	close(dataCh)
	//mongo client termination
	model.Terminate(client)
	fmt.Println("Graceful shutdown")
}
