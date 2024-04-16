package consumer

import (
	"context"
	"fmt"
	"sync"

	"Go_Project/model"

	"github.com/IBM/sarama"
)

var AuthCh = make(chan *sarama.ConsumerMessage)

type ConsumerHandler struct{}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("setup process")
	return nil
}

func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("cleanup process")
	return nil
}

// ConsumeClaim function
// consumes data and print that
// put data in authentication channel for further process
func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("consumer claim process")
	model.Session = session
	for message := range claim.Messages() {

		fmt.Printf("Offset %d, Partition %d - Received message: %s\n", message.Offset, message.Partition, message.Value)

		select {
		case <-session.Context().Done():
			fmt.Println("Consumer Exiting..")
			return nil
		case AuthCh <- message:
		}

	}

	return nil
}

// consumer function
// initiates kafka consumer with sarama
// consumer starts consuming data from kafka
func Consumer(ctx context.Context, Broker string, Topic string, GroupID string, authCh chan *sarama.ConsumerMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(AuthCh)

	//kafka consumer intialization
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup([]string{Broker}, GroupID, config)
	if err != nil {
		fmt.Printf("Error creating consumer group: %v\n", err)
		return
	}
	defer func() {
		if err := consumerGroup.Close(); err != nil {
			fmt.Printf("Error closing consumer group: %v\n", err)
		}
	}()

	var wgi sync.WaitGroup
	wgi.Add(1)

	AuthCh = authCh
	handler := &ConsumerHandler{}
	// consumer starts consuming data
	go func() {
		defer wgi.Done()

		for {
			select {
			case <-ctx.Done():
				fmt.Println("consumer GoRoutine shuting down..")
				return
			default:
				err := consumerGroup.Consume(ctx, []string{Topic}, handler)
				if err != nil {
					fmt.Printf("Error consuming: %v\n", err)
					return
				}
			}
		}
	}()

	wgi.Wait()
}
