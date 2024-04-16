// model.go

package model

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type User struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Age    int    `json:"age"`
	Email  string `json:"email"`
	City   string `json:"city"`
	Mobile string `json:"mobile"`
}

var collection *mongo.Collection
var Session sarama.ConsumerGroupSession

// Initialize function takes context and session string
// create and connect to mongodb client
// Access desired Database and collection
// return connected client and error if any
func Initialize(ctx context.Context, connectionString string) (*mongo.Client, error) {
	// Create a MongoDB client
	client, err := mongo.NewClient(options.Client().ApplyURI(connectionString))
	if err != nil {
		return nil, fmt.Errorf("Error creating Mongo Client: %w", err)
	}

	// Connect to MongoDB
	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("Mongo client connection failed: %w", err)
	}

	fmt.Println("Mongo Client connected")

	// Access the desired collection
	collection = client.Database("userDB").Collection("user")
	fmt.Println("MongoDB collection created")

	return client, nil
}

// Terminate function
// takes MongoDB client check against nil reference
// Disconnect client
func Terminate(client *mongo.Client) {
	if client != nil {
		client.Disconnect(context.Background())
		fmt.Println("Mongo Client Disconnected...")
	} else {
		fmt.Println("Mongo Client was not properly initialized")
	}
}

// start function
// takes context , client and dataChannel
// reads from the data channel and store new user into database
// mark session for every processed user query
func Start(ctx context.Context, client *mongo.Client, dataCh chan *sarama.ConsumerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	// Ensure client is not nil before proceeding
	if client == nil {
		fmt.Println("Mongo Client is nil, cannot proceed...")
		return
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("model GoRoutine shutting down...")
			return
		case message := <-dataCh:
			var user User
			if err := json.Unmarshal(message.Value, &user); err != nil {
				fmt.Println("Error unmarshaling JSON:", err)
				continue
			}

			// Check if email already exists in the database
			var existingUser User
			err := collection.FindOne(ctx, bson.M{"email": user.Email}).Decode(&existingUser)
			if err == nil {
				fmt.Println("email already registered")
				// Session.MarkMessage(message, "") // Uncomment if Session is defined
				continue
			} else if err != mongo.ErrNoDocuments {
				fmt.Println(err)
				continue
			}

			// Check if mobile number already exists in the database
			err = collection.FindOne(ctx, bson.M{"mobile": user.Mobile}).Decode(&existingUser)
			if err == nil {
				fmt.Println("mobile number already registered")
				// Session.MarkMessage(message, "") // Uncomment if Session is defined
				continue
			} else if err != mongo.ErrNoDocuments {
				fmt.Println(err)
				continue
			}

			// Insert user into the database
			_, err = collection.InsertOne(ctx, user)
			if err != nil {
				fmt.Println("Error inserting Data:", err)
				continue
			}
			fmt.Println("Data insertion successful userId:", user.ID)
			Session.MarkMessage(message, "")
		}
	}
}
