package validate

import (
	"Go_Project/model"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	"github.com/IBM/sarama"
)

// ValidateUser
// checks if the user data is valid
// put valid data into dataChannel for database storing purposes
func ValidateUser(ctx context.Context, authCh chan *sarama.ConsumerMessage, dataCh chan *sarama.ConsumerMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("validation GoRoutine shuting down ...")
			return
		case message := <-authCh:
			// Data to be validated
			var user model.User
			if err := json.Unmarshal(message.Value, &user); err != nil {
				fmt.Println("Error unmarshaling JSON:", err)
				continue
			}

			// Check if any required fields are empty
			if user.Name == "" {
				fmt.Println("name is required ")
				continue
			}
			if user.Age <= 0 {
				fmt.Println("age must be greater than 0 ")
				continue
			}
			if user.Email == "" {
				fmt.Println("email is required ")
				continue
			}
			if user.City == "" {
				fmt.Println("city is required ")
				continue
			}
			if user.Mobile == "" {
				fmt.Println("mobile number is required ")
				continue
			}

			// Validate email format using regex
			emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
			if !emailRegex.MatchString(user.Email) {
				fmt.Println("invalid email format ")
				continue
			}

			// Validate mobile number format using regex
			mobileRegex := regexp.MustCompile(`^\+\d{1,3} \(\d{3}\) \d{3}-\d{4}$`)
			if !mobileRegex.MatchString(user.Mobile) {
				fmt.Println("invalid mobile number format ")
				continue
			}
			//put data into dataChannel
			select {
			case <-ctx.Done():
				fmt.Println("Validation Incomplete Exiting..")
				return
			default:
				dataCh <- message
			}

		}
	}
}
