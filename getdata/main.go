package main

// snippet-start:[dynamodb.go.read_item.imports]
import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"fmt"
	"log"
)

// snippet-end:[dynamodb.go.read_item.imports]

// snippet-start:[dynamodb.go.read_item.struct]
// Create struct to hold info about new item
type Order struct {
	ClientId    string
	State       string
	TotalAmount int
}

// snippet-end:[dynamodb.go.read_item.struct]

func main() {
	// snippet-start:[dynamodb.go.read_item.session]
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)
	// snippet-end:[dynamodb.go.read_item.session]

	// snippet-start:[dynamodb.go.read_item.call]
	tableName := "Orders"
	pk := "order#2"
	sk := "order"

	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {
				S: aws.String(pk),
			},
			"SK": {
				S: aws.String(sk),
			},
		},
	})
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}
	// snippet-end:[dynamodb.go.read_item.call]

	// snippet-start:[dynamodb.go.read_item.unmarshall]
	if result.Item == nil {
		msg := "Could not find '" + *&pk + "'"
		panic(fmt.Sprintf("%v", errors.New(msg)))
	}

	item := Order{}

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	fmt.Println("Found item:")
	fmt.Println("ClientId:  ", item.ClientId)
	fmt.Println("TotalAmount: ", item.TotalAmount)
	fmt.Println("State:  ", item.State)
	// snippet-end:[dynamodb.go.read_item.unmarshall]

}
