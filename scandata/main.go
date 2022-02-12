package main

// snippet-start:[dynamodb.go.scan_items.imports]
import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"fmt"
	"log"
)

// snippet-end:[dynamodb.go.scan_items.imports]

// snippet-start:[dynamodb.go.scan_items.struct]
// Create struct to hold info about new item
type Order struct {
	ClientId    string
	State       string
	TotalAmount int
}

// snippet-end:[dynamodb.go.scan_items.struct]

// Get the movies with a minimum rating of 8.0 in 2011
func main() {
	// snippet-start:[dynamodb.go.scan_items.session]
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)
	// snippet-end:[dynamodb.go.scan_items.session]

	// snippet-start:[dynamodb.go.scan_items.vars]
	tableName := "Orders"
	state := "PENDIENTE"
	// snippet-end:[dynamodb.go.scan_items.vars]

	// snippet-start:[dynamodb.go.scan_items.expr]
	// Create the Expression to fill the input struct with.
	// Get all movies in that year; we'll pull out those with a higher rating later
	filt := expression.Name("State").Equal(expression.Value(state))

	// Or we could get by ratings and pull out those with the right year later
	//    filt := expression.Name("info.rating").GreaterThan(expression.Value(min_rating))

	// Get back the title, year, and rating
	//proj := expression.NamesList(expression.Name("ClientId"), expression.Name("State"), expression.Name("TotalAmount"))

	expr, err := expression.NewBuilder().WithFilter(filt).Build()
	if err != nil {
		log.Fatalf("Got error building expression: %s", err)
	}
	// snippet-end:[dynamodb.go.scan_items.expr]

	// snippet-start:[dynamodb.go.scan_items.call]
	// Build the query input parameters
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		//ProjectionExpression:      expr.Projection(),
		TableName: aws.String(tableName),
	}

	// Make the DynamoDB Query API call
	result, err := svc.Scan(params)
	if err != nil {
		log.Fatalf("Query API call failed: %s", err)
	}
	// snippet-end:[dynamodb.go.scan_items.call]

	// snippet-start:[dynamodb.go.scan_items.process]
	numItems := 0

	for _, i := range result.Items {
		item := Order{}

		err = dynamodbattribute.UnmarshalMap(i, &item)

		if err != nil {
			log.Fatalf("Got error unmarshalling: %s", err)
		}

		// Which ones had a higher rating than minimum?
		//if item.Rating > minRating {
		// Or it we had filtered by rating previously:
		//   if item.Year == year {
		numItems++

		fmt.Println("State: ", item.State)
		fmt.Println("ClientId: ", item.ClientId)
		fmt.Println("TotalAmount:", item.TotalAmount)
		fmt.Println()
		//}
	}
	// snippet-end:[dynamodb.go.scan_items.process]
}
