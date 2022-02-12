package main

import (
	"context"
	"example/streamsdata-v2/stream"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodbstreams/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
)

type Item struct {
	ClientId    string
	PK          string
	SK          string
	State       string
	TotalAmount int32
}

func main() {
	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-2"),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Using the Config value, create the DynamoDB client
	dynamoSvc := dynamodb.NewFromConfig(cfg)
	streamSvc := dynamodbstreams.NewFromConfig(cfg)
	table := "Orders"

	streamSubscriber := stream.NewStreamSubscriber(dynamoSvc, streamSvc, table)
	ch, errCh := streamSubscriber.GetStreamDataAsync()

	go func(errCh <-chan error) {
		for err := range errCh {
			log.Println("Stream Subscriber error: ", err)
		}
	}(errCh)

	for record := range ch {
		fmt.Println("from channel:", &record, record.Dynamodb.SequenceNumber)
		item := Item{}
		attributevalue.UnmarshalMap(record.Dynamodb.NewImage, &item)
		log.Println("record:: ", item)
	}
}
