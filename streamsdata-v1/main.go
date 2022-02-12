package main

import (
	"fmt"
	"log"

	"example/streamsdata-v1/stream"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

func main() {
	cfg := aws.NewConfig().WithRegion("us-east-2")
	sess := session.New()
	streamSvc := dynamodbstreams.New(sess, cfg)
	dynamoSvc := dynamodb.New(sess, cfg)
	table := "Orders"

	streamSubscriber := stream.NewStreamSubscriber(dynamoSvc, streamSvc, table)
	ch, errCh := streamSubscriber.GetStreamDataAsync()

	go func(errCh <-chan error) {
		for err := range errCh {
			log.Println("Stream Subscriber error: ", err)
		}
	}(errCh)

	for record := range ch {
		fmt.Println("from channel:", record)
	}
}
