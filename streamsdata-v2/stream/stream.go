package stream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	typeStreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

type StreamSubscriber struct {
	dynamoSvc         *dynamodb.Client
	streamSvc         *dynamodbstreams.Client
	table             *string
	ShardIteratorType typeStreams.ShardIteratorType
	Limit             *int32
}

func NewStreamSubscriber(
	dynamoSvc *dynamodb.Client,
	streamSvc *dynamodbstreams.Client,
	table string) *StreamSubscriber {
	s := &StreamSubscriber{dynamoSvc: dynamoSvc, streamSvc: streamSvc, table: &table}
	s.applyDefaults()
	return s
}

func (r *StreamSubscriber) applyDefaults() {
	r.ShardIteratorType = typeStreams.ShardIteratorTypeLatest
}

func (r *StreamSubscriber) SetLimit(v int32) {
	r.Limit = aws.Int32(v)
}

func (r *StreamSubscriber) SetShardIteratorType(s typeStreams.ShardIteratorType) {
	r.ShardIteratorType = s
}

func (r *StreamSubscriber) GetStreamDataAsync() (<-chan *typeStreams.Record, <-chan error) {

	ch := make(chan *typeStreams.Record, 1)
	errCh := make(chan error, 1)

	needUpdateChannel := make(chan struct{}, 1)
	needUpdateChannel <- struct{}{}

	allShards := make(map[string]struct{})
	shardProcessingLimit := 4
	shardsCh := make(chan *dynamodbstreams.GetShardIteratorInput, shardProcessingLimit)
	lock := sync.Mutex{}

	go func() {
		tick := time.NewTicker(time.Minute)
		fmt.Println("creat tick: ", tick)
		for {
			select {
			case <-tick.C:
				fmt.Println("new time: ", tick.C)
				needUpdateChannel <- struct{}{}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-needUpdateChannel:
				streamArn, err := r.getLatestStreamArn()
				if err != nil {
					errCh <- err
					return
				}
				ids, err := r.getShardIds(streamArn)
				fmt.Println("num ids shards:", len(ids))
				if err != nil {
					errCh <- err
					return
				}
				for _, sObj := range ids {
					lock.Lock()
					if _, ok := allShards[*sObj.ShardId]; !ok {
						fmt.Println(*sObj.ShardId)
						allShards[*sObj.ShardId] = struct{}{}
						shardsCh <- &dynamodbstreams.GetShardIteratorInput{
							StreamArn:         streamArn,
							ShardId:           sObj.ShardId,
							ShardIteratorType: r.ShardIteratorType,
						}
					}
					lock.Unlock()
				}

			}
		}

	}()

	limit := make(chan struct{}, shardProcessingLimit)

	go func() {
		time.Sleep(time.Second * 5)
		log.Println("Starting read shards...")
		for shardInput := range shardsCh {
			log.Println("process shard: ", *shardInput.ShardId)
			limit <- struct{}{}
			go func(sInput *dynamodbstreams.GetShardIteratorInput) {
				err := r.processShard(sInput, ch)
				if err != nil {
					errCh <- err
				}
				// TODO: think about cleaning list of shards: delete(allShards, *sInput.ShardId)
				<-limit
			}(shardInput)
		}
	}()
	return ch, errCh
}

// Obtener los shardIds asociados a un streamArn
func (r *StreamSubscriber) getShardIds(streamArn *string) (ids []typeStreams.Shard, err error) {
	des, err := r.streamSvc.DescribeStream(context.TODO(), &dynamodbstreams.DescribeStreamInput{
		StreamArn: streamArn,
	})
	if err != nil {
		return nil, err
	}
	// No shards
	if 0 == len(des.StreamDescription.Shards) {
		return nil, nil
	}

	return des.StreamDescription.Shards, nil
}

// obtiene el nombdre del ultimo StreamArn
func (r *StreamSubscriber) getLatestStreamArn() (*string, error) {
	tableInfo, err := r.dynamoSvc.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{TableName: r.table})
	if err != nil {
		return nil, err
	}
	if nil == tableInfo.Table.LatestStreamArn {
		return nil, errors.New("empty table stream arn")
	}
	return tableInfo.Table.LatestStreamArn, nil
}

func (r *StreamSubscriber) processShard(input *dynamodbstreams.GetShardIteratorInput, ch chan<- *typeStreams.Record) error {
	iter, err := r.streamSvc.GetShardIterator(context.TODO(), input)
	if err != nil {
		log.Println("[GetShardIterator] finished process shard: ", *input.ShardId)
		return err
	}
	if iter.ShardIterator == nil {
		log.Println("[ShardIterator nil] finished process shard: ", *input.ShardId)
		return nil
	}

	nextIterator := iter.ShardIterator

	for nextIterator != nil {
		recs, err := r.streamSvc.GetRecords(context.TODO(), &dynamodbstreams.GetRecordsInput{
			ShardIterator: nextIterator,
			Limit:         r.Limit,
		})
		if awsErr, ok := err.(*typeStreams.TrimmedDataAccessException); ok && awsErr.ErrorCode() == "TrimmedDataAccessException" {
			//Trying to request data older than 24h, that's ok
			//http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetShardIterator.html -> Errors
			log.Println("[GetRecords trim] finished process shard: ", *input.ShardId)
			return nil
		}
		if err != nil {
			log.Println("[GetRecords] finished process shard: ", *input.ShardId)
			return err
		}

		if len(recs.Records) > 0 {
			log.Printf("nro. records %v ", len(recs.Records))
		}
		for _, record := range recs.Records {
			log.Printf("send record... ")
			ch <- &record
		}

		nextIterator = recs.NextShardIterator

		sleepDuration := time.Second

		// Nil next itarator, shard is closed
		if nextIterator == nil {
			sleepDuration = time.Millisecond * 10
			log.Println("sleep duration: ", sleepDuration.String(), *input.ShardId)
		} else if len(recs.Records) == 0 {
			// Empty set, but shard is not closed -> sleep a little
			sleepDuration = time.Second * 10
		}

		time.Sleep(sleepDuration)
	}
	log.Println("[nextIterator nil] finished process shard: ", *input.ShardId)
	return nil
}
