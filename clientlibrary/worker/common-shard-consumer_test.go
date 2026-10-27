package worker

import (
	"crypto/md5" //nolint:gosec // the KPL checksum
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ODudek/go-kcl/clientlibrary/config"
	kcl "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	"github.com/ODudek/go-kcl/clientlibrary/metrics"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
	"github.com/ODudek/go-kcl/internal/kpl"
)

type recordingProcessor struct {
	batches [][]types.Record
}

func (p *recordingProcessor) Initialize(*kcl.InitializationInput) {}
func (p *recordingProcessor) Shutdown(*kcl.ShutdownInput)         {}
func (p *recordingProcessor) ProcessRecords(in *kcl.ProcessRecordsInput) error {
	p.batches = append(p.batches, in.Records)
	return nil
}

// Issue #12: one malformed KPL aggregated record made the whole batch vanish
// (ProcessRecords was not called, and the next checkpoint skipped past it),
// or panicked the consumer. Every record now reaches the processor.
func TestProcessRecords_MalformedAggregatedRecordDoesNotDropTheBatch(t *testing.T) {
	// Valid magic and digest, but the partition key index points past the
	// empty partition key table: the old deaggregator panicked on it.
	message := []byte{0x1a, 0x05, 0x08, 0x07, 0x1a, 0x01, 'x'}
	digest := md5.Sum(message) //nolint:gosec // the KPL checksum
	malformed := append(append([]byte{0xF3, 0x89, 0x9A, 0xC2}, message...), digest[:]...)

	records := []types.Record{
		{SequenceNumber: aws.String("1"), Data: kpl.Aggregate([]types.Record{
			{PartitionKey: aws.String("a"), Data: []byte("a1")},
			{PartitionKey: aws.String("b"), Data: []byte("b1")},
		})},
		{SequenceNumber: aws.String("2"), Data: malformed},
		{SequenceNumber: aws.String("3"), PartitionKey: aws.String("c"), Data: []byte("plain")},
	}
	processor := &recordingProcessor{}
	sc := &commonShardConsumer{
		shard:           &par.ShardStatus{ID: "s1", Mux: &sync.RWMutex{}},
		recordProcessor: processor,
		kclConfig:       config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1"),
		mService:        metrics.NoopMonitoringService{},
	}

	require.NotPanics(t, func() {
		require.NoError(t, sc.processRecords(time.Now(), records, aws.Int64(0), nil))
	})

	require.Len(t, processor.batches, 1)
	var data []string
	for _, r := range processor.batches[0] {
		data = append(data, string(r.Data))
	}
	assert.Equal(t, []string{"a1", "b1", string(malformed), "plain"}, data)
}
