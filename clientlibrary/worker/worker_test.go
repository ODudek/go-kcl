package worker

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"

	"github.com/ODudek/go-kcl/clientlibrary/config"
)

func newTestConfig() *config.KinesisClientLibConfiguration {
	return config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1")
}

// The worker no longer builds AWS clients or a DynamoDB checkpointer itself:
// a missing one is reported by Start instead of failing later or exiting.
func TestStart_RequiresAKinesisClient(t *testing.T) {
	w := NewWorker(nil, newTestConfig()).WithCheckpointer(&fakeCheckpointer{})

	assert.ErrorIs(t, w.Start(), ErrNoKinesisClient)
}

func TestStart_RequiresACheckpointer(t *testing.T) {
	w := NewWorker(nil, newTestConfig()).WithKinesis(kinesis.New(kinesis.Options{Region: "us-east-1"}))

	assert.ErrorIs(t, w.Start(), ErrNoCheckpointer)
}
