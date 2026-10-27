package awsclients

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ODudek/go-kcl/clientlibrary/config"
)

// isolate keeps the developer's AWS environment out of the test.
func isolate(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	t.Setenv("AWS_CONFIG_FILE", missing)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")
}

func newConfig() *config.KinesisClientLibConfiguration {
	return config.NewKinesisClientLibConfigWithCredentials("app", "stream", "eu-central-1", "worker-1",
		credentials.NewStaticCredentialsProvider("kinesis-key", "secret", ""),
		credentials.NewStaticCredentialsProvider("dynamo-key", "secret", ""))
}

func TestNewKinesisClient_UsesTheKCLConfiguration(t *testing.T) {
	isolate(t)
	cfg := newConfig().WithKinesisEndpoint("http://localhost:4566")

	client, err := NewKinesisClient(context.Background(), cfg)

	require.NoError(t, err)
	opts := client.Options()
	assert.Equal(t, "eu-central-1", opts.Region)
	assert.Equal(t, "http://localhost:4566", aws.ToString(opts.BaseEndpoint))
	creds, err := opts.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "kinesis-key", creds.AccessKeyID)
}

func TestNewDynamoDBClient_UsesTheKCLConfiguration(t *testing.T) {
	isolate(t)
	cfg := newConfig().WithDynamoDBEndpoint("http://localhost:8000")

	client, err := NewDynamoDBClient(context.Background(), cfg)

	require.NoError(t, err)
	opts := client.Options()
	assert.Equal(t, "eu-central-1", opts.Region)
	assert.Equal(t, "http://localhost:8000", aws.ToString(opts.BaseEndpoint))
	creds, err := opts.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "dynamo-key", creds.AccessKeyID)
}

func TestNewKinesisClient_WithoutEndpointUsesTheDefaultResolution(t *testing.T) {
	isolate(t)

	client, err := NewKinesisClient(context.Background(), newConfig())

	require.NoError(t, err)
	assert.Nil(t, client.Options().BaseEndpoint)
}
