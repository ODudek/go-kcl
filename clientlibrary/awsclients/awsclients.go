// Package awsclients builds the AWS service clients a Worker needs from the
// KCL configuration: region, credentials and endpoint override, on top of the
// AWS SDK's default configuration chain (environment, shared config files,
// SSO, web identity, EC2/ECS roles, ...).
//
// It is optional. Importing it links github.com/aws/aws-sdk-go-v2/config and
// its credential providers; an application that builds its own clients and
// passes them to Worker.WithKinesis and DynamoCheckpoint.WithDynamoDB does
// not need it.
package awsclients

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/ODudek/go-kcl/clientlibrary/config"
)

// NewKinesisClient returns a Kinesis client for cfg.RegionName, signed with
// cfg.KinesisCredentials (the default chain when nil), at cfg.KinesisEndpoint
// when set.
func NewKinesisClient(ctx context.Context, cfg *config.KinesisClientLibConfiguration) (*kinesis.Client, error) {
	awsCfg, err := load(ctx, cfg.RegionName, cfg.KinesisCredentials)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config for Kinesis: %w", err)
	}
	return kinesis.NewFromConfig(awsCfg, func(o *kinesis.Options) {
		if cfg.KinesisEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.KinesisEndpoint)
		}
	}), nil
}

// NewDynamoDBClient returns a DynamoDB client for cfg.RegionName, signed with
// cfg.DynamoDBCredentials (the default chain when nil), at
// cfg.DynamoDBEndpoint when set.
func NewDynamoDBClient(ctx context.Context, cfg *config.KinesisClientLibConfiguration) (*dynamodb.Client, error) {
	awsCfg, err := load(ctx, cfg.RegionName, cfg.DynamoDBCredentials)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config for DynamoDB: %w", err)
	}
	return dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.DynamoDBEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.DynamoDBEndpoint)
		}
	}), nil
}

// load reads the default AWS configuration for region, with the standard
// retryer capped at the SDK's default maximum backoff, as the KCL used before.
func load(ctx context.Context, region string, creds aws.CredentialsProvider) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.AddWithMaxBackoffDelay(retry.NewStandard(), retry.DefaultMaxBackoff)
		}),
	}
	if creds != nil {
		opts = append(opts, awsconfig.WithCredentialsProvider(creds))
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}
