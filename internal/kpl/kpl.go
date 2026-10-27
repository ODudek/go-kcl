// Package kpl deaggregates Kinesis records written by the Kinesis Producer
// Library (KPL) in its aggregated format, and aggregates records into it.
//
// An aggregated record is the 4-byte magic F3 89 9A C2, an AggregatedRecord
// protobuf message, and the 16-byte MD5 digest of that message:
//
//	message AggregatedRecord {
//	  repeated string partition_key_table     = 1;
//	  repeated string explicit_hash_key_table = 2;
//	  repeated Record records                 = 3;
//	}
//	message Record {
//	  required uint64 partition_key_index     = 1;
//	  optional uint64 explicit_hash_key_index = 2;
//	  required bytes  data                    = 3;
//	  repeated Tag    tags                    = 4;
//	}
//
// See https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
package kpl

import (
	"bytes"
	"crypto/md5" //nolint:gosec // the KPL format uses MD5 as a checksum, not for security
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/encoding/protowire"
)

// magic starts every KPL aggregated record.
var magic = []byte{0xF3, 0x89, 0x9A, 0xC2}

const digestSize = md5.Size

// Field numbers of the AggregatedRecord and Record messages. Explicit hash
// keys and tags are skipped: a kinesis types.Record has no field for them.
const (
	fieldPartitionKeyTable protowire.Number = 1
	fieldRecords           protowire.Number = 3

	fieldPartitionKeyIndex protowire.Number = 1
	fieldData              protowire.Number = 3
)

// Deaggregate expands every KPL aggregated record in records into the user
// records it holds, in order; any other record is returned unchanged. A user
// record keeps the sequence number, arrival time and encryption type of the
// Kinesis record that carried it, and its Data shares that record's memory.
//
// A record with the KPL magic and a valid digest whose message is malformed
// (it does not parse, misses a required field, or names a partition key that
// is not in its table) is returned unchanged too, so no record is ever
// dropped; the returned error, joined over such records, says which ones.
func Deaggregate(records []types.Record) ([]types.Record, error) {
	out := make([]types.Record, 0, len(records))
	var errs []error
	for _, record := range records {
		message, ok := aggregatedMessage(record.Data)
		if !ok {
			out = append(out, record)
			continue
		}
		users, err := userRecords(message, record)
		if err != nil {
			errs = append(errs, fmt.Errorf("KPL aggregated record %s is malformed, passed on unchanged: %w",
				aws.ToString(record.SequenceNumber), err))
			out = append(out, record)
			continue
		}
		out = append(out, users...)
	}
	return out, errors.Join(errs...)
}

// aggregatedMessage returns the AggregatedRecord message of data, and false
// when data is not a KPL aggregated record: too short, without the magic, or
// with a digest that does not match.
func aggregatedMessage(data []byte) ([]byte, bool) {
	if len(data) <= len(magic)+digestSize || !bytes.HasPrefix(data, magic) {
		return nil, false
	}
	message := data[len(magic) : len(data)-digestSize]
	digest := md5.Sum(message) //nolint:gosec // checksum, see import
	if !bytes.Equal(digest[:], data[len(data)-digestSize:]) {
		return nil, false
	}
	return message, true
}

// userRecord is one Record message of an AggregatedRecord.
type userRecord struct {
	partitionKeyIndex uint64
	data              []byte
}

// userRecords parses an AggregatedRecord message into the user records it
// holds, carried by parent. Fields may come in any order, so the partition
// key indexes are checked once the whole message is read.
func userRecords(message []byte, parent types.Record) ([]types.Record, error) {
	var partitionKeys []string
	var records []userRecord
	for len(message) > 0 {
		num, typ, n := protowire.ConsumeTag(message)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		message = message[n:]
		switch {
		case num == fieldPartitionKeyTable && typ == protowire.BytesType:
			var key []byte
			key, n = protowire.ConsumeBytes(message)
			partitionKeys = append(partitionKeys, string(key))
		case num == fieldRecords && typ == protowire.BytesType:
			var raw []byte
			raw, n = protowire.ConsumeBytes(message)
			if n >= 0 {
				r, err := parseUserRecord(raw)
				if err != nil {
					return nil, fmt.Errorf("record %d: %w", len(records), err)
				}
				records = append(records, r)
			}
		default:
			n = protowire.ConsumeFieldValue(num, typ, message)
		}
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		message = message[n:]
	}

	out := make([]types.Record, 0, len(records))
	for i, r := range records {
		if r.partitionKeyIndex >= uint64(len(partitionKeys)) {
			return nil, fmt.Errorf("record %d: partition key index %d out of range, table has %d keys",
				i, r.partitionKeyIndex, len(partitionKeys))
		}
		out = append(out, types.Record{
			ApproximateArrivalTimestamp: parent.ApproximateArrivalTimestamp,
			Data:                        r.data,
			EncryptionType:              parent.EncryptionType,
			PartitionKey:                aws.String(partitionKeys[r.partitionKeyIndex]),
			SequenceNumber:              parent.SequenceNumber,
		})
	}
	return out, nil
}

// parseUserRecord parses a Record message; both required fields must be set.
func parseUserRecord(message []byte) (userRecord, error) {
	var r userRecord
	var hasIndex, hasData bool
	for len(message) > 0 {
		num, typ, n := protowire.ConsumeTag(message)
		if n < 0 {
			return r, protowire.ParseError(n)
		}
		message = message[n:]
		switch {
		case num == fieldPartitionKeyIndex && typ == protowire.VarintType:
			r.partitionKeyIndex, n = protowire.ConsumeVarint(message)
			hasIndex = true
		case num == fieldData && typ == protowire.BytesType:
			r.data, n = protowire.ConsumeBytes(message)
			hasData = true
		default:
			n = protowire.ConsumeFieldValue(num, typ, message)
		}
		if n < 0 {
			return r, protowire.ParseError(n)
		}
		message = message[n:]
	}
	if !hasIndex {
		return r, errors.New("partition_key_index not set")
	}
	if !hasData {
		return r, errors.New("data not set")
	}
	return r, nil
}

// Aggregate encodes records into one KPL aggregated record, with a partition
// key table of their distinct partition keys.
func Aggregate(records []types.Record) []byte {
	var message []byte
	keyIndex := map[string]uint64{}
	var keys []string
	for _, r := range records {
		key := aws.ToString(r.PartitionKey)
		if _, ok := keyIndex[key]; !ok {
			keyIndex[key] = uint64(len(keys))
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		message = protowire.AppendTag(message, fieldPartitionKeyTable, protowire.BytesType)
		message = protowire.AppendString(message, key)
	}
	for _, r := range records {
		var record []byte
		record = protowire.AppendTag(record, fieldPartitionKeyIndex, protowire.VarintType)
		record = protowire.AppendVarint(record, keyIndex[aws.ToString(r.PartitionKey)])
		record = protowire.AppendTag(record, fieldData, protowire.BytesType)
		record = protowire.AppendBytes(record, r.Data)
		message = protowire.AppendTag(message, fieldRecords, protowire.BytesType)
		message = protowire.AppendBytes(message, record)
	}

	digest := md5.Sum(message) //nolint:gosec // checksum, see import
	out := make([]byte, 0, len(magic)+len(message)+digestSize)
	out = append(out, magic...)
	out = append(out, message...)
	return append(out, digest[:]...)
}
