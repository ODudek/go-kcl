package kpl

import (
	"crypto/md5" //nolint:gosec // the KPL checksum
	"encoding/hex"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// goldenAggregated was encoded by github.com/awslabs/kinesis-aggregation with
// partition keys [pk-a pk-b], an explicit hash key table, a tagged first
// record, and the records first@pk-a, second@pk-b, third@pk-a.
const goldenAggregated = "f3899ac20a04706b2d610a04706b2d6212033132331a13080010001a05666972737422060a016b1201761a0a08011a067365636f6e641a0908001a057468697264cb43bb26e62959455ab611604d90bb3c"

func kinesisRecord(seq string, data []byte) types.Record {
	arrival := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	return types.Record{
		SequenceNumber:              aws.String(seq),
		PartitionKey:                aws.String("outer"),
		ApproximateArrivalTimestamp: &arrival,
		EncryptionType:              types.EncryptionTypeKms,
		Data:                        data,
	}
}

func user(key, data string) types.Record {
	return types.Record{PartitionKey: aws.String(key), Data: []byte(data)}
}

// withChecksum wraps message as an aggregated record with a valid digest.
func withChecksum(message []byte) []byte {
	digest := md5.Sum(message) //nolint:gosec // the KPL checksum
	out := append(append([]byte{}, magic...), message...)
	return append(out, digest[:]...)
}

type summary struct{ seq, key, data string }

func summarize(records []types.Record) []summary {
	out := make([]summary, 0, len(records))
	for _, r := range records {
		out = append(out, summary{aws.ToString(r.SequenceNumber), aws.ToString(r.PartitionKey), string(r.Data)})
	}
	return out
}

func TestDeaggregate_ReadsTheKPLFormat(t *testing.T) {
	data, err := hex.DecodeString(goldenAggregated)
	require.NoError(t, err)
	parent := kinesisRecord("seq-1", data)

	got, err := Deaggregate([]types.Record{parent})

	require.NoError(t, err)
	assert.Equal(t, []summary{{"seq-1", "pk-a", "first"}, {"seq-1", "pk-b", "second"}, {"seq-1", "pk-a", "third"}}, summarize(got))
	for _, r := range got {
		assert.Equal(t, parent.ApproximateArrivalTimestamp, r.ApproximateArrivalTimestamp)
		assert.Equal(t, parent.EncryptionType, r.EncryptionType)
	}
}

func TestAggregate_RoundTrips(t *testing.T) {
	users := []types.Record{user("a", "1"), user("b", "2"), user("a", ""), user("c", "4")}

	got, err := Deaggregate([]types.Record{kinesisRecord("seq-9", Aggregate(users))})

	require.NoError(t, err)
	assert.Equal(t, []summary{{"seq-9", "a", "1"}, {"seq-9", "b", "2"}, {"seq-9", "a", ""}, {"seq-9", "c", "4"}}, summarize(got))
}

func TestDeaggregate_PassesNonAggregatedRecordsThrough(t *testing.T) {
	valid := Aggregate([]types.Record{user("k", "v")})
	badDigest := append([]byte{}, valid...)
	badDigest[len(badDigest)-1] ^= 0xFF

	for name, data := range map[string][]byte{
		"empty":         nil,
		"plain":         []byte(`{"id":1}`),
		"magic only":    magic,
		"too short":     append(append([]byte{}, magic...), make([]byte, digestSize)...),
		"bad digest":    badDigest,
		"magic missing": valid[1:],
	} {
		t.Run(name, func(t *testing.T) {
			record := kinesisRecord("seq-1", data)

			got, err := Deaggregate([]types.Record{record})

			require.NoError(t, err)
			assert.Equal(t, []types.Record{record}, got)
		})
	}
}

// Issue #12: a malformed aggregated record made the whole batch disappear
// (DeaggregateRecords returned nil), or panicked the consumer on a partition
// key index out of range. It is now passed on unchanged and reported, and the
// other records of the batch are deaggregated as usual.
func TestDeaggregate_PassesMalformedAggregatedRecordsOnUnchanged(t *testing.T) {
	record := func(fields ...[]byte) []byte {
		var b []byte
		for _, f := range fields {
			b = append(b, f...)
		}
		return b
	}
	keyTable := protowire.AppendString(protowire.AppendTag(nil, fieldPartitionKeyTable, protowire.BytesType), "k")
	userWith := func(fields ...[]byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, fieldRecords, protowire.BytesType), record(fields...))
	}
	index := func(i uint64) []byte {
		return protowire.AppendVarint(protowire.AppendTag(nil, fieldPartitionKeyIndex, protowire.VarintType), i)
	}
	data := protowire.AppendString(protowire.AppendTag(nil, fieldData, protowire.BytesType), "d")

	for name, message := range map[string][]byte{
		"truncated message":     {0x0a, 0x10, 'k'},
		"invalid tag":           {0x00},
		"index out of range":    record(keyTable, userWith(index(1), data)),
		"huge index":            record(keyTable, userWith(index(1<<63), data)),
		"no partition key":      record(userWith(index(0), data)),
		"index missing":         record(keyTable, userWith(data)),
		"data missing":          record(keyTable, userWith(index(0))),
		"truncated user record": record(keyTable, protowire.AppendTag(nil, fieldRecords, protowire.BytesType), []byte{0x05, 0x08}),
	} {
		t.Run(name, func(t *testing.T) {
			before := kinesisRecord("seq-1", Aggregate([]types.Record{user("a", "1"), user("b", "2")}))
			malformed := kinesisRecord("seq-2", withChecksum(message))
			plain := kinesisRecord("seq-3", []byte("plain"))

			var got []types.Record
			var err error
			require.NotPanics(t, func() { got, err = Deaggregate([]types.Record{before, malformed, plain}) })

			require.Error(t, err)
			assert.Contains(t, err.Error(), "KPL aggregated record seq-2 is malformed")
			require.Len(t, got, 4)
			assert.Equal(t, []summary{{"seq-1", "a", "1"}, {"seq-1", "b", "2"}}, summarize(got[:2]))
			assert.Equal(t, malformed, got[2])
			assert.Equal(t, plain, got[3])
		})
	}
}

// The partition key table may follow the records that index into it.
func TestDeaggregate_AcceptsFieldsInAnyOrder(t *testing.T) {
	var user []byte
	user = protowire.AppendTag(user, fieldData, protowire.BytesType)
	user = protowire.AppendString(user, "d")
	user = protowire.AppendTag(user, fieldPartitionKeyIndex, protowire.VarintType)
	user = protowire.AppendVarint(user, 0)
	var message []byte
	message = protowire.AppendTag(message, fieldRecords, protowire.BytesType)
	message = protowire.AppendBytes(message, user)
	message = protowire.AppendTag(message, fieldPartitionKeyTable, protowire.BytesType)
	message = protowire.AppendString(message, "k")

	got, err := Deaggregate([]types.Record{kinesisRecord("seq-1", withChecksum(message))})

	require.NoError(t, err)
	assert.Equal(t, []summary{{"seq-1", "k", "d"}}, summarize(got))
}

func TestDeaggregate_ReportsEveryMalformedRecord(t *testing.T) {
	bad := withChecksum([]byte{0x00})

	got, err := Deaggregate([]types.Record{kinesisRecord("seq-1", bad), kinesisRecord("seq-2", bad)})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "seq-1")
	assert.Contains(t, err.Error(), "seq-2")
	assert.Len(t, got, 2)
}

func FuzzDeaggregate(f *testing.F) {
	golden, _ := hex.DecodeString(goldenAggregated)
	f.Add(golden)
	f.Add([]byte("plain"))
	f.Fuzz(func(t *testing.T, message []byte) {
		got, err := Deaggregate([]types.Record{kinesisRecord("seq", withChecksum(message))})
		if err != nil {
			require.Len(t, got, 1, "a malformed record is passed on alone")
		}
	})
}
