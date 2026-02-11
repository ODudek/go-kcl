/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package worker

import (
	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	kcl "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type (
	// PreparedCheckpointer holds a prepared checkpoint at a specific sequence number.
	// It delegates to an IRecordProcessorCheckpointer for persistence, so checkpoints
	// are subject to the same forward-only validation.
	PreparedCheckpointer struct {
		pendingCheckpointSequenceNumber *kcl.ExtendedSequenceNumber
		checkpointer                    kcl.IRecordProcessorCheckpointer
	}

	// RecordProcessorCheckpointer enables record processors to checkpoint their progress.
	// The library creates one instance per shard assignment and passes it to the
	// IRecordProcessor lifecycle methods.
	RecordProcessorCheckpointer struct {
		shard      *par.ShardStatus
		checkpoint chk.Checkpointer
	}
)

// NewRecordProcessorCheckpoint creates a new checkpointer for the given shard
// that persists progress via the provided Checkpointer backend.
func NewRecordProcessorCheckpoint(shard *par.ShardStatus, checkpoint chk.Checkpointer) kcl.IRecordProcessorCheckpointer {
	return &RecordProcessorCheckpointer{
		shard:      shard,
		checkpoint: checkpoint,
	}
}

func (pc *PreparedCheckpointer) GetPendingCheckpoint() *kcl.ExtendedSequenceNumber {
	return pc.pendingCheckpointSequenceNumber
}

func (pc *PreparedCheckpointer) Checkpoint() error {
	return pc.checkpointer.Checkpoint(pc.pendingCheckpointSequenceNumber.SequenceNumber)
}

func (rc *RecordProcessorCheckpointer) Checkpoint(sequenceNumber *string) error {
	// checkpoint the last sequence of a closed shard
	if sequenceNumber == nil {
		rc.shard.SetCheckpoint(chk.ShardEnd)
	} else {
		rc.shard.SetCheckpoint(aws.ToString(sequenceNumber))
	}

	return rc.checkpoint.CheckpointSequence(rc.shard)
}

func (rc *RecordProcessorCheckpointer) PrepareCheckpoint(_ *string) (kcl.IPreparedCheckpointer, error) {
	return &PreparedCheckpointer{}, nil
}
