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

package interfaces

type (
	// IPreparedCheckpointer represents a checkpoint that has been prepared but not
	// yet committed. This allows decoupling checkpoint preparation from persistence.
	IPreparedCheckpointer interface {
		// GetPendingCheckpoint returns the sequence number for the prepared checkpoint,
		// or nil if no checkpoint is pending.
		GetPendingCheckpoint() *ExtendedSequenceNumber

		// Checkpoint persists the prepared checkpoint, making it durable.
		// Returns an error if the checkpoint store is unavailable, the processor
		// has been shut down, or the sequence number is out of range.
		Checkpoint() error
	}

	// IRecordProcessorCheckpointer allows record processors to persist their progress.
	// The library provides an instance to processors via ProcessRecordsInput and ShutdownInput.
	IRecordProcessorCheckpointer interface {
		// Checkpoint records progress at the provided sequence number. Upon failover,
		// the library will start fetching records after this sequence number.
		// Pass nil to checkpoint at SHARD_END (shard closed, all records delivered).
		//
		// Returns an error if the checkpoint store is unavailable, the processor
		// has been shut down, or the sequence number is out of range.
		Checkpoint(sequenceNumber *string) error

		// PrepareCheckpoint creates a pending checkpoint at the provided sequence number
		// without committing it immediately. Returns an IPreparedCheckpointer that
		// can be committed later via its Checkpoint method.
		PrepareCheckpoint(sequenceNumber *string) (IPreparedCheckpointer, error)
	}
)
