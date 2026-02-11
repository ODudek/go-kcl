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
	// IRecordProcessor defines the interface for processing records from a Kinesis shard.
	//
	// Applications using the Kinesis Client Library must implement this interface.
	// The lifecycle is: Initialize -> (ProcessRecords)* -> Shutdown.
	// This mirrors the Amazon KCL v2 Java IRecordProcessor interface.
	IRecordProcessor interface {
		// Initialize is called once when the record processor is first assigned to a shard.
		// Use this to initialize any resources needed for processing.
		Initialize(initializationInput *InitializationInput)

		// ProcessRecords processes a batch of data records from the shard.
		// Upon failover, the new instance will receive records with sequence numbers
		// greater than the last checkpointed position.
		ProcessRecords(processRecordsInput *ProcessRecordsInput)

		// Shutdown is called when the record processor is no longer needed.
		// When ShutdownInput.ShutdownReason is TERMINATE, you MUST checkpoint before returning.
		// When it is ZOMBIE or REQUESTED, do NOT checkpoint as another processor may have
		// already started processing records for this shard.
		Shutdown(shutdownInput *ShutdownInput)
	}

	// IRecordProcessorFactory creates IRecordProcessor instances.
	// The library calls CreateProcessor for each shard assignment. Implementations
	// can create a new processor per shard or reuse instances (if thread-safe).
	IRecordProcessorFactory interface {
		// CreateProcessor returns a new IRecordProcessor for a shard.
		CreateProcessor() IRecordProcessor
	}
)
