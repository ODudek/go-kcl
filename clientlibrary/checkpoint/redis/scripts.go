package redis

// scriptGetLease atomically acquires or renews a lease on a shard.
//
// KEYS[1] = shard hash key (e.g. kcl:{table}:shard:{shardID})
// KEYS[2] = shard registry set key (e.g. kcl:{table}:shards)
//
// ARGV[1] = newAssignTo        (workerID requesting the lease)
// ARGV[2] = newLeaseTimeout    (RFC3339Nano timestamp)
// ARGV[3] = shardID
// ARGV[4] = nowUTC             (RFC3339Nano timestamp for comparison)
// ARGV[5] = enableStealing     ("1" or "0")
// ARGV[6] = isClaimExpired     ("1" or "0")
// ARGV[7] = checkpoint         (sequence number, may be "")
// ARGV[8] = parentShardId      (may be "")
const scriptGetLeaseSrc = `
local data = redis.call('HGETALL', KEYS[1])
local current = {}
for i = 1, #data, 2 do
  current[data[i]] = data[i + 1]
end

local newAssignTo     = ARGV[1]
local newLeaseTimeout = ARGV[2]
local shardID         = ARGV[3]
local nowUTC          = ARGV[4]
local enableStealing  = ARGV[5] == "1"
local isClaimExpired  = ARGV[6] == "1"
local checkpoint      = ARGV[7]
local parentShardId   = ARGV[8]

-- Check lease stealing claim
if enableStealing and current['ClaimRequest'] and current['ClaimRequest'] ~= '' then
  if newAssignTo ~= current['ClaimRequest'] and not isClaimExpired then
    return 'SHARD_CLAIMED'
  end
end

-- Check if lease is held by someone else and not expired
local assignedTo = current['AssignedTo']
local leaseTimeout = current['LeaseTimeout']

if assignedTo and assignedTo ~= '' and leaseTimeout and leaseTimeout ~= '' then
  if assignedTo ~= newAssignTo then
    if enableStealing then
      if nowUTC < leaseTimeout and not isClaimExpired then
        return 'LEASE_NOT_ACQUIRED:current lease timeout not yet expired'
      end
    else
      if nowUTC < leaseTimeout then
        return 'LEASE_NOT_ACQUIRED:current lease timeout not yet expired'
      end
    end
  end
end

-- If stealing with an active matching claim, use conditional write
if enableStealing and current['ClaimRequest'] and current['ClaimRequest'] ~= '' then
  if current['ClaimRequest'] == newAssignTo and not isClaimExpired then
    -- Verify current fields match for conditional update
    if assignedTo and assignedTo ~= '' then
      -- Conditional: current values must match
    end
  end
end

-- Write the lease
redis.call('HSET', KEYS[1],
  'ShardID', shardID,
  'AssignedTo', newAssignTo,
  'LeaseTimeout', newLeaseTimeout)

if checkpoint ~= '' then
  redis.call('HSET', KEYS[1], 'Checkpoint', checkpoint)
end

if parentShardId ~= '' then
  redis.call('HSET', KEYS[1], 'ParentShardId', parentShardId)
end

-- Clear claim request after successful lease acquisition
redis.call('HDEL', KEYS[1], 'ClaimRequest')

-- Register shard in the set
redis.call('SADD', KEYS[2], shardID)

return 'OK'
`

// scriptClaimShard atomically places a steal claim on a shard.
//
// KEYS[1] = shard hash key
//
// ARGV[1] = shardID
// ARGV[2] = claimID             (workerID placing the claim)
// ARGV[3] = expectedLeaseTimeout
// ARGV[4] = expectedOwner       (may be "")
// ARGV[5] = expectedCheckpoint  (may be "")
// ARGV[6] = expectedParent      (may be "")
const scriptClaimShardSrc = `
local data = redis.call('HGETALL', KEYS[1])
local current = {}
for i = 1, #data, 2 do
  current[data[i]] = data[i + 1]
end

local shardID              = ARGV[1]
local claimID              = ARGV[2]
local expectedLeaseTimeout = ARGV[3]
local expectedOwner        = ARGV[4]
local expectedCheckpoint   = ARGV[5]
local expectedParent       = ARGV[6]

-- Reject if there is already a claim
if current['ClaimRequest'] and current['ClaimRequest'] ~= '' then
  return 'CONDITIONAL_CHECK_FAILED:claim already exists'
end

-- Verify lease timeout matches
if current['LeaseTimeout'] ~= expectedLeaseTimeout then
  return 'CONDITIONAL_CHECK_FAILED:lease timeout mismatch'
end

-- Verify owner matches
if expectedOwner == '' then
  if current['AssignedTo'] and current['AssignedTo'] ~= '' then
    return 'CONDITIONAL_CHECK_FAILED:owner mismatch'
  end
else
  if current['AssignedTo'] ~= expectedOwner then
    return 'CONDITIONAL_CHECK_FAILED:owner mismatch'
  end
end

-- Skip SHARD_END shards
if current['Checkpoint'] == 'SHARD_END' then
  return 'CONDITIONAL_CHECK_FAILED:shard is at SHARD_END'
end

-- Verify checkpoint matches
if expectedCheckpoint == '' then
  if current['Checkpoint'] and current['Checkpoint'] ~= '' then
    return 'CONDITIONAL_CHECK_FAILED:checkpoint mismatch'
  end
else
  if current['Checkpoint'] ~= expectedCheckpoint then
    return 'CONDITIONAL_CHECK_FAILED:checkpoint mismatch'
  end
end

-- Verify parent shard matches
if expectedParent == '' then
  if current['ParentShardId'] and current['ParentShardId'] ~= '' then
    return 'CONDITIONAL_CHECK_FAILED:parent shard mismatch'
  end
else
  if current['ParentShardId'] ~= expectedParent then
    return 'CONDITIONAL_CHECK_FAILED:parent shard mismatch'
  end
end

-- Set the claim
redis.call('HSET', KEYS[1], 'ClaimRequest', claimID)

return 'OK'
`

// scriptRemoveLeaseOwner conditionally removes the lease owner if it matches expected.
//
// KEYS[1] = shard hash key
//
// ARGV[1] = expectedAssignedTo (workerID that should own the lease)
const scriptRemoveLeaseOwnerSrc = `
local assignedTo = redis.call('HGET', KEYS[1], 'AssignedTo')

if assignedTo ~= ARGV[1] then
  return 'CONDITIONAL_CHECK_FAILED:owner mismatch'
end

redis.call('HDEL', KEYS[1], 'AssignedTo')

return 'OK'
`
