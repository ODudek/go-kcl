package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// mockRedisClient implements RedisClient for unit testing.
type mockRedisClient struct {
	data     map[string]map[string]string // hash key -> field -> value
	sets     map[string]map[string]bool   // set key -> members
	pingErr  error
	hsetErr  error
	hdelErr  error
	delErr   error
	saddErr  error
	sremErr  error
	closeErr error
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{
		data: make(map[string]map[string]string),
		sets: make(map[string]map[string]bool),
	}
}

func (m *mockRedisClient) Ping(_ context.Context) *goredis.StatusCmd {
	cmd := goredis.NewStatusCmd(context.Background())
	if m.pingErr != nil {
		cmd.SetErr(m.pingErr)
	} else {
		cmd.SetVal("PONG")
	}
	return cmd
}

func (m *mockRedisClient) HGetAll(_ context.Context, key string) *goredis.MapStringStringCmd {
	cmd := goredis.NewMapStringStringCmd(context.Background())
	if hash, ok := m.data[key]; ok {
		result := make(map[string]string, len(hash))
		for k, v := range hash {
			result[k] = v
		}
		cmd.SetVal(result)
	} else {
		cmd.SetVal(map[string]string{})
	}
	return cmd
}

func (m *mockRedisClient) HSet(_ context.Context, key string, values ...interface{}) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(context.Background())
	if m.hsetErr != nil {
		cmd.SetErr(m.hsetErr)
		return cmd
	}
	if m.data[key] == nil {
		m.data[key] = make(map[string]string)
	}
	for i := 0; i < len(values)-1; i += 2 {
		field, _ := values[i].(string)
		val, _ := values[i+1].(string)
		m.data[key][field] = val
	}
	cmd.SetVal(int64(len(values) / 2))
	return cmd
}

func (m *mockRedisClient) HDel(_ context.Context, key string, fields ...string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(context.Background())
	if m.hdelErr != nil {
		cmd.SetErr(m.hdelErr)
		return cmd
	}
	var deleted int64
	if hash, ok := m.data[key]; ok {
		for _, f := range fields {
			if _, exists := hash[f]; exists {
				delete(hash, f)
				deleted++
			}
		}
	}
	cmd.SetVal(deleted)
	return cmd
}

func (m *mockRedisClient) Del(_ context.Context, keys ...string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(context.Background())
	if m.delErr != nil {
		cmd.SetErr(m.delErr)
		return cmd
	}
	var deleted int64
	for _, k := range keys {
		if _, ok := m.data[k]; ok {
			delete(m.data, k)
			deleted++
		}
	}
	cmd.SetVal(deleted)
	return cmd
}

func (m *mockRedisClient) SAdd(_ context.Context, key string, members ...interface{}) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(context.Background())
	if m.saddErr != nil {
		cmd.SetErr(m.saddErr)
		return cmd
	}
	if m.sets[key] == nil {
		m.sets[key] = make(map[string]bool)
	}
	var added int64
	for _, member := range members {
		s, _ := member.(string)
		if !m.sets[key][s] {
			m.sets[key][s] = true
			added++
		}
	}
	cmd.SetVal(added)
	return cmd
}

func (m *mockRedisClient) SRem(_ context.Context, key string, members ...interface{}) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(context.Background())
	if m.sremErr != nil {
		cmd.SetErr(m.sremErr)
		return cmd
	}
	var removed int64
	if set, ok := m.sets[key]; ok {
		for _, member := range members {
			s, _ := member.(string)
			if set[s] {
				delete(set, s)
				removed++
			}
		}
	}
	cmd.SetVal(removed)
	return cmd
}

func (m *mockRedisClient) SMembers(_ context.Context, key string) *goredis.StringSliceCmd {
	cmd := goredis.NewStringSliceCmd(context.Background())
	if set, ok := m.sets[key]; ok {
		members := make([]string, 0, len(set))
		for s := range set {
			members = append(members, s)
		}
		cmd.SetVal(members)
	} else {
		cmd.SetVal([]string{})
	}
	return cmd
}

func (m *mockRedisClient) Close() error {
	return m.closeErr
}

// mockScripter implements Scripter for Lua script testing.
type mockScripter struct {
	results map[string]interface{} // script SHA -> result
	errors  map[string]error       // script SHA -> error
}

func newMockScripter() *mockScripter {
	return &mockScripter{
		results: make(map[string]interface{}),
		errors:  make(map[string]error),
	}
}

func (s *mockScripter) setResult(script *goredis.Script, result interface{}, err error) {
	hash := script.Hash()
	s.results[hash] = result
	s.errors[hash] = err
}

// Eval and EvalSha are called internally by redis.Script.Run().
// We look up the result by the SHA hash.

func (s *mockScripter) Eval(_ context.Context, script string, keys []string, args ...interface{}) *goredis.Cmd {
	cmd := goredis.NewCmd(context.Background())
	// Not used directly — redis.Script calls EvalSha first, then Eval on NOSCRIPT
	cmd.SetVal("OK")
	return cmd
}

func (s *mockScripter) EvalSha(_ context.Context, sha1 string, keys []string, args ...interface{}) *goredis.Cmd {
	cmd := goredis.NewCmd(context.Background())
	if err, ok := s.errors[sha1]; ok && err != nil {
		cmd.SetErr(err)
		return cmd
	}
	if result, ok := s.results[sha1]; ok {
		cmd.SetVal(result)
	} else {
		cmd.SetVal("OK")
	}
	return cmd
}

func (s *mockScripter) EvalRO(_ context.Context, _ string, _ []string, _ ...interface{}) *goredis.Cmd {
	return goredis.NewCmd(context.Background())
}

func (s *mockScripter) EvalShaRO(_ context.Context, _ string, _ []string, _ ...interface{}) *goredis.Cmd {
	return goredis.NewCmd(context.Background())
}

func (s *mockScripter) ScriptExists(_ context.Context, _ ...string) *goredis.BoolSliceCmd {
	cmd := goredis.NewBoolSliceCmd(context.Background())
	cmd.SetVal([]bool{true})
	return cmd
}

func (s *mockScripter) ScriptLoad(_ context.Context, _ string) *goredis.StringCmd {
	cmd := goredis.NewStringCmd(context.Background())
	cmd.SetVal("OK")
	return cmd
}
