package utils

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A random version 4 UUID, which, unlike version 1, carries no MAC address.
var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestMustNewUUID_IsARandomVersion4UUID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := MustNewUUID()
		assert.Regexp(t, uuidV4, id)
		assert.False(t, seen[id], "duplicate %s", id)
		seen[id] = true
	}
}
