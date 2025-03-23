package assignment

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/toshok/consumergroup/pkgs/types"
)

func TestStickyAssignor(t *testing.T) {
	assignor := &StickyAssignor{}
	prev := map[string][]types.PartitionID{
		"a": {1, 2},
		"b": {3},
	}
	members := []string{"a", "b"}
	partitions := []types.PartitionID{1, 2, 3}

	result := assignor.Assign(members, partitions, prev)
	assert.Len(t, result["a"], 2)
	assert.Len(t, result["b"], 1)
	assert.ElementsMatch(t, append(result["a"], result["b"]...), partitions)
}
