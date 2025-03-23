package assignment

import "github.com/toshok/consumergroup/pkgs/types"

type RoundRobinAssignor struct{}

func (s *RoundRobinAssignor) Name() string {
	return "round-robin"
}

func (s *RoundRobinAssignor) Assign(members []string, partitions []types.PartitionID, prevAssigments map[string][]types.PartitionID) map[string][]types.PartitionID {
	newAssignments := make(map[string][]types.PartitionID)

	i := 0
	for _, p := range partitions {
		m := members[i%len(members)]
		newAssignments[m] = append(newAssignments[m], p)
		i++
	}

	return newAssignments
}
