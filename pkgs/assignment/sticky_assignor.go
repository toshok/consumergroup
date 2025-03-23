package assignment

import "github.com/toshok/consumergroup/pkgs/types"

type StickyAssignor struct{}

func (s *StickyAssignor) Name() string {
	return "sticky"
}

func (s *StickyAssignor) Assign(members []string, partitions []types.PartitionID, prevAssigments map[string][]types.PartitionID) map[string][]types.PartitionID {
	newAssignments := make(map[string][]types.PartitionID)
	used := make(map[types.PartitionID]bool)

	// Try to preserve previous assignments
	for _, member := range members {
		for _, p := range prevAssigments[member] {
			if !used[p] {
				newAssignments[member] = append(newAssignments[member], p)
				used[p] = true
			}
		}
	}

	// Assign remaining partitions in round-robin
	unassigned := []types.PartitionID{}
	for _, p := range partitions {
		if !used[p] {
			unassigned = append(unassigned, p)
		}
	}

	i := 0
	for _, p := range unassigned {
		m := members[i%len(members)]
		newAssignments[m] = append(newAssignments[m], p)
		i++
	}

	return newAssignments
}
