package assignment

import "github.com/toshok/consumergroup/pkgs/types"

type Strategy interface {
	Name() string
	Assign(members []string, partitions []types.PartitionID, prev map[string][]types.PartitionID) map[string][]types.PartitionID
}
