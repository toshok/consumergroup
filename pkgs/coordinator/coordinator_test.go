package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	pb "github.com/toshok/consumergroup/pkgs/proto_gen"
)

func setupCoordinatorTest() (*CoordinatorServer, context.Context) {
	server, err := NewServer(ServerConfig{PartitionCount: 5})
	if err != nil {
		panic(err)
	}
	return server, context.TODO()
}

func TestJoinGroup_NewLeader(t *testing.T) {
	coordinator, ctx := setupCoordinatorTest()
	resp, err := coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-1"})
	assert.NoError(t, err)
	assert.Equal(t, "worker-1", resp.LeaderId, "First worker should be leader")
}

func TestJoinGroup_MultipleWorkers(t *testing.T) {
	t.Skip("another deadlock.  do we need to mock time and advance it?")
	coordinator, ctx := setupCoordinatorTest()
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-1"})
	resp2, err := coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-2"})
	assert.NoError(t, err)
	assert.NotEqual(t, "worker-2", resp2.LeaderId, "Second worker should not be leader")
}

func TestRebalanceOnWorkerJoin(t *testing.T) {
	t.Skip("another deadlock.  do we need to mock time and advance it?")
	coordinator, ctx := setupCoordinatorTest()
	coordinator.state = StateStable
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-1"})
	initialGen := coordinator.generation
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-2"})
	assert.Greater(t, coordinator.generation, initialGen, "Generation should increment on new worker join")
}

func TestRebalanceOnWorkerLeave(t *testing.T) {
	t.Skip("another deadlock.  do we need to mock time and advance it?")
	coordinator, ctx := setupCoordinatorTest()
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-1"})
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-2"})
	initialGen := coordinator.generation
	_, _ = coordinator.LeaveGroup(ctx, &pb.LeaveGroupRequest{MemberId: "worker-1"})
	assert.Greater(t, coordinator.generation, initialGen, "Generation should increment when a worker leaves")
}

func TestWorkerHeartbeats(t *testing.T) {
	coordinator, ctx := setupCoordinatorTest()
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-1"})
	_, err := coordinator.Heartbeat(ctx, &pb.HeartbeatRequest{MemberId: "worker-1", Generation: int32(coordinator.generation)})
	assert.NoError(t, err, "Heartbeat should succeed for active worker")
}

func TestWorkerRejoinOnHeartbeatFailure(t *testing.T) {
	coordinator, ctx := setupCoordinatorTest()
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-1"})
	initialGen := coordinator.generation
	coordinator.generation++ // Simulate rebalance event

	_, err := coordinator.Heartbeat(ctx, &pb.HeartbeatRequest{MemberId: "worker-1", Generation: int32(initialGen)})
	assert.Error(t, err, "Worker should be forced to rejoin after rebalance")
}

func TestLeaderReassignsOnWorkerExit(t *testing.T) {
	t.Skip("This test hangs things.  we need a simulated clock to make it work")
	coordinator, ctx := setupCoordinatorTest()
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-1"})
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-2"})
	assert.Equal(t, coordinator.leaderID, "worker-1", "Worker-1 should be leader")

	_, _ = coordinator.LeaveGroup(ctx, &pb.LeaveGroupRequest{MemberId: "worker-1"})
	assert.NotEqual(t, coordinator.leaderID, "worker-1", "Leader should be reassigned when the leader leaves")
	assert.Equal(t, coordinator.leaderID, "worker-2", "Worker-2 should be the new leader")
}

func TestPruneDeadMemberTriggersRebalance(t *testing.T) {
	t.Skip("This test hangs things.  we need a simulated clock to make it work")
	coordinator, ctx := setupCoordinatorTest()
	_, _ = coordinator.JoinGroup(ctx, &pb.JoinGroupRequest{MemberId: "worker-1"})
	coordinator.members["worker-1"].lastHeartbeat = time.Now().Add(-time.Hour) // Simulate dead
	initialGen := coordinator.generation
	coordinator.pruneDeadMembers(1 * time.Second) // One-shot call for test
	assert.Greater(t, coordinator.generation, initialGen, "Generation should increment when dead member is pruned")
}
