package worker

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/toshok/consumergroup/pkgs/assignment"
	pb "github.com/toshok/consumergroup/pkgs/proto_gen"
	"github.com/toshok/consumergroup/pkgs/types"
)

// const heartbeatInterval = 10 * time.Second
const heartbeatInterval = 1 * time.Second

type AssignedPartitionsFunc func(assignedPartitions []types.PartitionID)

type Worker struct {
	id                string // our id
	leaderId          string // our leader's id (will be == id for the leader)
	conn              *grpc.ClientConn
	client            pb.ConsumerCoordinatorClient
	partitionsChanged AssignedPartitionsFunc

	// guards a bunch of fields in this struct
	mu sync.Mutex

	// these are just used on the leader
	generation      int32
	assignor        assignment.Strategy
	partitions      []types.PartitionID
	prevAssignments map[string][]types.PartitionID
	logger          zerolog.Logger
}

func New(coordinatorAddress string, partitionsChanged AssignedPartitionsFunc) (*Worker, error) {
	conn, err := grpc.Dial(coordinatorAddress, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	return &Worker{
		id:                uuid.New().String(),
		logger:            log.With().Str("worker_id", id).Logger(),
		conn:              conn,
		client:            pb.NewConsumerCoordinatorClient(conn),
		partitionsChanged: partitionsChanged,
		prevAssignments:   make(map[string][]types.PartitionID),
	}, nil
}

func (w *Worker) joinGroup() error {
	w.logger.Debug().Msg("Joining group")
	resp, err := w.client.JoinGroup(context.TODO(), &pb.JoinGroupRequest{
		MemberId: w.id,
	})
	if err != nil {
		return err
	}

	w.mu.Lock()
	w.leaderId = resp.LeaderId

	if w.id == w.leaderId {
		w.generation = resp.Generation
		w.partitions = []types.PartitionID{}
		for _, p := range resp.Partitions {
			w.partitions = append(w.partitions, types.PartitionID(p))
		}

		assignor, err := assignment.NewStrategy(resp.AssignmentStrategy)
		if err != nil {
			w.mu.Unlock()
			return err
		}

		w.assignor = assignor
	}
	w.mu.Unlock()

	return w.syncGroup(resp.Members, resp.Generation)
}

func (w *Worker) syncGroup(members []string, generation int32) error {
	w.logger.Debug().Msg("Syncing group")
	w.mu.Lock()
	w.generation = generation
	isLeader := w.id == w.leaderId
	w.mu.Unlock()

	var pbAssignments map[string]*pb.Assignment
	if isLeader {
		// w.logger.Debug().Msg("I am the leader, calculating assignments")
		// w.logger.Debug().Msgf("Members: %v", members)
		// w.logger.Debug().Msgf("Partitions: %v", w.partitions)
		// w.logger.Debug().Msgf("PrevAssignments: %v", w.prevAssignments)

		assignments := w.assignor.Assign(members, w.partitions, w.prevAssignments)
		w.logger.Debug().Msgf("new assignments: %v", assignments)

		pbAssignments = make(map[string]*pb.Assignment)
		for member, parts := range assignments {
			partInt32s := []int32{}
			for _, p := range parts {
				partInt32s = append(partInt32s, int32(p))
			}
			pbAssignments[member] = &pb.Assignment{Partitions: partInt32s}
		}

		w.prevAssignments = assignments
	}

	w.logger.Debug().Msg("calling SyncGroup")
	syncResp, err := w.client.SyncGroup(context.TODO(), &pb.SyncGroupRequest{
		MemberId:    w.id,
		Assignments: pbAssignments,
	})
	if err != nil {
		return err
	}
	w.logger.Debug().Msg("done calling SyncGroup")

	w.mu.Lock()
	partIDs := []types.PartitionID{}
	for _, p := range syncResp.Assignment.Partitions {
		partIDs = append(partIDs, types.PartitionID(p))
	}
	w.mu.Unlock()

	w.partitionsChanged(partIDs)

	return nil
}

func (w *Worker) sendHeartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		_, err := w.client.Heartbeat(context.TODO(), &pb.HeartbeatRequest{
			MemberId:   w.id,
			Generation: int32(w.generation),
		})

		if err != nil {
			w.logger.Warn().Msg("heartbeat failed")

			// If rebalance required, rejoin
			if grpc.Code(err) == codes.FailedPrecondition {
				w.logger.Debug().Msg("Detected rebalance, rejoining group")
				if err := w.joinGroup(); err != nil {
					w.logger.Fatal().Err(err).Msg("Failed to rejoin group")
				}
			}
		}
	}
}

func (w *Worker) Start() {
	w.logger.Debug().Msg("Started running")
	if err := w.joinGroup(); err != nil {
		w.logger.Fatal().Err(err).Msg("Failed to join group")
	}

	go w.sendHeartbeat()
}

func (w *Worker) Stop() {
	w.logger.Debug().Msg("Stopping")
	w.conn.Close()
}
