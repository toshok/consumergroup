package coordinator

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	pb "github.com/toshok/consumergroup/pkgs/proto_gen"
)

const listenAddressDefault = ":50051"
const assignmentStrategyDefault = "round-robin"
const rebalanceTimeoutDefault = 5 * time.Second
const pruneIntervalDefault = 30 * time.Second

type GroupState int

const (
	StateEmpty GroupState = iota
	StatePreparingRebalance
	StateCompletingRebalance
	StateStable
)

type memberInfo struct {
	lastHeartbeat time.Time
	assignment    *pb.Assignment
	syncReady     chan *pb.Assignment
	logger        zerolog.Logger
}

func (m *memberInfo) updateHeartbeat() {
	m.lastHeartbeat = time.Now()
}

type CoordinatorServer struct {
	pb.UnimplementedConsumerCoordinatorServer

	config     ServerConfig
	mu         sync.Mutex
	state      GroupState
	members    map[string]*memberInfo
	leaderID   string
	generation int

	// for rebalancing
	expectedMembers int
	rebalanceTimer  *time.Timer
	joinResponses   map[string]chan *pb.JoinGroupResponse
}

type ServerConfig struct {
	ListenAddress      string
	PartitionCount     int32
	AssignmentStrategy string
	RebalanceTimeout   time.Duration
	PruneInterval      time.Duration
}

func NewServer(config ServerConfig) (*CoordinatorServer, error) {
	if config.PartitionCount == 0 {
		return nil, fmt.Errorf("PartitionCount must be > 0")
	}
	if config.ListenAddress == "" {
		config.ListenAddress = listenAddressDefault
	}
	if config.AssignmentStrategy == "" {
		config.AssignmentStrategy = assignmentStrategyDefault
	}
	if config.RebalanceTimeout == 0 {
		config.RebalanceTimeout = rebalanceTimeoutDefault
	}
	if config.PruneInterval == 0 {
		config.PruneInterval = pruneIntervalDefault
	}

	rv := &CoordinatorServer{
		config:        config,
		members:       make(map[string]*memberInfo),
		joinResponses: make(map[string]chan *pb.JoinGroupResponse),
		state:         StateEmpty,
	}

	return rv, nil
}

func (s *CoordinatorServer) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	s.mu.Lock()

	logger := log.With().Str("worker_id", req.MemberId).Logger()

	logger.Debug().Int("generation", s.generation).Msg("JoinGroupRequest received")

	m, present := s.members[req.MemberId]
	if present {
		m.updateHeartbeat()
	} else {
		m = &memberInfo{
			lastHeartbeat: time.Now(),
			syncReady:     make(chan *pb.Assignment, 1),
			logger:        logger,
		}
		s.members[req.MemberId] = m
	}

	respCh := make(chan *pb.JoinGroupResponse, 1)
	s.joinResponses[req.MemberId] = respCh

	if s.state == StateStable || s.state == StateEmpty {
		s.beginRebalance()
	}

	if len(s.joinResponses) == s.expectedMembers {
		s.completeRebalance()
	}

	s.mu.Unlock()

	select {
	case resp := <-respCh:
		logger.Debug().Msg("Returning JoinGroupResponse")
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *CoordinatorServer) SyncGroup(ctx context.Context, req *pb.SyncGroupRequest) (*pb.SyncGroupResponse, error) {
	s.mu.Lock()
	/*
		if req.Generation < int32(s.generation) {
			s.mu.Unlock()
			// return nil, status.Errorf(codes.FailedPrecondition, "Rebalance required")
			panic("Rebalance required in SyncGroup")
		}
	*/

	m := s.members[req.MemberId]
	if m == nil {
		s.mu.Unlock()
		return nil, status.Error(codes.NotFound, "Member not in group")
	}

	m.logger.Debug().Msg("SyncGroupRequest received")

	// Leader does the assignments, so update everything and let the workers know
	if req.MemberId == s.leaderID {
		s.state = StateStable
		log.Debug().Msgf("Leader supplied assignments: %v", req.Assignments)
		for memberID, assignment := range req.Assignments {
			if m, ok := s.members[memberID]; ok {
				m.logger.Debug().Str("worker_id", memberID).Any("partitions", assignment.Partitions).Msg("Assigning partitions")
				m.assignment = assignment

				// don't block the leader goroutine that's filling things out
				if memberID == s.leaderID {
					continue
				}

				// for non-leaders, push the assignment to the worker's channel
				select {
				case m.syncReady <- assignment:
					m.logger.Debug().Str("worker_id", memberID).Msg("Pushed assignment")
				default:
					m.logger.Debug().Str("worker_id", memberID).Msg("Failed to push assignment")
				}
			}
		}

		// mark our state as Stable and give the leader back its assignment
		assignment := s.members[req.MemberId].assignment
		s.mu.Unlock()
		return &pb.SyncGroupResponse{
			Assignment: assignment,
			// chatgpt keeps adding lines without changing data structures...
			// Generation: int32(s.generation),
		}, nil
	}

	// Other workers wait for their assignment
	ch := s.members[req.MemberId].syncReady
	s.mu.Unlock()

	select {
	case assignment := <-ch:
		m.logger.Debug().Msg("returning SyncGroupResponse")
		return &pb.SyncGroupResponse{
			Assignment: assignment,
			// chatgpt keeps adding lines without changing data structures...
			// Generation: int32(s.generation),
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *CoordinatorServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, present := s.members[req.MemberId]
	if !present {
		return nil, status.Errorf(codes.NotFound, "Member not in group")
	}

	if req.Generation < int32(s.generation) {
		return nil, status.Errorf(codes.FailedPrecondition, "Rebalance required")
	}

	m.updateHeartbeat()
	return &pb.HeartbeatResponse{Success: true}, nil
}

func (s *CoordinatorServer) LeaveGroup(ctx context.Context, req *pb.LeaveGroupRequest) (*pb.LeaveGroupResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, present := s.members[req.MemberId]; !present {
		return nil, status.Errorf(codes.NotFound, "Member not in group")
	}

	delete(s.members, req.MemberId)
	if s.state == StateStable {
		s.beginRebalance()
	}

	if req.MemberId == s.leaderID {
		s.leaderID = ""
		for id := range s.members {
			s.leaderID = id
			break
		}
	}

	// XXX
	if len(s.members) == 0 {
		s.state = StateEmpty
	}

	return &pb.LeaveGroupResponse{Success: true}, nil
}

func (s *CoordinatorServer) pruneDeadMembers(timeout time.Duration) {
	ticker := time.NewTicker(timeout / 2)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		deletedAny := false
		for id, info := range s.members {
			if now.Sub(info.lastHeartbeat) > timeout {
				log.Debug().Str("worker_id", id).Dur("time_since_last_heartbeat", now.Sub(info.lastHeartbeat)).Msg("Pruning dead member")
				delete(s.members, id)
				deletedAny = true
				if id == s.leaderID {
					s.leaderID = ""
					for id := range s.members {
						s.leaderID = id
						break
					}
				}
			}
		}
		if len(s.members) == 0 {
			s.state = StateEmpty
		} else if deletedAny {
			if s.state == StateStable {
				s.beginRebalance()
			}
		}

		s.mu.Unlock()
	}
}

func (s *CoordinatorServer) beginRebalance() {
	s.state = StatePreparingRebalance
	s.generation++
	s.expectedMembers = len(s.members)

	log.Debug().Int("generation", s.generation).Int("expected_members", s.expectedMembers).Msg("STATE: PreparingRebalance")

	if s.rebalanceTimer != nil {
		s.rebalanceTimer.Stop()
	}
	s.rebalanceTimer = time.AfterFunc(s.config.RebalanceTimeout, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.completeRebalance()
	})
}

func (s *CoordinatorServer) completeRebalance() {
	if s.state != StatePreparingRebalance {
		return // XXX(toshok) this feels like it should be a panic, but the rest of the code currently relies on it
	}
	log.Debug().Msg("STATE: CompletingRebalance")
	s.state = StateCompletingRebalance

	memberIDs := make([]string, 0, len(s.members))
	for id := range s.members {
		memberIDs = append(memberIDs, id)
	}

	var partitions []int32
	for i := int32(0); i < s.config.PartitionCount; i++ {
		partitions = append(partitions, i)
	}

	leaderID := ""
	for id := range s.members {
		leaderID = id
		break
	}
	s.leaderID = leaderID

	resp := &pb.JoinGroupResponse{
		LeaderId:           leaderID,
		Members:            memberIDs,
		Partitions:         partitions,
		Generation:         int32(s.generation),
		AssignmentStrategy: s.config.AssignmentStrategy,
	}

	for _, ch := range s.joinResponses {
		select {
		case ch <- resp:
		default:
		}
	}

	s.joinResponses = make(map[string]chan *pb.JoinGroupResponse)
}

func (s *CoordinatorServer) Serve() error {
	lis, err := net.Listen("tcp", s.config.ListenAddress)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}

	grpcServer := grpc.NewServer()

	go s.pruneDeadMembers(30 * time.Second)
	pb.RegisterConsumerCoordinatorServer(grpcServer, s)

	log.Info().Str("listen_address", s.config.ListenAddress).Msg("Coordinator listening")
	return grpcServer.Serve(lis)
}
