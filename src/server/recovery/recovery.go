package recovery

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "ds_project/src/server/proto"
	"ds_project/src/server/log"
	"ds_project/src/server/statemachine"
)

type LogRecovery struct {
	pb.UnimplementedLogRecoveryServer
	stateMachine *statemachine.ScooterStateMachine
	log          *log.ReplicatedLog
}

func NewLogRecovery(stateMachine *statemachine.ScooterStateMachine, log *log.ReplicatedLog) *LogRecovery {
	return &LogRecovery{
		stateMachine: stateMachine,
		log:          log,
	}
}

func (r *LogRecovery) GetLog(ctx context.Context, req *pb.GetLogRequest) (*pb.GetLogResponse, error) {
	snapshotData, snapshotIndex := r.stateMachine.GetSnapshot()

	startIndex := req.StartingIndex
	if startIndex < snapshotIndex {
		startIndex = snapshotIndex + 1
	}


	entries := make([]*pb.LogEntry, 0)

	for i := startIndex; i < r.log.GetNextIndex(); i++ {
		entry := r.log.GetEntry(i)
		if entry != nil {
			entries = append(entries, &pb.LogEntry{
				Index:   entry.Index,
				Command: entry.Command,
			})
		}
	}
	return &pb.GetLogResponse{
		LogEntry:    entries,
		CommitIndex: r.log.GetCommitIndex(),
		SnapshotData: snapshotData,
		SnapshotIndex: snapshotIndex,
	}, nil
}

func Recover(servers []string, stateMachine *statemachine.ScooterStateMachine, log *log.ReplicatedLog) error {
	for _, server := range servers {
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()

		client := pb.NewLogRecoveryClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		request := &pb.GetLogRequest{
			StartingIndex: log.GetNextIndex(),
		}
		response, err := client.GetLog(ctx, request)
		if err != nil {
			continue
		}

		// Load snapshot if available and we're behind
		if len(response.SnapshotData) > 0 && response.SnapshotIndex >= log.GetNextIndex() {
			err := stateMachine.LoadSnapshot(response.SnapshotData, response.SnapshotIndex)
			if err != nil {
				continue
			}
			// Update all log indices to reflect snapshot state
			log.SetStoredIndex(response.SnapshotIndex)
			log.SetCommitIndex(response.SnapshotIndex)
			log.SetNextIndex(response.SnapshotIndex + 1)
		}

		// Apply log entries after the snapshot
		for _, entry := range response.LogEntry {
			log.Append(entry.Index, entry.Command)
			stateMachine.Apply(entry.Command)
		}
		log.SetCommitIndex(response.CommitIndex)
		return nil
	}
	return nil
}
