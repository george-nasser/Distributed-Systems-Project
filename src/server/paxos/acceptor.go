package paxos

import (
	"sync"
	"context"

	pb "ds_project/src/server/proto"

	"ds_project/src/server/log"
	"ds_project/src/server/statemachine"
)

type AcceptorInstance struct {
	lastRound     []int64
	lastGoodRound []int64
	v_i 		  int64
	decided		  bool
	decidedValue  int64
}
type Acceptor struct {
	pb.UnimplementedPaxosServer

	instance map[int64]*AcceptorInstance
	
	mutex sync.Mutex

	stateMachine *statemachine.ScooterStateMachine
	log          *log.ReplicatedLog

}
	
func NewAcceptor(stateMachine *statemachine.ScooterStateMachine, log *log.ReplicatedLog) *Acceptor {
	return &Acceptor{
		instance: make(map[int64]*AcceptorInstance),
		stateMachine: stateMachine,
		log:          log,
	}
}	

func (a *Acceptor) getInstance(instanceId int64) *AcceptorInstance {
	if _, exists := a.instance[instanceId]; !exists {
		a.instance[instanceId] = &AcceptorInstance{
			lastRound:     []int64{0, 0},
			lastGoodRound: []int64{0, 0},
			v_i:           0,
			decided:       false,
			decidedValue:  0,
		}
	}
	return a.instance[instanceId]
}

func (a *Acceptor) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	
	a.mutex.Lock()
	defer a.mutex.Unlock()

	instance := a.getInstance(req.InstanceId)

	if req.Round[0] > instance.lastRound[0] || (req.Round[0] == instance.lastRound[0] && req.Round[1] > instance.lastRound[1]) {
		instance.lastRound = req.Round
		return &pb.PromiseResponse{
			Round:  req.Round,
			Ack:          true,
			LastGoodRound:  instance.lastGoodRound,
			Value:        instance.v_i,
			InstanceId: req.InstanceId,
		}, nil
	}

	return &pb.PromiseResponse{
		    Round:  req.Round,
			Ack:          false,
			LastGoodRound:  instance.lastGoodRound,
			Value:        instance.v_i,
			InstanceId: req.InstanceId,
	}, nil
}

func (a *Acceptor) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	instance := a.getInstance(req.InstanceId)

	if req.Round[0] > instance.lastRound[0] ||
	  (req.Round[0] == instance.lastRound[0] && req.Round[1] >= instance.lastRound[1]) ||
	  (instance.lastRound[0] == 0 && instance.lastRound[1] == 0) {
		instance.lastRound = req.Round
		instance.lastGoodRound = req.Round
		instance.v_i = req.Value

		return &pb.AcceptedResponse{
			Round: req.Round,
			Ack:       true,
		}, nil
	}
	
	return &pb.AcceptedResponse{
		Round: req.Round,
		Ack:       false,
	}, nil
}

func (a *Acceptor) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {	
	a.mutex.Lock()
	defer a.mutex.Unlock()

	instance := a.getInstance(req.InstanceId)

	if !instance.decided {
		instance.decided = true
		instance.decidedValue = req.Value

		if req.Command != nil && len(req.Command) > 0 {
			a.log.Append(req.InstanceId, req.Command)
			a.stateMachine.Apply(req.Command)
		}
	}

	return &pb.CommitResponse{
	}, nil
}