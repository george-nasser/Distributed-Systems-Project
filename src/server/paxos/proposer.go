package paxos

import (
	"sync"
	"context"
	"fmt"
	"time"

	pb "ds_project/src/server/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Proposer struct {
	id		int64
	leader	int64
	round	[]int64
	value	int64
	servers []string
	localAcceptor *Acceptor

	mutex sync.Mutex
}

func NewProposer(id int64, servers []string, localAcceptor *Acceptor) *Proposer{
	return &Proposer{
		id:     id,
		servers: servers,
		round: []int64{0,id},
		localAcceptor: localAcceptor,
	}
}

func (p *Proposer) choose() []int64{
	p.round[0] += 1
	return p.round
}

func (p *Proposer) Propose(value int64, instanceId int64, command []byte) (int64, error){
	finalValue := value 
	p.mutex.Lock()
	round := p.choose()
	p.mutex.Unlock()

	totalAcceptors := len(p.servers) + 1
	majority := totalAcceptors/2 + 1

	promises := make([]*pb.PromiseResponse, 0)

	for _, acceptor := range p.servers {
		conn, err := grpc.Dial(acceptor, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		
		client := pb.NewPaxosClient(conn)
		ctx,cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		response, err := client.Prepare(ctx, &pb.PrepareRequest{
			Round: round,
			InstanceId: instanceId,
		})
		if err != nil {
			continue
		}

		if response.Ack {
			promises = append(promises, response)
		}
	}

	localPromise, _ := p.localAcceptor.Prepare(context.Background(), &pb.PrepareRequest{
		Round: round,
		InstanceId: instanceId,
	})
	if localPromise.Ack {
		promises = append(promises, localPromise)
	}

	if len(promises) < majority {
		return 0, fmt.Errorf("failed to reach majority in prepare phase got %d promises, need %d promises", len(promises), majority)
	}

	highestLastGoodRound := []int64{0,0}
	for _, promise := range promises {
		if promise.LastGoodRound[0] > highestLastGoodRound[0] ||
		   (promise.LastGoodRound[0] == highestLastGoodRound[0] && promise.LastGoodRound[1] > highestLastGoodRound[1]) {
			highestLastGoodRound = promise.LastGoodRound
			finalValue = promise.Value
		}
	}


	acceptedCount := 0
	for _, acceptor := range p.servers {
		conn, err := grpc.Dial(acceptor, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		
		client := pb.NewPaxosClient(conn)
		ctx,cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		response, err := client.Accept(ctx, &pb.AcceptRequest{
			Round: round,
			Value: finalValue,
			InstanceId: instanceId,
		})	
		if err != nil {
			continue
		}
		
		if response.Ack {
			acceptedCount += 1
		}

	}

	localAccept, _ := p.localAcceptor.Accept(context.Background(), &pb.AcceptRequest{
		Round: round,
		Value: finalValue,
		InstanceId: instanceId,
	})
	if localAccept.Ack {
		acceptedCount += 1
	}

	if acceptedCount < majority {
		return 0, fmt.Errorf("failed to reach majority in accept phase got %d accepts, need %d accepts", acceptedCount, majority)
	}

	for _, acceptor := range p.servers {
		go func(acceptor string) {
			conn, err := grpc.Dial(acceptor, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return 
			}
			defer conn.Close()
			
			client := pb.NewPaxosClient(conn)
			ctx,cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, err = client.Commit(ctx, &pb.CommitRequest{
				Value: finalValue,
				InstanceId: instanceId,
				Command: command,
			})
		}(acceptor)
	}

	p.localAcceptor.Commit(context.Background(), &pb.CommitRequest{
		Value: finalValue,
		InstanceId: instanceId,
		Command: command,
	})

	return finalValue, nil












}