package membership

import (
	"fmt"
	"sync"
	"context"
	"time"
	"sort"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Member struct {
	ID  int64
	Address string
}

type Membership struct {
	client *clientv3.Client
	leaseID clientv3.LeaseID
	id   int64
	address string

	members map[int64]Member
	currentLeaderID int64

	onLeaderChange func(leaderID int64)

	mutex sync.RWMutex
}


func NewMembership(id int64, address string, endpoints []string) (*Membership, error) {

	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	membership := &Membership{
		client: client,
		id: id,
		address: address,
		members: make(map[int64]Member),
	}

	return membership, nil
}

func (m *Membership) OnLeaderChange(callback func(leaderID int64)) {
	m.onLeaderChange = callback
}

func (m *Membership) Start(ctx context.Context) error {

	lease,err := m.client.Grant(ctx, 5)
	if err != nil {
		return err
	}
	m.leaseID = lease.ID

	_, err = m.client.Put(ctx, fmt.Sprintf("members/%d", m.id), m.address, clientv3.WithLease(m.leaseID))
	if err != nil {
		return err
	}

	ch, err := m.client.KeepAlive(ctx, m.leaseID)
	if err != nil {
		return err
	}
	
	go func() {
		for range ch {
		}
	}()

	return nil

}

func (m *Membership) Stop()	{
	m.client.Close()
}

func (n *Membership) electLeader()  {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if len(n.members) == 0 {
		return
	}

	memberIDs := make([]int64, 0, len(n.members))
	for id := range n.members {
		memberIDs = append(memberIDs, id)
	}

	sort.Slice(memberIDs, func(i, j int) bool {
		return memberIDs[i] < memberIDs[j]
	})

	if memberIDs[0] != n.currentLeaderID {	
		n.currentLeaderID = memberIDs[0]
		fmt.Printf("New leader elected: Server %d\n", n.currentLeaderID)
		if n.onLeaderChange != nil {
			go n.onLeaderChange(n.currentLeaderID)
		}
	}
}



func (m *Membership) Watch(ctx context.Context) {
	response, err := m.client.Get(ctx, "members/", clientv3.WithPrefix())
	if err == nil {
		for _, kv := range response.Kvs {
			var memberID int64
			fmt.Sscanf(string(kv.Key), "members/%d", &memberID)
			m.mutex.Lock()
			m.members[memberID] = Member{ID: memberID, Address: string(kv.Value)}
			m.mutex.Unlock()
		}
		m.electLeader()
	}

	watchChannel := m.client.Watch(ctx, "members/", clientv3.WithPrefix())
	for watchResponse := range watchChannel {
		for _, event := range watchResponse.Events {
			var memberID int64
			fmt.Sscanf(string(event.Kv.Key), "members/%d", &memberID)

			m.mutex.Lock()
			if event.Type == clientv3.EventTypePut {
				m.members[memberID] = Member{ID: memberID, Address: string(event.Kv.Value)}
				fmt.Printf("Server %d joined with address %s\n", memberID, string(event.Kv.Value))
			} else if event.Type == clientv3.EventTypeDelete {
				delete(m.members, memberID)
				fmt.Printf("Server %d has left\n", memberID)
			}
			m.mutex.Unlock()
			m.electLeader()
		}
	}
}

func (m *Membership) GetMembers() []Member {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	members := make([]Member, 0, len(m.members))
	for _, member := range m.members {
		members = append(members, member)
	}
	return members
}

func (m *Membership) GetLeader() (int64) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.currentLeaderID
}

func (m *Membership) IsLeader() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.id == m.currentLeaderID
}


			


