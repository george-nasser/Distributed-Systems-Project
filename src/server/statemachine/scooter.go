package statemachine

import (
	"fmt"
	"sync"
	"encoding/json"
)

type Scooter struct {
	ID        string	`json:"id"`
	IsAvailable bool	`json:"is_available"`
	TotalDistance float64	`json:"total_distance"`
	ReservationID string	`json:"current_reservation_id,omitempty"`
}

const (
	Create = "CREATE"
	Reserve = "RESERVE"
	Release = "RELEASE"
	Noop   = "NOOP"
)

type ScooterCommand struct {	
	CommandType   string `json:"command_type"`
	ScooterID     string `json:"scooter_id"`
	ReservationID string `json:"reservation_id,omitempty"`
	Distance      int64  `json:"distance,omitempty"`
}

type ScooterStateMachine struct {
	scooters map[string]*Scooter
	snapshotData []byte
	snapshotIndex int64
	mutex    sync.RWMutex
}

func NewScooterStateMachine() *ScooterStateMachine {
	return &ScooterStateMachine{
		scooters: make(map[string]*Scooter),
	}
}

func (sm *ScooterStateMachine) Apply(commandBytes []byte) error {
	var cmd ScooterCommand 

	 err := json.Unmarshal(commandBytes, &cmd)  
  	if err != nil{                            
      return err                             
  	}  


	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	switch cmd.CommandType {
	case Create:

		if _, exists := sm.scooters[cmd.ScooterID]; exists {
			return fmt.Errorf("Scooter %s already exists", cmd.ScooterID)
		}

		sm.scooters[cmd.ScooterID] = &Scooter{
			ID: cmd.ScooterID,
			IsAvailable: true,
			TotalDistance: 0,
		}

	case Reserve:

		scooter, exists := sm.scooters[cmd.ScooterID]
		
		if !exists {
			return fmt.Errorf("Scooter %s does not exist", cmd.ScooterID)
		}

		if !scooter.IsAvailable {
			return fmt.Errorf("Scooter %s is not available", cmd.ScooterID)
		}

		scooter.IsAvailable = false
		scooter.ReservationID = cmd.ReservationID


	case Release:

		scooter, exists := sm.scooters[cmd.ScooterID]

		if !exists {
			return fmt.Errorf("Scooter %s does not exist", cmd.ScooterID)
		}

		if scooter.IsAvailable {
			return fmt.Errorf("Scooter %s is already available", cmd.ScooterID)
		}

		scooter.IsAvailable = true
		scooter.TotalDistance += float64(cmd.Distance)
		scooter.ReservationID = ""

	case Noop:

	}

		return nil
}

func (sm *ScooterStateMachine) GetScooter(scooterID string) (*Scooter, bool) {

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	scooter, exists := sm.scooters[scooterID]
	return scooter, exists
}

func (sm *ScooterStateMachine) GetScooters() []*Scooter {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	scooterList := make([]*Scooter, 0, len(sm.scooters))

	for _, scooter := range sm.scooters {
		scooterList = append(scooterList, scooter)
	}

	return scooterList
}

func (sm *ScooterStateMachine) TakeSnapshot(index int64) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	data, err := json.Marshal(sm.scooters)

	if err != nil {
		return err
	}

	sm.snapshotData = data
	sm.snapshotIndex = index
	return nil
}

func (sm *ScooterStateMachine) GetSnapshot() ([]byte, int64) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	return sm.snapshotData, sm.snapshotIndex
}

func (sm* ScooterStateMachine) LoadSnapshot(data []byte, index int64) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	var scooters map[string]*Scooter

	if err := json.Unmarshal(data, &scooters); err != nil {
		return err
	}

	sm.scooters = scooters
	sm.snapshotIndex = index
	return nil
}

func (sm *ScooterStateMachine) GetSnapshotIndex() int64 {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	return sm.snapshotIndex
}