package api

import (
	"net/http"
	"encoding/json"

	"github.com/gin-gonic/gin"
	"ds_project/src/server/statemachine"
    "ds_project/src/server/paxos"
    "ds_project/src/server/log"
)

type API struct {
	stateMachine *statemachine.ScooterStateMachine
	proposer     *paxos.Proposer
	log          *log.ReplicatedLog
}

func NewAPI(stateMachine *statemachine.ScooterStateMachine, proposer *paxos.Proposer, log *log.ReplicatedLog) *API {
	return &API{
		stateMachine: stateMachine,
		proposer:     proposer,
		log:          log,
	}
}

func (api *API) GetScooters(context *gin.Context) {
	if context.Query("linearizable") == "true" {
		cmd := statemachine.ScooterCommand{
			CommandType: statemachine.Noop,
		}
		cmdBytes, _ := json.Marshal(cmd)
		index := api.log.GetNextIndex()
		_, err := api.proposer.Propose(int64(index), int64(index), cmdBytes)
		if err != nil {
			context.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to ensure linearizability: " + err.Error()})
			return
		}
	}

	scooters := api.stateMachine.GetScooters()
	context.JSON(http.StatusOK, scooters)
}

func (api *API) GetScooter(context *gin.Context) {
	if context.Query("linearizable") == "true" {
		cmd := statemachine.ScooterCommand{
			CommandType: statemachine.Noop,
		}
		cmdBytes, _ := json.Marshal(cmd)
		index := api.log.GetNextIndex()
		_, err := api.proposer.Propose(int64(index), int64(index), cmdBytes)
		if err != nil {
			context.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to ensure linearizability: " + err.Error()})
			return
		}
	}

	scooter, exists := api.stateMachine.GetScooter(context.Param("id"))
	if !exists {
		context.JSON(http.StatusNotFound, gin.H{"error": "Scooter not found"})
		return
	}
	context.JSON(http.StatusOK, scooter)
}

func (api *API) CreateScooter(context *gin.Context) {
	scooterID := context.Param("id")

	_, exists := api.stateMachine.GetScooter(scooterID)
	if exists {
		context.JSON(http.StatusConflict, gin.H{"error": "Scooter already exists"})
		return
	}

	cmd := statemachine.ScooterCommand{
		CommandType: statemachine.Create,
		ScooterID: scooterID,
	}
	cmdBytes, _ :=json.Marshal(cmd)
	index := api.log.GetNextIndex()
	_, err := api.proposer.Propose(int64(index), int64(index), cmdBytes)
	if err != nil {
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	context.JSON(http.StatusOK, gin.H{"status": "Scooter created", "id": scooterID})
}

func (api *API) ReserveScooter(context *gin.Context) {
	scooterID := context.Param("id")

	var body struct {
		ReservationID string `json:"reservation_id"`
	}
	context.BindJSON(&body)

	scooter, exists := api.stateMachine.GetScooter(scooterID)
	if !exists {
		context.JSON(http.StatusNotFound, gin.H{"error": "Scooter not found"})
		return
	}

	if !scooter.IsAvailable {
		context.JSON(http.StatusConflict, gin.H{"error": "Scooter is not available"})
		return
	}

	cmd := statemachine.ScooterCommand{
		CommandType: statemachine.Reserve,
		ScooterID: scooterID,
		ReservationID: body.ReservationID,
	}
	cmdBytes, _ :=json.Marshal(cmd)
	index := api.log.GetNextIndex()
	_, err := api.proposer.Propose(int64(index), int64(index), cmdBytes)
	if err != nil {
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	context.JSON(http.StatusOK, gin.H{"status": "Scooter reserved", "id": scooterID})
}

func (api *API) ReleaseScooter(context *gin.Context) {
	scooterID := context.Param("id")

	var body struct {
		Distance int64 `json:"distance"`
	}
	context.BindJSON(&body)

	if body.Distance < 0 {
		context.JSON(http.StatusBadRequest, gin.H{"error": "Distance cannot be negative"})
		return
	}

	scooter, exists := api.stateMachine.GetScooter(scooterID)
	if !exists {
		context.JSON(http.StatusNotFound, gin.H{"error": "Scooter not found"})
		return
	}

	if scooter.IsAvailable {
		context.JSON(http.StatusConflict, gin.H{"error": "Scooter is not reserved"})
		return
	}

	cmd := statemachine.ScooterCommand{
		CommandType: statemachine.Release,
		ScooterID: scooterID,
		Distance: body.Distance,
	}

	cmdBytes, _ :=json.Marshal(cmd)

	index := api.log.GetNextIndex()
	_, err := api.proposer.Propose(int64(index), int64(index), cmdBytes)
	if err != nil {
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	context.JSON(http.StatusOK, gin.H{"status": "Scooter released", "id": scooterID})
}


func (api *API) RegisterRoutes(router *gin.Engine) {
	router.GET("/scooters", api.GetScooters)
	router.GET("/scooters/:id", api.GetScooter)
	router.PUT("/scooters/:id", api.CreateScooter)
	router.POST("/scooters/:id/reservations", api.ReserveScooter)
	router.POST("/scooters/:id/releases", api.ReleaseScooter)
}

func (api *API) TakeSnapshot(context *gin.Context) {
	index := api.log.GetCommitIndex()
	err := api.stateMachine.TakeSnapshot(index)
	if err != nil {
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	api.log.Store(index)
	context.JSON(http.StatusOK, gin.H{"status": "Snapshot taken", "index": index})
}



