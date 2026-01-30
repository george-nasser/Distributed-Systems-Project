package main 


import (
	"fmt"
	"flag"
	"log"
	"net"
	"os"
	"strings"
	"context"
	"ds_project/src/server/paxos"
	pb "ds_project/src/server/proto"
	"google.golang.org/grpc"

	"ds_project/src/server/membership"
	"ds_project/src/server/recovery"
	"ds_project/src/server/statemachine"
	"ds_project/src/server/api"
	replicated_log "ds_project/src/server/log"
	"github.com/gin-gonic/gin"

)

func main() {
	id  := flag.Int64("id", 1, "Server ID")
	port := flag.String("port", "50051", "Server port")
	servers := flag.String("servers", "", "Comma separated list of server addresses")
	testingPort := flag.String("testport", "8081", "Testing server port")
	flag.Parse()

	var serverAddresses []string
	if *servers != "" {
		serverAddresses = strings.Split(*servers, ",")
	}

	statementMachine := statemachine.NewScooterStateMachine()
	replicatedLog := replicated_log.NewReplicatedLog()

	acceptor := paxos.NewAcceptor(statementMachine, replicatedLog)
	proposer := paxos.NewProposer(*id, serverAddresses, acceptor)

	etcdHost := "localhost:2379"
	if envEtcd := os.Getenv("ETCD_SERVER"); envEtcd != "" {
		etcdHost = envEtcd
	}
	etcEndpoints := []string{etcdHost}
	membershipService, err := membership.NewMembership(*id, "localhost:"+ *port, etcEndpoints)
	if err != nil {
		log.Fatalf("Failed to create membership service: %v", err)
	}

	ctx := context.Background()
	err = membershipService.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to start membership service: %v", err)
	}
	go membershipService.Watch(ctx)

	apiHandler := api.NewAPI(statementMachine, proposer, replicatedLog)

	//fmt.Printf("Server %d started\n", *id)

	listener, err := net.Listen("tcp", ":" + *port)
	if err != nil {
      log.Fatalf("Failed to listen: %v", err)
  	}

	grpcServer := grpc.NewServer()
	pb.RegisterPaxosServer(grpcServer, acceptor)
	pb.RegisterLogRecoveryServer(grpcServer, recovery.NewLogRecovery(statementMachine, replicatedLog))

	go grpcServer.Serve(listener)

	fmt.Printf("Server %d listening on port %s, HTTP on port %s\n", *id, *port, *testingPort)

	
	// http.HandleFunc("/propose", func(w http.ResponseWriter, r *http.Request) {
    //       valueRequest := r.URL.Query().Get("value")
	// 	  value := int64(25)
	// 	  if valueRequest != "" {
	// 		  fmt.Sscanf(valueRequest, "%d", &value)
	// 	  }
	// 	  instanceIdRequest := r.URL.Query().Get("instanceId")
	// 	  instanceId := int64(0)
	// 	  if instanceIdRequest != "" {
	// 		  fmt.Sscanf(instanceIdRequest, "%d", &instanceId)
	// 	  }
    //       result, err := proposer.Propose(value, instanceId)
    //       if err != nil {
    //           fmt.Fprintf(w, "Error: %v\n", err)
    //           return
    //       }
    //       fmt.Fprintf(w, "Instance %d decided value: %d\n", instanceId, result)
    //   })

    //   log.Fatal(http.ListenAndServe(":"+*testingPort, nil))

	router := gin.Default()
	apiHandler.RegisterRoutes(router)
	router.POST("/snapshot", apiHandler.TakeSnapshot)
	recovery.Recover(serverAddresses, statementMachine, replicatedLog)
	router.Run(":" + *testingPort)
}
