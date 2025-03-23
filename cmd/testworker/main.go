package main

import (
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/toshok/consumergroup/pkgs/types"
	"github.com/toshok/consumergroup/pkgs/worker"
)

func partitionsChanged(partitions []types.PartitionID) {
	log.Info().Msgf("Partitions changed: %v", partitions)
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to coordinator")
	}
	defer conn.Close()

	worker := worker.New(conn, partitionsChanged)
	worker.Run()

	for {
		// do nothing forever
		time.Sleep(1 * time.Second)
	}
}
