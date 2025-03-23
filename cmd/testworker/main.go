package main

import (
	"os"
	"time"

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

	worker, err := worker.New("localhost:50051", partitionsChanged)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to coordinator")
	}

	go worker.Start()

	for {
		// do nothing forever
		time.Sleep(1 * time.Second)
	}
}
