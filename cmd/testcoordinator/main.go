package main

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/toshok/consumergroup/pkgs/coordinator"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	server, err := coordinator.NewServer(coordinator.ServerConfig{
		ListenAddress:      ":50051",
		PartitionCount:     50,
		AssignmentStrategy: "round-robin",
		PruneInterval:      5 * time.Second,
		// PruneInterval:      30 * time.Second,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create coordinator server")
	}

	if err := server.Serve(); err != nil {
		log.Fatal().Err(err).Msg("failed to Serve()")
	}
}
