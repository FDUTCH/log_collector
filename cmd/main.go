package main

import (
	"github.com/FDUTCH/log_collector/collector"
	"github.com/joho/godotenv"
	"log/slog"
)

func main() {
	godotenv.Load()
	collector.Run(slog.Default())
}
