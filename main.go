// Downloads market data from IQFeed into GZipped CSV or TSV files
package main

import (
	"github.com/apex/log"
	"github.com/apex/log/handlers/cli"
	"github.com/apex/log/handlers/text"
	"os"
	"sync"
)

type Config struct {
	outDirectory string
	separator    string
	parallelism  int
	verbose      bool
	gzip 		 bool
}

func main() {
	config := Config{
		outDirectory: "data",
		separator:    ",",
		parallelism:  8,
		verbose:      true,
		gzip:         true,
	}

	if config.verbose {
		log.SetHandler(text.New(os.Stderr))
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetHandler(cli.New(os.Stderr))
	}

	createOutDirectory(config.outDirectory)
	symbols := getSymbols()
	wg := start(symbols, &config)

	wg.Wait()
}

func createOutDirectory(outDirectory string) {
	err := os.MkdirAll(outDirectory, os.ModePerm)
	if err != nil {
		panic(err)
	}
}

func getSymbols() []string {
	symbols := []string{"spy", "aapl", "ibm"}
	log.WithFields(log.Fields{"symbols": len(symbols)}).Info("Read symbols")
	return symbols
}

func start(symbols []string, config *Config) *sync.WaitGroup {
	symbolsQueue := make(chan string, len(symbols))

	for _, symbol := range symbols {
		symbolsQueue <- symbol
	}

	close(symbolsQueue)

	wg := sync.WaitGroup{}

	log.Debug("Starting downloaders")

	for i := 0; i < config.parallelism; i++ {
		go downloader(symbolsQueue, &wg, config)
		wg.Add(1)
	}

	return &wg
}

func downloader(symbolsQueue <-chan string, wg *sync.WaitGroup, config *Config) {
	log.Debug("Downloader started")

	for symbol := range symbolsQueue {
		DownloadTicks(symbol, config)
	}

	wg.Done()
}
