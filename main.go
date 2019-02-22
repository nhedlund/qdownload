// Downloads market data from IQFeed into CSV or TSV files
package main

import (
	"fmt"
	"github.com/apex/log"
	clilog "github.com/apex/log/handlers/cli"
	"github.com/apex/log/handlers/text"
	"github.com/urfave/cli"
	"os"
	"strings"
	"sync"
)

type Config struct {
	command      string
	outDirectory string
	parallelism  int
	tsv          bool
	verbose      bool
	gzip         bool
}

var (
	config = Config{
		command:      "tick",
		outDirectory: "data",
		parallelism:  8,
		tsv:          false,
		verbose:      false,
		gzip:         false,
	}
)

func main() {
	app := cli.NewApp()
	app.Name = "qdownload"
	app.Usage = "downloads historic market data from IQFeed"
	app.Version = "1.0.0"
	app.HideVersion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "out, o",
			Value:       "data",
			Usage:       "output directory",
			Destination: &config.outDirectory,
		},
		cli.IntFlag{
			Name:        "parallelism, p",
			Value:       8,
			Usage:       "number of parallel downloads",
			Destination: &config.parallelism,
		},
		cli.BoolFlag{
			Name:        "tsv, t",
			Usage:       "use tab separator instead of comma",
			Destination: &config.tsv,
		},
		cli.BoolFlag{
			Name:        "verbose, l",
			Usage:       "verbose logging",
			Destination: &config.verbose,
		},
		cli.BoolFlag{
			Name:        "gzip, g",
			Usage:       "compress files with gzip",
			Destination: &config.gzip,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "eod",
			Usage:  "download EOD bars",
			Action: runCommand,
		},
		{
			Name:   "minute",
			Usage:  "download minute bars",
			Action: runCommand,
		},
		{
			Name:   "tick",
			Usage:  "download tick data",
			Action: runCommand,
		},
	}

	app.Action = showUsageWhenMissingCommand

	err := app.Run(os.Args)

	if err != nil {
		log.WithError(err).Error("Application error")
	}
}

func showUsageWhenMissingCommand(c *cli.Context) error {
	cli.ShowAppHelp(c)
	fmt.Println("")
	return cli.NewExitError("command argument is missing", 2)
}

func runCommand(c *cli.Context) error {
	config.command = c.Command.Name
	setupLogging(config)
	createOutDirectory(config.outDirectory)
	symbols := getSymbols()
	wg := start(symbols, &config)

	wg.Wait()
	return nil
}

func setupLogging(config Config) {
	if config.verbose {
		log.SetHandler(text.New(os.Stderr))
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetHandler(clilog.New(os.Stderr))
	}
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

	downloadFunc := getDownloadCommandFunction()
	wg := sync.WaitGroup{}

	log.Debug("Starting downloaders")

	for i := 0; i < config.parallelism; i++ {
		go downloader(symbolsQueue, &wg, config, downloadFunc)
		wg.Add(1)
	}

	return &wg
}

func getDownloadCommandFunction() DownloadFunc {
	switch strings.ToLower(config.command) {
	case "tick":
		return DownloadTicks
	}

	log.Fatalf("Unsupported download function: %s", config.command)
	return nil
}

func downloader(symbolsQueue <-chan string, wg *sync.WaitGroup, config *Config, downloadFunc DownloadFunc) {
	log.Debug("Downloader started")

	for symbol := range symbolsQueue {
		downloadFunc(symbol, config)
	}

	wg.Done()
}
