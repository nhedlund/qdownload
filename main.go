// Downloads market data from IQFeed into CSV or TSV files
package main

import (
	"fmt"
	"github.com/apex/log"
	clilog "github.com/apex/log/handlers/cli"
	"github.com/apex/log/handlers/text"
	"github.com/urfave/cli"
	"io/ioutil"
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
	app.ArgsUsage = "<symbols or symbols file>"

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
			Usage:  "Download EOD bars",
			Action: runCommand,
		},
		{
			Name:   "minute",
			Usage:  "Download minute bars",
			Action: runCommand,
		},
		{
			Name:   "tick",
			Usage:  "Download tick data",
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
	return showUsageWithError(c, "Command argument is missing")
}

func showUsageWithError(c *cli.Context, message string) error {
	cli.ShowAppHelp(c)
	fmt.Println("")
	return cli.NewExitError(fmt.Sprintf("ERROR: %s", message), 2)
}

func runCommand(c *cli.Context) error {
	if c.NArg() == 0 {
		return showUsageWithError(c, "Comma separated symbols or symbols filename argument missing")
	}

	config.command = c.Command.Name
	setupLogging(config)
	createOutDirectory(config.outDirectory)
	symbols, err := getSymbols(c.Args()[0])
	if err != nil {
		return err
	}

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

func getSymbols(symbolsOrSymbolsFile string) ([]string, error) {
	var symbols []string

	if strings.Contains(symbolsOrSymbolsFile, ",") || !fileExists(symbolsOrSymbolsFile) {
		symbols = strings.Split(symbolsOrSymbolsFile, ",")
	} else {
		content, err := ioutil.ReadFile(symbolsOrSymbolsFile)
		if err != nil {
			return nil, err
		}
		textContent := string(content)
		symbols = strings.Split(string(textContent), "\n")
	}

	var sanitizedSymbols []string

	for _, symbol := range symbols {
		symbol = strings.Trim(symbol, " \r")
		if symbol != "" {
			sanitizedSymbols = append(sanitizedSymbols, symbol)
		}
	}

	log.WithFields(log.Fields{"symbols": len(sanitizedSymbols)}).Info("Read symbols")
	return sanitizedSymbols, nil
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
