// Downloads market data from IQFeed into CSV or TSV files
package main

import (
	"fmt"
	"github.com/apex/log"
	clilog "github.com/apex/log/handlers/cli"
	"github.com/apex/log/handlers/text"
	"gopkg.in/urfave/cli.v1"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	protocol        string
	command         string
	startDate       string
	endDate         string
	outDirectory    string
	timeZone        string
	intervalType    string
	intervalLength  int
	parallelism     int
	tsv             bool
	detailedLogging bool
	gzip            bool
	endTimestamp    bool
	useLabels       bool
}

var (
	config = Config{
		protocol:        "6.2",
		command:         "",
		startDate:       "",
		endDate:         "",
		outDirectory:    "data",
		timeZone:        "ET",
		intervalType:    "",
		intervalLength:  0,
		parallelism:     8,
		tsv:             false,
		detailedLogging: false,
		gzip:            false,
		endTimestamp:    false,
		useLabels:       false,
	}
)

func detailedLoggingEnabled(args []string) bool {
	for _, arg := range args {
		if arg == "-d" || arg == "--detailed-logging" {
			return true
		}
	}

	return false
}

func main() {
	detailedLogging := detailedLoggingEnabled(os.Args)
	setupLogging(detailedLogging)

	app := cli.NewApp()
	app.Name = "qdownload"
	app.Usage = "downloads historic market data from IQFeed"
	app.Version = "1.0.0"
	app.HideVersion = true
	app.ArgsUsage = "<symbols or symbols file>"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "start, s",
			Value:       "",
			Usage:       "start date filter: yyyymmdd",
			Destination: &config.startDate,
		},
		cli.StringFlag{
			Name:        "end, e",
			Value:       "",
			Usage:       "end date filter: yyyymmdd",
			Destination: &config.endDate,
		},
		cli.StringFlag{
			Name:        "out, o",
			Value:       "data",
			Usage:       "output directory",
			Destination: &config.outDirectory,
		},
		cli.StringFlag{
			Name:        "timezone, z",
			Value:       "ET",
			Usage:       "timestamps time zone",
			Destination: &config.timeZone,
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
			Name:        "detailed-logging, d",
			Usage:       "detailed log output",
			Destination: &config.detailedLogging,
		},
		cli.BoolFlag{
			Name:        "gzip, g",
			Usage:       "compress files with gzip",
			Destination: &config.gzip,
		},
		cli.BoolFlag{
			Name:        "end-timestamp, m",
			Usage:       "use end of bar timestamps instead of start",
			Destination: &config.endTimestamp,
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
		{
			Name:      "interval",
			Usage:     "Download interval bars: <length> <seconds|volume|ticks>",
			Action:    runCommand,
			ArgsUsage: "<length> <type>",
			Before: func(c *cli.Context) error {
				if len(c.Args()) < 3 {
					return fmt.Errorf("incorrect number of interval parameters: %d", len(c.Args()))
				}

				config.intervalType = c.Args()[1]
				intervalLength, err := strconv.Atoi(c.Args()[0])
				config.intervalLength = intervalLength

				if config.intervalLength <= 0 {
					err = fmt.Errorf("incorrect interval length: %s", c.Args()[0])
				} else {
					err = mapIntervalType(config.intervalType, &config.intervalType)
				}

				return err
			},
		},
	}

	app.Action = showUsageWhenMissingCommand

	err := app.Run(os.Args)

	if err != nil {
		log.WithError(err).Error("Application error")
	}
}

func mapIntervalType(argument string, intervalType *string) error {
	arg := strings.ToUpper(argument)

	if strings.HasPrefix(arg, "S") {
		*intervalType = "s"
	} else if strings.HasPrefix(arg, "V") {
		*intervalType = "v"
	} else if strings.HasPrefix(arg, "T") {
		*intervalType = "t"
	} else {
		return fmt.Errorf("incorrect interval type: %s", argument)
	}

	return nil
}

func showUsageWhenMissingCommand(c *cli.Context) error {
	return showUsageWithError(c, "Command argument is missing")
}

func showUsageWithError(c *cli.Context, message string) error {
	_ = cli.ShowAppHelp(c)
	fmt.Println("")
	return cli.NewExitError(fmt.Sprintf("ERROR: %s", message), 2)
}

func runCommand(c *cli.Context) error {
	if c.NArg() == 0 {
		return showUsageWithError(c, "Comma separated symbols or symbols filename argument missing")
	}

	if c.Command.Name == "eod" && config.startDate == "" && config.endDate == "" {
		log.Debug("[start|enddate required] set enddate=tomorrow")
		config.endDate = time.Now().Add(24 * time.Hour).Format("20060102")
	} else if c.Command.Name == "minute" && config.startDate == "" && config.endDate == "" {
		log.Debug("[start|enddate required] set enddate=tomorrow")
		config.endDate = time.Now().Add(24 * time.Hour).Format("20060102 150405")
	} else if c.Command.Name == "interval" && config.startDate == "" && config.endDate == "" {
		log.Debug("[start|enddate required] set enddate=tomorrow")
		config.endDate = time.Now().Add(24 * time.Hour).Format("20060102 150405")
	}

	config.command = c.Command.Name
	createOutDirectory(config.outDirectory)
	symbols, err := getSymbols(c.Args()[len(c.Args())-1])
	if err != nil {
		return err
	}

	log.Debug(fmt.Sprintf("config=%+v\n", config))
	wg := start(symbols, &config)

	wg.Wait()
	return nil
}

func setupLogging(detailedLogging bool) {
	if detailedLogging {
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
	case "eod":
		return DownloadEod
	case "minute":
		return DownloadMinute
	case "tick":
		return DownloadTicks
	case "interval":
		return DownloadInterval
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
