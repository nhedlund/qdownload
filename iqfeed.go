package main

import (
	"4d63.com/tz"
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apex/log"
)

type rowMapper func(iqfeedRow []string, tz *time.Location, config *Config) (outputRow string, err error)
type requestFactory func(symbol string, requestId string, config *Config) string

const (
	errorMessage               = "E"
	stateMessage               = "S"
	endMessage                 = "!ENDMSG!"
	secondTimestampFormat      = "2006-01-02 15:04:05"
	millisecondTimestampFormat = "2006-01-02 15:04:05.000"
	microsecondTimestampFormat = "2006-01-02 15:04:05.000000"
	csvSeparator               = ","
	tsvSeparator               = "\t"
	bufferSize                 = 4 * 1024 * 1024
)

type DownloadFunc func(string, *Config)

var (
	sourceLocation    *time.Location
)

func init() {
	location, err := tz.LoadLocation("America/New_York")

	if err != nil {
		log.Fatal("Could not load source time zone: America/New_York")
	}

	sourceLocation = location
}

func DownloadEod(symbol string, config *Config) {
	header := "date,open,high,low,close,volume,oi"
	download(symbol, createEodRequest, mapEodBar, header, config)
}

func DownloadMinute(symbol string, config *Config) {
	header := "datetime,open,high,low,close,volume"
	download(symbol, createMinuteRequest, mapMinuteBar, header, config)
}

func DownloadTicks(symbol string, config *Config) {
	header := "datetime,last,lastsize,totalsize,bid,ask,tickid,basis,market,cond,aggr,daycode"
	download(symbol, createTickRequest, mapTick, header, config)
}

func DownloadInterval(symbol string, config *Config) {
	header := "datetime,open,high,low,close,volume"
	download(symbol, createIntervalRequest, mapIntervalBar, header, config)
}

func download(symbol string, createRequest requestFactory, rowMapper rowMapper, csvHeader string, config *Config) {
	successful := false

	// Setup log context
	ctx := log.WithFields(log.Fields{
		"symbol": strings.ToUpper(symbol),
	})

	// Get target time zone
	targetLocation, err := getTargetLocation(config.timeZone)

	if err != nil {
		ctx.WithError(err).Error("Could not load target time zone")
		return
	}

	// Get output filename
	filename := getFilename(symbol, config)
	path := filepath.Join(config.outDirectory, filename)

	// Check if output file already exists
	if fileExists(path) {
		ctx.Info("Already downloaded")
		return
	}

	// Connect to IQFeed Historical socket
	started := millisecondsTimestamp()
	conn, err := net.Dial("tcp", "127.0.0.1:9100")

	if err != nil {
		ctx.WithError(err).Error("Could not connect to IQFeed at port 9100")
		return
	}
	defer conn.Close()

	// Set protocol
	_, err = fmt.Fprintf(conn, "S,SET PROTOCOL,%s\r\n", config.protocol)
	if err != nil {
		ctx.WithError(err).Error("Could not set protocol")
		return
	}

	// Send request
	requestId := "LH"
	request := createRequest(symbol, "", config)
	ctx.Debug(request)
	_, err = fmt.Fprintf(conn, "%s\r\n", request)

	if err != nil {
		ctx.WithError(err).Error("Could not send request")
		return
	}

	ctx.Info("Downloading")

	// Setup write pipeline
	tmpPath := fmt.Sprintf("%s.tmp", path)
	of, err := os.Create(tmpPath)

	if err != nil {
		ctx.WithError(err).Error("Could not create output file")
		return
	}

	var pipe io.WriteCloser = of

	if config.gzip {
		pipe = gzip.NewWriter(of)
	}

	writer := bufio.NewWriterSize(pipe, bufferSize)
	reader := csv.NewReader(bufio.NewReaderSize(conn, bufferSize))
	reader.FieldsPerRecord = -1

	// Defer closing output file and removing the file if an error occurred
	defer func() {
		_ = pipe.Close()
		_ = of.Close()

		if successful {
			err = os.Rename(tmpPath, path)
			if err != nil {
				ctx.WithError(err).Error("Rename temporary file to output file error")
			}
		}

		if !successful && fileExists(tmpPath) {
			err = os.Remove(tmpPath)
			if err != nil {
				ctx.WithError(err).Error("Delete temporary download output file error")
			}
		}
	}()

	// Write header
	header := csvHeader
	if config.tsv {
		header = strings.Replace(csvHeader, csvSeparator, tsvSeparator, -1)
	}
	_, err = fmt.Fprintln(writer, header)

	if err != nil {
		ctx.WithError(err).Error("Add header error")
		return
	}

	// Process rows
	rowCount := 0
	for {
		iqfeedRow, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			ctx.WithError(err).Error("Read row error")
			return
		}

		if config.detailedLogging {
			ctx.Debug(strings.Join(iqfeedRow, ","))
		}

		mappedRow, err := mapRow(iqfeedRow, requestId, rowMapper, targetLocation, config)

		if err == io.EOF {
			break
		} else if err != nil {
			ctx.WithError(err).Error("Map row error")
			return
		} else if mappedRow == "" {
			continue
		}

		_, err = fmt.Fprintln(writer, mappedRow)

		if err != nil {
			ctx.WithError(err).Error("Write output row error")
			return
		}

		rowCount++
	}

	err = writer.Flush()

	if err != nil {
		ctx.WithError(err).Error("Flush output file error")
	}

	successful = true
	duration := millisecondsTimestamp() - started

	ctx.WithFields(log.Fields{
		"symbol":   strings.ToUpper(symbol),
		"duration": fmt.Sprintf("%dms", duration),
		"rows":     rowCount}).Info("Completed")
}

func getFilename(symbol string, config *Config) string {
	filename := symbol

	if config.tsv {
		filename = fmt.Sprintf("%s.tsv", filename)
	} else {
		filename = fmt.Sprintf("%s.csv", filename)
	}

	if config.gzip {
		filename = fmt.Sprintf("%s.gz", filename)
	}

	return filename
}

func getTargetLocation(timeZone string) (*time.Location, error) {
	abbreviations := map[string]string{
		"UTC": "UTC", // Handle lower case utc -> UTC
		"":    "America/New_York",
		"ET":  "America/New_York",
		"EST": "America/New_York",
		"CT":  "America/Chicago",
		"CST": "America/Chicago",
		"PT":  "America/Los_Angeles",
		"PST": "America/Los_Angeles",
	}

	if timeZoneFromAbbreviation, found := abbreviations[strings.ToUpper(timeZone)]; found {
		timeZone = timeZoneFromAbbreviation
	}

	return tz.LoadLocation(timeZone)
}

func mapRow(iqfeedRow []string, requestId string, rowMapper rowMapper, targetLocation *time.Location, config *Config) (outputRow string, err error) {
	if len(iqfeedRow) == 0 {
		return "", fmt.Errorf("empty row")
	}
	if iqfeedRow[0] == stateMessage {
		return "", nil
	}
	if iqfeedRow[0] != requestId {
		return "", fmt.Errorf("incorrect request id")
	}
	if iqfeedRow[1] == endMessage {
		return "", io.EOF
	}
	if iqfeedRow[1] == errorMessage && len(iqfeedRow) >= 3 {
		return "", fmt.Errorf("iqfeed error: %s", iqfeedRow[2])
	}

	outputRow, err = rowMapper(iqfeedRow, targetLocation, config)

	if err != nil && err.Error() == "too few columns" {
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("map row error(%s): %s", err.Error(), iqfeedRow)
	}

	if config.tsv {
		outputRow = strings.Replace(outputRow, csvSeparator, tsvSeparator, -1)
	}

	return outputRow, nil
}

func millisecondsTimestamp() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

// EOD

func createEodRequest(symbol string, requestId string, config *Config) string {
	// HDT,[Symbol],[BeginDate],[EndDate],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
	return fmt.Sprintf("HDT,%s,%s,%s,,1,%s", strings.ToUpper(symbol), config.startDate, config.endDate, "")
}

func mapEodBar(iqfeedRow []string, tz *time.Location, config *Config) (outputRow string, err error) {
	if len(iqfeedRow) < 8 {
		return "", fmt.Errorf("too few columns")
	}

	// Columns from IQFeed (unorthodox ordering of OHLC with High first):
	// 1          2     3    4     5      6       7
	// timestamp, high, low, open, close, volume, openInterest

	return fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s",
			iqfeedRow[1],  // date
			iqfeedRow[4],  // open
			iqfeedRow[2],  // high
			iqfeedRow[3],  // low
			iqfeedRow[5],  // close
			iqfeedRow[6],  // volume
			iqfeedRow[7]), // open interest
		nil
}

// Minute bars

func createMinuteRequest(symbol string, requestId string, config *Config) string {
	// HIT,[Symbol],[Interval],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType],[LabelAtBeginning]<CR><LF>
	return fmt.Sprintf("HIT,%s,60,%s,%s,,,,1,%s", strings.ToUpper(symbol), config.startDate, config.endDate, "")
}

func mapMinuteBar(iqfeedRow []string, tz *time.Location, config *Config) (outputRow string, err error) {
	if len(iqfeedRow) < 7 {
		return "", fmt.Errorf("too few columns")
	}

	// NOTE: In version 5 of the IQFeed protocol minute bars are timestamped at the end of the bar
	//       and have to be adjusted -1 minute to be at the start of the bar (same as EOD bars)
	timestamp, err := time.ParseInLocation(secondTimestampFormat, iqfeedRow[1], sourceLocation)

	if err != nil {
		return "", fmt.Errorf("could not parse minute bar timestamp: %s", err)
	}

	if !config.endTimestamp {
		timestamp = timestamp.Add(-time.Minute * 1)
	}

	timestamp = timestamp.In(tz)

	// Columns from IQFeed (unorthodox ordering of OHLC with High first):
	// 1          2     3    4     5      6            7             8
	// timestamp, high, low, open, close, totalVolume, periodVolume, numberOfTrades

	return fmt.Sprintf("%s,%s,%s,%s,%s,%s",
			timestamp.Format(secondTimestampFormat), // datetime
			iqfeedRow[4],                            // open
			iqfeedRow[2],                            // high
			iqfeedRow[3],                            // low
			iqfeedRow[5],                            // close
			iqfeedRow[7]),                           // volume
		nil
}

// Interval bars

func createIntervalRequest(symbol string, requestId string, config *Config) string {
	// HIT,[Symbol],[Interval],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType],[LabelAtBeginning]<CR><LF>
	label := ""

	if config.useLabels && config.endTimestamp {
		label = ",0"
	} else if config.useLabels && !config.endTimestamp {
		label = ",1"
	}

	return fmt.Sprintf("HIT,%s,%d,%s,%s,,,,1,%s,,%s%s", strings.ToUpper(symbol), config.intervalLength, config.startDate, config.endDate, "", config.intervalType, label)
}

func mapIntervalBar(iqfeedRow []string, tz *time.Location, config *Config) (outputRow string, err error) {
	if len(iqfeedRow) < 7 {
		return "", fmt.Errorf("too few columns")
	}

	timestamp, err := time.ParseInLocation(secondTimestampFormat, iqfeedRow[1], sourceLocation)

	if err != nil {
		return "", fmt.Errorf("could not parse interval bar timestamp: %s", err)
	}

	timestamp = timestamp.In(tz)

	// Columns from IQFeed (unorthodox ordering of OHLC with High first):
	// 1          2     3    4     5      6            7             8
	// timestamp, high, low, open, close, totalVolume, periodVolume, numberOfTrades

	return fmt.Sprintf("%s,%s,%s,%s,%s,%s",
			timestamp.Format(secondTimestampFormat), // datetime
			iqfeedRow[4],                            // open
			iqfeedRow[2],                            // high
			iqfeedRow[3],                            // low
			iqfeedRow[5],                            // close
			iqfeedRow[7]),                           // volume
		nil
}

// Ticks

func createTickRequest(symbol string, requestId string, config *Config) string {
	// HTT,[Symbol],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
	return fmt.Sprintf("HTT,%s,%s,%s,,,,1,%s", strings.ToUpper(symbol), config.startDate, config.endDate, "")
}

func mapTick(iqfeedRow []string, tz *time.Location, config *Config) (outputRow string, err error) {
	if len(iqfeedRow) < 12 {
		return "", fmt.Errorf("too few columns")
	}

	timestamp, err := time.ParseInLocation(microsecondTimestampFormat, iqfeedRow[1], sourceLocation)

	if err != nil {
		return "", fmt.Errorf("could not parse interval bar timestamp: %s", err)
	}

	timestamp = timestamp.In(tz)

	return fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
			timestamp.Format(microsecondTimestampFormat), // datetime
			iqfeedRow[2],   // last
			iqfeedRow[3],   // last size
			iqfeedRow[4],   // total volume
			iqfeedRow[5],   // bid
			iqfeedRow[6],   // ask
			iqfeedRow[7],   // tick id
			iqfeedRow[8],   // basis for last
			iqfeedRow[9],   // trade market center
			iqfeedRow[10],  // trade conditions
			iqfeedRow[11],  // trade aggressor
			iqfeedRow[12],  // day code
		),
		nil
}
