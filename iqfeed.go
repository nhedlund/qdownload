package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/apex/log"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

type rowMapper func(iqfeedRow []string) (outputRow string, err error)

const (
	errorMessage         = "E"
	stateMessage         = "S"
	endMessage           = "!ENDMSG!"
	maxDataPoints string = "3"
	csvSeparator  string = ","
	bufferSize           = 4*1024*1024
)

var (
	nextRequestId int64 = 1
)

func DownloadTicks(symbol string, config *Config) {
	header := "datetime,last,lastsize,totalsize,bid,ask,tickid,basis,market,cond"
	download(symbol, tickMapper, header, config)
}

func download(symbol string, rowMapper rowMapper, csvHeader string, config *Config) {
	// Setup log context
	ctx := log.WithFields(log.Fields{
		"symbol": strings.ToUpper(symbol),
	})

	// Connect to IQFeed Historical socket
	started := millisecondsTimestamp()
	conn, err := net.Dial("tcp", "127.0.0.1:9100")

	if err != nil {
		ctx.WithError(err).Error("Could not connect to IQFeed at port 9100")
		return
	}
	defer conn.Close()


	// Set protocol
	_, err = fmt.Fprintf(conn, "S,SET PROTOCOL,5.1\r\n")

	if err != nil {
		ctx.WithError(err).Error("Could not set protocol")
		return
	}

	// Send request
	requestId := fmt.Sprintf("%d", atomic.AddInt64(&nextRequestId, 1))

	// Ticks
	// HTT,[Symbol],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
	request := fmt.Sprintf("HTT,%s,,,%s,,,1,%s", strings.ToUpper(symbol), maxDataPoints, requestId)

	// Minute bars (with timestamp as bar end)
	// HID,[Symbol],[Interval],[Days],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType],[LabelAtBeginning]<CR><LF>
	// Minute data, data direction: Ascending, RequestID: #100
	//request := fmt.Sprintf("HID,%s,,,,,,1,#%d", strings.ToUpper(symbol), requestId)
	_, err = fmt.Fprintf(conn, "%s\r\n", request)

	if err != nil {
		ctx.WithError(err).Error("Could not send request")
		return
	}

	ctx.Info("Downloading")

	// Setup write pipeline
	filename := getFilename(symbol, config)
	path := filepath.Join(config.outDirectory, filename)
	of, err := os.Create(path)

	if err != nil {
		ctx.WithError(err).Error("Could not create output file")
		return
	}

	var pipe io.WriteCloser = of

	if config.gzip {
		pipe = gzip.NewWriter(of)
	}

	writer := bufio.NewWriterSize(pipe, 4*1024*1024)
	reader := csv.NewReader(bufio.NewReaderSize(conn, 4*1024*1024))
	reader.FieldsPerRecord = -1
	defer pipe.Close()

	// Write header
	header := strings.Replace(csvHeader, csvSeparator, config.separator, -1)
	_, err = fmt.Fprintln(writer, header)

	if err != nil {
		ctx.WithError(err).Error("Add header error")
		return
	}

	// Process rows
	rowCount := 0
	for {
		row, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			ctx.WithError(err).Error("Read row error")
			return
		}

		if config.verbose {
			ctx.Debug(strings.Join(row, " "))
		}

		if len(row) == 0 {
			ctx.Error("Got empty row")
			return
		}
		if row[0] == stateMessage {
			continue
		}
		if len(row) >= 1 && row[0] != requestId {
			ctx.Error("Got other requests data row")
			return
		}
		if row[1] == endMessage {
			break
		}
		if row[1] == errorMessage && len(row) >= 3 {
			ctx.WithField("error", row[2]).Error("IQFeed returned error")
			return
		}

		outputRow, err := rowMapper(row)

		if err != nil && err.Error() == "too few columns" {
			continue
		} else if err != nil {
			ctx.WithError(err).Error("Map row error")
			return
		}

		if config.separator != csvSeparator {
			outputRow = strings.Replace(outputRow, csvSeparator, config.separator, -1)
		}

		_, err = fmt.Fprintln(writer, outputRow)

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

	duration := millisecondsTimestamp() - started

	ctx.WithFields(log.Fields{
		"symbol":   strings.ToUpper(symbol),
		"duration": fmt.Sprintf("%dms", duration),
		"rows":     rowCount}).Info("Completed")
}

func getFilename(symbol string, config *Config) string {
	filename := symbol

	if config.separator == "," {
		filename = fmt.Sprintf("%s.csv", filename)
	} else if config.separator == "\t" {
		filename = fmt.Sprintf("%s.tsv", filename)
	} else {
		filename = fmt.Sprintf("%s.txt", filename)
	}

	if config.gzip {
		filename = fmt.Sprintf("%s.gz", filename)
	}

	return filename
}

func tickMapper(iqfeedRow []string) (outputRow string, err error) {
	if len(iqfeedRow) < 11 {
		return "", errors.New(fmt.Sprintf("too few columns"))
	}

	return fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
			iqfeedRow[1],   // datetime
			iqfeedRow[2],   // last
			iqfeedRow[3],   // last size
			iqfeedRow[4],   // total size
			iqfeedRow[5],   // bid
			iqfeedRow[6],   // ask
			iqfeedRow[7],   // tick id
			iqfeedRow[8],   // basis
			iqfeedRow[9],   // market
			iqfeedRow[10]), // conditions
		nil
}

func millisecondsTimestamp() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}
