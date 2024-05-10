package main

import (
	"4d63.com/tz"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	testRequestId                    = "999"
	testProtocolMessage              = "S,CURRENT PROTOCOL,5.1"
	testEndMessage                   = "999,!ENDMSG!,"
	testNoDataMessage                = "999,E,!NO_DATA!,,"
	testValidIqfeedEodBar            = "999,2019-02-21,24.0600,23.8038,23.8700,24.0000,29183,0,"
	testTooFewColumnsIqfeedEodBar    = "999,2019-02-21,24.0600,23.8038,23.8700,24.0000,29183"
	testValidIqfeedMinuteBar         = "999,2019-02-26 12:22:00,23.8000,23.8000,23.8000,23.8000,13578,100,0,"
	testTooFewColumnsIqfeedMinuteBar = "999,2019-02-26 12:22:00,23.8000,23.8000,23.8000,23.8000"
	testValidIqfeedTick              = "999,2019-02-25 11:30:06.691000,23.8800,12,6714,23.8700,23.9700,6,O,25,3D87,0,13"
	testTooFewColumnsIqfeedTick      = "999,2019-02-25 11:30:06.691,23.8800,12,6714,23.8700,23.9700,6,O,25"
	testIncorrectRequestIdIqfeedTick = "111,2019-02-25 11:30:06.691,23.8800,12,6714,23.8700,23.9700,6,O,25,3D87,"
	et                               *time.Location
)

func init() {
	location, err := tz.LoadLocation("America/New_York")

	if err != nil {
		log.Fatal("Could not load source time zone: America/New_York")
	}

	et = location
}

func TestMapRow(t *testing.T) {
	t.Run("valid tick to csv", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedTick, ",")

		mappedRow, err := mapRow(columns, testRequestId, mapTick, et, createConfig(0, "", false, false))

		assert.Equal(t, "2019-02-25 11:30:06.691000,23.8800,12,6714,23.8700,23.9700,6,O,25,3D87,0,13", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("valid tick to tsv", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedTick, ",")

		mappedRow, err := mapRow(columns, testRequestId, mapTick, et, createConfig(0, "", false, true))

		assert.Equal(t, "2019-02-25 11:30:06.691000\t23.8800\t12\t6714\t23.8700\t23.9700\t6\tO\t25\t3D87\t0\t13", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("no columns", func(t *testing.T) {
		var columns []string

		mappedRow, err := mapRow(columns, testRequestId, mapTick, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "empty row")
	})

	t.Run("protocol message", func(t *testing.T) {
		columns := strings.Split(testProtocolMessage, ",")

		mappedRow, err := mapRow(columns, testRequestId, mapTick, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("incorrect request id", func(t *testing.T) {
		columns := strings.Split(testIncorrectRequestIdIqfeedTick, ",")

		mappedRow, err := mapRow(columns, testRequestId, mapTick, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "incorrect request id")
	})

	t.Run("end message", func(t *testing.T) {
		columns := strings.Split(testEndMessage, ",")

		mappedRow, err := mapRow(columns, testRequestId, mapTick, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("no data", func(t *testing.T) {
		columns := strings.Split(testNoDataMessage, ",")

		mappedRow, err := mapRow(columns, testRequestId, mapTick, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "iqfeed error: !NO DATA!")
	})

	t.Run("too few columns", func(t *testing.T) {
		columns := strings.Split(testTooFewColumnsIqfeedTick, ",")

		mappedRow, err := mapRow(columns, testRequestId, mapTick, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Nil(t, err)
	})
}

func TestEodBarMapper(t *testing.T) {
	t.Run("valid eod bar", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedEodBar, ",")

		mappedRow, err := mapEodBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "2019-02-21,23.8700,24.0600,23.8038,24.0000,29183,0", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("too few columns", func(t *testing.T) {
		columns := strings.Split(testTooFewColumnsIqfeedEodBar, ",")

		mappedRow, err := mapEodBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})

	t.Run("no columns", func(t *testing.T) {
		var columns []string

		mappedRow, err := mapEodBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})
}

func TestMinuteBarMapper(t *testing.T) {
	t.Run("valid minute bar with bar start timestamp", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedMinuteBar, ",")

		mappedRow, err := mapMinuteBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "2019-02-26 12:21:00,23.8000,23.8000,23.8000,23.8000,100", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("valid minute bar with bar start timestamp in cst time zone", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedMinuteBar, ",")
		cst, _ := tz.LoadLocation("America/Chicago")

		mappedRow, err := mapMinuteBar(columns, cst, createConfig(0, "", false, false))

		assert.Equal(t, "2019-02-26 11:21:00,23.8000,23.8000,23.8000,23.8000,100", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("valid minute bar with bar end timestamp", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedMinuteBar, ",")

		mappedRow, err := mapMinuteBar(columns, et, createConfig(0, "", true, false))

		assert.Equal(t, "2019-02-26 12:22:00,23.8000,23.8000,23.8000,23.8000,100", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("too few columns", func(t *testing.T) {
		columns := strings.Split(testTooFewColumnsIqfeedMinuteBar, ",")

		mappedRow, err := mapMinuteBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})

	t.Run("no columns", func(t *testing.T) {
		var columns []string

		mappedRow, err := mapMinuteBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})
}

func TestIntervalBarMapper(t *testing.T) {
	t.Run("valid 60 second interval bar with bar start timestamp", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedMinuteBar, ",")

		// Here mapIntervalBar assumes protocol 6.0 which returns normal timestamps
		mappedRow, err := mapIntervalBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "2019-02-26 12:22:00,23.8000,23.8000,23.8000,23.8000,100", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("valid 60 second interval bar with bar start timestamp in cst time zone", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedMinuteBar, ",")
		cst, _ := tz.LoadLocation("America/Chicago")

		// Here mapIntervalBar assumes protocol 6.0 which returns normal timestamps
		mappedRow, err := mapIntervalBar(columns, cst, createConfig(0, "", false, false))

		assert.Equal(t, "2019-02-26 11:22:00,23.8000,23.8000,23.8000,23.8000,100", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("valid 60 second interval bar with bar end timestamp", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedMinuteBar, ",")

		// Here mapIntervalBar assumes protocol 5 which returns end of bar timestamps
		mappedRow, err := mapIntervalBar(columns, et, createConfig(0, "", true, false))

		assert.Equal(t, "2019-02-26 12:22:00,23.8000,23.8000,23.8000,23.8000,100", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("too few columns", func(t *testing.T) {
		columns := strings.Split(testTooFewColumnsIqfeedMinuteBar, ",")

		mappedRow, err := mapMinuteBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})

	t.Run("no columns", func(t *testing.T) {
		var columns []string

		mappedRow, err := mapMinuteBar(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})
}

func TestTickMapper(t *testing.T) {
	t.Run("valid tick", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedTick, ",")

		mappedRow, err := mapTick(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "2019-02-25 11:30:06.691000,23.8800,12,6714,23.8700,23.9700,6,O,25,3D87,0,13", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("valid tick in cst time zone", func(t *testing.T) {
		columns := strings.Split(testValidIqfeedTick, ",")
		cst, _ := tz.LoadLocation("America/Chicago")

		mappedRow, err := mapTick(columns, cst, createConfig(0, "", false, false))

		assert.Equal(t, "2019-02-25 10:30:06.691000,23.8800,12,6714,23.8700,23.9700,6,O,25,3D87,0,13", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("too few columns", func(t *testing.T) {
		columns := strings.Split(testTooFewColumnsIqfeedTick, ",")

		mappedRow, err := mapTick(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})

	t.Run("no columns", func(t *testing.T) {
		var columns []string

		mappedRow, err := mapTick(columns, et, createConfig(0, "", false, false))

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})
}

func TestCreateEodRequest(t *testing.T) {
	t.Run("eod request", func(t *testing.T) {
		request := createEodRequest("spy", "R91", createConfig(0, "", false, false))

		assert.Equal(t, "HDT,SPY,20190122,20190221,,1,", request)
	})
}

func TestCreateMinuteRequest(t *testing.T) {
	t.Run("minute request", func(t *testing.T) {
		request := createMinuteRequest("spy", "R91", createConfig(0, "", false, false))

		assert.Equal(t, "HIT,SPY,60,20190122,20190221,,,,1,", request)
	})
}

func TestCreateIntervalRequest(t *testing.T) {
	t.Run("interval request with bar start timestamp", func(t *testing.T) {
		request := createIntervalRequest("spy", "R91", createConfig(5, "S", false, false))

		assert.Equal(t, "HIT,SPY,5,20190122,20190221,,,,1,,,S,1", request)
	})
	t.Run("interval request with bar end timestamp", func(t *testing.T) {
		request := createIntervalRequest("spy", "R91", createConfig(5, "S", true, false))

		assert.Equal(t, "HIT,SPY,5,20190122,20190221,,,,1,,,S", request)
	})
}

func TestCreateTickRequest(t *testing.T) {
	t.Run("tick request", func(t *testing.T) {
		request := createTickRequest("spy", "R91", createConfig(0, "", false, false))

		assert.Equal(t, "HTT,SPY,20190122,20190221,,,,1,", request)
	})
}

func createConfig(intervalLength int, intervalType string, endTimestamp bool, tsv bool) *Config {
	var config = Config{
		protocol:        "5.1",
		command:         "",
		startDate:       "20190122",
		endDate:         "20190221",
		outDirectory:    "data",
		intervalType:    intervalType,
		intervalLength:  intervalLength,
		parallelism:     8,
		tsv:             tsv,
		detailedLogging: false,
		gzip:            false,
		endTimestamp:    endTimestamp,
		useLabels:       false,
	}

	if intervalLength > 0 && !endTimestamp {
		config.protocol = "6.0"
		config.useLabels = true
	}

	return &config
}
