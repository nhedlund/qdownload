# qdownload

[![Build Status](https://dev.azure.com/orisoft/qdownload/_apis/build/status/nhedlund.qdownload?branchName=master)](https://dev.azure.com/orisoft/qdownload/_build/latest?definitionId=7&branchName=master)

qdownload is a command line IQFeed CSV market data download tool that can download EOD bars, minute bars or tick data.

Use it to download a list of symbols, or a large number of symbols stored in a text file.

<p align="center"><img src="/demo2.gif?raw=true"/></p>

## Features

* Daily bars
* Minute bars
* Interval bars (volume, ticks or seconds)
* Tick data
* Parallel downloads (8 by default)
* CSV (default) or TSV format
* Uncompressed (default) or GZipped files
* Start and end date filter (all data by default)
* Bars timestamps at start of bar (default), or end of bar
* Optional time zone conversion of timestamps

## Requirements

* Go to compile the project: [Download Go](https://golang.org/dl/)
* IQFeed subscription and client:
  * Windows and Mac: [IQFeed Client](http://www.iqfeed.net/index.cfm?displayaction=support&section=download)
  * Linux: [IQFeed Docker Image](https://github.com/aanari/iqfeed-docker)

## Installation

Use go get and install to download and compile qdownload:

```bash
go get -u github.com/nhedlund/qdownload
go install github.com/nhedlund/qdownload
```

## Usage

Run qdownload without any arguments for usage:

```bash
$ qdownload
USAGE:
   qdownload [global options] command [command options] <symbols or symbols file>

COMMANDS:
     eod       Download EOD bars
     minute    Download minute bars
     tick      Download tick data
     interval  Download interval bars: <length> <seconds|volume|ticks>
     help, h   Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --start value, -s value        start date filter: yyyymmdd
   --end value, -e value          end date filter: yyyymmdd
   --out value, -o value          output directory (default: "data")
   --timezone value, -z value     timestamps time zone (default: "ET")
   --parallelism value, -p value  number of parallel downloads (default: 8)
   --tsv, -t                      use tab separator instead of comma
   --detailed-logging, -d         detailed log output
   --gzip, -g                     compress files with gzip
   --end-timestamp, -m            use end of bar timestamps instead of start
   --help, -h                     show help
```

Download Apple and Microsoft stock price minute bars from 2007 to today:

```bash
$ qdownload minute aapl,msft
   • Read symbols              symbols=2
   • Downloading               symbol=AAPL
   • Downloading               symbol=MSFT
   • Completed                 duration=19177ms rows=1428612 symbol=MSFT
   • Completed                 duration=43670ms rows=1853585 symbol=AAPL
```

Download daily bars for 10 different symbols from a list in symbols.txt:

```bash
$ qdownload eod symbols.txt
   • Read symbols              symbols=10
   • Downloading               symbol=FB
   • Downloading               symbol=SPY
   • Downloading               symbol=NFLX
   • Downloading               symbol=AMZN
   • Downloading               symbol=GOOG
   • Downloading               symbol=AAPL
   • Downloading               symbol=GLD
   • Downloading               symbol=SLV
   • Completed                 duration=468ms rows=1238 symbol=GOOG
   • Downloading               symbol=USO
   • Completed                 duration=473ms rows=1703 symbol=FB
   • Downloading               symbol=FARM
   • Completed                 duration=612ms rows=4219 symbol=NFLX
   • Completed                 duration=611ms rows=3229 symbol=SLV
   • Completed                 duration=624ms rows=3591 symbol=GLD
   • Completed                 duration=745ms rows=5828 symbol=SPY
   • Completed                 duration=769ms rows=7342 symbol=AAPL
   • Completed                 duration=876ms rows=5478 symbol=AMZN
   • Completed                 duration=604ms rows=3242 symbol=USO
   • Completed                 duration=608ms rows=5429 symbol=FARM
```

Download volume 1000 interval bars for SPY starting from 2019-04-18:

```bash
$ qdownload -s 20190418 interval 1000 volume spy
   • Using newer protocol required for bar start timestamps, requiring at least IQFeed 6.0
   • Read symbols              symbols=1
   • Downloading               symbol=SPY
   • Completed                 duration=1126ms rows=50104 symbol=SPY
```

Download volume 1000 interval bars for SPY with timestamp at end of bar,
starting from 2019-04-18:

```bash
$ qdownload -m -s 20190418 interval 1000 volume spy
   • Read symbols              symbols=1
   • Downloading               symbol=SPY
   • Completed                 duration=1120ms rows=50359 symbol=SPY
```

### Timezone support

By default intraday timestamps use the default IQFeed time zone US Eastern Time.

However it is possible to output timestamps in a different timezone.

Use the -z option to set the output timezone to either a shortcut alias or an IANA time zone from:
[Time Zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)

Examples:

* ET or EST for US Eastern Time (New York)
* CT or CST for US Central Time (Chicago)
* PT or PST for US Pacific Time (Los Angeles)
* UTC
* America/New_York
* Europe/Stockholm
