# qdownload

[![Build Status](https://dev.azure.com/orisoft/qdownload/_apis/build/status/nhedlund.qdownload?branchName=master)](https://dev.azure.com/orisoft/qdownload/_build/latest?definitionId=7&branchName=master)

qdownload is a command line IQFeed CSV market data download tool that can download EOD bars, minute bars or tick data.

Use it to download a list of symbols, or a large number of symbols stored in a text file.

## Features

* Daily bars
* Minute bars
* Tick data
* Parallel downloads (8 by default)
* CSV (default) or TSV format
* Uncompressed (default) or GZipped files
* Start and end date filter (all data by default)

## Requirements

* Go to compile the project: [Download Go](https://golang.org/dl/)
* IQFeed subscription and client:
  * Windows and Mac: [IQFeed Client](http://www.iqfeed.net/index.cfm?displayaction=support&section=download)
  * Linux: [IQFeed Docker Image](https://github.com/aanari/iqfeed-docker)

## Installation

Use go get to download and compile qdownload:

```bash
go get -u github.com/nhedlund/qdownload
```

## Usage

Run qdownload without any arguments for usage:

```bash
$ qdownload
USAGE:
   qdownload [global options] command <symbols or symbols file>

COMMANDS:
     eod      Download EOD bars
     minute   Download minute bars
     tick     Download tick data
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --start value, -s value        start date filter: yyyymmdd
   --end value, -e value          end date filter: yyyymmdd
   --out value, -o value          output directory (default: "data")
   --parallelism value, -p value  number of parallel downloads (default: 8)
   --tsv, -t                      use tab separator instead of comma
   --detailed-logging, -d         detailed log output
   --gzip, -g                     compress files with gzip
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

## Demo

<p align="center"><img src="/demo.gif?raw=true"/></p>


