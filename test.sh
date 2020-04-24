#!/bin/bash
set -ex

logfile=~/go/src/github.com/zakimal/raft/log

go test -v -race -run $@ | tee ${logfile}

go run ./tools/log-visualizer.go < ${logfile}