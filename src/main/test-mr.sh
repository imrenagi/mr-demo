#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
export GOPATH="$here/../../"
echo ""
echo "==> Part 1: Run sequential map reduce"
go test -run Sequential mapreduce/...
echo ""
echo "==> Part 2: Run sequential word count"
(cd "$here" && sh ./test-wc.sh > /dev/null)
echo ""
echo "==> Part 3: Run paralel map reduce"
go test -run TestParallel mapreduce/...
echo ""
echo "==> Part 4: Run fault tolerant map reduce"
go test -run Failure mapreduce/...

rm "$here"/mrtmp.* 
