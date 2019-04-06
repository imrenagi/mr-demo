Simplified Map Reduce Engine in Go
===

This demo is inspired from MIT distributed system course

Test sequential word count:
```
cd src/main
./test-wc.sh
```

Test distributed word count:
```
cd src/main
go run wc.go master localhost:7777 pg-*.txt
go run wc.go worker localhost:7777 localhost:7779 
go run wc.go worker localhost:7777 localhost:7778
...
```

Test map reduce engine:
```
cd src/mapreduce
go test -run Sequential
go test -run TestParallel
go test -run Failure
```
