# Kafka consumergroup-like api for non-kafka workloads

Sometimes you have partitioned or sharded data, and need to run periodic tasks on it.  Consumergroup
lets you scale a pool of workers that will coordinate on that task execution.


100% not ready for anything.  just look how few tests there are.

Building it
```
% make
protoc \
	  --proto_path=protos \
	  --go_out=pkgs \
	  --go_opt=Mconsumergroup.proto=./proto_gen \
	  --go-grpc_out=pkgs \
	  --go-grpc_opt=Mconsumergroup.proto=./proto_gen \
	  protos/consumergroup.proto
go build ./cmd/testcoordinator/...
go build ./cmd/testworker/...
```

Then run `./testcoordinator` and (in separate terminals) as many `./testworker`s as you'd like.  Both
will log a bunch, and each `testworker` will print out its current partition assignment whenever it
receives a new one.

## Todo

* look at the kafka consumer group api some more and make things a bit closer
* actually write tests that exercise the myriad corner cases
* give the coordinator some sort of durable store.  right now everything is in memory.
* fix sticky assignment, and figure out how to apply non-STW rebalancing stuff kafka is using.

## Questions
* maybe look at the kafka code and see if there's a way to pull out the consumergroup code and use it directly?  have I wasted my time here?
* for non-kafka situations, it seems like high partition count can be harmful for performance. is there a sane way to make multiple workers cooperate on the workload in a _single_ partition?