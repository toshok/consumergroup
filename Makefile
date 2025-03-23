
all::

.PHONY: protos
protos:
	protoc \
	  --proto_path=protos \
	  --go_out=pkgs \
	  --go_opt=Mconsumergroup.proto=./proto_gen \
	  --go-grpc_out=pkgs \
	  --go-grpc_opt=Mconsumergroup.proto=./proto_gen \
	  protos/consumergroup.proto

.PHONY: testcoordinator
testcoordinator:
	go build ./cmd/testcoordinator/...

.PHONY: testworker
testworker:
	go build ./cmd/testworker/...

all:: protos testcoordinator testworker
