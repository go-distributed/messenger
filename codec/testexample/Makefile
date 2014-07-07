RELATIVE_GOPATH=../../../../../../

all: gogoproto.proto
	protoc --proto_path=${RELATIVE_GOPATH}/src:${RELATIVE_GOPATH}/src/code.google.com/p/gogoprotobuf/protobuf:. --gogo_out=. gogoproto.proto
	sed -i 's/code\.google\.com/github\.com\/go-distributed\/messenger\/3rdparty\/code.google.com/g' *.go
