all: protogen
PROTOWRAP=\
	protowrap \
		-I $${GOPATH}/src \
		-I $${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		--go_out=Mgoogle/api/annotations.proto=github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,plugins=grpc:$${GOPATH}/src \
		--grpc-gateway_out=logtostderr=true:. \
		--swagger_out=logtostderr=true:. \
		--proto_path $${GOPATH}/src \
		--print_structure \
		--only_specified_files

protogen:
	export CWD=$$(pwd) && \
	cd $${GOPATH}/src && \
		$(PROTOWRAP) $${CWD}/**/*.proto
	go install -v github.com/fuserobotics/reporter/dbproto
	rm ./dbproto/*.swagger.json
