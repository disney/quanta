# ########################################################## #
# Makefile for Golang Project
# Includes cross-compiling, installation, cleanup
# ########################################################## #

.PHONY: list clean install build build_all test vet lint format all

# Check for required command tools to build or stop immediately
EXECUTABLES = go pwd uname date cat
K := $(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),some string,$(error "No $(exec) in PATH)))

# Vars
ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BIN_DIR=bin
BIN_NODE=quanta-node
BIN_PROXY=quanta-proxy
BIN_KINESIS=quanta-kinesis-consumer
BIN_PRODUCER=quanta-s3-kinesis-producer
BIN_KCL=quanta-kcl-consumer
BIN_ADMIN=quanta-admin
BIN_RBAC=quanta-rbac-util
BIN_RUN=sqlrunner
COVERAGE_DIR=coverage
COV_PROFILE=${COVERAGE_DIR}/test-coverage.txt
COV_HTML=${COVERAGE_DIR}/test-coverage.html
PKG_NODE=github.com/disney/quanta
PKG_LOADER=github.com/disney/quanta/${BIN_LOADER}
PKG_PROXY=github.com/disney/quanta/${BIN_PROXY}
PKG_KINESIS=github.com/disney/quanta/${BIN_KINESIS}
PKG_KCL=github.com/disney/quanta/${BIN_KCL}
PKG_PRODUCER=github.com/disney/quanta/${BIN_PRODUCER}
PKG_ADMIN=github.com/disney/quanta/${BIN_ADMIN}
PKG_RBAC=github.com/disney/quanta/${BIN_RBAC}
PKG_RUN=github.com/disney/quanta/${BIN_RUN}
#PLATFORMS=darwin linux 
PLATFORM?=linux
ARCHITECTURES?=arm64 amd64
VERSION=$(shell cat version.txt | grep quanta | cut -d' ' -f2)
BUILD=`date +%FT%T%z`
UNAME=$(shell uname)
GOLIST=$(shell go list ./...)

# Binary Build
LDFLAGS=-ldflags "-X main.Version=${VERSION} -X main.Build=${BUILD} -X ${PKG_PROXY}-lib.Version=${VERSION} -X ${PKG_PROXY}-lib.Build=${BUILD} -X ${PKG_ADMIN}-lib.Version=${VERSION} -X ${PKG_ADMIN}-lib.Build=${BUILD}"

# Test Build
LDFLAGS_TEST=-ldflags "-X ${PKG}.Version=${VERSION} -X ${PKG}.Build=${BUILD} -X ${PKG}.Version=${VERSION} -X ${PKG}.Build=${BUILD}"

default: build

all: clean format vet build test install

list:
	@echo "Available GNU make targets..."
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | xargs

vet:
	go vet ${PKG}

lint:
	go get golang.org/x/lint/golint
	golint -set_exit_status ${GOLIST}

format:
	go fmt ${PKG}

build: format vet build_proxy
	$(NOECHO) $(NOOP)

build_node: format vet
	CGO_ENABLED=0 go build -o ${BIN_DIR}/${BIN_NODE}-linux-arm64 ${LDFLAGS} ${PKG_NODE}

build_admin: format vet
	CGO_ENABLED=0 go build -o ${BIN_DIR}/${BIN_ADMIN} ${LDFLAGS} ${PKG_ADMIN}

build_proxy: format vet
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	CGO_ENABLED=0 go build -o $(BIN_DIR)/$(BIN_PROXY)-$(PLATFORM)-$(GOARCH) ${LDFLAGS} ${PKG_PROXY} \
	))

build_all: format vet
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	CGO_ENABLED=0 go build -o $(BIN_DIR)/$(BIN_PROXY)-$(PLATFORM)-$(GOARCH) ${LDFLAGS} ${PKG_PROXY} \
	))
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	CGO_ENABLED=0 go build -o $(BIN_DIR)/$(BIN_KINESIS)-$(PLATFORM)-$(GOARCH) ${LDFLAGS} ${PKG_KINESIS} \
	))
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	CGO_ENABLED=0 go build -o $(BIN_DIR)/$(BIN_ADMIN)-$(PLATFORM)-$(GOARCH) ${LDFLAGS} ${PKG_ADMIN} \
	))
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	CGO_ENABLED=0 go build -o $(BIN_DIR)/$(BIN_NODE)-$(PLATFORM)-$(GOARCH) ${LDFLAGS} ${PKG_NODE} \
	))
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	CGO_ENABLED=0 go build -o $(BIN_DIR)/$(BIN_RBAC)-$(PLATFORM)-$(GOARCH) ${LDFLAGS} ${PKG_RBAC} \
	))
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	CGO_ENABLED=0 go build -o $(BIN_DIR)/$(BIN_RUN)-$(PLATFORM)-$(GOARCH) ${LDFLAGS} ${PKG_RUN} \
	))

push_kinesis_docker: build_all
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	docker buildx build --push --platform linux/${GOARCH} -t containerregistry.disney.com/digital/$(BIN_KINESIS):${VERSION}-$(GOARCH) --build-arg arch="${GOARCH}" --build-arg platform="${PLATFORM}" -f Docker/DeployKinesisConsumerDockerfile . \
	))

push_proxy_docker: build_all
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	docker buildx build --push --platform linux/${GOARCH} -t containerregistry.disney.com/digital/$(BIN_PROXY):${VERSION}-$(GOARCH) --build-arg arch="${GOARCH}" --build-arg platform="${PLATFORM}" -f Docker/DeployProxyDockerfile . \
	))

build_proxy_docker: build_all
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	docker build -t containerregistry.disney.com/digital/$(BIN_PROXY)-$(PLATFORM)-$(GOARCH) --build-arg arch="${GOARCH}" --build-arg platform="${PLATFORM}" -f Docker/DeployProxyDockerfile . \
	))

build_kinesis_docker: build_all
	$(foreach GOARCH, $(ARCHITECTURES),\
	$(shell export GOARCH=$(GOARCH);\
	docker build -t containerregistry.disney.com/digital/$(BIN_KINESIS)-$(PLATFORM)-$(GOARCH) --build-arg arch="${GOARCH}" --build-arg platform="${PLATFORM}" -f Docker/DeployKinesisConsumerDockerfile . \
	))

# note: Consul must be running for this to work
test: build_all
	# tests and code coverage TODO: fix coverage (atw)
	# mkdir -p $(COVERAGE_DIR)
	# export TZ=UTC; go test ${GOLIST} -short -v ${LDFLAGS_TEST} -coverprofile ${COV_PROFILE}
	# go tool cover -html=${COV_PROFILE} -o ${COV_HTML}
	./test/run-go-tests.sh
ifeq ($(UNAME), Darwin)
	# open ${COV_HTML}
endif

test-integration: build_all
	./test/run-go-integration-tests.sh

test-all:  test test-integration

kcl:
	CGO_ENABLED=0 go build -o ${BIN_DIR}/${BIN_KCL} ${LDFLAGS} ${PKG_KCL}

admin:
	CGO_ENABLED=0 go build -o ${BIN_DIR}/${BIN_ADMIN} ${LDFLAGS} ${PKG_ADMIN}

producer:
	CGO_ENABLED=0 go build -o ${BIN_DIR}/${BIN_PRODUCER} ${LDFLAGS} ${PKG_PRODUCER}
	docker build -t containerregistry.disney.com/digital/quanta-s3-producer -f Docker/DeployProducerDockerfile .

kcl:
	CGO_ENABLED=0 go build -o ${BIN_DIR}/${BIN_KCL} ${LDFLAGS} ${PKG_KCL}
	docker build -t containerregistry.disney.com/digital/quanta-kcl-consumer -f Docker/DeployKCLConsumerDockerfile .

docs: 
	go get golang.org/x/tools/cmd/godoc
	open http://localhost:6060/${PKG_NODE}
	godoc -http=":6060"
	
install:
	go install ${PKG_NODE}

# Remove only what we've created
clean:
	if [ -d ${BIN_DIR} ] ; then rm -rf ${BIN_DIR} ; fi
	if [ -d ${COVERAGE_DIR} ] ; then rm -rf ${COVERAGE_DIR} ; fi
	go clean -cache -modcache -i -r
