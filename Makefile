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
BIN_LOADER=quanta-loader
BIN_PROXY=quanta-proxy
BIN_KINESIS=quanta-kinesis-consumer
BIN_PRODUCER=quanta-s3-kinesis-producer
BIN_KCL=quanta-kcl-consumer
BIN_ADMIN=quanta-admin
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
#PLATFORMS=darwin linux 
PLATFORMS=linux 
ARCHITECTURES=amd64
VERSION=$(shell cat version.txt | grep quanta | cut -d' ' -f2)
BUILD=`date +%FT%T%z`
UNAME=$(shell uname)
GOLIST=$(shell go list ./...)

# Binary Build
LDFLAGS=-ldflags "-X main.Version=${VERSION} -X main.Build=${BUILD}"

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
	go get github.com/GoASTScanner/gas/cmd/gas
	golint -set_exit_status ${GOLIST}
	#gas ${PKG}

format:
	go fmt ${PKG}

build: format vet
	go build -o ${BIN_DIR}/${BIN_NODE} ${LDFLAGS} ${PKG_NODE}
	go build -o ${BIN_DIR}/${BIN_PROXY} ${LDFLAGS} ${PKG_PROXY}

build_all: format vet
	$(foreach GOOS, $(PLATFORMS),\
	$(foreach GOARCH, $(ARCHITECTURES), $(shell export GOOS=$(GOOS); export GOARCH=$(GOARCH); go build -v -o $(BIN_DIR)/$(BIN)-$(GOOS)-$(GOARCH) $(LDFLAGS) $(PKG))))

test: build_all
	# tests and code coverage
	mkdir -p $(COVERAGE_DIR)
	export TZ=UTC; go test ${GOLIST} -short -v ${LDFLAGS_TEST} -coverprofile ${COV_PROFILE}
	go tool cover -html=${COV_PROFILE} -o ${COV_HTML}
ifeq ($(UNAME), Darwin)
	open ${COV_HTML}
endif

kinesis:
	go build -o ${BIN_DIR}/${BIN_KINESIS} ${LDFLAGS} ${PKG_KINESIS}

kcl:
	go build -o ${BIN_DIR}/${BIN_KCL} ${LDFLAGS} ${PKG_KCL}

admin:
	go build -o ${BIN_DIR}/${BIN_ADMIN} ${LDFLAGS} ${PKG_ADMIN}

loader:
	go build -o ${BIN_DIR}/${BIN_LOADER} ${LDFLAGS} ${PKG_LOADER}

producer:
	go build -o ${BIN_DIR}/${BIN_PRODUCER} ${LDFLAGS} ${PKG_PRODUCER}
	docker build -t containerregistry.disney.com/digital/quanta-s3-producer -f Docker/DeployProducerDockerfile .

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
