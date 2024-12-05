module github.com/disney/quanta

go 1.21

toolchain go1.23.2

require (
	github.com/Jeffail/tunny v0.0.0-20190930221602-f13eb662a36a
	github.com/RoaringBitmap/roaring/v2 v2.4.2
	github.com/akrylysov/pogreb v0.9.1
	github.com/alecthomas/kong v0.2.17
	github.com/araddon/dateparse v0.0.0-20210207001429-0eec95c9db7e
	github.com/araddon/gou v0.0.0-20211019181548-e7d08105776c
	github.com/aviddiviner/go-murmur v0.0.0-20150519214947-b9740d71e571
	github.com/aws/aws-sdk-go v1.44.146
	github.com/aws/aws-sdk-go-v2 v1.16.4
	github.com/aws/aws-sdk-go-v2/config v1.15.9
	github.com/aws/aws-sdk-go-v2/credentials v1.12.4
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.9.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.26.10
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.6
	github.com/banzaicloud/logrus-runtime-formatter v0.0.0-20190729070250-5ae5475bae5e
	github.com/bbalet/stopwords v1.0.0
	github.com/bufbuild/protocompile v0.14.1
	github.com/dnaeon/go-uuid-endianness v0.0.0-20180112091600-de1a6da5f8b7
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.5.3
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.6.0
	github.com/hamba/avro/v2 v2.15.0
	github.com/harlow/kinesis-consumer v0.3.5
	github.com/hashicorp/consul/api v1.10.1
	github.com/hashicorp/consul/sdk v0.8.0
	github.com/hashicorp/go-memdb v1.3.1
	github.com/jmespath/go-jmespath v0.4.0
	github.com/jmoiron/sqlx v1.3.1
	github.com/leekchan/timeutil v0.0.0-20150802142658-28917288c48d
	github.com/lestrrat-go/jwx v1.2.26
	github.com/lytics/cloudstorage v0.2.13
	github.com/lytics/datemath v0.0.0-20180727225141-3ada1c10b5de
	github.com/mattn/go-sqlite3 v1.14.6
	github.com/mb0/glob v0.0.0-20160210091149-1eb79d2de6c4
	github.com/mssola/user_agent v0.5.2
	github.com/pborman/uuid v1.2.1
	github.com/prometheus/client_golang v1.11.0
	github.com/rlmcpherson/s3gof3r v0.5.0
	github.com/siddontang/go-mysql v1.1.0
	github.com/sirupsen/logrus v1.8.1
	github.com/steakknife/bloomfilter v0.0.0-20180922174646-6819c0d2a570
	github.com/stretchr/testify v1.9.0
	github.com/stvp/rendezvous v0.0.0-20151118195501-67b5f26b3e18
	github.com/vmware/vmware-go-kcl v1.5.0
	github.com/xitongsys/parquet-go v1.5.5-0.20201031234703-4d9f11317375
	github.com/xitongsys/parquet-go-source v0.0.0-20220527110425-ba4adb87a31b
	golang.org/x/net v0.10.0
	golang.org/x/sync v0.8.0
	golang.org/x/text v0.9.0
	google.golang.org/api v0.103.0
	google.golang.org/grpc v1.53.0
	google.golang.org/protobuf v1.34.2
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/confluentinc/confluent-kafka-go.v1 v1.4.2
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cloud.google.com/go v0.107.0 // indirect
	cloud.google.com/go/compute v1.15.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.8.0 // indirect
	cloud.google.com/go/storage v1.28.0 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/apache/thrift v0.13.1-0.20201008052519-daf620915714 // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.3.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.13.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.7 // indirect
	github.com/aws/smithy-go v1.11.2 // indirect
	github.com/awslabs/kinesis-aggregation/go v0.0.0-20210630091500-54e17340d32f // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/confluentinc/confluent-kafka-go v1.4.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/fatih/color v1.9.0 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-hclog v0.12.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/serf v0.9.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.15.2 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.1 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/go-testing-interface v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pingcap/errors v0.11.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07 // indirect
	github.com/steakknife/hamming v0.0.0-20180906055917-c99c65617cd3 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/crypto v0.9.0 // indirect
	golang.org/x/oauth2 v0.4.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
