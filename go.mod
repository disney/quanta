module github.com/disney/quanta

go 1.14

require (
	github.com/Jeffail/tunny v0.0.0-20190930221602-f13eb662a36a
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/akrylysov/pogreb v0.9.1
	github.com/alecthomas/kong v0.2.17
	github.com/araddon/dateparse v0.0.0-20210207001429-0eec95c9db7e
	github.com/araddon/gou v0.0.0-20190110011759-c797efecbb61
	github.com/araddon/qlbridge v0.0.2
	github.com/armon/go-metrics v0.3.4 // indirect
	github.com/aviddiviner/go-murmur v0.0.0-20150519214947-b9740d71e571
	github.com/aws/aws-sdk-go v1.41.7
	github.com/aws/aws-sdk-go-v2 v1.16.4
	github.com/aws/aws-sdk-go-v2/config v1.15.9
	github.com/aws/aws-sdk-go-v2/credentials v1.12.4
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.9.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.26.10
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.6
	github.com/bbalet/stopwords v1.0.0
	github.com/confluentinc/confluent-kafka-go v1.4.2 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/hamba/avro v1.6.0
	github.com/harlow/kinesis-consumer v0.3.5
	github.com/hashicorp/consul/api v1.10.1
	github.com/hashicorp/consul/sdk v0.8.0
	github.com/hashicorp/go-hclog v0.14.1 // indirect
	github.com/hashicorp/go-memdb v1.3.1 // indirect
	github.com/jmoiron/sqlx v1.3.1 // indirect
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.15.2 // indirect
	github.com/leekchan/timeutil v0.0.0-20150802142658-28917288c48d // indirect
	github.com/lestrrat-go/jwx v1.0.8
	github.com/lytics/datemath v0.0.0-20180727225141-3ada1c10b5de // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mb0/glob v0.0.0-20160210091149-1eb79d2de6c4 // indirect
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/mssola/user_agent v0.5.2 // indirect
	github.com/pborman/uuid v1.2.1
	github.com/prometheus/client_golang v1.11.0
	github.com/rlmcpherson/s3gof3r v0.5.0
	github.com/siddontang/go-mysql v1.1.0
	github.com/steakknife/bloomfilter v0.0.0-20180922174646-6819c0d2a570
	github.com/steakknife/hamming v0.0.0-20180906055917-c99c65617cd3 // indirect
	github.com/stretchr/testify v1.7.1
	github.com/stvp/rendezvous v0.0.0-20151118195501-67b5f26b3e18
	github.com/vmware/vmware-go-kcl v1.5.0
	github.com/xitongsys/parquet-go v1.5.5-0.20201031234703-4d9f11317375
	github.com/xitongsys/parquet-go-source v0.0.0-20220527110425-ba4adb87a31b
	golang.org/x/net v0.17.0
	golang.org/x/sync v0.1.0
	golang.org/x/text v0.13.0
	google.golang.org/genproto v0.0.0-20200904004341-0bd0a958aa1d // indirect
	google.golang.org/grpc v1.33.1
	google.golang.org/protobuf v1.27.1
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/confluentinc/confluent-kafka-go.v1 v1.4.2
	gopkg.in/yaml.v2 v2.4.0
)

replace github.com/araddon/qlbridge => github.com/guymolinari/qlbridge v0.0.0-20230216153635-376aef7e44d3
