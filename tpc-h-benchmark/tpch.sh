# "ASSUME_ROLE_ARN=arn:aws:iam::141988508569:role/service-dfs-prod-data-consumer-role" \
#    -e "ASSUME_ROLE_ARN_REGION=us-east-1" \
go run tpc-h-kinesis-producer.go ~/TPC-H\ V3.0.1/dbgen region loadtest
go run tpc-h-kinesis-producer.go ~/TPC-H\ V3.0.1/dbgen nation loadtest
go run tpc-h-kinesis-producer.go ~/TPC-H\ V3.0.1/dbgen customer loadtest
go run tpc-h-kinesis-producer.go ~/TPC-H\ V3.0.1/dbgen part loadtest
go run tpc-h-kinesis-producer.go ~/TPC-H\ V3.0.1/dbgen supplier loadtest
go run tpc-h-kinesis-producer.go ~/TPC-H\ V3.0.1/dbgen partsupp loadtest
go run tpc-h-kinesis-producer.go ~/TPC-H\ V3.0.1/dbgen orders loadtest
go run tpc-h-kinesis-producer.go ~/TPC-H\ V3.0.1/dbgen lineitem loadtest
