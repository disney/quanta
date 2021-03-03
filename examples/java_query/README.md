# Java MySQL proxy JDBC example
This is an example showing how to connect to Quanta and execute queries

## Building
`mvn package assembly:single`

## Running
`java -jar target/jdbc-example-1.0-SNAPSHOT-jar-with-dependencies.jar localhost:4000 tests.sql <jwt_access_token>`

