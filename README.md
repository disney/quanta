# Quanta Overview

*Quanta* is an open-source, generalized HTAP (Hybrid Transactional/Analytical Processing) database engine built on the Roaring Bitmap libraries. Designed as a highly performant alternative to traditional databases, *Quanta* emulates a subset of the MySQL networking protocol, providing compatibility with many MySQL drivers and tools. While it doesn’t support transactions or stored procedures, *Quanta* enables access to a wide ecosystem of MySQL-compatible resources and does support user-defined functions (UDFs).

The primary advantage of *Quanta* is its ability to provide subsecond access to large datasets with real-time updates. Data is compressed upon import and accessed directly in this format, allowing for high-performance querying on highly compressed data. Additionally, *Quanta* manages high cardinality strings by storing them in a distributed, persistent hashtable across Data Nodes. The architecture is similar to Apache Cassandra, allowing for both scalability and fault tolerance, with a future roadmap goal to enable active/active high availability and disaster recovery across multiple data centers.

## Architecture

The architecture of *Quanta* supports horizontal scalability, low-latency access, and efficient data ingestion and querying. Here are the core components:

- **Client Applications**: Applications connect to *Quanta* using industry-standard MySQL drivers, which communicate with the Query Processor via a Network Load Balancer.

- **Query Processor (Proxy)**: This component handles SQL queries from client applications. Multiple instances of the Query Processor are deployed for scalability, and a load balancer distributes MySQL connections across all instances. Each Query Processor can connect to all active Data Nodes, where data is transmitted as serialized byte arrays representing compressed bitmaps. The Query Processor re-hydrates these bitmaps and performs bitmap operations (e.g., AND, OR, difference) to deliver the final query response.

- **Data Nodes**: These nodes form the primary storage and processing layer, organized as a cluster to handle data ingestion and retrieval tasks. Data Nodes communicate with the Query Processor via gRPC, sending compact byte arrays to optimize network load. *Quanta* also stores high cardinality strings in a distributed hashtable across Data Nodes for efficient retrieval.

- **Kinesis Consumers**: The Kinesis Consumers ingest data streams from Amazon Kinesis, communicating with the Data Nodes via gRPC. They transform incoming data into bitmaps, buffer and aggregate them with OR operations, and then send the results to the appropriate Data Nodes for storage. This approach allows *Quanta* to pre-aggregate data efficiently before it reaches the Data Nodes.

- **Consul (Service Discovery and Metadata Storage)**: Consul enables service discovery by identifying the network endpoints of active Data Nodes, which are then accessible to upstream components like the Query Processors and Kinesis Consumers. Consul also leverages a key/value store to manage schema metadata for tables and fields, enabling consistent access to schema information across the system.

## Roadmap

*Quanta*'s roadmap focuses on expanding SQL capabilities, scalability, and performance optimization. Key goals include:

1. **Enhanced SQL Support**: Adding support for SQL features like GROUP BY, HAVING clauses, and multiple aggregations in the SELECT list to enable complex analytical queries, particularly for TPC-H benchmarking.
   
2. **Autoscaling and Resource Optimization**: Developing an autoscaler to dynamically add or remove Data Nodes based on workload, with metrics-driven scaling to manage resource use efficiently.

3. **Optimized Data Distribution and Load Balancing**: Improving the data distribution strategy based on resource utilization. Dynamic data distribution will help balance load across nodes more effectively.

4. **Active/Active HA/DR**: Implementing active/active high availability and disaster recovery across multiple data centers for improved resilience.

5. **Conflict Resolution and Time Synchronization**: Adding conflict resolution strategies using vector clocks or version vectors. With AWS’s Time Sync service, *Quanta* aims to eventually support microsecond-level time synchronization for conflict management.

6. **GPU-Accelerated Bitmap Processing**: Exploring GPU acceleration for core bitmap operations to improve performance on large-scale data processing tasks.


# Requirements 

Go version 1.14.14 or later.
HashiCorp Consul 1.4.x or later.


# Getting Started

[A Quick Start Guide can be found here](https://github.com/disney/quanta/tree/master/test/README.md)

# Build and Deployment

[Build and Deployment Instructions](https://github.com/disney/quanta/tree/master/Docker/README.md)

# Configuration Documentation
[Schema File Configuration Docs](https://github.com/disney/quanta/tree/master/configuration/README.md)

# Tool and Driver compatibility
* MySQL command line client (5.7.0)
* Java JDBC driver (8.0.11)
* Python MySQL connector
* Node.js MySQL driver
* MySQL Workbench (coming soon!)

# Authentication
OpenID connect (JWT tokens) is the currently supported mechanism for authentication.  This
is used to gate access to the quanta proxy service allowing access to the underlying database.  An external service
such as AWS Cognito is required to manage user accounts and issue valid tokens.  User policies and governance are
managed in this service layer.

JWT tokens can be utilized in two ways:
1. For connections via a database driver (Java JDBC, Python connector, etc.) the access token can be submitted
via the 'userName' specifier.  The 'password' parameter should contain an empty string.
2. For connections using MySQL compatible tools, the proxy has a separate web service endpoint (typically
port 4001) that can be used to redeem access tokens for MySQL compatible username/password combination.  These
credentials can then be used to satisfy this use case.  These credentials are temporary and expire according to
the access token TTL.

## Cognito Command Line Tool
[Here is the location of a helpful OSS tool for managing tokens](https://github.com/RafPe/go-cognito-authy). It
can be used to change your password and obtain new tokens.  This is an example of how to use AWS cognito.  Please
refer to your authentication provider documentation for more info.

## Token Exchange Service
Example of service usage:
```bash
curl -v -H "Content-Type: application/text" -d "eyJraWQiOiJTaWlTQmJrYUhJYlVnMTFwelplTk9tbENNdWxHWk5lRkVoK1Y0MldqSlVzPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI1MWI2YWFhZS0wYWM4LTRjNjAtYWQ4ZS1hZjAwYmE5ZjNjM2UiLCJldmVudF9pZCI6IjliNzQ3NjJjLTg0OWItNDkzZS1hODFhLWZmZjFkZDU4Mjc4YiIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYXdzLmNvZ25pdG8uc2lnbmluLnVzZXIuYWRtaW4iLCJhdXRoX3RpbWUiOjE2MTE3ODIxNDUsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC51cy1lYXN0LTEuYW1hem9uYXdzLmNvbVwvdXMtZWFzdC0xX2hRVjdYRTJKeiIsImV4cCI6MTYxMTg2ODU0NSwiaWF0IjoxNjExNzgyMTQ1LCJqdGkiOiI1ZTI4ZmYyNS0yODllLTQxODQtODkyNy00Yjc0ZWE0YmUwZWYiLCJjbGllbnRfaWQiOiIxbmpsbWc0ajluN3NqZjM0bzJxdWpkb2FzOSIsInVzZXJuYW1lIjoiZ21vbGluYXIifQ.EqVcCsKs0ABUsrJ7xV_btySlSxMjEibTNEXEQd7cIScKBTaougB3Uwm68O_8Z-II-A85xUlvV74Xb9QDzwM86eJNMYeME4eS9lS_OBZMYTesYdKkh-SBNU2htIbMJQRUiUhQMPMFmX06ex-sprlZjdmNIBYhqOR2J8mbKzWU2RZk_Dt3EmcVVPJJX13SRE-kx3g33tTSJaquSJAD-mjDirrcZg3zRQ4hcRJyf8gb8p97iZmQFh3K8XmmRuJDuDa_6c_hj0p_iHfm2pdJLTP1mhnJ16LE5kE3NIT8t0-Bo4bYF6c9xfNKdOAZk8AmU68bHIi_Msz1MYu3nWWK4iQujg" http://10.0.210.181:4001/
```


# Road Map
The current version is 0.8 and is currently in "alpha" state.

## Version 0.9
This version once release will be considered "beta" and will include the following:
* Cluster administration/monitoring tools and API.
* Ability to add/remove data nodes in a running cluster.
* RBAC interfaces.
* Support for SQL Subqueries.
* Support for temporary tables.


## Version 1.0
* Support for SQL Views.
* Hierarchic (non-Star Schema) joins.
* Intermediate results caching.


# Issues

The process for reporting bugs is as follows:

1. Write a unit test that reproduces the issue.
2. Create a branch off of develop containing the test case.
3. Submit a pull request.


# Contributing

Contributions are always welcome.  The process is straightforward:

1. Create your feature branch off of the develop branch (git checkout -b my-new-feature)
2. Write Tests!
3. Make sure the codebase adhere to the Go coding standards by executing `gofmt -s -w ./` followed by `golint`
4. Commit your changes (git commit -am 'Add some feature')
5. Push to the branch (git push origin my-new-feature)
6. Create new Pull Request.

