
# Quanta

Quanta - Generalized roaring bitmap based HTAP database engine.


[![](https://godoc.org/github.com/awootton/quanta?status.svg)](http://godoc.org/github.com/awootton/quanta) 
[![Go Report Card](https://goreportcard.com/badge/github.com/awootton/quanta)](https://goreportcard.com/report/github.com/awootton/quanta) 
![build](https://github.com/awootton/quanta/actions/workflows/build+test.yml/badge.svg)


It is built around the [Roaring Bitmap Libraries](http://RoaringBitmap.org) and emulates a subset of the MySQL networking protocol.  In many ways it can be used as a drop in replacement for the MySQL engine.  It does not currently implement transactions and stored procedure (although user defined functions (UDF) is supported.).  This approach enables access to a large ecosystem of database drivers and tools.

It's primary advantage over other database platforms is that it supports subsecond access to large data sets and supports updates in real time.  The secret sauce is in that data is compressed as it is imported into the platform and can
be directly accessed in this format.  High cardinality strings are stored in a persistent hashtable that is distributed accross multiple data server nodes.   The architecuture is similar to Apache Cassandra and is scalable and fault tolarant.  Longer term goals of the platform roadmap include active/active HA/DR across multiple data centers.


# Requirements 

Go version 1.14.14 or later.
HashiCorp Consul 1.4.x or later.


# Getting Started

A quick start guide can be found here (//TODO)

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

