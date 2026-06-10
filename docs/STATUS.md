#Current State
Currently re-engaging with the code base.  Focusing on Quanta-in-a-Box quick start environment.  Gaps in the documentation.   Need a dirt simple quick start script and documenation with sample data and queries.  Build engagement with the OSS community.

#Known Limitations
Quanta stores data in-memory and has no paging ability.  The closest analog is Redis.   There are utility functions in the admin tool to offline older data as necessary.

#Supported SQL
* There is no support for DDL functions to create tables.  Defining database tables is via the creation of JSON files per the schema definition documentation and submitting schema files to the quanta admin tool.
* There is support for SQL insert, update, and delete for ad-hoc data manipulation.  For bulk loading data there are streaming load tools that are much more performant.
* The current syntax for SQL predicates (WHERE) do not support comparisons between columns, only columns against static values
* Prepared SQL is supported for performance.
* ORDER BY is supported but the result set must fit in memory.
* Basic support for INNER and OUTER joins.  At the moment only single aggregates are supported (need to verify this as we need this for GROUP BY support.
* Basic subquery support.  (Needs verification a testing)
* If a field is a standard bitmap of the StringEnum type, it is possible to use the topn() function to generate rankings.  For example if a column in a table stores a name of a country or country code you could query the database to return a list of the countries by population ranked descending or ascending.
* GROUP BY support is in progress.  Needs examples and tests.  It may be possible to implement this using the built in functionality within qlbridge (with HAVING as well).  However, StringEnum types could be leveraged to very rapidly group data.  I believe this was at least partially implemented.  Could be a very powerful analytical feature.
* There is support for user defined functions in the SELECT list and the WHERE clauses as well.  Need extensive tests.
* It is possible to unload data in various formats (Parquet, CSV, etc.) using SELECT INTO.  Adapters exist for AWS S3 as well.
* Need to support multiple aggregates in the SELECT list.  

#TPC-H Coverage
*  Need to figure out how large of a data set can fit in memory with QIAB.  There already are schema files for representing and loading the TPC-H data.
*  Need to choose a few TPC-H queries to try. Will definately need multipe aggregates per select with GROUP by.
*  Need to have a highly performant data load capability to be competitive with current benchmarks.

#Quanta-in-a-box
QIAB is in fairly good shape.  Just need documentation, simple startup scripts and good demo data with example queries.

#Streaming Demo
* A constand public domain source of streaming data is available on reversebeacon.net via telnet.  I wrote a streaming adapter to load data into quanta.  Need to dig up this code.
* Search for other data sources that are public domain.

#Known Technical Debt
* Clean up unit tests.  Many are commented out.
* Lots of dead code cleanup
* Lots of things to do.
