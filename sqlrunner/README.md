The sql runner is a tool that will execute Sql statements from a file. The tool can be used as both a test tool or a tool that simply executes a set of sql statements including inserts and updates.

**Command Line**
- _Full command line example_:  ./sqlrunner -script_file test.sql -script_delimiter : -validate -env dev -log_level DEBUG
- _Working command line example_:  ./sqlrunner -script_file test.sql -validate -env dev

**Parameters**
- **script_file** - (required) This is a file the sql runner reads and executes.  This file contains a set of Sql statements to execute.
- **script_delimiter** - (optional) The delimiter to use in the file.  The default is a '@'.  This delimiter separates the Sql statement from the expected row count.
- **host** - (required) The Quanta host to connect to.
- **user** - (required) The Quanta user to use to connect.
- **password** - (required) The Quanta password to use to connect.
- **database** - (optional) The Quanta database to connect to.  The default is 'quanta'.
- **port** - (optional) The port to connect to.  The default is 4000.
- **validate** - (optional) boolean that specifies where to perform data validation or not.  If you just want to insert data, this could be removed from the command line.
- **log_level** - set this to DEBUG if you want to enable additional logging.

**Script File**
- Each line in the script file contains the Sql statement to execute.

_Rules_
- Comment out lines by prepending the line with either a # or --
- If you have the validate option enabled, separate the Sql statement from the expected row count with a '@' (or whatever delimiter you specify).
- Only Select, Insert, and Update statements are currently supported.  Insert and Update statements do not require an expected rowcount with the validate option.
- quanta-admin statements - any valid quanta-admin statement can be added to the sql script file.
- To test error conditions, add the error number after the delimiter ('@' by default).

_Statement Line Examples_
- -- This is a commented line.  It could also begin with a pound (#).
- -- The following insert statement does not have an expected rowcount:
- insert into customers (cust_id, name, address, city, state, zip, phone, phoneType) values('1','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');
- -- The following select statement does have an expected rowcount.
- select * from customers where name = 'Abe';@1 
- -- The following quanta-admin statement drops a table
- quanta-admin drop customers_qa
- -- The following statement tests an error condition
- select * from badtable;@1105

#### Running with local quanta-in-a-box

Make sure that consul is running locally on port 8500.
In a terminal:

```consul agent -dev```

Check the README in ./start-local Run it. 

Here is an example command line (cd to sqlrunner).

```
go run ./driver.go -script_file ./sqlscripts/joins_sql.sql -validate -host 127.0.0.1 -user MOLIG004 -db quanta -port 4000 -log_level DEBUG
```

or, in vs code launch.json

```
 {
    "name": "Sql test join",
    "type": "go",
    "request": "launch",
    "mode": "auto",
    "program": "./sqlrunner/driver.go",
    "args": ["-script_file", "./sqlscripts/joins_sql.sql",
    "-validate",
    "-host","127.0.0.1",
    "-user", "MOLIG004",
    "-db","quanta", 
    "-port","4000", 
    "-log_level", "DEBUG"
    ]
}
```
