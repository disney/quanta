# Configuration (schema.yaml and related)

## Table level
### tableName (required)
Specifies the name of the underlying roaring bitmap index.
Example
```
tableName: cityzip
```

### primaryKey (required)
The primary key field name.  Points to the `fieldName` name of an attribute that is defined under Attributes. If 
the primary key is comprised of multiple fields then use a `+` sign to designate a compound key.  
* Note: if the table is partitioned by time with a (`timeQuantumType`) then the first field must be a `Date` or 
`DateTime` type.
Example
```
primaryKey: zip+id
```

### secondaryKeys (optional)
The alternate key definitions.  Similar to how primary keys are defined however multiple keys can be specified separated by commas.
Example
```
secondaryKeys: city+zip
```

### timeQuantumType (optional)
This string value (typically 'YMD') is used to specify the granularity of time partition.  If not specified then 
the table is not partitioned.


## Attribute configuration

### sourceName
Path to the attribute in the Parquet file.   

### fieldName (required)
The name of the attribute as it will appear in the table.  Must consist of lowercase letters, numbers and underscore characters.  The first character must be a letter.

### type (required)
The underlying data type of the attribute used for representation in the backing store.  Must be one of the following:

| Type       | Description                                                                                                 |
|------------|-------------------------------------------------------------------------------------------------------------|
| `NotExist` | This type is generating by the **analyzer** if the data type cannot be determined.                          |
| `String`   | Alphanumeric string values.  A corresponding `maxLen` parameter should be provided.                         |
| `Integer`  | Integer values up to a maximum of 32 bits.  `minValue` and `maxValue` parameters should be specified.       |
| `Float`    | Floating point values.  A corresponding `fractionLen` should be provided.                                   |
| `Date`     | Date value.  Format is determined dynamically.                                                              |
| `DateTime` | Timestamp value.  Similar to date.                                                                          |
| `Boolean`  | Boolean value.                                                                                              |
| `JSON`     | This type is useful when custom mappings are employed.  Structure parsed and valuated by the custom plugin. |

### configuration (optional) (required for custom mappers)
Configuration for custom mappers.  This is applicable when the `mappingStrategy` parameter is set to either **Custom** or **CustomBSI**


### mappingStrategy (required)
(See the section under Mapping Strategies for the full documentation of built in mappers)
For custom mappers this value must be **Custom** for ranked or timeseries field types or **CustomBSI** for BSI fields.
BSI fields can be `Integer`, `Float`, `Date`, or `DateTime` types.  They can support range queries but not "TopN" aggregates.
Standard bitmap  field types cannot support range queries but can support "TopN".  In practice this is not a problem because
enumerations naturally fit with ranked types but not range queries.  

A special `mappingStrategy` **Delegated** is provided in situations where a custom mapper is provided for an attribute 
that has a `sourceName` with a custom mapper but generates data to underlying roaring data fields of a different name
and or type.  These "target" fields should be marked as `Delegated`.  In a nutshell the underlying system just makes 
sure to create these fields when the **loader** process initializes.  The custom mapper code does the actual
population of the value(s).  


### desc (optional)
This string value is a human readable description of the attribute.  It is passed onto the UI layer if provided.

### minValue (optional, applies only for Integer and Float types)
This signed numeric value specifies the smallest possible value for BSI type fields. For `Float` types it should 
nominally set to `-2147483648`.  If not set, it will be automatically sized.

### maxValue (optional, applies only for Integer and Float types)
This numeric value specifies the largest possible value for BSI type fields.  For `Date` and `DateTime` types it
should be set to `2147483647` which is the largest positive signed 32 bit value.  For `Float` it should be set to
this value as well.  If not set, it will be automatically sized.

### required (optional)
If set to **true** a value must be provided for this field whenever a new row (tuple) is inserted.  If it is not
then one may be provided by default (see below).  This only is enforced by online "inserts" and not the bulk 
loader (yet)

### defaultValue (optional) 
This is a function that is called by "blind inserts" when a new row (tuple) is inserted via the query bridge.
It is not currently enforced by the bulk loader.
Example:
```
- sourceName: last_updated_dt
  fieldName: last_updated_dt
  type: DateTime
  mappingStrategy: SysSecBSI
  maxValue: 4294967295
  highCard: false
  # Call the bridge "now()" function that returns the current timestamp
  defaultValue: now()
```

## highCard (optional annotation)
This parameter if set to **true** markes this attribute as being of "high cardinality".  It is an annotation
added by the **analyzer** component and is just for reference purposes.


## Mapping Stategies
For the various field types `String`, `Integer`, `Boolean`, `DateTime`, etc.,  there are multiple ways to 
represent this data in a roaring bitmap index.  Various methods have advantages in terms of performance.  Others
provide enhanced query ability.  The platform provides a number of built in options that can be used "out of the box".  
In the event there is no suitable mapper then a custom implementation can be "plugged" into the platform.

There is a list of the "built-in" strategies:

| Name                | Description                                                                               |
|---------------------|-------------------------------------------------------------------------------------------|
| `BoolDirect`        | RowID false = 0, true = 1                                                                 |
| `IntToBoolDirect`   | Value is 0 or 1, Transform to boolean, Map to rowID 1 = false, 2 = true                   |
| `IntDirect`         | Value is used directly as rowID                                                           |
| `IntLinear`         | Linear buckets (Min, Max, Resolution)                                                     |
| `IntBuckets`        | Varable buckets for logarithmic representations                                           |
| `IntBSI`            | BSI range encoded integer                                                                 |
| `FloatScaleBSI`     | Converts float to int by scaling by fractional part -> BSI range encoding                 |
| `FloatLinear`       | Linear floating point buckets (Min, Max, Resolution)                                      |
| `FloatBuckets`      | Varable buckets for logarithmic representations                                           |
| `YearToDay`         | Date/Datetime day granularity                                                             |
| `YearToMonth`       | Date/Datetime month granularity                                                           |
| `Year`              | Date/Datetime year granularity                                                            |
| `DayOfYear`         | Date/Datetime integer day in year granularity                                             |
| `DayOfMonth`        | Date/Datetime integer day in month granularity                                            |
| `DayOfWeek`         | Date/Datetime integer day of week granularity                                             |
| `TimeOfDay`         | Datetime time of day in seconds granularity                                               |
| `HourOfDay`         | Datetime hour in day                                                                      |
| `SysMillisBSI`      | Datetime to BSI range milliseconds granularity                                            |
| `SysSecBSI`         | Datetime to BSI range seconds granularity (UNIX time)                                     |
| `StringEnum`        | Direct mapping of set of enumerated strings to rowID (<500 cardinality)                   |
| `StringHashBSI`     | Hash string to 32 bit rowID (>500 cardinality)                                            |
| `StringToIntDirect` | Parse string to int and use directly.                                                     |
| `BoolRegex`         | Map string to a boolean value via a pattern (requires configuration)                      |
| `Contains`          | Map string to an array of contained strings (non overlapping) (requires configuration)    |
| `Custom`            | Custom mapper plugin for ranked and time series field types                               |
| `CustomBSI`         | Custom mapper plugin for BSI fields                                                       |


## Sample files
Files contained in this subproject can be copied to your configuration root directory.

