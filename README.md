# hbase-jruby

*hbase-jruby* is a simple JRuby binding for HBase.

*hbase-jruby* provides the followings:
- Easy, Ruby-esque interface for the fundamental HBase operations
- ActiveRecord-like method chaining for data retrieval
- Automatic Hadoop/HBase dependency resolution

## Installation

    gem install hbase-jruby

## Change in 0.3.0

0.3.0 introduces the concept of table schema described as a Hash.

Using table schema greatly simplifies the way you access data:
- It allows you to omit column family names
- Automatically convert types when writing or reading data

This document has been shortened focusing on this new way of accessing data.
For old-school low-level APIs, refer to
[the older versions of this document](https://github.com/junegunn/hbase-jruby/blob/b56e21f933f0b388aa5d4d708467273463b76d73/README.md).

One downside of using schema is that it doesn't work well with _non-string column qualifiers_.
For example `to_h` and `to_H` returns Hash indexed only by Symbols (for known columns)
and Strings (for unknown ones). If you need to use non-string qualifiers,
you should fall back to low-level APIs described in the following sections.

## A quick example

```ruby
require 'hbase-jruby'

HBase.resolve_dependency! 'cdh4.2.1'

hbase = HBase.new
table = hbase[:book]

```

## Setting up

### Resolving Hadoop/HBase dependency

To be able to access HBase from JRuby, Hadoop/HBase dependency must be satisfied.
This can be done by either setting up CLASSPATH variable beforehand
or by `require`ing relevant JAR files after launching JRuby.

### `HBase.resolve_dependency!`

Well, there's an easier way.
Call `HBase.resolve_dependency!` helper method passing one of the arguments listed below.

| Argument   | Dependency               | Default version | Required executable |
| ---------- | ------------------------ | --------------- | ------------------- |
| cdh4.2[.*] | Cloudera CDH4.2          | cdh4.2.1        | mvn                 |
| cdh4.1[.*] | Cloudera CDH4.1          | cdh4.1.4        | mvn                 |
| cdh3[u*]   | Cloudera CDH3            | cdh3u6          | mvn                 |
| 0.95[.*]   | Apache HBase 0.95        | 0.95.0          | mvn                 |
| 0.94[.*]   | Apache HBase 0.94        | 0.94.6.1        | mvn                 |
| 0.92[.*]   | Apache HBase 0.92        | 0.92.2          | mvn                 |
| *POM PATH* | Custom Maven POM file    | -               | mvn                 |
| `:local`   | Local HBase installation | -               | hbase               |

(Default version is used when an argument prefix is given without specific patch version.
 e.g. `cdh4.2` defaults to `cdh4.2.0`)

#### Examples

```ruby
# Load JAR files from CDH4 using Maven
HBase.resolve_dependency! 'cdh4.2.0'
HBase.resolve_dependency! 'cdh4.1.3'

# Load JAR files of HBase 0.94.x using Maven
HBase.resolve_dependency! '0.94.6.1'
HBase.resolve_dependency! '0.94.2', verbose: true

# Dependency resolution with custom POM file
HBase.resolve_dependency! '/path/to/my/pom.xml'
HBase.resolve_dependency! '/path/to/my/pom.xml', profile: 'trunk'

# Load JAR files from local HBase installation
# (equivalent to: export CLASSPATH=$CLASSPATH:`hbase classpath`)
HBase.resolve_dependency! :local
```

(If you're behind an http proxy, set up your ~/.m2/settings.xml file
as described in [this page](http://maven.apache.org/guides/mini/guide-proxies.html))

### Log4j logs from HBase

You may want to suppress (or customize) log messages from HBase.

```ruby
# With an external log4j.properties or log4j.xml file
HBase.log4j = '/your/log4j.properties'
HBase.log4j = '/your/log4j.xml'

# With a Hash
HBase.log4j = { 'log4j.threshold' => 'ERROR' }
```

### Connecting to HBase

```ruby
# HBase on localhost
hbase = HBase.new

# HBase on remote host
hbase = HBase.new 'hbase.zookeeper.quorum' => 'remote-server.mydomain.net'

# Extra configuration
hbase = HBase.new 'hbase.zookeeper.quorum'       => 'remote-server.mydomain.net',
                  'hbase.client.retries.number'  => 3,
                  'hbase.client.scanner.caching' => 1000,
                  'hbase.rpc.timeout'            => 120000

# Close HBase connection
hbase.close
```

## Accessing data with HBase::Table instance

`HBase#[]` method (or `HBase#table`) returns an `HBase::Table` instance
which represents the table of the given name.

```ruby
table = hbase.table(:test_table)

# Or simply,
table = hbase[:test_table]
```

### Creating a table

```ruby
# Drop table if exists
table.drop! if table.exists?

# Create table with two column families
table.create! cf1: {},
              cf2: { compression: :snappy, bloomfilter: :row }
```

## Basic operations

We'll assume that `table` object in the following examples is given the book schema shown above.
Columns that are not predefined in the schema can be referenced
using `FAMILY:QUALIFIER` notation (or ColumnKey object when non-string qualifier).

### PUT

```ruby
# Putting a single row
table.put 'rowkey1', title: "Hello World", year: 2013

# Putting multiple rows
table.put 'rowkey1' => { title: 'foo',    year: 2013 },
          'rowkey2' => { title: "bar",    year: 2014 },
          'rowkey3' => { title: 'foobar', year: 2015 }

# Putting values with timestamps
table.put 'rowkey1' => {
    title: {
      1353143856665 => "Hello world",
      1352978648642 => "Goodbye world"
    },
    year: 2013
  }
```

### GET

```ruby
book = table.get('rowkey1')

# Rowkey
rowkey = row.rowkey

# Access columns in schema
title  = book[:title]
author = book[:author]
year   = book[:year]

# Columns not in the schema are returned as Java byte arrays
# So they need to be converted manually as follows
extra = HBase::Util.from_bytes :bigdecimal, book['cf2:extra']
# or, simply
extra = book.bigdecimal 'cf2:extra'
```

### Batch-GET

```ruby
# Pass an array of row keys as the parameter
books = table.get(['rowkey1', 'rowkey2', 'rowkey3'])
```

#### Intra-row scan

Intra-row scan can be done with `each` method which yields `HBase::Cell` instances.

```ruby
# Intra-row scan (all versions)
row.each do |cell|
  family    = cell.family
  qualifier = cell.qualifier :string  # Column qualifier as String
  timestamp = cell.timestamp
  value     = cell.value
end

# Array of HBase::Cells
cells = row.to_a
```

### DELETE

```ruby
# Deletes a row
table.delete('rowkey1')

# Deletes all columns in the specified column family
table.delete('rowkey1', :cf1)

# Deletes a column
table.delete('rowkey1', :author)

# Deletes a column with empty qualifier.
# (!= deleing the entire columns in the family. See the trailing colon.)
table.delete('rowkey1', 'cf1:')

# Deletes a version of a column
table.delete('rowkey1', :author, 1352978648642)

# Deletes multiple versions of a column
table.delete('rowkey1', :author, 1352978648642, 1352978649642)

# Batch delete
table.delete(['rowkey1'], ['rowkey2'], ['rowkey3', :author, 1352978648642, 135297864964])
```

However, the last syntax seems a bit unwieldy when you just wish to delete a few rows.
In that case, use simpler `delete_row` method.

```ruby
table.delete_row 'rowkey1'

table.delete_row 'rowkey1', 'rowkey2', 'rowkey3'
```

### Atomic increment of column values

```ruby
# Atomically increase cf2:reviews by one
table.increment('rowkey1', reviews: 1)

# Atomically increase two columns by one and five respectively
table.increment('rowkey1', reviews: 1, stars: 5)
```

### SCAN

`HBase::Table` itself is an enumerable object.

```ruby
# Full scan
table.each do |row|
  p row.to_h
end
```

## Scoped access

You can control how you retrieve data by chaining
the following methods of `HBase::Table` (or `HBase::Scoped`).

| Method           | Description                                                     |
|------------------|-----------------------------------------------------------------|
| `range`          | Specifies the rowkey range of scan                              |
| `project`        | To retrieve only a subset of columns                            |
| `filter`         | Filtering conditions of scan                                    |
| `while`          | Allows early termination of scan (server-side)                  |
| `at`             | Only retrieve data with the specified timestamp                 |
| `time_range`     | Only retrieve data within the specified time range              |
| `limit`          | Limits the number of rows                                       |
| `versions`       | Limits the number of versions of each column                    |
| `caching`        | Sets the number of rows for caching during scan                 |
| `batch`          | Limits the maximum number of values returned for each iteration |
| `with_java_scan` | *(ADVANCED)* Access Java Scan object in the given block         |
| `with_java_get`  | *(ADVANCED)* Access Java Get object in the given block          |

Each invocation to these methods returns an `HBase::Scoped` instance with which
you can retrieve data with the following methods.

| Method      | Description                                                             |
|-------------|-------------------------------------------------------------------------|
| `get`       | Fetches rows by the given rowkeys                                       |
| `each`      | Scans the scope of the table (`HBase::Scoped` instance is `Enumerable`) |
| `count`     | Efficiently counts the number of rows in the scope                      |
| `aggregate` | Performs aggregation using Coprocessor (To be described shortly)        |

### Example of scoped access

```ruby
import org.apache.hadoop.hbase.filter.RandomRowFilter

table.range('A'..'Z').                      # Row key range,
      project(:author).                     # Select cf1:author column
      project('cf2').                       # Select cf2 family as well
      filter(category: 'Comics').           # Filter by cf1:category value
      filter(year: [1990, 2000, 2010]).     # Set-inclusion condition on cf1:year
      filter(weight: 2.0..4.0).             # Range filter on cf1:weight
      filter(RandomRowFilter.new(0.5)).     # Any Java HBase filter
      while(revies: { gt: 20 }).            # Early termination of scan
      time_range(Time.now - 600, Time.now). # Scan data of the last 10 minutes
      limit(10).                            # Limits the size of the result set
      versions(2).                          # Only fetches 2 versions for each value
      batch(100).                           # Batch size for scan set to 100
      caching(1000).                        # Caching 1000 rows
      with_java_scan { |scan|               # Directly access Java Scan object
        scan.setCacheBlocks false
      }.
      to_a                                  # To Array of HBase::Rows
```

### *range*

`HBase::Scoped#range` method is used to filter rows based on their row keys.

```ruby
# 100 ~ 900 (inclusive end)
table.range(100..900)

# 100 ~ 900 (exclusive end)
table.range(100...900)

# 100 ~ 900 (exclusive end)
table.range(100, 900)

# 100 ~
table.range(100)

#     ~ 900 (exclusive end)
table.range(nil, 900)
```

Optionally, prefix filter can be applied as follows.

```ruby
# Prefix filter
# Row keys with "APPLE" prefix
#   Start key is automatically set to "APPLE",
#   stop key "APPLF" to avoid unnecessary disk access
table.range(prefix: 'APPLE')

# Row keys with "ACE", "BLUE" or "APPLE" prefix
#   Start key is automatically set to "ACE",
#   stop key "BLUF"
table.range(prefix: ['ACE', 'BLUE', 'APPLE'])

# Prefix filter with start key and stop key.
table.range('ACE', 'BLUEMARINE', prefix: ['ACE', 'BLUE', 'APPLE'])
```

Subsequent calls to `#range` override the range previously defined.

```ruby
# Previous ranges are discarded
scope.range(1, 100).
      range(50..100).
      range(prefix: 'A').
      range(1, 1000)
  # Same as `scope.range(1, 1000)`
```

### *filter*

You can configure server-side filtering of rows and columns with `HBase::Scoped#filter` calls.
Multiple calls have conjunctive effects.

```ruby
# Range scanning the table with filters
table.range(nil, 1000).
      filter(
        # Equality match
        year: 2013,

        # Range of numbers or characters: Checks if the value falls within the range
        weight: 2.0..4.0
        author: 'A'..'C'

        # Regular expression: Checks if the value matches the regular expression
        summary: /classic$/i,

        # Hash: Tests the value with 6 types of operators (:gt, :lt, :gte, :lte, :eq, :ne)
        reviews: { gt: 100, lte: 200 },

        # Array of the aforementioned types: OR condition (disjunctive)
        category: ['Fiction', 'Comic', /science/i, { ne: 'Political Science' }]

      # Multiple calls for conjunctive filtering
      filter(summary: /instant/i).

      # Any number of Java filters can be applied
      filter(org.apache.hadoop.hbase.filter.RandomRowFilter.new(0.5)).
  each do |record|
  # ...
end
```

### *while*

`HBase::Scoped#while` method takes the same parameters as `filter` method, the difference is that
each filtering condition passed to `while` method is wrapped by `WhileMatchFilter`,
which aborts scan immediately when the condition is not met at a certain row.
See the following example.

```ruby
(0...30).each do |idx|
  table.put idx, year: 2000 + idx % 10
end

table.filter(year: { lte: 2001 }).map { |r| r.rowkey :fixnum }
  # [0, 1, 10, 11, 20, 21]
table.while(year: { lte: 2001 }).map { |r| r.rowkey :fixnum }
  # [0, 1]
  #   Scan terminates immediately when condition not met.
```

### *project*

`HBase::Scoped#project` allows you to fetch only a subset of columns from each row.
Multiple calls have additive effects.

```ruby
# Fetches cf1:title, cf1:author, and all columns in column family cf2 and cf3
scoped.project(:title, :author, :cf2).
       project(:cf3)
```

HBase filters can not only filter rows but also columns.
Since column filtering can be thought of as a kind of projection,
it makes sense to internally apply column filters in `HBase::Scoped#project`,
instead of in `HBase::Scoped#filter`, although it's still perfectly valid
to pass column filter to filter method.

```ruby
# Column prefix filter:
#   Fetch columns whose qualifiers start with the specified prefixes
scoped.project(prefix: 'alice').
       project(prefix: %w[alice bob])

# Column range filter:
#   Fetch columns whose qualifiers within the ranges
scoped.project(range: 'a'...'c').
       project(range: ['i'...'k', 'x'...'z'])

# Column pagination filter:
#   Fetch columns within the specified intra-scan offset and limit
scoped.project(offset: 1000, limit: 10)
```

When using column filters on *fat* rows with many columns,
it's advised that you limit the batch size with `HBase::Scoped#batch` call
to avoid fetching all columns at once.
However setting batch size allows multiple rows with the same row key are returned during scan.

```ruby
# Let's say that we have rows with more than 10 columns whose qualifiers start with `str`
puts scoped.range(1..100).
            project(prefix: 'str').
            batch(10).
            map { |row| [row.rowkey(:fixnum), row.count].map(&:to_s).join ': ' }

  # 1: 10
  # 1: 10
  # 1: 5
  # 2: 10
  # 2: 2
  # 3: 10
  # ...
```

### Scoped SCAN / GET

```ruby
scoped = table.versions(1)                 # Limits the number of versions
              .filter(year: 1990...2000)
              .range('rowkey0'..'rowkey2') # Range of rowkeys.
              .project('cf1', 'cf2:x')     # Projection

# Scoped GET
#   Nonexistent or filtered rows are returned as nils
scoped.get(['rowkey1', 'rowkey2', 'rowkey4'])

# Scoped SCAN
scoped.each do |row|
  row.each do |cell|
    # Intra-row scan
  end
end

# Scoped COUNT
#   When counting the number of rows, use `HTable::Scoped#count`
#   instead of just iterating through the scope, as it internally
#   minimizes amount of data fetched with KeyOnlyFilter
scoped.count

# This should be even faster as it dramatically reduces the number of RPC calls
scoped.caching(5000).count
```

## Basic aggregation using coprocessor

You can perform some basic aggregation using the built-in coprocessor called
`org.apache.hadoop.hbase.coprocessor.AggregateImplementation`.

To enable this feature, call `enable_aggregation!` method,
which adds the coprocessor to the table.

```ruby
table.enable_aggregation!
# Just a shorthand notation for
#   table.add_coprocessor! 'org.apache.hadoop.hbase.coprocessor.AggregateImplementation'
```

Then you can get the sum, average, minimum, maximum, row count, and standard deviation
of the projected columns.

```ruby
# cf1:a must hold 8-byte integer values
table.project(:reviews).aggregate(:sum)
table.project(:reviews).aggregate(:avg)
table.project(:reviews).aggregate(:min)
table.project(:reviews).aggregate(:max)
table.project(:reviews).aggregate(:std)
table.project(:reviews).aggregate(:row_count)

# Aggregation of multiple columns
table.project(:reviews, :stars).aggregate(:sum)
```

By default, aggregate method assumes that the projected values are 8-byte integers.
For other data types, you can pass your own ColumnInterpreter.

```ruby
table.project(:price).aggregate(:sum, MyColumnInterpreter.new)
```

## Table inspection

```ruby
# Table properties
table.properties
  # {:max_filesize       => 2147483648,
  #  :readonly           => false,
  #  :memstore_flushsize => 134217728,
  #  :deferred_log_flush => false}

# Properties of the column families
table.families
  # {"cf"=>
  #   {:blockcache            => true,
  #    :blocksize             => 65536,
  #    :bloomfilter           => "NONE",
  #    :cache_blooms_on_write => false,
  #    :cache_data_on_write   => false,
  #    :cache_index_on_write  => false,
  #    :compression           => "NONE",
  #    :compression_compact   => "NONE",
  #    :data_block_encoding   => "NONE",
  #    :evict_blocks_on_close => false,
  #    :in_memory             => false,
  #    :keep_deleted_cells    => false,
  #    :min_versions          => 0,
  #    :replication_scope     => 0,
  #    :ttl                   => 2147483647,
  #    :versions              => 3}}
```

There are also `raw_` variants of `properties` and `families`.
They return properties in their internal String format (mainly used in HBase shell).
(See [HTableDescriptor.values](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HTableDescriptor.html#values) and
[HColumnDescriptor.values](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HColumnDescriptor.html#values))

```ruby
table.raw_properties
  # {"IS_ROOT"      => "false",
  #  "IS_META"      => "false",
  #  "MAX_FILESIZE" => "2147483648"}

table.raw_families
  # {"cf" =>
  #   {"DATA_BLOCK_ENCODING" => "NONE",
  #    "BLOOMFILTER"         => "NONE",
  #    "REPLICATION_SCOPE"   => "0",
  #    "VERSIONS"            => "3",
  #    "COMPRESSION"         => "NONE",
  #    "MIN_VERSIONS"        => "0",
  #    "TTL"                 => "2147483647",
  #    "KEEP_DELETED_CELLS"  => "false",
  #    "BLOCKSIZE"           => "65536",
  #    "IN_MEMORY"           => "false",
  #    "ENCODE_ON_DISK"      => "true",
  #    "BLOCKCACHE"          => "true"}}
```

These String key-value pairs are not really a part of the public API of HBase, and thus might change over time.
However, they are most useful when you need to create a table with the same properties as the existing one.

```ruby
hbase[:dupe_table].create!(table.raw_families, table.raw_properties)
```

With `regions` method, you can even presplit the new table just like the old one.

```ruby
hbase[:dupe_table].create!(
  table.raw_families,
  table.raw_properties.merge(splits: table.regions.map { |r| r[:start_key] }.compact))
```

## Table administration

`HBase#Table` provides a number of *bang_methods!* for table administration tasks.
They run synchronously, except when mentioned otherwise (e.g. `HTable#split!`).
Some of them take an optional block to allow progress monitoring
and come with non-bang, asynchronous counterparts.

### Creation and alteration

```ruby
# Create a table with configurable table-level properties
table.create!(
    # 1st Hash: Column family specification
    {
      cf1: { compression: snappy },
      cf2: { bloomfilter: row }
    },

    # 2nd Hash: Table properties
    max_filesize:       256 * 1024 ** 2,
    deferred_log_flush: false,
    splits:             [1000, 2000, 3000]
)

# Alter table properties (synchronous with optional block)
table.alter!(
  max_filesize:       512 * 1024 ** 2,
  memstore_flushsize: 64 * 1024 ** 2,
  readonly:           false,
  deferred_log_flush: true
) { |progress, total|
  # Progress report with an optional block
  puts [progress, total].join('/')
}

# Alter table properties (asynchronous)
table.alter(
  max_filesize:       512 * 1024 ** 2,
  memstore_flushsize: 64 * 1024 ** 2,
  readonly:           false,
  deferred_log_flush: true
)
```

#### List of column family properties

http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HColumnDescriptor.html

Some of the properties are only available on recent versions of HBase.

| Property                 | Type          | Description                                                                                                        |
|--------------------------|---------------|--------------------------------------------------------------------------------------------------------------------|
| `:blockcache`            | Boolean       | If MapFile blocks should be cached                                                                                 |
| `:blocksize`             | Fixnum        | Blocksize to use when writing out storefiles/hfiles on this column family                                          |
| `:bloomfilter`           | Symbol/String | Bloom filter type: `:none`, `:row`, `:rowcol`, or uppercase Strings                                                |
| `:cache_blooms_on_write` | Boolean       | If we should cache bloomfilter blocks on write                                                                     |
| `:cache_data_on_write`   | Boolean       | If we should cache data blocks on write                                                                            |
| `:cache_index_on_write`  | Boolean       | If we should cache index blocks on write                                                                           |
| `:compression`           | Symbol/String | Compression type: `:none`, `:gz`, `:lzo`, `:lz4`, `:snappy`, or uppercase Strings                                  |
| `:compression_compact`   | Symbol/String | Compression type: `:none`, `:gz`, `:lzo`, `:lz4`, `:snappy`, or uppercase Strings                                  |
| `:data_block_encoding`   | Symbol/String | Data block encoding algorithm used in block cache: `:none`, `:diff`, `:fast_diff`, `:prefix`, or uppercase Strings |
| `:encode_on_disk`        | Boolean       | If we want to encode data block in cache and on disk                                                               |
| `:evict_blocks_on_close` | Boolean       | If we should evict cached blocks from the blockcache on close                                                      |
| `:in_memory`             | Boolean       | If we are to keep all values in the HRegionServer cache                                                            |
| `:keep_deleted_cells`    | Boolean       | If deleted rows should not be collected immediately                                                                |
| `:min_versions`          | Fixnum        | The minimum number of versions to keep (used when timeToLive is set)                                               |
| `:replication_scope`     | Fixnum        | Replication scope                                                                                                  |
| `:ttl`                   | Fixnum        | Time-to-live of cell contents, in seconds                                                                          |
| `:versions`              | Fixnum        | The maximum number of versions. (By default, all available versions are retrieved.)                                |

#### List of table properties

http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HTableDescriptor.html

| Property              | Type    | Description                                                                                             |
|-----------------------|---------|---------------------------------------------------------------------------------------------------------|
| `:max_filesize`       | Fixnum  | The maximum size upto which a region can grow to after which a region split is triggered                |
| `:readonly`           | Boolean | If the table is read-only                                                                               |
| `:memstore_flushsize` | Fixnum  | The maximum size of the memstore after which the contents of the memstore are flushed to the filesystem |
| `:deferred_log_flush` | Boolean | Defer the log edits syncing to the file system                                                          |
| `:splits`             | Array   | Region split points                                                                                     |

### Managing column families

```ruby
# Add column family
table.add_family! :cf3, compression: :snappy, bloomfilter: :row

# Alter column family
table.alter_family! :cf2, bloomfilter: :rowcol

# Remove column family
table.delete_family! :cf1
```

### Coprocessors

```ruby
# Add Coprocessor
unless table.has_coprocessor?(cp_class_name1)
  table.add_coprocessor! cp_class_name1
end
table.add_coprocessor! cp_class_name2, path: path, priority: priority, params: params

# Remove coprocessor
table.remove_coprocessor! cp_class_name1
```

### Region splits (asynchronous)

```ruby
table.split!(1000)
table.split!(2000, 3000)
```

### Snapshots

```ruby
# Returns a list of all snapshot information
hbase.snapshots

# Table snapshots
table.snapshots
# Equivalent to
#   hbase.snapshots.select { |info| info[:table] == table.name }

# Creating a snapshot
table.snapshot! 'my_table_snapshot'
```

### Advanced table administration

You can perform other types of administrative tasks
with the native Java [HBaseAdmin object](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/HBaseAdmin.html),
which can be obtained by `HBase#admin` method. Optionally, a block can be given
so that the object is automatically closed at the end of the given block.

```ruby
admin = hbase.admin
# ...
admin.close

# Access native HBaseAdmin object within the block
hbase.admin do |admin|
  admin.snapshot       'my_snapshot', 'my_table'
  admin.cloneSnapshot  'my_snapshot', 'my_clone_table'
  admin.deleteSnapshot 'my_snapshot'
  # ...
end
```

## Advanced topics

### Lexicographic scan order

HBase stores rows in the lexicographic order of the rowkeys in their byte array representations.
Thus the type of row key affects the scan order.

```ruby
(1..15).times do |i|
  table.put i, data
  table.put i.to_s, data
end

table.range(1..3).map { |r| r.rowkey :fixnum }
  # [1, 2, 3]
table.range('1'..'3').map { |r| r.rowkey :string }
  # %w[1 10 11 12 13 14 15 2 3]
```

### Non-string column qualifier

If a column qualifier is not a String, *an HBase::ColumnKey instance* should be used
instead of a conventional `FAMILY:QUALIFIER` String.

```ruby
table.put 'rowkey',
  'cf1:col1'                    => 'Hello world',
  HBase::ColumnKey(:cf1, 100)   => "Byte representation of an 8-byte integer",
  HBase::ColumnKey(:cf1, bytes) => "Qualifier is an arbitrary byte array"

table.get('rowkey')[HBase::ColumnKey(:cf1, 100)]
# ...
```

### Shorter integers

A Ruby Fixnum is an 8-byte integer, which is equivalent `long` type in Java.
When you want to use shorter integer types such as int, short, or byte,
you can then use the special Hash representation of integers.

```ruby
# 4-byte int value as the rowkey
table.put({ int: 12345 }, 'cf1:a' => { byte: 100 },   # 1-byte integer
                          'cf1:b' => { short: 200 },  # 2-byte integer
                          'cf1:c' => { int: 300 },    # 4-byte integer
                          'cf1:4' => 400)             # Ordinary 8-byte integer

row = table.get(int: 12345)
```

### Working with byte arrays

In HBase, virtually everything is stored as a byte array.
Although *hbase-jruby* tries hard to hide the fact,
at some point you may need to get your hands dirty with native Java byte arrays.
For example, it's [a common practice] [1] to use a composite row key,
which is a concatenation of several components of different types.

  [1]: http://blog.sematext.com/2012/08/09/consider-using-fuzzyrowfilter-when-in-need-for-secondary-indexes-in-hbase/

`HBase::ByteArray` is a boxed class for native Java byte arrays,
which makes byte array manipulation much easier.

A ByteArray can be created as a concatenation of any number of objects.

```ruby
ba = HBase::ByteArray[100, 3.14, {int: 300}, "Hello World"]
```

Then you can slice it and decode each part,

```ruby
# Slicing
first  = ba[0, 8]
second = ba[8...16]

first.decode(:fixnum)  # 100
second.decode(:float)  # 3.14
```

append, prepend more elements to it,

```ruby
ba.unshift 200, true
ba << { short: 300 }
```

concatenate another ByteArray,

```ruby
ba += HBase::ByteArray[1024]
```

or shift decoded objects from it.

```ruby
ba.shift(:fixnum)
ba.shift(:boolean)
ba.shift(:fixnum)
ba.shift(:float)
ba.shift(:int)
ba.shift(:string, 11)  # Byte length must be given as Strings are not fixed in size
```

`ByteArray#java` method returns the underlying native Java byte array.

```ruby
ba.java  # Returns the native Java byte array (byte[])
```

## Test

```bash
#!/bin/bash

# Test HBase 0.94 on localhost
export HBASE_JRUBY_TEST_ZK='127.0.0.1'
export HBASE_JRUBY_TEST_DIST='0.94'

# Test both for 1.8 and 1.9
for v in --1.8 --1.9; do
  export JRUBY_OPTS=$v
  rake test
done
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
