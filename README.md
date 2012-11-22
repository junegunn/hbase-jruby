# hbase-jruby

*hbase-jruby* provides Ruby-esque interface for accessing HBase from JRuby.

Of course JRuby allows you to directly call native Java APIs of HBase,
but doing so requires a lot of keystrokes even for the most basic operations and
leads to verbose code that will be frowned upon by any sane Rubyist.
Anyhow, JRuby is Ruby, not Java, right?

## A quick example

```ruby
require 'hbase-jruby'

hbase = HBase.new
table = hbase.table(:test_table)

# PUT
table.put :rowkey1 => { 'cf1:a' => 100, 'cf2:b' => "Hello" }

# GET
row = table.get(:rowkey1)
number = row.integer('cf1:a')
string = row.string('cf1:b')

# SCAN
table.range('rowkey1'..'rowkey9').
      filter('cf1:a' => 100..200,             # cf1:a between 100 and 200
             'cf2:b' => 'Hello',              # cf1:b = 'Hello'
             'cf2:c' => ['foo', 'bar']).      # cf2:c in ('foo', 'bar')
      project('cf1:a', 'cf2').each do |row|
  puts row.integer('cf1:a')
end

# DELETE
table.delete(:rowkey9)
```

## Installation

Add this line to your application's Gemfile:

    gem 'hbase-jruby'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install hbase-jruby

## Setting up

### Resolving Hadoop/HBase dependency

To be able to access HBase from JRuby, Hadoop/HBase dependency must be satisfied.
This can be done by setting up CLASSPATH variable beforehand
or by `require`ing relevant JAR files after launch.
However, downloading all the JAR files and manually putting them in CLASSPATH is a PITA,
especially when HBase is not installed on local system.

*hbase-jruby* includes `HBase.resolve_dependency!` helper method,
which resolves Hadoop/HBase dependency.

#### Preconfigured dependencies

Apache Maven is the de facto standard dependency management mechanism for Java projects.
Current version of *hbase-jruby* is shipped with Maven dependency specifications
for the following Hadoop/HBase distributions.

* cdh4.1.2
* cdh3u5 (NOT TESTED, yet)

```ruby
require 'hbase-jruby'

HBase.resolve_dependency! 'cdh4.1.2'
```

#### Customized dependencies

If you use another version of HBase and Hadoop,
you can use your own Maven pom.xml file with its customized Hadoop/HBase dependency

```ruby
HBase.resolve_dependency! '/project/my-hbase/pom.xml'
```

#### Using `hbase classpath` command

If you have HBase installed on your system, it's possible to find the JAR files
for that local installation with `hbase classpath` command.
You can tell `resolve_dependency!` method to do so by giving it special `:hbase` parameter.

```ruby
HBase.resolve_dependency! :hbase
```

### Connecting to HBase

```ruby
# HBase on localhost
hbase = HBase.new

# HBase on remote host
hbase = HBase.new 'hbase.zookeeper.quorum' => 'remote-server.mydomain.net'

# Extra configuration
hbase = HBase.new 'hbase.zookeeper.quorum' => 'remote-server.mydomain.net',
                  'hbase.client.retries.number' => 3

# Close HBase connection
hbase.close
```

## Accessing data with HBase::Table instance

`HBase#table` method creates an `HBase::Table` instance which represents an HBase table.

```ruby
table = hbase.table(:test_table)
```

`HBase::Table` instance must be closed after use.

```ruby
# Always close table instance after use
table.close

# If block is given, table is automatically closed at the end of the block
hbase.table(:test_table) do |table|
  # ...
end
```

## Table administraion

### Creating tables

```ruby
# Drop table if exists
table.drop! if table.exists?

# Create table with two column families
table.create! :cf1 => {},
              :cf2 => { :compression => :snappy, :bloomfilter => :row }
```

### Table inspection

```ruby
puts table.inspect
```

## Basic operations

### PUT

```ruby
# Putting a single row
table.put 'rowkey1', 'cf1:col1' => "Hello", 'cf2:col2' => "World"

# Putting multiple rows
table.put 'rowkey1' => { 'cf1:col1' => "Hello",   'cf2:col2' => "World" },
          'rowkey2' => { 'cf1:col1' => "Howdy",   'cf2:col2' => "World" },
          'rowkey3' => { 'cf1:col1' => "So long", 'cf2:col2' => "World" }
```

### GET

HBase stores everything as a byte array, so when you fetch data from HBase,
you need to explicitly specify the type of each value stored.

```ruby
row = table.get('rowkey1')

# Rowkey
rowk = row.rowkey

# Column value as a raw Java byte array
col0 = row.raw 'cf1:col0'

# Decode column values
col1 = row.string  'cf1:col1'
col2 = row.fixnum  'cf1:col2'
col3 = row.bignum  'cf1:col3'
col4 = row.float   'cf1:col4'
col5 = row.boolean 'cf1:col5'
col6 = row.symbol  'cf1:col6'

# Decode multiple columns at once
row.string ['cf1:str1', 'cf1:str2']
  # [ "Hello", "World" ]
```

#### Batch GET

```ruby
# Pass an array of row keys as the parameter
rows = table.get(['rowkey1', 'rowkey2', 'rowkey3'])
```

#### Decode all versions with plural-form (-s) methods

```ruby
# Decode all versions as Hash indexed by their timestamps
row.strings 'cf1:str'
  # {1353143856665=>"Hello", 1353143856662=>"Goodbye"}

# Decode all versions of multiple columns
row.strings ['cf1:str1', 'cf1:str2']
  # [
  #   {1353143856665=>"Hello", 1353143856662=>"Goodbye"},
  #   {1353143856665=>"World", 1353143856662=>"Cruel world"}
  # ]

# Plural-form methods are provided for any other data types as well
cols0 = row.raws     'cf1:col0'
cols1 = row.strings  'cf1:col1'
cols2 = row.fixnums  'cf1:col2'
cols3 = row.bignums  'cf1:col3'
cols4 = row.floats   'cf1:col4'
cols5 = row.booleans 'cf1:col5'
cols6 = row.symbols  'cf1:col6'
```

#### Intra-row scan

Intra-row scan can be done with `each` method which yields `HBase::Cell` instances.

```ruby
# Intra-row scan (all versions)
row.each do |cell|
  family    = cell.family
  qualifier = cell.qualifier(:string)  # Column qualifier as String
  timestamp = cell.timestamp

  # Cell value as Java byte array
  bytes     = cell.bytes

  # Typed access
  # value_as_string = cell.string
  # value_as_fixnum = cell.fixnum
  # ...
end
```

#### `to_hash`

```ruby
# Returns the Hash representation of the record with the specified schema
schema = {
  'cf1:col1' => :string,
  'cf1:col2' => :fixnum,
  'cf1:col3' => :bignum,
  'cf1:col4' => :float,
  'cf1:col5' => :boolean,
  'cf1:col6' => :symbol }

table.get('rowkey1').to_hash(schema)

# Returns all versions for each column indexed by their timestamps
table.get('rowkey1').to_hash_with_versions(schema)
```

### DELETE

```ruby
# Deletes a row
table.delete('rowkey1')

# Deletes all columns in the specified column family
table.delete('rowkey1', 'cf1')

# Deletes a column
table.delete('rowkey1', 'cf1:col1')

# Deletes a column with empty qualifier.
# (!= deleing the entire columns in the family. See the trailing colon.)
table.delete('rowkey1', 'cf1:')

# Deletes a version of a column
table.delete('rowkey1', 'cf1:col1', 1352978648642)

# Batch delete
table.delete(['rowkey1'], ['rowkey2'], ['rowkey3', 'cf1:col1'])

# Truncate table
table.truncate!
```

### Atomic increment of column values

```ruby
# Atomically increase cf1:counter by one
table.increment('rowkey1', 'cf1:counter', 1)

# Atomically increase two columns by one an two respectively
table.increment('rowkey1', 'cf1:counter' => 1, 'cf1:counter2' => 2)
```

### SCAN

`HBase::Table` itself is an enumerable object.

```ruby
# Full scan
table.each do |row|
  # ...
end
```

## Scoped access

SCAN and GET operation is actually implemented in enumerable `HBase::Scoped` class,
whose instance is implicitly used/returned by `HBase::Table#each` call.

```ruby
scoped = table.each
```

An `HBase::Scoped` object provides a set of methods for controlling data retrieval
such as `range`, `filter`, `project`, `versions`, `caching`, et cetera.
However, it doesn't respond to data manipulation methods (`put`, `delete` and `increment`),
and methods for table administration.

An `HBase::Table` object also responds to data retrieval methods described above,
which is simply forwarded to a new `HBase::Scoped` object implicitly created.
For example, `table.range(start, end)` is just a shorthand notation for
`table.each.range(start, end)`.

### Chaining scoping methods

Scoping methods can be chained as follows.

```ruby
# Chaining methods
import org.apache.hadoop.hbase.filter.RandomRowFilter

table.range('A'..'Z').                   # Row key range,
      project('cf1:a').                  # Select cf1:a column
      project('cf2').                    # Select cf2 family as well
      filter('cf1:a' => 'Hello').        # Filter by cf1:a value
      filter('cf2:d' => 100..200).       # Range filter on cf2:d
      filter('cf2:e' => [10, 20..30]).   # Set-inclusion condition on cf2:e
      filter(RandomRowFilter.new(0.5)).  # Any HBase filter
      limit(10).                         # Limits the size of the result set
      versions(2).                       # Only fetches 2 versions for each value
      batch(100).                        # Batch size for scan set to 100
      caching(100).                      # Caching 100 rows
      to_a                               # To Array
```

### range

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
table.range(:prefix => 'APPLE')

# Row keys with "APPLE" prefix from 'A' ~ 'B'
table.range('A'...'B', :prefix => 'APPLE')

# Row keys with "ACE" or "APPLE" or "BLUE" prefix from 'A' ~ 'C'
table.range('A'...'C', :prefix => ['ACE', 'APPLE', 'BLUE'])
```

Subsequent calls to `#range` override the range previously defined.

```ruby
# Previous ranges are discarded
scope.range(1, 100).range(50..100).range(1, 1000)
```

### filter

Specifies server-side filtering of rows and columns.
Multiple calls have conjunctive effects.

```ruby
# Range scanning the table with filters
table.range(nil, 1000).
      filter('cf1:a' => 'Hello',                  # cf1:a = 'Hello'
             'cf1:b' => 100...200,                # cf1:b between 100 and 200
             'cf1:c' => %w[A B C],                # cf1:c in ('A', 'B', 'C')
             'cf1:d' => ['A'...'B', 'C'],         # ('A' <= cf1:d < 'B') or cf1:d = 'C'
             'cf1:e' => { gt: 1000, lte: 2000 }). # cf1:e > 1000 and cf1:e <= 2000
             'cf1:f' => { ne: 1000 }).            # cf1:f != 1000
                                                  #   Supported operators: gt, lt, gte, lte, eq, ne
      filter('cf1:g' => ['Alice'..'Bob', 'Cat']). # Multiple calls for conjunctive filtering
      filter(org.apache.hadoop.hbase.filter.RandomRowFilter.new(0.5)).
                                                  # Any number of Java filters can be applied
      each do |record|
  # ...
end
```

### project

`HBase::Scoped#project` allows you to fetch only a subset of columns in rows.
Multiple calls have additive effects.

```ruby
# Fetches cf1:a and all columns in column family cf2 and cf3
scoped.project('cf1:a', 'cf2').
       project('cf3')
```

HBase filters can not only filter rows but also columns.
Since column filtering can be thought of as a kind of projection,
it makes sense to internally utilize column filters in `HBase::Scoped#project`,
instead of in `HBase::Scoped#filter`, although it's still perfectly valid
to pass column filter to filter method.

```ruby
# Column prefix filter:
#   Fetch columns whose qualifier starts with the specified prefixes
scoped.project(:prefix => 'alice').
       project(%w[alice bob])

# Column range filter:
#   Fetch columns whose qualifier within the ranges
scoped.project(:range => 'a'...'c').
       project(:range => ['i'...'k', 'x'...'z'])

# Column pagination filter (Cannot be chained)
#   Fetch columns within the specified offset and limit
scoped.project(:offset => 1000, :limit => 10)
```

When using column filters on *fat* rows with many columns,
it's advised that you limit the batch size with `HBase::Scoped#batch` call
to avoid fetching all columns at once.
However setting batch size allows multiple rows with the same row key are returned during scan.

```ruby
puts scoped.range(1..100).
            project(:prefix => 'str').
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
scoped = table.versions(1).                       # Limits the number of versions
               filter('cf1:a' => 'Hello',         # With filters
                      'cf1:b' => 100...200,
                      'cf1:c' => 'Alice'..'Bob').
               range('rowkey0'..'rowkey2')        # Range of rowkeys.
               project('cf1', 'cf2:x')            # Projection

# Scoped GET
# Nonexistent or filtered rows are returned as nils
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
#   minimizes amount of data fetched.
scoped.count
```

## Advanced topics

### Lexicographic scan order

HBase stores rows in the lexicographic order of the rowkeys in their byte array representations.
Thus the type of rowkey affects the scan order.

```ruby
(1..15).times do |i|
  table.put i, data
  table.put i.to_s, data
end

table.range(1..3).map { |r| r.rowkey :integer }
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

table.get('rowkey').string('cf1:col1')
table.get('rowkey').string(HBase::ColumnKey(:cf1, 100))
# ...
```


### Table administration

```ruby
# Create a table with configurable table-level properties
table.create!(
    # 1st Hash: Column family specification
    { :cf1 => { :compression => :snappy }, :cf2 => {} },

    # 2nd Hash: Table properties
    :max_filesize       => 256 * 1024 ** 2,
    :deferred_log_flush => false)

# Alter table properties
table.alter!(
  :max_filesize       => 512 * 1024 ** 2,
  :memstore_flushsize =>  64 * 1024 ** 2,
  :readonly           => false,
  :deferred_log_flush => true
)

# Add column family
table.add_family! :cf3, :compression => :snappy,
                         :bloomfilter => :row

# Alter column family
table.alter_family! :cf2, :bloomfilter => :rowcol

# Remove column family
table.delete_family! :cf1

# Advanced table administration with HBaseAdmin object
# http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/HBaseAdmin.html
admin = hbase.admin
```

## Test

```
export HBASE_JRUBY_TEST_ZK='your-hbaase.domain.net'
rake test
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
