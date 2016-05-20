# hbase-jruby

*hbase-jruby* is a simple JRuby binding for HBase.

*hbase-jruby* provides the followings:
- Easy, Ruby-esque interface for the fundamental HBase operations
- ActiveRecord-like method chaining for data retrieval

## Installation

    gem install hbase-jruby

### Using hbase-jruby in HBase shell

You can use this gem in HBase shell without external JRuby installation.

First, clone this repository,

```sh
git clone --depth 1 https://github.com/junegunn/hbase-jruby.git
```

then start up the shell (`hbase shell`) and type in the following line:

```ruby
$LOAD_PATH << 'hbase-jruby/lib'; require 'hbase-jruby'
```

Now, you're all set.

```ruby
# Start using it!
hbase = HBase.new

hbase.list

hbase[:my_table].create! :f
hbase[:my_table].put 100, 'f:a' => 1, 'f:b' => 'two', 'f:c' => 3.14
hbase[:my_table].get(100).double('f:c') # Returns 3.14
```

## A quick example

```ruby
require 'hbase-jruby'

# HBase client dependencies
$CLASSPATH << 'hbase-client-dep-1.0.jar'

# Connect to HBase
hbase = HBase.new 'localhost'

# Table object
table = hbase[:test_table]
table.drop! if table.exists?
table.create! :cf1 => {}, :cf2 => {}

# PUT
table.put 'rowkey1' => { 'cf1:a' => 100, 'cf2:b' => 'Hello' },
          'rowkey2' => { 'cf1:a' => 200, 'cf2:b' => 'world' }

# GET
row = table.get('rowkey1')
number = row.fixnum('cf1:a')
string = row.string('cf1:b')

# SCAN
table.range('rowkey1'..'rowkey9')
     .filter('cf1:a' => 100..200,         # cf1:a between 100 and 200
             'cf1:b' => 'Hello',          # cf1:b = 'Hello'
             'cf2:c' => /world/i,         # cf2:c matches /world/i
             'cf2:d' => ['foo', /^BAR/i]) # cf2:d = 'foo' OR matches /^BAR/i
     .project('cf1:a', 'cf2').
     .each do |row|
  puts row.fixnum('cf1:a')
end

# DELETE
table.delete('rowkey9')
```

## A quick example using schema definition

```ruby
require 'hbase-jruby'

# HBase client dependencies
$CLASSPATH << 'hbase-client-dep-1.0.jar'

# Connect to HBase on localhost
hbase = HBase.new

# Define table schema for easier data access
hbase.schema = {
  # Schema for `book` table
  book: {
    # Columns in cf1 family
    cf1: {
      title:     :string,     # String (UTF-8)
      author:    :string,
      category:  :string,
      year:      :short,      # Short integer (2-byte)
      pages:     :int,        # Integer (4-byte)
      price:     :bigdecimal, # BigDecimal
      height:    :float,      # Single-precision floating-point number (4-byte)
      weight:    :double,     # Double-precision floating-point number (8-byte)
      in_print:  :boolean,    # Boolean (true | false)
      image:     :raw         # Java byte array; no automatic type conversion
      thumbnail: :byte_array  # HBase::ByteArray
    },
    # Columns in cf2 family
    cf2: {
      summary:  :string,
      reviews:  :fixnum,      # Long integer (8-byte)
      stars:    :fixnum,
      /^comment\d+/ => :string
    }
  }
}

# Create book table with two column families
table = hbase[:book]
unless table.exists?
  table.create! cf1: { min_versions: 2 },
                cf2: { bloomfilter: :rowcol, versions: 5 }
end

# PUT
table.put 1,
  title:     'The Golden Bough: A Study of Magic and Religion',
  author:    'Sir James G. Frazer',
  category:  'Occult',
  year:      1890,
  pages:     1006,
  price:     BigDecimal('21.50'),
  weight:    3.0,
  in_print:  true,
  image:     File.open('thumbnail.png', 'rb') { | f          | f.read }.to_java_bytes,
  summary:   'A wide-ranging, comparative study of mythology and religion',
  reviews:   52,
  stars:     226,
  comment1:  'A must-have',
  comment2:  'Rewarding purchase'

# GET (using schema)
book     = table.get(1)
title    = book[:title]
comment2 = book[:comment2]
as_hash  = book.to_h

# GET (not using schema)
title    = book.string('cf1:title')       # cf:cq notation
year     = book.short('cf1:year')
reviews  = book.fixnum('cf2:reviews')
stars    = book.fixnum(['cf2', 'stars'])  # Array notation of [cf, cq]

# SCAN
table.range(0..100)
     .project(:cf1, :reviews, :summary)
     .filter(year:     1880...1900,
             in_print: true,
             category: ['Comics', 'Fiction', /cult/i],
             price:    { lt: BigDecimal('30.00') },
             summary:  /myth/i)
     .each do                                   | book       |

  # Update columns
  table.put book.rowkey, price: book[:price] + BigDecimal('1')

  # Atomic increment
  table.increment book.rowkey, reviews: 1, stars: 5

  # Delete two columns
  table.delete book.rowkey, :comment1, :comment2
end

# Delete row
table.delete 1
```

## Setting up

### Resolving Hadoop/HBase dependency

To be able to access HBase from JRuby, Hadoop/HBase dependency must be
satisfied. This can be done either by setting up CLASSPATH beforehand (e.g.
`CLASSPATH=$(hbase classpath) jruby ...`) or by `require`ing relevant JAR
files after launching JRuby.

You might want to check out pre-built uberjars for various versions of HBase
client in [hbase-client-dep releases page][client].

```ruby
require 'hbase-jruby'
$CLASSPATH << 'hbase-client-dep-1.0.jar'

hbase = HBase.new
```

[client]: https://github.com/junegunn/hbase-client-dep/releases

### Log4j logs from HBase

You can suppress (or customize) log messages from HBase.

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
hbase = HBase.new 'remote-server.mydomain.net'

# The above is equivalent to the following:
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

## List of operations

| Operation          | Description                                                                                   |
| ------------------ | --------------------------------------------------------------------------------------------- |
| PUT                | Puts data into the table                                                                      |
| GET                | Retrieves data from the table by one or more rowkeys                                          |
| SCAN               | Scans the table for a given range of rowkeys                                                  |
| DELETE             | Deletes data in the table                                                                     |
| INCREMENT          | Atomically increments one or more columns                                                     |
| APPEND             | Appends values to one or more columns within a single row                                     |
| Checked PUT/DELETE | Atomically checks if the pre-exising data matches the expected value and puts or deletes data |
| MUTATE             | Performs multiple mutations (PUTS and DELETES) atomically on a single row                     |
| Batch execution    | Performs multiple actions (PUT, GET, DELETE, INCREMENT, APPEND) at once                       |

### Defining table schema for easier data access

HBase stores everything as plain Java byte arrays. So it's completely up to
users to encode and decode column values of various types into and from byte
arrays, and that is a quite tedious and error-prone task.

To remedy this situation, `hbase-jruby` implements the concept of table schema.

Using table schema greatly simplifies the way you access data:
- With schema, byte array conversion becomes automatic
- It allows you to omit column family names (e.g. `:title` instead of `"cf1:title"`)

We'll use the following schema throughout the examples.

```ruby
hbase.schema = {
  # Schema for `book` table
  book: {
    # Columns in cf1 family
    cf1: {
      title:    :string,     # String (UTF-8)
      author:   :string,
      category: :string,
      year:     :short,      # Short integer (2-byte)
      pages:    :int,        # Integer (4-byte)
      price:    :bigdecimal, # BigDecimal
      weight:   :double,     # Double-precision floating-point number
      in_print: :boolean,    # Boolean (true | false)
      image:    :raw         # Java byte array; no automatic type conversion
    },
    # Columns in cf2 family
    cf2: {
      summary:  :string,
      reviews:  :fixnum,     # Long integer (8-byte)
      stars:    :fixnum,
      /^comment\d+/ => :string
    }
  }
}
```

Columns that are not defined in the schema can be referenced
using `FAMILY:QUALIFIER` notation or 2-element Array of column family name (as
Symbol) and qualifier, however since there's no type information, they are
returned as Java byte arrays, which have to be decoded manually.

### PUT

```ruby
# Putting a single row
# - Row keys can be of any type, in this case, we use String type
table.put 'rowkey1', title: "Hello World", year: 2013

# Putting multiple rows
table.put 'rowkey1' => { title: 'foo',    year: 2013 },
          'rowkey2' => { title: 'bar',    year: 2014 },
          'rowkey3' => { title: 'foobar', year: 2015 }

# Putting values with timestamps
table.put 'rowkey1',
  title: {
    1353143856665 => 'Hello world',
    1352978648642 => 'Goodbye world'
  },
  year: 2013

# Putting values with the same timestamp
table.put('rowkey1',
  {
    title: 'foo',
    year: 2016
  },
  1463678960135
)
```

### GET

```ruby
book = table.get('rowkey1')

# Rowkey
rowkey = row.rowkey         # Rowkey as raw Java byte array
rowkey = row.rowkey :string # Rowkey as String

# Access columns in schema
title  = book[:title]
author = book[:author]
year   = book[:year]

# Convert to simple Hash
hash = book.to_h

# Convert to Hash containing all versions of values indexed by their timestamps
all_hash = table.versions(:all).get('rowkey1').to_H

# Columns not defined in the schema are returned as Java byte arrays
# They need to be decoded manually
extra = HBase::Util.from_bytes(:bigdecimal, book['cf2:extra'])
# or, simply
extra = book.bigdecimal 'cf2:extra'
```

#### Batch-GET

```ruby
# Pass an array of row keys as the parameter
books = table.get(['rowkey1', 'rowkey2', 'rowkey3'])
```

#### `to_h`

`to_h` and `to_H` return the Hash representation of the row.
(The latter returns all values with their timestamp)

If a column is defined in the schema, it is referenced using its quailifier in
Symbol type. If a column is not defined, it is represented as a 2-element Array
of column family in Symbol and column qualifier as ByteArray.
Even so, to make it easier to reference those columns, an extended version of
Hash is returned with which you can also reference them with `FAMILY:QUALIFIER`
notation or `[cf, cq]` array notation.

```ruby
table.put 1000,
  title:      'Hello world', # Known column
  comment100: 'foo',         # Known column
  'cf2:extra' => 'bar',      # Unknown column
  [:cf2, 10]  => 'foobar'    # Unknown column, non-string qualifier

book = table.get 10000
hash = book.to_h
  # {
  #   :title => "Hello world",
  #   [:cf2, HBase::ByteArray<0, 0, 0, 0, 0, 0, 0, 10>] =>
  #       byte[102, 111, 111, 98, 97, 114]@6f28bb44,
  #   :comment100 => "foo",
  #   [:cf2, HBase::ByteArray<101, 120, 116, 114, 97>] =>
  #       byte[98, 97, 114]@77190cfc}
  # }

hash['cf2:extra']
  # byte[98, 97, 114]@77190cfc

hash[%w[cf2 extra]]
  # byte[98, 97, 114]@77190cfc

hash[[:cf2, HBase::ByteArray['extra']]]
  # byte[98, 97, 114]@77190cfc

hash['cf2:extra'].to_s
  # 'bar'

# Columns with non-string qualifiers must be referenced using 2-element Array notation
hash['cf2:10']
  # nil
hash[[:cf2, 10]]
  # byte[102, 111, 111, 98, 97, 114]@6f28bb44

hash_with_versions = table.versions(:all).get(10000).to_H
  # {
  #   :title => {1369019227766 => "Hello world"},
  #   [:cf2, HBase::ByteArray<0, 0, 0, 0, 0, 0, 0, 10>] =>
  #       {1369019227766 => byte[102, 111, 111, 98, 97, 114]@6f28bb44},
  #   :comment100 => {1369019227766 => "foo"},
  #   [:cf2, HBase::ByteArray<101, 120, 116, 114, 97>]  =>
  #       {1369019227766 => byte[98, 97, 114]@77190cfc}}
  # }
```

#### Intra-row scan

Intra-row scan can be done using `each` method which yields `HBase::Cell` instances.

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
# Delete a row
table.delete('rowkey1')

# Delete all columns in the specified column family
table.delete('rowkey1', :cf1)

# Delete a column
table.delete('rowkey1', :author)

# Delete multiple columns
table.delete('rowkey1', :author, :title, :image)

# Delete a column with empty qualifier.
# (!= deleing the entire columns in the family. See the trailing colon.)
table.delete('rowkey1', 'cf1:')

# Delete a version of a column
table.delete('rowkey1', :author, 1352978648642)

# Delete multiple versions of a column
table.delete('rowkey1', :author, 1352978648642, 1352978649642)

# Delete multiple versions of multiple columns
# - Two versions of :author
# - One version of :title
# - All versions of :image
table.delete('rowkey1', :author, 1352978648642, 1352978649642, :title, 1352978649642, :image)

# Batch delete; combination of aforementioned arguments each given as an Array
table.delete(['rowkey1'], ['rowkey2'], ['rowkey3', :author, 1352978648642, 135297864964])
```

However, the last syntax seems a bit unwieldy when you just wish to delete a few rows.
In that case, use simpler `delete_row` method.

```ruby
table.delete_row 'rowkey1'

table.delete_row 'rowkey1', 'rowkey2', 'rowkey3'
```

### INCREMENT: Atomic increment of column values

```ruby
# Atomically increase cf2:reviews by one
inc = table.increment('rowkey1', reviews: 1)
puts inc[:reviews]

# Atomically increase two columns by one and five respectively
inc = table.increment('rowkey1', reviews: 1, stars: 5)
puts inc[:stars]
```

### APPEND

```ruby
ret = table.append 'rowkey1', title: ' (limited edition)', summary: ' ...'
puts ret[:title]   # Updated title
```

### Checked PUT and DELETE

```ruby
table.check(:rowkey, in_print: false)
     .put(in_print: true, price: BigDecimal('10.0'))

table.check(:rowkey, in_print: false)
     .delete(:price, :image)
       # Takes the same parameters as those of HBase::Table#delete
       # except for the first rowkey
       #   https://github.com/junegunn/hbase-jruby#delete
```

### MUTATE: Atomic mutations on a single row (PUTs and DELETEs)

```ruby
# Currently Put and Delete are supported
# - Refer to mutateRow method of org.apache.hadoop.hbase.client.HTable
table.mutate(rowkey) do |m|
  m.put comment3: 'Nice', comment4: 'Great'
  m.delete :comment1, :comment2
end
```

### Batch execution

*Disclaimer*: The ordering of execution of the actions is not defined.
Refer to the documentation of batch method of [HTable class](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/HTable.html).

```ruby
ret = table.batch do |b|
  b.put rowkey1, 'cf1:a' => 100, 'cf1:b' => 'hello'
  b.get rowkey2
  b.append rowkey3, 'cf1:b' => 'world'
  b.delete rowkey3, 'cf2', 'cf3:z'
  b.increment rowkey3, 'cf1:a' => 200, 'cf1:c' => 300
end
```

`batch` method returns an Array of Hashes which contains the results of the
actions in the order they are specified in the block. Each Hash has `:type` entry
(:get, :put, :append, etc.) and `:result` entry. If the type of an action is
:put or :delete, the `:result` will be given as a boolean. If it's an
:increment or :append, a plain Hash will be returned as the `:result`, just like
in [increment](https://github.com/junegunn/hbase-jruby#increment-atomic-increment-of-column-values)
and [append](https://github.com/junegunn/hbase-jruby#append) methods.
For :get action, `HBase::Row` instance will be returned or nil if not found.

If one or more actions has failed, `HBase::BatchException` will be raised.
Although you don't get to receive the return value from batch method,
you can still access the partial results using `results` method of
`HBase::BatchException`.

```ruby
results =
  begin
    table.batch do |b|
      # ...
    end
  rescue HBase::BatchException => e
    e.results
  end
```

### SCAN

`HBase::Table` itself is an enumerable object.

```ruby
# Full scan
table.each do |row|
  p row.to_h
end

# Returns Enumerator when block is not given
table.each.with_index.each_slice(10).to_a
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
      while(reviews: { gt: 20 }).           # Early termination of scan
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

You can configure server-side filtering of rows and columns with
`HBase::Scoped#filter` calls. Multiple calls have conjunctive effects.

```ruby
# Range scanning the table with filters
table.range(nil, 1000).
      filter(
        # Equality match
        year: 2013,

        # Range of numbers or characters: Checks if the value falls within the range
        weight: 2.0..4.0,
        author: 'A'..'C'

        # Will match rows *without* price column
        price: nil,

        # Regular expression: Checks if the value matches the regular expression
        summary: /classic$/i,

        # Hash: Tests the value with 6 types of operators (:gt, :lt, :gte, :lte, :eq, :ne)
        reviews: { gt: 100, lte: 200 },

        # Array of the aforementioned types: OR condition (disjunctive)
        category: ['Fiction', 'Comic', /science/i, { ne: 'Political Science' }]).

      # Multiple calls for conjunctive filtering
      filter(summary: /instant/i).

      # Any number of Java filters can be applied
      filter(org.apache.hadoop.hbase.filter.RandomRowFilter.new(0.5)).
  each do |record|
  # ...
end
```

### *while*

`HBase::Scoped#while` method takes the same parameters as `filter` method, the
difference is that each filtering condition passed to `while` method is wrapped
by `WhileMatchFilter`, which aborts scan immediately when the condition is not
met at a certain row. See the following example.

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
to avoid fetching all columns at once. However setting batch size allows
multiple rows with the same row key are returned during scan.

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
#   minimizes the amount of data transfer using KeyOnlyFilter
#   (and FirstKeyOnlyFilter when no filter is set)
scoped.count

# This should be even faster as it dramatically reduces the number of RPC calls
scoped.caching(1000).count

# count method takes an options Hash:
# - :caching (default: nil)
# - :cache_blocks (default: true)
scoped.count(caching: 5000, cache_blocks: false)
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

These String key-value pairs are not really a part of the public API of HBase,
and thus might change over time. However, they are most useful when you need to
create a table with the same properties as the existing one.

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

`HBase#Table` provides a number of *bang_methods!* for table administration
tasks. They run synchronously, except when mentioned otherwise (e.g.
`HTable#split!`). Some of them take an optional block to allow progress
monitoring and come with non-bang, asynchronous counterparts. If you're
running an old version of HBase cluster, you'll have to `disable!` the table
before altering it.

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
| ------------------------ | ------------- | ------------------------------------------------------------------------------------------------------------------ |
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
| `:config`                | Hash          | Additional XML configuration                                                                                       |

#### List of table properties

http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HTableDescriptor.html

| Property              | Type          | Description                                                                                               |
| --------------------- | ------------- | --------------------------------------------------------------------------------------------------------- |
| `:max_filesize`       | Fixnum        | The maximum size upto which a region can grow to after which a region split is triggered                  |
| `:readonly`           | Boolean       | If the table is read-only                                                                                 |
| `:memstore_flushsize` | Fixnum        | The maximum size of the memstore after which the contents of the memstore are flushed to the filesystem   |
| `:deferred_log_flush` | Boolean       | Defer the log edits syncing to the file system (deprecated in 0.96)                                       |
| `:durability`         | Symbol/String | Durability setting of the table                                                                           |
| `:split_policy`       | String/Class  | Region split policy                                                                                       |
| `:splits`             | Array         | Region split points                                                                                       |
| `:config`             | Hash          | Additional XML configuration                                                                              |

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

### Thread-safety

You can freely share a `HBase::Table` instance among threads, as it is backed by
thread-local HTable instances. ([HTable instance in itself is not
thread-safe](https://hbase.apache.org/book/client.html))

```ruby
table = hbase[:my_table]

10.times.map do |i|
  Thread.new do
    table.put i, data
  end
end.each(&:join)
```

### Lexicographic scan order

HBase stores rows in the lexicographic order of the rowkeys in their byte array
representations. Therefore, the type of the row key affects the scan order.

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

If a column qualifier is not a String, a 2-element Array should be used.

```ruby
table.put 'rowkey',
  [:cf1, 100  ] => "Byte representation of an 8-byte integer",
  [:cf1, bytes] => "Qualifier is an arbitrary byte array"

table.get('rowkey')[:cf1, 100]
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
                          'cf1:d' => 400)             # Ordinary 8-byte integer

row = table.get(int: 12345)
```

The use of these Hash-notations can be minimized if we define table schema as follows.

```ruby
hbase.schema[table.name] = {
  cf1: {
    a: :byte,
    b: :short,
    c: :int,
    d: :fixnum
  }
}

table.put({ int: 12345 }, a: 100, b: 200, c: 300, d: 400)
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
second.decode(:double)  # 3.14
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
ba.shift(:double)
ba.shift(:int)
ba.shift(:string, 11)  # Byte length must be given as Strings are not fixed in size
```

`ByteArray#java` method returns the underlying native Java byte array.

```ruby
ba.java  # Returns the native Java byte array (byte[])
```

## API documentation

[http://www.rubydoc.info/gems/hbase-jruby/](http://www.rubydoc.info/gems/hbase-jruby/)

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
