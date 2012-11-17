# hbase-jruby

hbase-jruby provides Ruby-esque interface for accessing HBase from JRuby.

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
table.range('rowkey1'..'rowkey9').filter('cf1:a' => 100..200).each do |row|
  puts row.integer('cf1:a')
end
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

hbase-jruby includes `HBase.resolve_dependency!` helper method,
which resolves Hadoop/HBase dependency.

#### Preconfigured dependencies

A natural way to resolve class dependencies in JVM environment is to use Maven.
Current version of hbase-jruby is shipped with Maven dependency specifications
for the following Hadoop/HBase distributions.

* cdh4.1.2
* cdh3u5 (NOT TESTED :p)

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

HBase stores everything as byte array, so when you fetch data from HBase,
you need to explicitly specify the type of each value stored.

```ruby
row = table.get('rowkey1')

rowk = row.rowkey
col1 = row.string  'cf1:col1'
col2 = row.fixnum  'cf1:col2'
col3 = row.bignum  'cf1:col3'
col4 = row.float   'cf1:col4'
col5 = row.boolean 'cf1:col5'
col6 = row.json    'cf1:col6'

# Returns the Hash representation of the record with the specified schema
schema = {
  'cf1:col1' => :string,
  'cf1:col2' => :fixnum,
  'cf1:col3' => :bignum,
  'cf1:col4' => :float,
  'cf1:col5' => :boolean,
  'cf1:col6' => :json }
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

# Atomically increase two columns
table.increment('rowkey1', 'cf1:counter' => 1, 'cf1:counter2' => 2)
```

## Scanning the table

`HBase::Table` itself is an enumerable object.

```ruby
# Full scan
table.each do |record|
  # ...
end

# First row
table.first.to_hash schema
```

Scan operation is actually implemented in enumerable `HBase::Scoped` class,
whose instance is implicitly used/returned by `HBase::Table#each` call.

```ruby
scoped = table.each
```

An `HBase::Scoped` object responds to a various scan related methods,
such as `range`, `filter`, `project`, `versions`, `caching`, et cetera.
However, it doesn't respond to non-scan methods like `put`, `get`, `delete` and `increment`,
and methods for table administration.
`HBase::Table` object also responds to scan related methods described above,
which is simply forwarded to a new `HBase::Scoped` object implicitly created.

### Range scan on rowkeys

```ruby
table.range(100..900).each  { |row| process row }  # 100 ~ 900 (inclusive end)
table.range(100...900).each { |row| process row }  # 100 ~ 900 (exclusive end)
table.range(100, 900).each  { |row| process row }  # 100 ~ 900 (exclusive end)
table.range(100).each       { |row| process row }  # 100 ~
table.range(nil, 900).each  { |row| process row }  #     ~ 900 (exclusive end)
```

### Filters

```ruby
# Range scanning the table with filters
table.range(nil, 1000).
      filter('cf1:a' => 'Hello',
             'cf1:b' => 100...200,
             'cf1:c' => 'Alice'..'Bob').each do |record|
  # ...
end

# Fetches a subset of columns
table.project('cf1:a', 'cf2').each do |record|
  # ...
end

# Chaining methods
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter

table.range('A'..'Z').                          # Range scan
      project('cf1:a').                         # Select cf1:a column
      project('cf2').                           # Select cf2 family as well
      filter('cf1:a' => 'Hello').               # Filter by cf1:a value
      filter('cf2:d' => 100..200).              # Range filter on cf2:d
      filter(ColumnPaginationFilter.new(3, 1)). # Any HBase filter
      limit(10).                                # Limits the size of the result set
      versions(2).                              # Only fetches 2 versions for each value
      caching(100).                             # 100 rows for caching
      to_a
```

### Counting the rows

When counting the number of rows, use `HTable#count` instead of iterating through the scope,
as it internally minimizes amount of data fetched.

```ruby
# Counting the table
table.count
table.range('A'..'C').count
table.range('A'..'C').filter('cf1:a' => 100..200).count
```

## Advanced uses

### Using non-string rowkeys

```ruby
table = hbase.table TABLE_NAME, string_rowkey: false
table.put my_byte_array, 'cf1:hello' => 'world'
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
