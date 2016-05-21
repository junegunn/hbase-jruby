Changelog
=========

0.7.1
-----

- Support setting one timestamp for all columns (#41)

0.7.0
-----

- Distinguish float and double
    - `Cell#float` / `Cell#double`
    - `Row#float` / `Row#double`
    - `Row#floats` / `Row#doubles`
    - `Util.to_bytes 3.14` (double by default)
    - `Util.to_bytes :float => 3.14`
    - `Util.to_bytes :double => 3.14`
    - `Util.from_bytes :float, bytes'`
- Removed automatic disable/enable to support online alter
- Fixed `HBase::Table#alter_family[!]` not to drop previous non-default
  settings
- Performance improvement with column key interpretation cache
- Dependency resolution is now deprecated
- HBase 1.0 client compatibility

0.6.4
-----

### Performance improvement (experimental)

`HBase::Table` instance can be set up to cache the interpretations of the
column keys using thread-locals which can lead to 3-times faster Put
generation in tight loops.

```ruby
table = hbase[:my_table, cache: true]
# ...
table.close
```

However, the option is off by default because of the following issues:

1. You should not use it with the tables with unlimited number of columns.
2. Caching does not keep track of the updates of the schema. If you change the
   schema of an `HBase` object after you created an `HBase::Table` object with
   the cache turned on from it, the object may hold stale information on the
   types of the columns. `HBase::Table#close` method will clean up the cache.

0.6.3
-----

- Refactoring
    - Changed `HBase::Row#to_h` call to return a Hash extended with
      `HBase::Row::HashExtension` module which makes it possible to extend the
      returned Hash by monkey-patching the module
    - `HBase::Row::HashExtension` overrides `#[]` method to allow looking up data
      with both Array and String notation of column keys: `[CF, CQ]` or `CF:CQ`
- Hash for describing column and table options can now contain XML entries
  using additional `:config` key.

```ruby
hbase[:my_table].create! :d,
    # HTableDescriptor#setValue
   'MAX_FILESIZE' => 1 << 33,
   :config => {
     # HTableDescriptor#setConfiguration
     'hbase.hstore.compaction.max.size' => 1 << 30
   }
```

0.6.2
-----

- Removed automatic cleanup of thread-local HTable instances
    - Fixes issue #35
    - Close of individual HTable instance turned out to be unnecessary
        - `flushCommits` not required as `hbase-jruby` runs in auto-flush mode
        - No need to close shared connection or its thread pool

0.6.1
-----

- Fixed error on JRuby 1.7.13 (#34)

0.6.0
-----

0.6.0 includes a few backward-incompatible changes.

- Default number of versions for a scan or get is now changed to 1 to match Java API
    - Make sure to set versions to `:all` if you're calling `to_H` or plural datatype methods
        - `scoped.versions(:all).to_H`
        - `scoped.versions(:all).strings('d:title')`
- Removed `HBase::Table::BatchAction#mutate` method
    - RowMutation is not allowed in `HBase::Table#batch`
        - https://issues.apache.org/jira/browse/HBASE-11421
- Added CDH5.1 dependency profile

0.5.1
-----
- Fixed `Table#aggregate` for HBase 0.98 or above

0.5.0
-----
- Added CDH5.0 (HBase 0.96.1.1) and CDH4.6 dependency profiles
    - If you share HBase instance between threads, you may need to set
      `hbase.hconnection.threads.core` due to
      [a bug](https://issues.apache.org/jira/browse/HBASE-10449) in CDH5.5.0.
- Use HConnection instead of deprecated HTablePool when possible

0.4.6
-----
- [#29 Fix possible HTable leaks](https://github.com/junegunn/hbase-jruby/issues/29)
- [#30 Make `HBase.log4j=` callable before dependencies are met](https://github.com/junegunn/hbase-jruby/issues/30)

0.4.5
-----
- Fixed HBase 0.96 compatibily issues and tested on HBase 0.96 and 0.98
- Added `:split_policy` table property
- `Table#properties` and `Table#families` now include previously unknown
  properties as String-String pairs
- Added `Row#byte_array` which returns an instance of `HBase::ByteArray`
    - Equivalent to `HBase::ByteArray[row.raw(col)]`


0.4.4
-----
- Fixed `HBase::Table#raw_families/raw_properties` on HBase shell

0.4.3
-----
- Fixed `HBase::Table#add_coprocessor[!]` and `HBase::Table#remove_coprocessor[!]`

0.4.2
-----
- Fixed bug when using schema with non-String/Symbol qualifier
- Updated dependency profiles
  - Added CDH4.5, CDH4.4 (same as CDH4.3)
  - 0.95, 0.96 (experimental, currently not working)
- Improved compatibility with HBase shell which is based on JRuby 1.6.5

0.4.1
-----
- Fixed .META. scanning with range prefix ([#26](https://github.com/junegunn/hbase-jruby/issues/26))
- Added `ByteArray#as` as a synonym for `ByteArray#decode`

0.4.0
-----
- Added support for append operation: `HBase::Table#append`
- Added support for atomic mutations on a single row: `HBase::Table#mutate`
- Added support for batch operations: `HBase::Table#batch`
  - This method does not take an argument and requires a block
  - Don't be confused with the shortcut method to `HBase::Scoped#batch(batch_size)`
- Changed `HBase::Table#increment` to return the updated values as a Hash
- Fixed HBase.resolve_dependency!(:local) on CDH distribution
- Empty-qualifier must be given as 'cf:', and not 'cf'
- Added `HBase::Row#empty?` method
- Added `HBase::ByteArray#to_s` method

0.3.5
-----
- Improved `Scoped#count` method
  - KeyOnlyFilter turned out to be compatible with SingleColumnValueFilter
  - Now takes an optional Hash: `scoped.count(cache_blocks: false, caching: 100)`
  - Changed not to disable server-side block caching by default
- Supports `Scoped#limit` even when `Scan.setMaxResultSize` is not implemented
  - `Scoped#limit(nil)` will remove the previously set value

0.3.4
-----
- Cleanup all thread-local (fiber-local) HTable references when connection is closed
- Added `HBase#reset_table_pool` method for recreating HTable pool

0.3.3
-----

0.3.3 changes the way null values are handled, and introduces interface for CAS operations.
It is strongly advised that you upgrade to 0.3.3 since it contains important fixes.

- [PUT will not store null values](https://github.com/junegunn/hbase-jruby/issues/15)
- `filter(column: nil)` will match rows without the column
- `filter(column: value)` will *NOT* match rows without the column
  - However, `filter(column: { ne: value })` *WILL* match rows without the column
- [Added `HBase::Table#check` method for check-and-put and check-and-delete operations](https://github.com/junegunn/hbase-jruby/issues/14)
  - `bool = table.check(1, in_print: false).delete(:price)`
  - `bool = table.check(1, in_print: true).put(price: 100)`
- Fix: invalid count when filter is used

0.3.2
-----
- Added CDH4.3 dependency profile

0.3.1
-----
- Fixed a bug in `'cf:cq' => :type` shortcut schema definition
- Added schema data type validation
- Fixed reference to ambiguous column names

0.3.0
-----
- Easier data access with table schema
  - `HBase::Table#schema=` method to provide the schema of the table
  - You can omit column family names on predefined columns
  - Automatic type conversion for known columns
- *0.3.0 brings many backward-incompatible changes*
  - *`Row#to_hash` and `Row#to_hash_with_versions` are now deprecated*. Use `to_h` and `to_H` instead without arguments.
  - Default parameters for `HBase::Row#rowkey` and `HBase::Cell#rowkey` are now `:raw` instead of `:string`.
  - `HBase::ColumnKey` is removed. Use plain 2-element Arrays instead.
  - Enumerable classes (Table, Scoped, Row, ByteArray) now return Enumerator on each method when block not given
  - `Cell#value` can now return the correct data type if defined in the schema. For Java byte array, use `Cell#raw`.
  - `Row#[type|types]` methods no more take Array of columns as arguments
- Added `HBase::Table#scoped` method to return `HBase::Scoped` object for the table
- Added `HBase::Cell#{eql?,hash}` method for equaility check

0.2.6
-----
- Fixed `HBase::Scoped#filter` method to take short integer (byte, short, int) values
- Fixed `HBase::Scoped#range` method to take short integer (byte, short, int) values

0.2.5
-----
- Added `HBase::Table#snapshots` method
- Added `HBase::Table#snapshot!` method
- Added `HBase#snapshots` method
- Added `HBase::Cell#{raw,int,short,byte}` methods
- Updated dependency profiles: cdh4.2.1, cdh4.1.4

0.2.4
-----
- Fixed NameError when HBase::ByteArray is used without first creating an HBase instance
- Updated dependency profiles: 0.95.0, 0.94.6.1, 0.92.2, cdh3u6

0.2.3
-----
- Fix: [Thread.current[:htable] must be local to each connection](https://github.com/junegunn/hbase-jruby/issues/4)
- Fix: [`HBase.log4j=` to support XML based configuration](https://github.com/junegunn/hbase-jruby/issues/5)
- Automatically set versions to 1 when counting records
- New dependency profile: `cdh4.2`

0.2.2
-----
- Added `HBase::Table#delete_row` method
- Dependency profiles as prefixes
  - Supported prefixes: `cdh4.1`, `cdh3`, `0.94`, `0.92`
  - e.g. `HBase.resolve_dependency! 'cdh4.1.3'`
- Advanced data access with `Scoped#with_java_scan` and `Scoped#with_java_get`

0.2.1
-----
- Fix: NameError even when appropriate CLASSPATH is set

0.2.0
-----
- Deprecated `HBase::Table#close`. You don't need to close Table instances.
- Added `HBase::Table#split` and `HBase::Table#split!` method
- Added `:splits` option to `HTable#create!` method to pre-split the table
- Added table inspection methods: `properties`, `families`, and `regions`
- Added raw inspection methods: `raw_properties` and `raw_families`
- Added `HBase.log4j=` method
- Added `HBase::Scoped#at`, `HBase::Scoped#time_range` method
- Changed parameters to `HBase.resolve_dependency!` method
- Ruby Time object can be used as timestamp in put and delete methods
- Using closed HBase connection is disallowed
- Ruby 1.8 compatibility mode (Oops!)

0.1.6
-----
- Maven dependencies for 0.94 and 0.92
- Progress reporting for synchronous table administration
- Added asynchronous versions of table administration methods

0.1.5
-----
- Added support for shorter integers
- Extended `HBase::ByteArray` for easy manipulation of Java byte arrays

0.1.4
-----
- Fix: Start/stop row not correctly set when byte array rowkey range specified
- More efficient count with FirstKeyOnlyFilter
- Added `HBase::ByteArray` method as a shorthand notation for `HBase::ByteArray.new`
- Added `HBase::ByteArray#+` method for concatenating two byte arrays
- Added `HBase::Util::java_bytes?` method
- Documentation

0.1.3
-----
- Supports Ruby 1.8 compatibility mode
- Fix: Correct return value from `HBase::resolve_dependency!`
- Fix: Appropriately close result scanners

0.1.2
-----

- Dropped Bignum support. Automatic conversion from Fixnum to Bignum (or vice versa)
  will produce columns whose values are of heterogeneous types, that are impossible to be read in a consistent way.
  You can use BigDecimal type instead for large numbers.
- Added `HBase::Scoped#while` which allows early termination of scan
  with [WhileMatchFilter](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/WhileMatchFilter.html)
- Filtering with regular expressions
- Implemented comparator for `HBase::Result`
- Added coprocessor administration methods
- Basic aggregation with Coprocessor
- `HBase::Scoped#count` with block
- Allows PUT operation with timestamps
