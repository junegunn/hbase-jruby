Changelog
=========

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
