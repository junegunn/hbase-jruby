Changelog
=========

0.2.0
-----
- Deprecated `HBase::Table#close`. You don't need to close Table instances.
- Added `HBase::Table#split` and `HBase::Table#split!` method
- Added `:splits` option to `HTable#create!` method to pre-split the table
- Added table inspection methods: `properties`, `families`, and `regions`
- Added raw inspection methods: `raw_properties` and `raw_families`
- Added `HBase.log4j=` method
- Added `HBase::Scoped#at`, `HBase::Scoped#time_range` method
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
