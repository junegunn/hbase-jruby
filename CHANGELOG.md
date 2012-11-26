Changelog
=========

0.1.2
-----

- Dropped Bignum support. Implicit conversion from Fixnum to Bignum (and vice versa),
  will produce columns with heterogeneous data types. Use BigDecimal type instead.
- `HBase::Scope#while` added.
  Allows early termination of scan with [WhileMatchFilter](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/WhileMatchFilter.html)

