#!/usr/bin/env ruby
# encoding: utf-8

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'
require 'set'

class TestSchema < TestHBaseJRubyBase
  def teardown
    @hbase.schema[@table.name] = {}

    # Same
    @hbase.schema.delete @table.name
  end

  def test_invalid_schema_type
    @hbase.schema = { @table.name => { :cf1 => { :a => 'string' } } }

    assert_raise(ArgumentError) do
      @hbase.schema = { @table.name => { :cf1 => { :a => :xxx } } }
    end
  end

  def test_schema
    @hbase.schema = {
      @table.name => {
        :cf1 => {
          :a    => :fixnum,
          :b    => :symbol,
          :c    => :int,
          /^d/i => :float,
          'd2'  => :short,
        },
        # Every column from cf2 is :string
        :cf2 => { :e => :string },

        # cf3:f is a 8-byte integer
        :cf3 => { :f => :fixnum },
        'cf3:g' => :float
      }
    }

    data = {
      :a  => 100,
      :b  => :symb,
      :c  => 200,
      :d  => 3.14,
      :d2 => 300,
      :e  => 'Hello',
      :f  => 400,
      [:cf1, HBase::ByteArray['x']] => 500
    }

    # PUT
    @table.put 1, 'cf1:a' => data[:a], 'cf1:b'  => data[:b],    'cf1:c' => data[:c],
                  'cf1:d' => data[:d], 'cf1:d2' => data[:d2],   'cf2:e'  => data[:e],
                  'cf3:f' => data[:f], 'cf1:x'  => data[[:cf1, HBase::ByteArray['x']]]
    @table.put 2, data

    # GET
    row = @table.get(2)
    assert_equal Set[ *@table.get(1).to_h.values.map { |b| HBase::ByteArray.new b }],
                 Set[ *          row.to_h.values.map { |b| HBase::ByteArray.new b }]
    assert_equal Set[ *@table.get(1).to_h.keys ], Set[ *row.to_h.keys ]
    assert_equal Set[ *data.keys ],               Set[ *row.to_h.keys ]

    assert_equal data[:a],  row[:a]
    assert_equal data[:a],  row['a']

    assert_equal data[:b],  row[:b]
    assert_equal data[:b],  row['b']

    assert_equal data[:c],  row[:c]
    assert_equal data[:c],  row['c']

    assert_equal data[:d],  row[:d]
    assert_equal data[:d],  row['d']

    assert_equal data[:d2], row[:d2]
    assert_equal data[:d2], row['d2']

    assert_equal data[:e],  row['cf2:e']
    assert_equal data[:f],  row['cf3:f']

    assert_equal data[[:cf1, HBase::ByteArray['x']]], HBase::Util.from_bytes(:long, row['cf1:x'])

    data1 = @table.get(1).to_h
    data1[:a] *= 2
    data1[:b] = :new_symbol
    @table.put 3, data1
    @table.increment 3, :a => 5

    # PUT again
    assert_equal data[:a] * 2 + 5, @table.get(3)[:a]
    assert_equal :new_symbol,      @table.get(3)[:b]
    assert_equal :new_symbol,      @table.get(3)['b']
    assert_equal :new_symbol,      @table.get(3)['cf1:b']

    # PROJECT
    assert_equal [:a, :c, :e, :f],
      @table.project(:a, 'cf1:c', :cf2, :cf3).first.to_h.keys

    # FILTER
    assert_equal 1, @table.filter(:b => :new_symbol).count
    assert_equal 2, @table.filter(:b => data[:b]).count

    assert_equal 2, @table.filter(  'cf1:a' => 50..110).count
    assert_equal 2, @table.filter(      'a' => 50..110).count
    assert_equal 2, @table.filter(       :a => 50..110).count
    assert_equal 1, @table.filter(:a => { :gt => 150 }).count

    # cf:g (automatic type conversion)
    @table.put   3,    :g => 3.14
    assert_equal 3.14, @table.get(3)[:g]
    @table.put   3,    :g => 314
    assert_equal 314,  @table.get(3)[:g]

    # cf3:g vs. cf2:g
    @table.put   4,    :g => 3.14, 'cf2:g' => 'String'
    assert_equal 3.14,     @table.get(4)[:g]
    assert_equal 'String', @table.get(4)['cf2:g'].to_s
    assert_equal 3.14,     @table.get(4).to_h[:g]
    assert_equal 'String', @table.get(4).to_h['cf2:g'].to_s
  end

  def test_schema_readme
    @hbase.schema[@table.name] = {
      :cf1 => {
        :name   => :string,
        :age    => :fixnum,
        :sex    => :symbol,
        :height => :float,
        :weight => :float,
        :alive  => :boolean
      },
      :cf2 => {
        :description => :string,
        /^score.*/   => :float
      }
    }

    data = {
      :name        => 'John Doe',
      :age         => 20,
      :sex         => :male,
      :height      => 6.0,
      :weight      => 175,
      :description => 'N/A',
      :score1      => 8.0,
      :score2      => 9.0,
      :score3      => 10.0,
      :alive       => true
    }

    @table.put(100 => data)

    john = @table.get(100)

    data.each do |k, v|
      assert_equal v, john[k]
      assert_equal v, john[k.to_s]
    end

    assert_equal 20,   john[:age]
    assert_equal 10.0, john[:score3]

    assert_equal data, john.to_h
    assert_equal data, Hash[ john.to_H.map { |k, v| [k, v.first.last] } ]
  end

  def test_schema_book
    table = @table

    @hbase.schema[table.name] = {
      # Columns in cf1 family
      :cf1 => {
        :title    => :string,
        :author   => :string,
        :category => :string,
        :year     => :short,
        :pages    => :int,
        :price    => :bigdecimal,
        :weight   => :float,
        :in_print => :boolean,
        :image    => :raw
      },
      # Columns in cf2 family
      :cf2 => {
        :summary      => :string,
        :reviews      => :fixnum,
        :stars        => :fixnum,
        /^comment\d+/ => :string
      }
    }

    # Put data (rowkey: 1)
    data = {
      :title    => 'The Golden Bough: A Study of Magic and Religion',
      :author   => 'Sir James G. Frazer',
      :category => 'Occult',
      :year     => 1890,
      :pages    => 1006,
      :price    => BigDecimal('21.50'),
      :weight   => 3.0,
      :in_print => true,
      :image    => File.open(__FILE__, 'rb') { |f| f.read }.to_java_bytes, # 안녕?
      :summary  => 'A wide-ranging, comparative study of mythology and religion',
      :reviews  => 52,
      :stars    => 226,
      :comment1 => 'A must-have',
      :comment2 => 'Rewarding purchase'
    }
    table.put 1, data
    # Since we can't directly compare java byte arrays
    data[:image] = HBase::ByteArray[ data[:image] ]

    # Get data (rowkey: 1)
    book = table.get 1

    assert_equal data,            book.to_h.tap { |h| h[:image] = HBase::ByteArray[ h[:image] ] }
    assert_equal data[:title],    book['title']
    assert_equal data[:comment2], book['comment2']

    assert HBase::Util.java_bytes?(book[:image])
    if defined?(Encoding) # --1.8
      assert_equal Encoding::ASCII_8BIT, book[:image].to_s.encoding
    end

    assert_equal true, book.to_H.values.map(&:keys).flatten.all? { |e| e.is_a? Fixnum }

    # Scan table
    table.range(0..100).
          filter(:year     => 1880...1900,
                 :in_print => true,
                 :category => ['Comics', 'Fiction', /cult/i],
                 :price    => { :lt => BigDecimal('30.00') },
                 :summary  => /myth/i).
          project(:cf1, :reviews).
          each do |book|

      assert_equal data[:title], book[:title]
      assert_equal data[:reviews], book[:reviews]
      assert_equal nil, book[:summary]

      # Update price
      table.put book.rowkey => { :price => book[:price] + BigDecimal('1') }

      # Atomic increment
      table.increment book.rowkey, :reviews => 1, :stars => 5
    end

    assert_equal data[:price]   + 1.0, table.get(1)[:price]
    assert_equal data[:reviews] + 1,   table.get(1)[:reviews]
    assert_equal data[:stars]   + 5,   table.get(1)[:stars]

    # Coprocessor
    table.enable_aggregation!
    table.put 2, :reviews => 100, :stars => 500
    assert_equal data[:reviews] + 1 + data[:stars] + 5 + 100 + 500,
      table.project(:reviews, :stars).aggregate(:sum)
    #table.disable_aggregation!

    # Undefined columns
    table.put 1, 'cf1:x'      => 1000
    table.put 1, [:cf1, :y]   => 2000
    table.put 1, [:cf1, 2013] => 3000
    assert_equal 1000, table.get(1).fixnum('cf1:x')

    [
      [:cf1, HBase::ByteArray['x']],
      ['cf1', HBase::ByteArray['x']],
      [:cf1, :x],
      [:cf1, 'x'],
      ['cf1', :x],
      %w[cf1 x],
      'cf1:x'
    ].each do |param|
      assert_equal 1000, HBase::Util.from_bytes(:fixnum, table.get(1)[param])
      assert_equal 1000, HBase::Util.from_bytes(:fixnum, table.get(1)[*param])
      assert_equal 1000, HBase::Util.from_bytes(:fixnum, table.get(1).to_h[param])
    end

    assert_equal 2000, HBase::Util.from_bytes(:fixnum, table.get(1)[:cf1, :y])
    assert_equal 3000, HBase::Util.from_bytes(:fixnum, table.get(1)[:cf1, 2013])
    assert_equal 3000, HBase::Util.from_bytes(:fixnum, table.get(1)[[:cf1, 2013]])
    assert_equal 3000, HBase::Util.from_bytes(:fixnum, table.get(1).to_h[[:cf1, HBase::ByteArray[2013]]])
    assert_equal 3000, HBase::Util.from_bytes(:fixnum, table.get(1).to_h[[:cf1, 2013]])

    # Delete :title column of book 1
    table.delete 1, :title
    assert_equal nil, table.get(1)[:title]
    assert_equal data[:author], table.get(1)[:author]

    # Delete column family
    table.delete 1, :cf1
    assert_equal nil, table.get(1)[:author]
    assert_equal data[:summary], table.get(1)[:summary]

    # Delete book 1
    table.delete 1
    assert_equal nil, table.get(1)

    # Drop table for subsequent tests
    table.drop!
  end
end

