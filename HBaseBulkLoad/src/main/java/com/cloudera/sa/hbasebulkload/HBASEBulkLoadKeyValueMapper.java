/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.hbasebulkload;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author vsingh
 */
class HBASEBulkLoadKeyValueMapper extends
    Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  CSVParser csvParser;
  String hbaseTabName = "";
  String hbaseColumnFamily = "";
  String[] hbaseColumns = {""};
  ImmutableBytesWritable hKey = new ImmutableBytesWritable();
  Put hPut;
  String hbaseColumnsSeperator;
  String[] outputFields;
  int noOfColumns = 0;

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    Configuration config = context.getConfiguration();
    hbaseTabName = config.get(HBASEBulkLoadConstants.HBASE_TABLE_KEY);
    hbaseColumnFamily = config.get(HBASEBulkLoadConstants.HBASE_COLUMN_FAMILY_KEY);

    hbaseColumnsSeperator = config.get(
        HBASEBulkLoadConstants.HBASE_COLUMN_SEPERATOR_KEY);
    if (hbaseColumnsSeperator == null || hbaseColumnsSeperator.length() != 1) {
      throw new IllegalArgumentException("column delimiter must be exactly one character");
    }
    csvParser = new CSVParserBuilder()
        .withSeparator(hbaseColumnsSeperator.charAt(0))
        .build();
    hbaseColumns = csvParser.parseLine(config.get(
        HBASEBulkLoadConstants.HBASE_COLUMNS_KEY));
    noOfColumns = hbaseColumns.length;

  }

  /*@Override
   protected void cleanup(Context context) {
   }*/
  @Override
  protected void map(LongWritable key, Text line, Context context)
      throws IOException, InterruptedException {

    hPut = buildPut(line.toString(), csvParser, hbaseColumnFamily, hbaseColumns);
    hKey.set(hPut.getRow());
    context.write(hKey, hPut);
  }

  static Put buildPut(String line, CSVParser parser, String columnFamily, String[] columns)
      throws IOException {
    if (parser == null) {
      throw new IllegalArgumentException("parser must not be null");
    }
    if (line == null || line.isBlank()) {
      throw new IllegalArgumentException("input row must not be blank");
    }
    if (columnFamily == null || columnFamily.isBlank()) {
      throw new IllegalArgumentException("column family must not be blank");
    }
    if (columns == null || columns.length < 2) {
      throw new IllegalArgumentException("columns must include a row key and at least one value");
    }

    for (String column : columns) {
      if (column == null || column.isBlank()) {
        throw new IllegalArgumentException("column names must not be blank");
      }
    }
    String[] fields = parser.parseLine(line);
    if (fields.length != columns.length || fields[0] == null || fields[0].isBlank()) {
      throw new IllegalArgumentException("input row does not contain all configured columns");
    }

    byte[] family = org.apache.hadoop.hbase.util.Bytes.toBytes(columnFamily);
    Put put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(fields[0]));
    for (int i = 1; i < columns.length; i++) {
      if (fields[i] != null && !fields[i].isBlank()) {
        put.addColumn(
            family,
            org.apache.hadoop.hbase.util.Bytes.toBytes(columns[i]),
            org.apache.hadoop.hbase.util.Bytes.toBytes(fields[i]));
      }
    }
    return put;
  }
}
