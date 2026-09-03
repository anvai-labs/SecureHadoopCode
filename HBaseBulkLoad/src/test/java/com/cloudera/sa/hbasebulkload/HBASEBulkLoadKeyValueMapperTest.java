package com.cloudera.sa.hbasebulkload;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

class HBASEBulkLoadKeyValueMapperTest {
  private final CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

  @Test
  void buildsUtf8PutAndSkipsBlankValues() throws IOException {
    Put put = HBASEBulkLoadKeyValueMapper.buildPut(
        "clé,José,", parser, "profile", new String[] {"row", "name", "empty"});

    assertArrayEquals(Bytes.toBytes("clé"), put.getRow());
    assertEquals(1, put.size());
    Cell cell = put.getFamilyCellMap().get(Bytes.toBytes("profile")).get(0);
    assertEquals("name", Bytes.toString(CellUtil.cloneQualifier(cell)));
    assertEquals("José", Bytes.toString(CellUtil.cloneValue(cell)));
  }

  @Test
  void rejectsMalformedRowsAndConfiguration() {
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadKeyValueMapper.buildPut("", parser, "f", new String[] {"row", "v"}));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadKeyValueMapper.buildPut("key", parser, "f", new String[] {"row", "v"}));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadKeyValueMapper.buildPut("key,value,extra", parser, "f", new String[] {"row", "v"}));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadKeyValueMapper.buildPut(",value", parser, "f", new String[] {"row", "v"}));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadKeyValueMapper.buildPut("key,value", parser, "", new String[] {"row", "v"}));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadKeyValueMapper.buildPut("key,value", parser, "f", new String[] {"row", ""}));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadKeyValueMapper.buildPut("key,value", parser, "f", new String[] {"", "v"}));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadKeyValueMapper.buildPut("key,value", null, "f", new String[] {"row", "v"}));
  }
}
