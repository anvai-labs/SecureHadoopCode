package com.cloudera.sa.hbasebulkload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class HBASEBulkLoadDriverTest {
  @Test
  void validatesConfiguresAndDelegatesWithoutDeletingOutput() throws Exception {
    AtomicReference<HBASEBulkLoadDriver.BulkLoadRequest> captured = new AtomicReference<>();
    HBASEBulkLoadDriver driver = new HBASEBulkLoadDriver((config, request) -> {
      captured.set(request);
      assertEquals("EVENTS", config.get(HBASEBulkLoadConstants.HBASE_TABLE_KEY));
      assertEquals("profile", config.get(HBASEBulkLoadConstants.HBASE_COLUMN_FAMILY_KEY));
      assertEquals("ROW,NAME", config.get(HBASEBulkLoadConstants.HBASE_COLUMNS_KEY));
      assertEquals(",", config.get(HBASEBulkLoadConstants.HBASE_COLUMN_SEPERATOR_KEY));
      return HBASEBulkLoadConstants.SUCCESS;
    });
    driver.setConf(new Configuration(false));

    assertEquals(0, driver.run(new String[] {
        "input", "output", " EVENTS ", "profile", "ROW,NAME", ","
    }));
    assertEquals("input", captured.get().input().toString());
    assertEquals("output", captured.get().output().toString());
  }

  @Test
  void rejectsBadArgumentCountsAndValuesBeforePipelineRuns() throws Exception {
    HBASEBulkLoadDriver driver = new HBASEBulkLoadDriver((config, request) -> {
      throw new AssertionError("pipeline must not run");
    });
    driver.setConf(new Configuration(false));

    assertEquals(2, driver.run(new String[] {"too", "few"}));
    assertThrows(IllegalArgumentException.class,
        () -> driver.run(new String[] {"in", "out", "table", "family", "row,value", "||"}));
    assertThrows(IllegalArgumentException.class,
        () -> driver.run(new String[] {"in", "out", " ", "family", "row,value", ","}));
  }

  @Test
  void normalizesRequiredValuesAndRejectsMissingValues() {
    assertEquals("table", HBASEBulkLoadDriver.requireValue(" table ", "table"));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadDriver.requireValue(null, "table"));
    assertThrows(IllegalArgumentException.class,
        () -> HBASEBulkLoadDriver.requireValue("  ", "table"));
  }

  @Test
  void clusterAdapterConfiguresAndLoadsOnlyAfterSuccessfulJob() throws Exception {
    Configuration config = new Configuration(false);
    HBASEBulkLoadDriver.BulkLoadRequest request = HBASEBulkLoadDriver.BulkLoadRequest.from(
        new String[] {"input", "output", "events", "f", "row,value", ","});
    Job job = mock(Job.class);
    Connection connection = mock(Connection.class);
    Table table = mock(Table.class);
    RegionLocator locator = mock(RegionLocator.class);
    BulkLoadHFiles loader = mock(BulkLoadHFiles.class);
    TableName tableName = TableName.valueOf("events");
    when(connection.getTable(tableName)).thenReturn(table);
    when(connection.getRegionLocator(tableName)).thenReturn(locator);
    when(job.waitForCompletion(true)).thenReturn(true);

    try (MockedStatic<HBaseConfiguration> hbase = mockStatic(HBaseConfiguration.class);
         MockedStatic<Job> jobs = mockStatic(Job.class);
         MockedStatic<ConnectionFactory> connections = mockStatic(ConnectionFactory.class);
         MockedStatic<FileInputFormat> inputs = mockStatic(FileInputFormat.class);
         MockedStatic<FileOutputFormat> outputs = mockStatic(FileOutputFormat.class);
         MockedStatic<TableMapReduceUtil> tableMr = mockStatic(TableMapReduceUtil.class);
         MockedStatic<HFileOutputFormat2> hfiles = mockStatic(HFileOutputFormat2.class);
         MockedStatic<BulkLoadHFiles> bulkLoads = mockStatic(BulkLoadHFiles.class)) {
      jobs.when(() -> Job.getInstance(config, HBASEBulkLoadDriver.class.getName() + "-events"))
          .thenReturn(job);
      connections.when(() -> ConnectionFactory.createConnection(config)).thenReturn(connection);
      bulkLoads.when(() -> BulkLoadHFiles.create(config)).thenReturn(loader);

      assertEquals(0, new HBASEBulkLoadDriver.HadoopBulkLoadPipeline().execute(config, request));

      hbase.verify(() -> HBaseConfiguration.addHbaseResources(config));
      inputs.verify(() -> FileInputFormat.addInputPath(job, request.input()));
      outputs.verify(() -> FileOutputFormat.setOutputPath(job, request.output()));
      tableMr.verify(() -> TableMapReduceUtil.initTableReducerJob("events", null, job));
      tableMr.verify(() -> TableMapReduceUtil.addDependencyJars(job));
      hfiles.verify(() -> HFileOutputFormat2.configureIncrementalLoad(job, table, locator));
      verify(loader).bulkLoad(tableName, request.output());
      verify(table).close();
      verify(locator).close();
      verify(connection).close();
    }
  }

  @Test
  void clusterAdapterDoesNotBulkLoadFailedJob() throws Exception {
    Configuration config = new Configuration(false);
    HBASEBulkLoadDriver.BulkLoadRequest request = HBASEBulkLoadDriver.BulkLoadRequest.from(
        new String[] {"input", "output", "events", "f", "row,value", ","});
    Job job = mock(Job.class);
    Connection connection = mock(Connection.class);
    Table table = mock(Table.class);
    RegionLocator locator = mock(RegionLocator.class);
    BulkLoadHFiles loader = mock(BulkLoadHFiles.class);
    TableName tableName = TableName.valueOf("events");
    when(connection.getTable(tableName)).thenReturn(table);
    when(connection.getRegionLocator(tableName)).thenReturn(locator);
    when(job.waitForCompletion(true)).thenReturn(false);

    try (MockedStatic<HBaseConfiguration> ignored1 = mockStatic(HBaseConfiguration.class);
         MockedStatic<Job> jobs = mockStatic(Job.class);
         MockedStatic<ConnectionFactory> connections = mockStatic(ConnectionFactory.class);
         MockedStatic<FileInputFormat> ignored2 = mockStatic(FileInputFormat.class);
         MockedStatic<FileOutputFormat> ignored3 = mockStatic(FileOutputFormat.class);
         MockedStatic<TableMapReduceUtil> ignored4 = mockStatic(TableMapReduceUtil.class);
         MockedStatic<HFileOutputFormat2> ignored5 = mockStatic(HFileOutputFormat2.class);
         MockedStatic<BulkLoadHFiles> bulkLoads = mockStatic(BulkLoadHFiles.class)) {
      jobs.when(() -> Job.getInstance(config, HBASEBulkLoadDriver.class.getName() + "-events"))
          .thenReturn(job);
      connections.when(() -> ConnectionFactory.createConnection(config)).thenReturn(connection);
      bulkLoads.when(() -> BulkLoadHFiles.create(config)).thenReturn(loader);

      assertEquals(1, new HBASEBulkLoadDriver.HadoopBulkLoadPipeline().execute(config, request));
      verify(loader, never()).bulkLoad(tableName, request.output());
    }
  }
}
