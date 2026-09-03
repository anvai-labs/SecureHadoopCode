package com.cloudera.sa.hbasebulkload;

import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutCombiner;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class HBASEBulkLoadDriver extends Configured implements Tool {
  private final BulkLoadPipeline pipeline;

  public HBASEBulkLoadDriver() {
    this(new HadoopBulkLoadPipeline());
  }

  HBASEBulkLoadDriver(BulkLoadPipeline pipeline) {
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new HBASEBulkLoadDriver(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration config = Objects.requireNonNull(getConf(), "configuration");
    String[] remaining = new GenericOptionsParser(config, args).getRemainingArgs();
    if (remaining.length != 6) {
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    BulkLoadRequest request = BulkLoadRequest.from(remaining);
    config.set(HBASEBulkLoadConstants.HBASE_TABLE_KEY, request.table());
    config.set(HBASEBulkLoadConstants.HBASE_COLUMN_FAMILY_KEY, request.columnFamily());
    config.set(HBASEBulkLoadConstants.HBASE_COLUMNS_KEY, request.columns());
    config.set(HBASEBulkLoadConstants.HBASE_COLUMN_SEPERATOR_KEY, request.delimiter());
    return pipeline.execute(config, request);
  }

  static String requireValue(String value, String label) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(label + " must not be blank");
    }
    return value.trim();
  }

  record BulkLoadRequest(
      Path input, Path output, String table, String columnFamily, String columns, String delimiter) {
    static BulkLoadRequest from(String[] args) {
      String delimiter = requireValue(args[5], "column delimiter");
      if (delimiter.length() != 1) {
        throw new IllegalArgumentException("column delimiter must be exactly one character");
      }
      return new BulkLoadRequest(
          new Path(requireValue(args[0], "input path")),
          new Path(requireValue(args[1], "output path")),
          requireValue(args[2], "HBase table"),
          requireValue(args[3], "HBase column family"),
          requireValue(args[4], "HBase columns"),
          delimiter);
    }
  }

  @FunctionalInterface
  interface BulkLoadPipeline {
    int execute(Configuration config, BulkLoadRequest request) throws Exception;
  }

  static final class HadoopBulkLoadPipeline implements BulkLoadPipeline {
    @Override
    public int execute(Configuration config, BulkLoadRequest request) throws Exception {
      HBaseConfiguration.addHbaseResources(config);
      Job job = Job.getInstance(config, HBASEBulkLoadDriver.class.getName() + "-" + request.table());
      job.setInputFormatClass(TextInputFormat.class);
      job.setJarByClass(HBASEBulkLoadDriver.class);
      job.setMapperClass(HBASEBulkLoadKeyValueMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(Put.class);
      job.setCombinerClass(PutCombiner.class);
      job.setReducerClass(PutSortReducer.class);
      FileInputFormat.addInputPath(job, request.input());
      FileOutputFormat.setOutputPath(job, request.output());

      TableName tableName = TableName.valueOf(request.table());
      try (Connection connection = ConnectionFactory.createConnection(config);
           Table table = connection.getTable(tableName);
           RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
        TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), null, job);
        TableMapReduceUtil.addDependencyJars(job);
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
      }

      if (!job.waitForCompletion(true)) {
        return HBASEBulkLoadConstants.FAILURE;
      }
      BulkLoadHFiles.create(config).bulkLoad(tableName, request.output());
      return HBASEBulkLoadConstants.SUCCESS;
    }
  }
}
