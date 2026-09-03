package com.cloudera.sa.securewordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class SecureWordCountDriver extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new SecureWordCountDriver(), args));
  }

  @Override
  public int run(String[] arguments) throws Exception {
    Configuration configuration = getConf();
    String[] paths = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
    if (paths.length != 2) {
      ToolRunner.printGenericCommandUsage(System.err);
      return 2;
    }
    return completionStatus(createJob(configuration, paths[0], paths[1]));
  }

  static int completionStatus(Job job) throws IOException, InterruptedException,
      ClassNotFoundException {
    return job.waitForCompletion(true) ? 0 : 1;
  }

  static Job createJob(Configuration configuration, String input, String output)
      throws IOException {
    Job job = Job.getInstance(configuration, SecureWordCountDriver.class.getSimpleName());
    job.setJarByClass(SecureWordCountDriver.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }
}
