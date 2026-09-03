package com.cloudera.sa.securewordcount;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.jupiter.api.Test;

class SecureWordCountDriverTest {
  @Test
  void rejectsAnythingOtherThanOneInputAndOneOutput() throws Exception {
    SecureWordCountDriver driver = new SecureWordCountDriver();
    driver.setConf(new Configuration(false));
    assertEquals(2, driver.run(new String[] {"only-input"}));
    assertEquals(2, driver.run(new String[] {"input", "output", "extra"}));
  }

  @Test
  void configuresTheExpectedMapReduceJob() throws Exception {
    Job job = SecureWordCountDriver.createJob(
        new Configuration(false), "input-path", "output-path");
    assertEquals(TextInputFormat.class, job.getInputFormatClass());
    assertEquals(TokenizerMapper.class, job.getMapperClass());
    assertEquals(IntSumReducer.class, job.getCombinerClass());
    assertEquals(IntSumReducer.class, job.getReducerClass());
    assertEquals(Text.class, job.getMapOutputKeyClass());
    assertEquals(IntWritable.class, job.getMapOutputValueClass());
    assertEquals("input-path", FileInputFormat.getInputPaths(job)[0].getName());
    assertEquals("output-path", FileOutputFormat.getOutputPath(job).getName());
  }

  @Test
  void mapsSuccessfulAndFailedJobsToProcessExitCodes() throws Exception {
    Job successful = mock(Job.class);
    Job failed = mock(Job.class);
    when(successful.waitForCompletion(true)).thenReturn(true);
    when(failed.waitForCompletion(true)).thenReturn(false);

    assertEquals(0, SecureWordCountDriver.completionStatus(successful));
    assertEquals(1, SecureWordCountDriver.completionStatus(failed));
  }
}
