package com.cloudera.sa.securewordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

final class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private static final Logger LOGGER = Logger.getLogger(TokenizerMapper.class.getName());
  private static final IntWritable ONE = new IntWritable(1);
  private Counter processedLines;

  @Override
  protected void setup(Context context) {
    processedLines = context.getCounter("TokenizerMapperLines", "TokenizerMapperProcessed");
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    StringTokenizer tokenizer = new StringTokenizer(value.toString());
    while (tokenizer.hasMoreTokens()) {
      context.write(new Text(tokenizer.nextToken()), ONE);
    }
    processedLines.increment(1);
  }

  @Override
  protected void cleanup(Context context) {
    LOGGER.info(() -> "Counter for lines: " + processedLines.getValue());
  }
}
