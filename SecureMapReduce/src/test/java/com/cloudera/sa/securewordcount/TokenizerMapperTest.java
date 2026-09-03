package com.cloudera.sa.securewordcount;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TokenizerMapperTest {
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void emitsWordsWithoutWhitespaceAndCountsTheLine() throws Exception {
    TokenizerMapper mapper = new TokenizerMapper();
    Mapper.Context context = mock(Mapper.Context.class);
    Counter counter = mock(Counter.class);
    when(context.getCounter("TokenizerMapperLines", "TokenizerMapperProcessed")).thenReturn(counter);
    when(counter.getValue()).thenReturn(1L);
    mapper.setup(context);
    mapper.map(new LongWritable(7), new Text("alpha\tbeta  gamma"), context);
    mapper.cleanup(context);

    ArgumentCaptor<Text> keys = ArgumentCaptor.forClass(Text.class);
    ArgumentCaptor<IntWritable> counts = ArgumentCaptor.forClass(IntWritable.class);
    verify(context, times(3)).write(keys.capture(), counts.capture());
    assertEquals(List.of("alpha", "beta", "gamma"),
        keys.getAllValues().stream().map(Text::toString).toList());
    assertEquals(List.of(1, 1, 1),
        counts.getAllValues().stream().map(IntWritable::get).toList());
    verify(counter).increment(1);
    verify(counter).getValue();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void whitespaceOnlyLineEmitsNothingButStillCountsTheLine() throws Exception {
    TokenizerMapper mapper = new TokenizerMapper();
    Mapper.Context context = mock(Mapper.Context.class);
    Counter counter = mock(Counter.class);
    when(context.getCounter("TokenizerMapperLines", "TokenizerMapperProcessed")).thenReturn(counter);
    mapper.setup(context);
    mapper.map(new LongWritable(0), new Text(" \t  "), context);
    verify(context, never()).write(any(), any());
    verify(counter).increment(1);
  }
}
