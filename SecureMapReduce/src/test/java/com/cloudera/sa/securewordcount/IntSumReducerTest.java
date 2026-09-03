package com.cloudera.sa.securewordcount;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class IntSumReducerTest {
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void sumsAllValuesForAWord() throws Exception {
    IntSumReducer reducer = new IntSumReducer();
    Reducer.Context context = mock(Reducer.Context.class);
    Text word = new Text("secure");
    reducer.reduce(word, List.of(new IntWritable(2), new IntWritable(3)), context);
    ArgumentCaptor<IntWritable> result = ArgumentCaptor.forClass(IntWritable.class);
    verify(context).write(org.mockito.ArgumentMatchers.eq(word), result.capture());
    assertEquals(5, result.getValue().get());
  }
}
