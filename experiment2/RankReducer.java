package com.example;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer extends Reducer<IntWritable, IntWritable, Text, NullWritable> {

  private Text outKey = new Text();

  public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
    List<Integer> valuesList = new ArrayList<Integer>();
    for (IntWritable value : values) {
      valuesList.add(value.get());
    }
    Collections.sort(valuesList);
    for (int i = 0; i < valuesList.size(); i++) {
      outKey.set(String.format("%d %d", i + 1, valuesList.get(i)));
      context.write(outKey, NullWritable.get());
    }
  }
}