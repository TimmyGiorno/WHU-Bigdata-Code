package com.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MergeReducer extends Reducer<Text, Text, Text, NullWritable> {

  private Text outKey = new Text();

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Set<String> uniqueValues = new HashSet<String>();
    for (Text value : values) {
      uniqueValues.add(value.toString());
    }
    for (String uniqueValue : uniqueValues) {
      outKey.set(String.format("%s\t%s", key.toString(), uniqueValue));
      context.write(outKey, NullWritable.get());
    }
  }
}