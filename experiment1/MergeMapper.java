package com.example;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Text outKey = new Text();
  private Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] parts = value.toString().split("    ");
    outKey.set(parts[0]);
    outValue.set(parts[1]);
    context.write(outKey, outValue);
  }
}