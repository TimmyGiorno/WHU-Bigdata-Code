package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

  private IntWritable outKey = new IntWritable();
  private IntWritable outValue = new IntWritable();

  public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
    outValue.set(Integer.parseInt(value.toString()));
    outKey.set(2);
    context.write(outKey, outValue);
  }
}