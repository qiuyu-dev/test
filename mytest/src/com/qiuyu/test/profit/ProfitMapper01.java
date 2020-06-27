package com.qiuyu.test.profit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper01 extends Mapper<LongWritable, Text, Text, IntWritable> {
@Override
protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
		throws IOException, InterruptedException {
       String line =  value.toString();
       String name = line.split("\\|")[1].split(" ")[0];
       int profit = Integer.parseInt(line.split("\\|")[1].split(" ")[1]);
       context.write(new Text(name), new IntWritable(profit));
}
}
