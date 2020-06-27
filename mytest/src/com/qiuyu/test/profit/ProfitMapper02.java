package com.qiuyu.test.profit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper02 extends Mapper<LongWritable, Text, Profit, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Profit, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String name = line.split("\t")[0];
		int profit = Integer.parseInt(line.split("\t")[1]);
		Profit p = new Profit();
		p.setName(name);
		p.setProfit(profit);
		context.write(p, NullWritable.get());
		
	}
}
