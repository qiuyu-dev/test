package com.qiuyu.test.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecFriendsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String friendInfo = line.split("\t")[0];
		int deep = Integer.parseInt(line.split("\t")[1]);
		context.write(new Text(friendInfo), new IntWritable(deep));
	}
}
