package com.qiuyu.test.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper02 extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		super.map(key, value, context);
		String line = value.toString();
		String[] words = line.split(" ");
		for(String word:words) {
			//--需要做类型转换
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
