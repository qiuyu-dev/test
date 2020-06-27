package com.qiuyu.test.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int result = 0;
		for (IntWritable value : values) {
			System.err.println(key.toString() + ":" + value.get());
			result += value.get();
		}
		context.write(key, new IntWritable(result));
	}
}
