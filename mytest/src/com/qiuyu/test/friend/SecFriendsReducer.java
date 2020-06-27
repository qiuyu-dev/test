package com.qiuyu.test.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecFriendsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		Boolean flag = true;
		for (IntWritable value : values) {
			if (value.get() == 1) {
				flag = false;
				break;
			}
		}
		if (flag) {
			context.write(key, NullWritable.get());
		}
	}
}
