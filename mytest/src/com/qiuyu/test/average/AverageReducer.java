package com.qiuyu.test.average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
@Override
protected void reduce(Text key, Iterable<IntWritable> values,
		Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
//	super.reduce(arg0, arg1, arg2);
	int sum = 0;
	int count = 0;
	for(IntWritable value:values) {
		sum += value.get();
		count++;
	}
	int average = sum/count;
	context.write(key, new IntWritable(average));
}
}
