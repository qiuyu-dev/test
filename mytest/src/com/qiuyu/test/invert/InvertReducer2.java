package com.qiuyu.test.invert;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertReducer2 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String result = "";
		for (Text value : values) {
			result = result + " " + value.toString();
		}
		context.write(key, new Text(result));
	}
}
