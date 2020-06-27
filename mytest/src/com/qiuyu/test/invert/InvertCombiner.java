package com.qiuyu.test.invert;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertCombiner extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		SortBean result = new SortBean();
		int count = 0;
		for (Text value : values) {
			 result.setName(value.toString());
			 count++;
		}
		result.setValue(count);
		
		context.write(key, new Text(" " +result.getName()+ " " + result.getValue()));
		
	}
}
