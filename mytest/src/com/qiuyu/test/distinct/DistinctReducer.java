package com.qiuyu.test.distinct;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		super.reduce(arg0, arg1, arg2);
		 context.write(key, NullWritable.get()); 

	}
}
