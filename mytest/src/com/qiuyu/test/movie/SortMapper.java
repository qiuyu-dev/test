package com.qiuyu.test.movie;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, Movie, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Movie, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		Movie m = new Movie();
		String name = line.split(" ")[0];
		int hot = Integer.parseInt(line.split(" ")[1]);
		m.setName(name);
		m.setHot(hot);
		context.write(m, NullWritable.get());
	}
}
