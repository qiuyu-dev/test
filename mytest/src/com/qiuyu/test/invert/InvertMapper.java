package com.qiuyu.test.invert;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split(" ");
		// 获取当前行数据所归属的文件切片
		FileSplit split = (FileSplit) context.getInputSplit();
		// 获取全路径文件名
		String filename = split.getPath().getName();
		context.write(new Text(data[0]), new Text(filename));
		context.write(new Text(data[1]), new Text(filename));
	}

}
