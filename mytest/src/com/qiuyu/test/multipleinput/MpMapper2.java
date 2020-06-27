package com.qiuyu.test.multipleinput;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 处理格式:
 * jim|math 80 english 90
 * rose|math 75 english 80
 * tom|math 88 english 100
 *  
 * 可用Hadoop默认的格式读取器
 *
 */
public class MpMapper2 extends Mapper<LongWritable, Text, Text, Text> {
@Override
protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	String line = value.toString();
	String name = line.split("\\|")[0];
	String score = line.split("\\|")[1];
	context.write(new Text(name), new Text(score));
}
}
