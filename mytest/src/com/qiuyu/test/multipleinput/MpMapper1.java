package com.qiuyu.test.multipleinput;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 此Mapper处理的格式:
 * tom
 * math 88
 * english 100
 *  .....
 * 针对这种格式,可以调用自定义的格式读取器
 * 
 *
 */
public class MpMapper1 extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
