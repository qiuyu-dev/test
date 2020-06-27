package com.qiuyu.test.multipleinput;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MoReducer extends Reducer<Text, Text, Text, Text> {
	//--创建多输出源对象,泛型对应的reduce的输出key和输出value类型
	private MultipleOutputs<Text, Text> mos;
    
	/**
	 *在reduce组件的初始化方法中,对多输出源做初始化
	 */
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	/*
	 * 本例中,要根据多输出源,根据人名发送到不同的文件
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			if (key.toString().equals("tom")) {
				mos.write("tomfile", key, value);//--①参:自定义的文件输出名
			}
			if (key.toString().equals("rose")) {
				mos.write("rosefile", key, value);
			}
			if (key.toString().equals("jim")) {
				mos.write("jimfile", key, value);
			}
		}
	}

}
