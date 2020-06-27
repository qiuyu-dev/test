package com.qiuyu.test.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 知识点
 * 1.开发Mapper组件,需要让类继承Mapper
 * 2.LongWritable是Hadoop的序列化类型的Long
 * 3.Text是Hadoop序列化类型的序列化类型的String
 * 因为MapReduce是分布式计算框架,因为需要通过网络传输数据,所以需要进行序列
 * LongWritable
 * Text
 * IntWritable
 * DoubleWritable
 * BytesWritable
 * NullWritable
 * ....
 * 4.第一个泛型类型对应的Mapper组件的输入key类型,对应的是每行的行首偏移量
 * 5.第二个泛型类型对应的Mapper组件的输入value类型,对应的是每行的内容
 * 6.第三个泛型类型对应的Mapper组件的输出key类型,是程序员自定义的
 * 7.第四个泛型类型对应的Mapper组件的输出value类型,是程序员自定义的
 * 综上:开发Mapper组件时,第一个和第二个泛型写死,第三个和第四个泛型根据业务定
 * 8.开发Mapper组件,主要是重写map方法.
 * map方法的作用:
 * ①把输入key和输出value传给程序员
 * ②通过context对象输出key和value
 * 
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		super.map(key, value, context);
		//--将输入key和输出value输出,目的是验证之前的结论
		context.write(key, value);
	}
	
	
}
