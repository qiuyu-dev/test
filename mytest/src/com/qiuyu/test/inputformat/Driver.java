package com.qiuyu.test.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(Driver.class);
		job.setMapperClass(LineNumMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// --设定自定义的格式读取器组件
		job.setInputFormatClass(LineNumberFormat.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://10.42.91.255:9000/park01"));

		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/park01/result"));

		job.waitForCompletion(true);
	}
}
