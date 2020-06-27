package com.qiuyu.test.distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 要求:
 * 通过MapReducer框架处理distinct文件,实现数据去重
 * 最终输出的结果:
 * 192.168.234.21
 * 192.168.234.22
 * ......
 * 
 * 相关的提示:
 * Text->String  => toString()
 * String->Text  => new Text(String)
 * IntWritable->Int  => get()
 * Int->IntWritable  => new IntWritable(Int)
 * 本案例提示:NullWritable => NullWritable.get()
 *
 */
public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		job.setMapperClass(DistinctMapper.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(DistinctReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.81.128:9000/distinct"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.81.128:9000/distinct/result"));
		job.waitForCompletion(true);
	}

}
