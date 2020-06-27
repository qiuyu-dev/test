package com.qiuyu.test.average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 处理average.txt文件,统计每个人的平均分
 * 最终的结果:
 * tom 45
 * rose 60
 * jary 55
 *
 */
public class Driver {
	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf);
	job.setJarByClass(Driver.class);
	job.setMapperClass(AverageMapper.class);		
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setReducerClass(AverageReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.setInputPaths(job, new Path("hdfs://10.42.91.255:9000/average"));
	FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/average/result"));
	job.waitForCompletion(true);
	}
}
