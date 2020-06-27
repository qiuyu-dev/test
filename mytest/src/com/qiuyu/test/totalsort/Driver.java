package com.qiuyu.test.totalsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
//		private static final String HDFS_URL = "hdfs://10.42.91.255:9000";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		
		job.setMapperClass(TotalsortMapper.class);		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(TotalsortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(NumPartitioner.class);
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.42.91.255:9000/totalsort"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/totalsort/result"));
		job.waitForCompletion(true);
	}
}
