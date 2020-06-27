package com.qiuyu.test.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Item.class);
		
		job.setOutputKeyClass(Item.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.42.91.255:9000/join"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/join/result"));
		
		job.waitForCompletion(true);
	}

}
