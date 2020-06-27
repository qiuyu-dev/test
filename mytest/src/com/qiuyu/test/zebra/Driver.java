package com.qiuyu.test.zebra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(ZebraMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HttpAppHost.class);
		
		job.setReducerClass(ZebraReducer.class);
		job.setOutputKeyClass(HttpAppHost.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://10.42.91.255:9000/zebra"));		
		FileOutputFormat.setOutputPath(job,new Path("hdfs://10.42.91.255:9000/zebra/result"));
		
		job.waitForCompletion(true);
		
	}
}
