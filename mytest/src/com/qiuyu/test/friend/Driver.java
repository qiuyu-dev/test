package com.qiuyu.test.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
//		private static final String HDFS_URL = "hdfs://10.42.91.255:9000";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		job.setMapperClass(FriendsMapper.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FriendsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.42.91.255:9000/friends"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/friends/result"));
		if(job.waitForCompletion(true)) {
			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(Driver.class);
			job2.setMapperClass(SecFriendsMapper.class);		
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setReducerClass(SecFriendsReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(NullWritable.class);
			FileInputFormat.setInputPaths(job2, new Path("hdfs://10.42.91.255:9000/friends/result"));
			FileOutputFormat.setOutputPath(job2, new Path("hdfs://10.42.91.255:9000/friends/result02"));
			job2.waitForCompletion(true);
//			if(job2.waitForCompletion(true)) {
//				Job job3 = Job.getInstance(conf);
//				job3.setJarByClass(Driver.class);
//				job3.setMapperClass(ThirdMapper.class);		
//				job3.setMapOutputKeyClass(Text.class);
//				job3.setMapOutputValueClass(NullWritable.class);
//				job3.setReducerClass(ThirdReducer.class);
//				job3.setOutputKeyClass(Text.class);
//				job3.setOutputValueClass(NullWritable.class);
//				FileInputFormat.setInputPaths(job3, new Path("hdfs://10.42.91.255:9000/friends/result02"));
//				FileOutputFormat.setOutputPath(job3, new Path("hdfs://10.42.91.255:9000/friends/result03"));
//				job3.waitForCompletion(true);
//			}
			
		}
	}

}
