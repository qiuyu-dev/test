package com.qiuyu.test.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
		job.setMapperClass(ScoreMapper.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Student.class);
		job.setReducerClass(ScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Student.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.42.91.255:9000/score"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/score/result"));
		job.waitForCompletion(true);
	}
}
