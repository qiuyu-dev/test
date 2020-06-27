package com.qiuyu.test.flow;

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
		job.setMapperClass(FlowMapper.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Flow.class);
		job.setReducerClass(FlowReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Flow.class);
		//设置自定义分区组件类
		job.setPartitionerClass(FlowPartitioner.class);
		//设置分区数量，默认是1个分区
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.42.91.255:9000/flow"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/flow/result"));
		job.waitForCompletion(true);
	}

}
