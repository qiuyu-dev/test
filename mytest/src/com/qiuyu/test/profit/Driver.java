package com.qiuyu.test.profit;

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
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(ProfitMapper01.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(ProfitReduce01.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		FileInputFormat.addInputPath(job, new Path("hdfs://10.42.91.255:9000/profit"));		
		FileOutputFormat.setOutputPath(job,new Path("hdfs://10.42.91.255:9000/profit/result"));
		
		if(job.waitForCompletion(true)) {
			Job job2 = Job.getInstance(conf);
			
			job2.setJarByClass(Driver.class);
			job2.setMapperClass(ProfitMapper02.class);
			
			job2.setMapOutputKeyClass(Profit.class);
			job2.setMapOutputValueClass(NullWritable.class);		

			
			FileInputFormat.addInputPath(job2, new Path("hdfs://10.42.91.255:9000/profit/result"));		
			FileOutputFormat.setOutputPath(job2,new Path("hdfs://10.42.91.255:9000/profit/result02"));
			job2.waitForCompletion(true);
		}
		
	}
}
