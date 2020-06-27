package com.qiuyu.test.multipleinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.qiuyu.test.input.AuthInputFormat;
import com.qiuyu.test.outputformat.AuthOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(Driver.class);
		job.setMapperClass(MpMapper1.class);

		MultipleInputs.addInputPath(job,
				new Path("hdfs://10.42.91.255:9000/multipleinput/t1.txt"),
				AuthInputFormat.class,//自定义读取格式
				MpMapper1.class);
		MultipleInputs.addInputPath(job,
				new Path("hdfs://10.42.91.255:9000/multipleinput/t2.txt"),
				TextInputFormat.class,//Hadoop默认的格式读取器
				MpMapper2.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MoReducer.class);

		MultipleOutputs.addNamedOutput(
				job, 
				"tomfile",
				AuthOutputFormat.class, //自定义输出格式
				Text.class, 
				Text.class);
		
		MultipleOutputs.addNamedOutput(
				job, 
				"rosefile",
				TextOutputFormat.class, //Hadoop默认输出格式
				Text.class, 
				Text.class);
		
		MultipleOutputs.addNamedOutput(
				job, 
				"jimfile",
				TextOutputFormat.class, 
				Text.class, 
				Text.class);

		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/multipleinput/result"));

		job.waitForCompletion(true);
	}
}
