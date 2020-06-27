package com.qiuyu.test.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qiuyu.test.outputformat.AuthOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(Driver.class);
		job.setMapperClass(InputMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// --设定自定义的格式读取器组件
		job.setInputFormatClass(AuthInputFormat.class);
		// --设定自定义的格式输出组件
        job.setOutputFormatClass(AuthOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.42.91.255:9000/input"));

		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.42.91.255:9000/input/result"));

		job.waitForCompletion(true);
	}
}
