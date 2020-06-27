package com.qiuyu.test.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		final String HDFS_URL = "hdfs://192.168.81.128:9000";
		//--创建Hadoop的环境参数对象
		Configuration conf = new Configuration();
		//--获取MapReduce的job任务对象
		Job job = Job.getInstance(conf);
		//--设置job运行的主类class
		job.setJarByClass(Driver.class);
		//--设置Mapper组件的class
		job.setMapperClass(WordCountMapper02.class);	
		//--设置Mapper组件输出key的泛型类型
		job.setMapOutputKeyClass(Text.class);
		//--设置Mapper组件的输出value的泛型类型
		//--注意Text的导包
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WordCountReducer02.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//--需要设置处理的文件的路径,
		//--注意:MapReduce处理的文件必须位于HDFS上
		//--输入的路径可以写到目录级别,下例就是写到目录级别
		//--如果是目录级别,会处理指定目录的所有数据
		//--此外,也可以指到文件路径级别
		FileInputFormat.setInputPaths(job, new Path(HDFS_URL+"/word"));
		//--指定输出的结果文件路径
		FileOutputFormat.setOutputPath(job, new Path(HDFS_URL+"/word/result02"));
		job.waitForCompletion(true);
	}

}
