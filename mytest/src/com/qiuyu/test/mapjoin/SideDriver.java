package com.qiuyu.test.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SideDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(SideDriver.class);
		job.setMapperClass(SideMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Item.class);
		
		//--将小表数据做了缓存,这样指定后,每个MapTask都可以缓存此表数据
		job.addCacheFile(new Path("hdfs://10.42.91.255:9000/cachejoin/product.txt").toUri());
		
		
		FileInputFormat.addInputPath(job, new Path("hdfs://10.42.91.255:9000/mapjoin"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://10.42.91.255:9000/mapjoin/result"));
		
		job.waitForCompletion(true);
	}
}
