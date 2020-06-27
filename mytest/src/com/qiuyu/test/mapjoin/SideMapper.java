package com.qiuyu.test.mapjoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SideMapper extends Mapper<LongWritable, Text, Text, Item> {

	// --用于存储小表的数据
	private Map<String, Item> productMap;

	/*
	 * Mapper组件的初始化方法,用于初始小表数据,并加入到map中
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Item>.Context context)
			throws IOException, InterruptedException {
		// --key是商品id,值是商品对象
		productMap = new HashMap<String, Item>();

		// --通过上下文对象获取job的环境变量参数
		Configuration conf = context.getConfiguration();
		// --获取缓存小表的文件路径的URI
		URI[] localCacheFiles = context.getCacheFiles();

		// --获取HDFS文件系统
		FileSystem fs = FileSystem.get(localCacheFiles[0], conf);
		// --通过文件路径,获取小表文件的输入流
		FSDataInputStream in = fs.open(new Path(localCacheFiles[0]));

		// --是将小表数据封装到BufferReader,以便读取数据
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;

		while ((line = br.readLine()) != null) {
			String[] itemInfo = line.split(" ");
			Item item = new Item();
			item.setPid(itemInfo[0]);
			item.setName(itemInfo[1]);
			item.setPrice(Double.parseDouble(itemInfo[2]));

			productMap.put(item.getPid(), item);

		}
		br.close();

	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Item>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] orderInfo = line.split(" ");

		// --封装大表(订单表)数据
		Item item = new Item();
		item.setId(orderInfo[0]);
		item.setDate(orderInfo[1]);
		item.setPid(orderInfo[2]);
		item.setAmount(Integer.parseInt(orderInfo[3]));
		// --将大表数据和小表数据完成join
		item.setName(productMap.get(item.getPid()).getName());
		item.setPrice(productMap.get(item.getPid()).getPrice());

		context.write(new Text(item.getDate()), item);

	}
}
