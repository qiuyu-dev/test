package com.qiuyu.test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class TestDemo {
	private static final String HDFS_URL = "hdfs://192.168.81.128:9000";

	/*
	 * 通过API连接HDFS
	 */
	@Test
	public void connect() throws Exception{
		//--创建Hadoop的环境参数对象,可以通过此对象设置属性参数
		Configuration conf = new Configuration();
		//--比如:conf.set("dfs.replication","1");
		
		//--连接HDFS
		FileSystem fs = FileSystem.get(new URI(HDFS_URL),conf);
        fs.close();
	}
	
	/*
	 * 通过API,从HDFS下载文件到本地
	 */
	@Test
	public void getFile() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(HDFS_URL),conf);
		//--获取要下载的文件的输入流
		InputStream in = fs.open(new Path("/park01/1.txt"));		
		//--获取本地文件的输出流
		OutputStream out = new FileOutputStream("123.txt");
		//--完成流的对接,实现文件的下载
		IOUtils.copyBytes(in, out, conf);
		in.close();
		out.close();
		fs.close();
	}
	
	/*
	 * 通过API上传文件到HDFS
	 */
	@Test
	public void putFile() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(HDFS_URL),conf);
		//--获取本地文件的输入流
		InputStream in = new FileInputStream("123.txt");
		//--获取HDFS文件的输出流,路径需要写到文件路径级别
		OutputStream out = fs.create(new Path("/park02/1.txt"));
		//--完成上传
		IOUtils.copyBytes(in, out, conf);
		in.close();
		out.close();
		fs.close();
	}
	
	/*
	 * 通过API删除HDFS上指定的目录或文件
	 */
	@Test
	public void delete() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(HDFS_URL),conf);
		//--true表示递归删除指定目录
		fs.delete(new Path("/park03"),true);
		fs.close();
	}
	
	/*
	 * 查看指定目录下的文件或子目录信息,相当于ls指令
	 */
	@Test
	public void listFiles() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(HDFS_URL),conf);
		//--true表示递归查看文件信息
		RemoteIterator<LocatedFileStatus> rt = fs.listFiles(new  Path("/"), true);
	    while(rt.hasNext()) {
	    	LocatedFileStatus lfs = rt.next();
	    	System.out.println(lfs);
	    }
	    fs.close();
	}
	
	/*
	 * 目录重命名
	 */
	@Test
	public void rename() throws Exception{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI(HDFS_URL),conf);
		
		//--①参:原目录名  ②参:要修改的目录名
		fs.rename(new Path("/park01"),new Path("/park02"));
		fs.close();
	}
	
	@Test
	public void getFileBlockInfo() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(HDFS_URL),conf);
		//--获取指定文件的文件块信息
		//--此方法返回:文件的数量,文件块的起始位置,文件块的大小,文件块存储的datanode位置
		//--①参:路径 ②参:查看文件块的起始位置  ③参:参看文件长度的位置
		BlockLocation[] bls =fs.getFileBlockLocations(new Path("/park01/1.txt"), 0, Integer.MAX_VALUE);
		for(BlockLocation bl:bls) {
			System.out.println(bl);
		}
		fs.close();
		
	}
	
	public static void main(String[] args) {
		String t = "103_20150615143630_00_00_000.csv";
		String datetime = t.substring(4, 18);
		System.out.println("datetime=" + datetime);
	}
}
