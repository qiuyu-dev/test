package com.qiuyu.test.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class AuthRecordReader extends RecordReader<Text, Text>{
	// --切片对象
	private FileSplit fs;
	// --输入key
	private Text key;
	// --输入value
	private Text value;

	// --Hadoop工具类提供的行读取器,可以按行读取数据
	private LineReader lineReader;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// --获取切片对象
		fs = (FileSplit) split;
		// --获取Map处理数据所归属的切片对应的路径
		Path path = fs.getPath();
		// --通过上下文对象,获取job的环境参数对象
		Configuration conf = context.getConfiguration();
		// --获取分布式文件系统对象
		FileSystem system = path.getFileSystem(conf);
		// --获取当前处理文件数据的输入流
		InputStream in = system.open(path);
		// --将输入流传给行读取器,后续可以方便的通过此读取器,按行处理数据
		// --综上,初始化方法最终的目的是处理LineReader
		lineReader = new LineReader(in);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key = new Text();
		value = new Text();
		// --临时变量,用于存储每行数据的
		Text tmp = new Text();

		// --readLine是读取一行数据,返回值类型Int,表示读取到的字节数
		// --如果>0,表示读到数据
		// --如果=0,表示读完数据了,没有行可读
		if (lineReader.readLine(tmp) > 0) {
            key.set(tmp);
            //连读两行
            for(int i = 0 ; i < 2 ;i++) {
            	lineReader.readLine(tmp);
            	value.append(tmp.getBytes(), 0, tmp.getLength());
            	value.append(" ".getBytes(), 0, 1);
            }

			return true;
		} else {
			// --没有行数据可读,终止调用
			return false;
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		//去掉空格
		return new Text(value.toString().trim());
		
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		lineReader = null;
		
	}

}
