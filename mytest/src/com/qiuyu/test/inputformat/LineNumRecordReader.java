package com.qiuyu.test.inputformat;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * 此组件决定了Mapper输入key和输入value的内容
 * 
 * @author tedu
 *
 */
public class LineNumRecordReader extends RecordReader<IntWritable, Text> {

	// --切片对象
	private FileSplit fs;
	// --输入key
	private IntWritable key;
	// --输入value
	private Text value;
	// --行号
	private int count = 0;
	// --Hadoop工具类提供的行读取器,可以按行读取数据
	private LineReader lineReader;

	/*
	 * 是组件的初始化方法,用于初始化相关的资源对象
	 */
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

	/*
	 * 此方法会被调用多次,如果返回值为true,就会再次被调用 直到返回false位置就不再调用了 此方法用于读取文件的行数据,相当于读取一行就调用一次,
	 * 直到整个文件所有行都读完了,就停止调用
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// --每次执行方法,对输入key和输入value做一个初始化
		key = new IntWritable();
		value = new Text();
		// --临时变量,用于存储每行数据的
		Text tmp = new Text();

		// --readLine是读取一行数据,返回值类型Int,表示读取到的字节数
		// --如果>0,表示读到数据
		// --如果=0,表示读完数据了,没有行可读
		if (lineReader.readLine(tmp) > 0) {
			// --行号处理
			count++;
			// --把行号赋值输入key
			key.set(count);
			// --把每行内容复制输入value
			// --注意:lineReader.readLine此方法调用一次,就会读取一行
			value.set(tmp);

			return true;
		} else {
			// --没有行数据可读,终止调用
			return false;
		}

	}

	/*
	 * 此方法用于将输入key传给Mapper组件 此方法会跟着nextKeyValue()调用,
	 */
	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {

		return key;
	}

	/*
	 * 将输入value传给Mapper组件
	 */
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return 0;
	}

	/*
	 * 用于资源清理工作
	 */
	@Override
	public void close() throws IOException {
		lineReader = null;

	}

}
