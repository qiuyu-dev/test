package com.qiuyu.test.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AuthRecordWriter<K, V> extends RecordWriter<K, V> {

	private FSDataOutputStream out;

	public AuthRecordWriter(FSDataOutputStream out) {
		// 获取输出结果文件的输出流
		this.out = out;
	}

	/**
	 * 此方法用于将输出key和输出value输出到结果文件 
	 * 如果一个job只有一个mapper,
	 * 则将Mapper的输出key,value输出
	 */
	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		out.write(key.toString().getBytes());
		out.write("|".getBytes());
		out.write(value.toString().getBytes());
		out.write("\r\n".getBytes());

	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();

	}

}
