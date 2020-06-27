package com.qiuyu.test.outputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 自定义格式输出组件,用于定义输出的结果文件的格式
 * 一般开发通过泛型更加通用
 *
 * @param <K>
 * @param <V>
 */
public class AuthOutputFormat<K,V> extends FileOutputFormat<K,V>{

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Path path = super.getDefaultWorkFile(job, "");
		Configuration conf = job.getConfiguration();
		FileSystem fileSystem = path.getFileSystem(conf);
		FSDataOutputStream out = fileSystem.create(path);
		return new AuthRecordWriter<K,V>(out);
	}

}
