package com.qiuyu.test.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 自定义格式输入器组件,可以定义Mapper组件的输入key和输入value类型
 * 默认情况,Mapper的输入key是每行行首偏移量
 * 输入value是每行内容.
 * 
 * 本例中,要把输入key变长每行行号,输入value是每行内容
 * 第一个泛型:输入key类型
 * 第二个泛型:输入value类型
 * @author tedu
 *
 */
public class LineNumberFormat extends FileInputFormat<IntWritable,Text>{

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		//--返回自定义的记录读取器
		return new LineNumRecordReader();
	}

}
