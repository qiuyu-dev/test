package com.qiuyu.test.score;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, Student> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Student>.Context context)
			throws IOException, InterruptedException {
	      String line = value.toString();
	      String[] info = line.split(" ");
	      Student student = new Student();
	      String name = info[1];
	      student.setName(name);
	      int score = Integer.parseInt(info[2]);
	      //获取当前行数据所归属的文件切片
	      FileSplit split = (FileSplit)context.getInputSplit();
	      //获取全路径文件名
	      String filename = split.getPath().getName();
//	      String course = filename.substring(filename.lastIndexOf("/"));
	      if(filename.equals("chinese.txt")) {
	    	  student.setChinese(score);
	      }else if(filename.equals("english.txt")) {
	    	  student.setEnglish(score);  
	      }else if(filename.equals("math.txt")) {
	    	  student.setMath(score);
	      }
	       
	      context.write(new Text(name), student);
	      
	}
}
