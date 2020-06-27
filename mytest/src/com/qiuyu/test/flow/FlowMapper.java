package com.qiuyu.test.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, Flow>{
@Override
protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Flow>.Context context)
		throws IOException, InterruptedException {
       String line = value.toString();
       String[] info = line.split(" ");
       Flow f = new Flow();
       f.setPhone(info[0]);
       f.setName(info[1]);
       f.setAddr(info[2]);
       f.setFlow(Integer.parseInt(info[3]));
       context.write(new Text(f.getName()), f);
}
}
