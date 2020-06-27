package com.qiuyu.test.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, Text> {
     @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
    		throws IOException, InterruptedException {
    	// TODO Auto-generated method stub
//    	super.reduce(arg0, arg1, arg2);
    	 String result = "";
    	 for(IntWritable value:values) {
    		 result=result + "," + value.get();
    	 }
    	 context.write(key, new Text(result));
    }
}
