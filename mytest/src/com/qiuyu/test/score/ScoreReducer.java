package com.qiuyu.test.score;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text, Student, Text, Student> {
@Override
protected void reduce(Text key, Iterable<Student> values, Reducer<Text, Student, Text, Student>.Context context)
		throws IOException, InterruptedException {
     Student result = new Student();
     for(Student value:values) {
    	 result.setChinese(result.getChinese() + value.getChinese());
    	 result.setEnglish(result.getEnglish() + value.getEnglish());
    	 result.setMath(result.getMath() + value.getMath());
     }
     result.setName(key.toString());
     context.write(key, result);
     
}
}
