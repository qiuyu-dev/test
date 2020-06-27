package com.qiuyu.test.invert;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String result = "";
		TreeSet<String> set = new TreeSet<String>();
		for (Text text : values) {
			set.add(text.toString());
		}
		
		Iterator<String> it = set.descendingIterator();
		while(it.hasNext()) {
			String v =(String)it.next();
			result +=v;
		}
		
		context.write(key, new Text(result));

	}
}
