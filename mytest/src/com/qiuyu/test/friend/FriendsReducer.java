package com.qiuyu.test.friend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendsReducer extends Reducer<Text, Text, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
//		Set<String> set = new HashSet<String>();
//		for (Text t : values) {
//			set.add(t.toString());
//		}
//		if (set.size() >= 2) {
//			Iterator<String> it = set.iterator();
//			while (it.hasNext()) {
//				String name = it.next();
//				for (Iterator<String> it2 = set.iterator(); it2.hasNext();) {
//					String name2 = it2.next();
//					if (!name2.equals(name)) {
//						context.write(new Text(name), new Text(name2));
//					}
//				}
//			}
//		}
		
		ArrayList<String> friendsList = new ArrayList<String>();
		for(Text value:values) {
			friendsList.add(value.toString());
			if(key.toString().compareTo(value.toString())<0) {
				context.write(new Text(key + "-" + value), new IntWritable(1));//1表示明确好友
				
			}else {
				context.write(new Text(value + "-" + key), new IntWritable(1));
			}
		}
		
		for(int i = 0; i < friendsList.size(); i++) {
			for(int j = 0 ; j < friendsList.size(); j++) {
				String f1 = friendsList.get(i);
				String f2 = friendsList.get(j);
				if(f1.compareTo(f2) < 0) {
					context.write(new Text(f1 + "-" + f2), new IntWritable(2));//2表示潜在好友
				}
			}
		}

	}
}
