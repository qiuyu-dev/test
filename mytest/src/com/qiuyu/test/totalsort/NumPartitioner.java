package com.qiuyu.test.totalsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NumPartitioner extends Partitioner<IntWritable, IntWritable> {

	@Override
	public int getPartition(IntWritable key, IntWritable value, int num) {
		// --按照数字的范围分区.0~100以内 100~1000以内 1000以上
		// --目的是可以用正则表达式
		String k = String.valueOf(key.get());
		if (k.matches("[0-9]") || k.matches("[0-9][0-9]")) {
			return 0;
		}
		if (k.matches("[0-9][0-9][0-9]")) {
			return 1;
		} else {
			return 2;
		}

	}

}
