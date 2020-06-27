package com.qiuyu.test.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, Flow> {
	@Override
	public int getPartition(Text key, Flow value, int numPartitions) {
		if (value.getAddr().equals("bj")) {
			return 0;
		}
		if (value.getAddr().equals("sh")) {
			return 1;
		}
		if (value.getAddr().equals("sz")) {
			return 2;
		} else {
			return 0;
		}

	}
}
