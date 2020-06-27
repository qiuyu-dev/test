package com.qiuyu.test.flow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, Flow, Text, Flow> {
@Override
protected void reduce(Text key, Iterable<Flow> values, Reducer<Text, Flow, Text, Flow>.Context context)
		throws IOException, InterruptedException {
        Flow result = new Flow();
        for(Flow value:values) {
        	result.setPhone(value.getPhone());
        	result.setName(value.getName());
        	result.setAddr(value.getAddr());
        	result.setFlow(result.getFlow()+value.getFlow());
        }
        
        context.write(key, result);
        
}
}
