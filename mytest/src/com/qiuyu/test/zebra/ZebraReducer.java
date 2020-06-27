package com.qiuyu.test.zebra;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ZebraReducer extends Reducer<Text, HttpAppHost, HttpAppHost, NullWritable> {
@Override
protected void reduce(Text key, Iterable<HttpAppHost> values,
		Reducer<Text, HttpAppHost, HttpAppHost, NullWritable>.Context context)
		throws IOException, InterruptedException {
     HttpAppHost result = new HttpAppHost();
     for(HttpAppHost value:values) {
    	 result.setAccepts(result.getAccepts() + value.getAccepts());
    	 result.setAttempts(result.getAttempts() + value.getAttempts());
    	 result.setTrafficUL(result.getTrafficUL() + value.getTrafficUL());
    	 result.setTrafficDL(result.getTrafficDL() + value.getTrafficDL());
    	 result.setRetranUL(result.getRetranUL() + value.getRetranUL());
    	 result.setRetranDL(result.getRetranDL() + value.getRetranDL());
    	 result.setTransDelay(result.getTransDelay() + value.getTransDelay());
    	 result.setReportTime(value.getReportTime());
    	 result.setCellid(value.getCellid());
    	 result.setUserIP(value.getUserIP());
    	 result.setAppServerIP(value.getAppServerIP());
    	 result.setAppServerPort(value.getAppServerPort());
    	 result.setUserPort(value.getUserPort());
    	 result.setAppType(value.getAppType());
    	 result.setAppSubtype(value.getAppSubtype());
    	 result.setHost(value.getHost());
    	 context.write(result, NullWritable.get());
     }
}
}
