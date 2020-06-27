package com.qiuyu.test.zebra;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ZebraMapper extends Mapper<LongWritable, Text, Text, HttpAppHost> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, HttpAppHost>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split("\\|");
		//
		HttpAppHost hah = new HttpAppHost();
		// 获取当前行数据所归属的文件切片
		FileSplit split = (FileSplit) context.getInputSplit();
		// 获取全路径文件名
		String filename = split.getPath().getName();
		String datetime = filename.substring(4, 18);
		hah.setReportTime(datetime);
		hah.setCellid(data[16]);
		hah.setAppType(Integer.parseInt(data[22]));
		hah.setAppSubtype(Integer.parseInt(data[23]));
		hah.setUserIP(data[26]);
		hah.setUserPort(Integer.parseInt(data[28]));
		hah.setAppServerIP(data[30]);
		hah.setAppServerPort(Integer.parseInt(data[32]));
		hah.setHost(data[58]);
        //请求响应码
		int appTypeCode = Integer.parseInt(data[18]);
		String transStatus = data[54];

		if (hah.getCellid() == null || hah.getCellid().equals("")) {
			hah.setCellid("000000000");
		}

		if (appTypeCode == 103) {
			hah.setAttempts(1);
		}

		if (appTypeCode == 103
				&& "10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205,206,302,304,306"
						.contains(transStatus)) {
			hah.setAccepts(1);
		} else {
			hah.setAccepts(0);
		}

		if (appTypeCode == 103) {
			hah.setTrafficUL(Long.parseLong(data[33]));
		}

		if (appTypeCode == 103) {
			hah.setTrafficDL(Long.parseLong(data[34]));
		}

		if (appTypeCode == 103) {
			hah.setRetranUL(Long.parseLong(data[39]));
		}

		if (appTypeCode == 103) {
			hah.setRetranDL(Long.parseLong(data[40]));
		}

		if (appTypeCode == 103) {
			hah.setTransDelay(Long.parseLong(data[20]) - Long.parseLong(data[19]));
		}

		String userKey = hah.getReportTime() + "|" + hah.getAppType() + "|" + hah.getAppSubtype() + "|"
				+ hah.getUserIP() + "|" + hah.getUserPort() + "|" + hah.getAppServerIP() + "|" + hah.getAppServerPort()
				+ "|" + hah.getHost() + "|" + hah.getCellid();
		context.write(new Text(userKey), hah);

	}
}
