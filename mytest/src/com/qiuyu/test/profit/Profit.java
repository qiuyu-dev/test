package com.qiuyu.test.profit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Profit implements WritableComparable<Profit> {
	private String name;
	private int profit;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getProfit() {
		return profit;
	}

	public void setProfit(int profit) {
		this.profit = profit;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(profit);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.profit = in.readInt();
		
	}

	@Override
	public int compareTo(Profit o) {
		// 降序
		return o.profit - this.profit;
	}

	@Override
	public String toString() {
		return "Profit [name=" + name + ", profit=" + profit + "]";
	}

}
