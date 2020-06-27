package com.qiuyu.test.invert;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SortBean  implements WritableComparable<SortBean>{
	private String name;
	private int value;
	
	public SortBean() {
		
	}
	
	public SortBean(String name,int value) {
		this.name = name;
		this.value = value;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(value);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.value = in.readInt();
		
	}

	@Override
	public int compareTo(SortBean o) {
		// TODO Auto-generated method stub
		return this.name.compareTo(o.name);
	}

	@Override
	public String toString() {
		return "SortBean [name=" + name + ", value=" + value + "]";
	}

}
