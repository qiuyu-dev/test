package com.qiuyu.test.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 用于封装属性数据的javabean
 * 自定义的javabean如果想让MapReducer能够处理,必须
 * 实现Writable接口,这是MapReduce的序列化机制
 *
 */
public class Flow implements Writable {

	private String phone;
	private String name;
	private String addr;
	private int flow;

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public int getFlow() {
		return flow;
	}

	public void setFlow(int flow) {
		this.flow = flow;
	}
	
	/*
	 * 字段序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeUTF(name);
		out.writeUTF(addr);
		out.writeInt(flow);

	}
	
	/*
	 * 字段反序列化方法
	 * 注意:要和序列化时的字段顺序一致
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.phone = in.readUTF();
		this.name = in.readUTF();
		this.addr = in.readUTF();
		this.flow = in.readInt();

	}

	@Override
	public String toString() {
		return "Flow [phone=" + phone + ", name=" + name + ", addr=" + addr + ", flow=" + flow + "]";
	}
	
	

}
