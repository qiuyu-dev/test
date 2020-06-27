package com.qiuyu.test.mapjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Item implements Writable,Cloneable{
	
	
	private String id="";
	private String date="";
	private String pid="";
	private int amount;
	private String name="";
	private double price;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(date);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(name);
		out.writeDouble(price);
		
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id=in.readUTF();
		this.date=in.readUTF();
		this.pid=in.readUTF();
		this.amount=in.readInt();
		this.name=in.readUTF();
		this.price=in.readDouble();
		
	}
	
	public Item clone(){
		Item o=null;
		try {
			o=(Item)super.clone();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return o;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@Override
	public String toString() {
		return "Item [订单id=" + id + ", 订单日期=" + date + ", pid=" + pid + ", 出货量=" + amount + ", 品牌=" + name
				+ ", 商品单价=" + price + "]";
	}
	
	
}

