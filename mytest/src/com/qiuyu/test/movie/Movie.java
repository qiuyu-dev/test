package com.qiuyu.test.movie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Movie implements WritableComparable<Movie> {

	private String name;
	private int hot;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	@Override
	public String toString() {
		return "Movie [name=" + name + ", hot=" + hot + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(hot);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.hot = in.readInt();

	}

	@Override
	public int compareTo(Movie o) {
		// 按热度值降序排序，如果升序this.hot - o.hot
		return o.hot - this.hot;
	}
}
