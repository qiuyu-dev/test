package com.qiuyu.test.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Item, Item, NullWritable> {
	Map<String, Item> productMap = new HashMap<>();

	@Override
	protected void reduce(Text key, Iterable<Item> values, Reducer<Text, Item, Item, NullWritable>.Context context)
			throws IOException, InterruptedException {

		List<Item> list = new ArrayList<>();

		for (Item value : values) {
			Item item = value.clone();
			list.add(item);
			if (value.getId().equals("")) {
				productMap.put(item.getPid(), item);
			}
		}

		for (Item value : list) {
			if (!value.getId().equals("")) {
				Item productItem = new Item();
				productItem.setId(value.getId());
				productItem.setDate(value.getDate());
				productItem.setAmount(value.getAmount());
				productItem.setPid(value.getPid());

				productItem.setName(productMap.get(value.getPid()).getName());
				productItem.setPrice(productMap.get(value.getPid()).getPrice());

				context.write(productItem, NullWritable.get());
			}
		}

	}
}
