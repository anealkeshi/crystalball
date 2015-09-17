package com.anilkc.project.pairapproach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.anilkc.project.Pair;

public class PairMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {

	private Map<Pair, Integer> map;

	/**
	 * Called once at the beginning of the task.
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		map = new HashMap<Pair, Integer>();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] datas = value.toString().split("\\s");

		System.out.println("Received line: " + value.toString());
		System.out.println("Processing....");

		for (int i = 0; i < datas.length - 1; i++) {
			String data = datas[i];
			for (int j = i + 1; j < datas.length; j++) {
				if (data.equals(datas[j])) {
					break;
				}

				Pair pair = new Pair(data, datas[j]);
				System.out.println(pair);

				if (!map.containsKey(pair)) {
					map.put(pair, 1);
				} else {
					map.put(pair, map.get(pair) + 1);
				}

				Pair specialPair = new Pair(data, "*");
				System.out.println(specialPair);

				if (!map.containsKey(specialPair)) {
					map.put(specialPair, 1);
				} else {
					map.put(specialPair, map.get(specialPair) + 1);
				}
			}
		}
	}

	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("Mapper output: ");
		for (Pair key : map.keySet()) {
			System.out.println(key + " count " + map.get(key));
			context.write(key, new IntWritable(map.get(key)));
		}
	}
}
