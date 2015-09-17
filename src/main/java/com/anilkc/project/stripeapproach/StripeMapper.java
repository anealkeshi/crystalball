package com.anilkc.project.stripeapproach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] datas = value.toString().split("\\s");

		System.out.println("Received line: " + value.toString());
		System.out.println("Processing....");

		MapWritable stripe;
		for (int i = 0; i < datas.length - 1; i++) {
			Text data = new Text(datas[i]);

			stripe = new MapWritable();

			for (int j = i + 1; j < datas.length; j++) {

				Text outKey = new Text(datas[j]);

				if (data.equals(outKey)) {
					break;
				}

				if (stripe.containsKey(outKey)) {
					stripe.put(outKey, new IntWritable(((IntWritable) stripe.get(outKey)).get() + 1));
				} else {
					stripe.put(outKey, new IntWritable(1));
				}
			}

			context.write(data, stripe);
		}
	}

	private String outWritableToText(MapWritable mapWritable) {
		String text = "";
		for (Writable key : mapWritable.keySet()) {
			text = text + "(" + (Text) key + ", " + mapWritable.get(key) + ") ";
		}
		return text;
	}

}
