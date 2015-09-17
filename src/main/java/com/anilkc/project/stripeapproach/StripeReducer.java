package com.anilkc.project.stripeapproach;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Anil
 *
 */
public class StripeReducer extends Reducer<Text, MapWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<MapWritable> stripes, Context context)
			throws IOException, InterruptedException {

		MapWritable outWritable = new MapWritable();
		int marginal = 0;
		for (MapWritable mapWritable : stripes) {

			for (Writable writable : mapWritable.keySet()) {

				if (outWritable.containsKey(writable)) {
					outWritable.put(writable, new DoubleWritable(((DoubleWritable) outWritable.get(writable)).get()
							+ ((IntWritable) mapWritable.get(writable)).get()));
				} else {
					outWritable.put(writable, new DoubleWritable(((IntWritable) mapWritable.get(writable)).get()));
				}

				marginal = marginal + ((IntWritable) mapWritable.get(writable)).get();

			}
		}

		for (Writable outKey : outWritable.keySet()) {
			outWritable.put(outKey,
					new DoubleWritable(((DoubleWritable) outWritable.get(outKey)).get() / (double) marginal));
		}

		context.write(key, new Text("[ " + outWritableToText(outWritable) + " ]"));
	}

	private String outWritableToText(MapWritable mapWritable) {
		String text = "";
		for (Writable key : mapWritable.keySet()) {
			text = text + "(" + (Text) key + ", " + mapWritable.get(key) + ") ";
		}
		return text;
	}

}
