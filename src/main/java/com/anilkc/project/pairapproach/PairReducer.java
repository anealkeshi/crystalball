package com.anilkc.project.pairapproach;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.anilkc.project.Pair;

/**
 * @author Anil
 *
 */
public class PairReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	int marginal = 0;

	protected void reduce(Pair pair, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		double relativeFrequency = 0.0;
		for (IntWritable intWritable : values) {
			int count = intWritable.get();

			if (pair.getSecondValue().equals("*")) {
				marginal = marginal + count;
			} else {
				sum = sum + count;
			}
		}
		relativeFrequency = (double) sum / (double) marginal;

		if (!pair.getSecondValue().equals("*")) {
			context.write(pair, new DoubleWritable(relativeFrequency));
		}
	}

}
