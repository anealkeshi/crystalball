package com.anilkc.crystalball.pairapproach;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anilkc.crystalball.CommonConstants;
import com.anilkc.crystalball.Pair;
import com.anilkc.crystalball.Utils;

/**
 * Pair approach reducer class
 * 
 * @author Anil
 *
 */
public class PairReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(PairReducer.class);

	int marginal = 0;

	protected void reduce(Pair pair, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		double relativeFrequency = 0.0;

		for (IntWritable intWritable : values) {
			int count = intWritable.get();

			if (pair.getSecondValue().equals(CommonConstants.SPECIAL_CHARACTER)) {
				marginal = marginal + count;
			} else {
				sum = sum + count;
			}
		}

		relativeFrequency = Utils.getFormattedDouble((double) sum / (double) marginal);

		if (!pair.getSecondValue().equals(CommonConstants.SPECIAL_CHARACTER)) {

			LOG.info("Pair Reducer Output: " + pair + " " + relativeFrequency);
			context.write(pair, new DoubleWritable(relativeFrequency));
		}
	}

}
