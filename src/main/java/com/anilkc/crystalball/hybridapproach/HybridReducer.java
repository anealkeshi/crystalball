package com.anilkc.crystalball.hybridapproach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anilkc.crystalball.Pair;
import com.anilkc.crystalball.Utils;

/**
 * Reducer class for Hybrid Approach
 * 
 * @author Anil
 *
 */
public class HybridReducer extends Reducer<Pair, IntWritable, Text, Text> {

	private static final Logger LOG = LoggerFactory.getLogger(HybridReducer.class);

	private int marginal = 0;
	private Map<String, Double> stripe;
	private String currentTerm = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		marginal = 0;
		stripe = new HashMap<String, Double>();
	}

	protected void reduce(Pair pair, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		if (null == currentTerm) {

			currentTerm = pair.getFirstValue();

		} else if (!currentTerm.equals(pair.getFirstValue())) {

			for (Map.Entry<String, Double> entry : stripe.entrySet()) {
				stripe.put(entry.getKey(), Utils.getFormattedDouble(entry.getValue() / marginal));

			}

			String finalMap = "[ " + Utils.converMaptoString(stripe) + " ]";
			LOG.info("Hybrid Reducer Output: " + currentTerm + " " + finalMap);

			context.write(new Text(currentTerm), new Text(finalMap));

			// reset for new term
			marginal = 0;
			stripe = new HashMap<String, Double>();
			currentTerm = pair.getFirstValue();
		}

		for (IntWritable intWritable : values) {

			if (stripe.containsKey(pair.getSecondValue())) {
				stripe.put(pair.getSecondValue(), stripe.get(pair.getSecondValue()) + intWritable.get());
			} else {
				stripe.put(pair.getSecondValue(), (double) intWritable.get());
			}
			marginal += intWritable.get();
		}

	}

	@Override
	protected void cleanup(Reducer<Pair, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {

		for (Map.Entry<String, Double> entry : stripe.entrySet()) {
			stripe.put(entry.getKey(), Utils.getFormattedDouble(entry.getValue() / marginal));

		}

		String finalMap = "[ " + Utils.converMaptoString(stripe) + " ]";
		LOG.info("Hybrid Reducer Output: " + currentTerm + " " + finalMap);

		context.write(new Text(currentTerm), new Text(finalMap));
	}

}
