package com.anilkc.crystalball.stripeapproach;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anilkc.crystalball.CommonConstants;
import com.anilkc.crystalball.Utils;

/**
 * Mapper class for Stripe approach
 * 
 * @author Anil
 *
 */
public class StripeMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(StripeMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] datas = value.toString().split(CommonConstants.SPACE_REGEX);

		LOG.info("Received line: " + value.toString());
		LOG.info("Processing....");

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

			LOG.info("Stripe Mapper Output: " + data + " " + Utils.convertMapWritableToString(stripe));
			context.write(data, stripe);
		}
	}
}
