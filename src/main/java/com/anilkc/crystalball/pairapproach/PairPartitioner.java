package com.anilkc.crystalball.pairapproach;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.anilkc.crystalball.Pair;

/**
 * @author Anil
 *
 */
public class PairPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable value, int numPartitions) {
		if (numPartitions == 0) {
			return 0;
		}
		if (new Integer(key.getFirstValue()) > 50) {
			return 0;
		} else {
			return 1 % numPartitions;
		}
	}

}
