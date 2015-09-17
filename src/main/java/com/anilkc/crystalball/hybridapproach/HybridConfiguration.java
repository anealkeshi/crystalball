package com.anilkc.crystalball.hybridapproach;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anilkc.crystalball.Pair;

/**
 * Configuration class for hybrid approach
 * 
 * @author Anil
 *
 */
public class HybridConfiguration extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(HybridConfiguration.class);

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HybridConfiguration(), args);
		System.exit(exitCode);
	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			LOG.error("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		LOG.info("Arg 0: " + args[0]);
		LOG.info("Arg 1: " + args[1]);

		Configuration conf = new Configuration();
		// conf.set("mapreduce.input.lineinputformat.linespermap", "1");
		Job job = new Job(conf, "HybridCrystalBall");
		job.setJarByClass(HybridConfiguration.class);

		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(HybridMapper.class);
		job.setReducerClass(HybridReducer.class);
		// job.setPartitionerClass(PairPartitioner.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		// MultipleInputs.addInputPath(job, new Path(args[1]),
		// TextInputFormat.class, CrystalBallMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		LOG.info("job.isSuccessful " + job.isSuccessful());
		return returnValue;
	}

}
