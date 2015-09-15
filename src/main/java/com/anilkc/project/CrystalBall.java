package com.anilkc.project;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.anilkc.project.pairapproach.CrystalBallMapper;
import com.anilkc.project.pairapproach.CrystalBallReducer;

public class CrystalBall extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CrystalBall(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		System.out.println("Arg 0: " + args[0]);
		System.out.println("Arg 1: " + args[1]);

		Job job = new org.apache.hadoop.mapreduce.Job();
		job.setJarByClass(CrystalBall.class);
		job.setJobName("CrystalBall");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(CrystalBallMapper.class);
		job.setReducerClass(CrystalBallReducer.class);
		job.setPartitionerClass(PairPartitioner.class);

		job.setNumReduceTasks(1);

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		return returnValue;
	}

}
