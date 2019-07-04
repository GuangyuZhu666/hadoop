package com.hadoop.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort {

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		
		if(args.length != 2){
			System.err.println("Usage: wordcount <in><out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "secondarysort");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReduce.class);
		
		job.setPartitionerClass(MyPatitioner.class);
		job.setGroupingComparatorClass(GroupingComprator.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1);
		
	}

}
