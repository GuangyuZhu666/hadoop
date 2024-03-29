package com.hadoop.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPatitioner extends Partitioner<IntPair, IntWritable>{

	@Override
	public int getPartition(IntPair key, IntWritable value, int numPartitions) {
		return Math.abs(key.getFirst() * 127) % numPartitions;
	}
	
}
