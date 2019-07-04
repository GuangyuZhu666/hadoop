package com.hadoop.secondarysort;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReduce extends Reducer<IntPair, IntWritable, IntWritable, IntWritable>{
	
	
	public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws
	       IOException, InterruptedException{
		 
	    IntWritable key1 = new IntWritable(key.getFirst());
		
		for(IntWritable value : values) {
			context.write(key1, value);
		}
		
	}

}
