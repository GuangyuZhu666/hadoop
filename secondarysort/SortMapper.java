package com.hadoop.secondarysort;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable>{
	
	private IntPair intpair = new IntPair();
	
	public void map(LongWritable key, Text value, Context context) throws
	       IOException, InterruptedException{
		
		StringTokenizer tokens = new StringTokenizer(value.toString());
		if(tokens.hasMoreTokens()) {
			int first, second;
			first = Integer.parseInt(tokens.nextToken());
			intpair.setFirst(first);
			if(tokens.hasMoreTokens()) {
				second = Integer.parseInt(tokens.nextToken());
				intpair.setSecond(second);
				context.write(intpair, new IntWritable(second));
			}
			
		}
	}

}
