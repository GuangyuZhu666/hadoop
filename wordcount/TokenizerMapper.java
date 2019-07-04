package com.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws 
	       IOException, InterruptedException{
	
		StringTokenizer tokens = new StringTokenizer(value.toString());
		while(tokens.hasMoreTokens()){
			word.set(tokens.nextToken());
			context.write(word, one);
		}
		
	}
	
}
