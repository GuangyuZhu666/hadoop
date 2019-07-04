package com.hadoop.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair>{
	
	private int first;
	private int second;
	
	public IntPair() {}
	
	public IntPair(int left, int right) {
		set(left, right);
	}
	
	public void set(int left, int right) {
		first = left;
		second = right;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException{
		first = in.readInt();
		second = in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeInt(first);
		out.writeInt(second);
	}
	
	@Override
	public int compareTo(IntPair next) {
		if(this.first != next.first) {
			return this.first < next.first ? -1:1;
		}else if(this.second != next.second) {
			return this.second < next.second ? -1:1;
		}else {
			return 0;
		}
	}
	
	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}
	
}
