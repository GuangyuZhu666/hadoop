package com.hadoop.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComprator extends WritableComparator{

	public GroupingComprator() {
		super(IntPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		int first1 = ((IntPair)w1).getFirst();
		int first2 = ((IntPair)w2).getFirst();
		return first1 == first2 ? 0 : (first1 < first2 ? -1 : 1);
	}
	
}
