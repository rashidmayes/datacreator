package com.rashidmayes.pub.dc.util;

import java.text.NumberFormat;

public class SizeHelper {
	private static final String[] suffixes = { "B", "K", "M", "G", "T", "P" };
	
	public static String getSize(double number) {

		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMaximumFractionDigits(2);
				
		double scale = 1;
		String suffix = suffixes[0];
		for (int i = suffixes.length-1; i >= 0; i--) {
			scale = Math.pow(2, 10*i);
			if ( number >= scale ) {
				suffix  = suffixes[i];
				break;
			}
		}
		
		return nf.format((number/scale)) + suffix;
	}

}