package com.aerospike.pub.dc.util;

import java.text.NumberFormat;

public class SizeHelper {

	public static final double K = 2L << 9;
	public static final double M = 2L << 19;
	public static final double G = 2L << 29;

	//public static final double K = Math.pow(10, 3);
	//public static final double M = Math.pow(10, 6);
	//public static final double G = Math.pow(10, 9);

	public static String getSize(long number) {
		String suffix;
		double sz;

		if (number > G) {
			// gb
			sz = (number / G);
			suffix = "G";
		} else if (number > M) {
			// mb
			sz = (number / M);
			suffix = "M";
		} else if (number > K) {
			// kb
			sz = number / K;
			suffix = "K";
		} else {
			sz = number;
			suffix = "";
		}

		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMaximumFractionDigits(2);
		
		return nf.format(sz) + suffix;
	}
}