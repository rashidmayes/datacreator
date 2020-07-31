package com.aerospike.pub.dc;

import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.util.Formatter;
import java.util.Map;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class TimerReporter implements Runnable {
		
	public static final NumberFormat percentFormat = NumberFormat.getPercentInstance();
	public static final NumberFormat numberFormat = NumberFormat.getNumberInstance();

	private static final String STAT_INFO_FORMAT = "%tT"
			+ " %-25s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s\n";
	
	
	private static final String STAT_TITLE_FORMAT = "%n%8s"
			+ " %-25s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s"
			+ " %15s\n";
	
	public void run() {
		
		StringBuilder infoBuffer = new StringBuilder();
		Formatter infoFormatter = new Formatter(infoBuffer);
		
		infoFormatter.format(STAT_TITLE_FORMAT, 
				""
				, "Name"
				
				, "Count"
				, "Mean Rate"
				, "1-Min Rate"
				, "5-Min Rate"
				, "15-Min Rate"
				, "min"
				, "max"
				, "mean"
				

				, "median"
				
				, "75%"
				, "95%"
				, "98%"
				, "99%"
				, "99.9%"
				);
		
		ZonedDateTime currentTime = ZonedDateTime.now();
		
		Map<String, Timer> timers = GenericDataCreator.metrics.getTimers();
		
		Snapshot snapshot;
		Timer timer;
		for ( String name : timers.keySet() ) {
			timer = timers.get(name);
			snapshot = timer.getSnapshot();
			
			infoFormatter.format(STAT_INFO_FORMAT, 
					currentTime
					, name
					
					, timer.getCount()
					, numberFormat.format(timer.getMeanRate())
					, numberFormat.format(timer.getOneMinuteRate())
					, numberFormat.format(timer.getFiveMinuteRate())
					, numberFormat.format(timer.getFifteenMinuteRate())
					
					
					, numberFormat.format(snapshot.getMin()/1000000f)
					, numberFormat.format(snapshot.getMax()/1000000f)					
					, numberFormat.format(snapshot.getMean()/1000000f)
		
					, numberFormat.format(snapshot.getMedian()/1000000f)
					
					, numberFormat.format(snapshot.get75thPercentile()/1000000f)					
					, numberFormat.format(snapshot.get95thPercentile()/1000000f)
					, numberFormat.format(snapshot.get98thPercentile()/1000000f)					
					, numberFormat.format(snapshot.get99thPercentile()/1000000f)
					, numberFormat.format(snapshot.get999thPercentile()/1000000f)
					
					);
		}
		

		infoFormatter.close();
		java.util.logging.Logger.getGlobal().info(infoBuffer.toString());
	
	}
}

