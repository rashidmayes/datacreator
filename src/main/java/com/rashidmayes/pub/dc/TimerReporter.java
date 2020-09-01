package com.rashidmayes.pub.dc;

import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.util.Formatter;
import java.util.Map;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.rashidmayes.pub.dc.util.SizeHelper;

public class TimerReporter implements Runnable {

	private static final NumberFormat numberFormat = NumberFormat.getNumberInstance();

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
	
	
	public void printTimers() {
		
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
			if ( timer.getCount() == 0 ) {
				continue;
			}
			
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
		infoBuffer.append("\n");
		java.util.logging.Logger.getGlobal().info(infoBuffer.toString());
	
	}	
	
	
	public void printCounters() {
		
		final StringBuilder infoBuffer = new StringBuilder();
		Formatter infoFormatter = new Formatter(infoBuffer);
		
		infoFormatter.format("%n%8s"
				+ " %-25s"
				+ " %15s\n", 
				""
				, "Name"
				, "Count");
		
		ZonedDateTime currentTime = ZonedDateTime.now();
		
		GenericDataCreator.metrics.getCounters().forEach((name, counter) -> {
			
			if ( counter.getCount() > 0 ) {
				infoFormatter.format("%tT"
						+ " %-25s"
						+ " %15s\n", 
						currentTime
						, name
						, SizeHelper.getSize(counter.getCount()));
			}
			
		});


		infoFormatter.close();
		infoBuffer.append("\n");
		java.util.logging.Logger.getGlobal().info(infoBuffer.toString());
	
	}	
	
	
	public void run() {
		
		printTimers();
		printCounters();
	
	}
}

