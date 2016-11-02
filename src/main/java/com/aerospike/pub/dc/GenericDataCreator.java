package com.aerospike.pub.dc;

import java.io.File;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.ParseException;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.aerospike.pub.dc.keygen.KeyGenerator;
import com.aerospike.pub.dc.util.SizeHelper;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class GenericDataCreator {

	public static final MetricRegistry metrics = new MetricRegistry();
	
	public static final Counter NETWORK = metrics.counter("records.network");
	public static final Counter STORAGE = metrics.counter("records.storage");
	public static final Counter WAIT = metrics.counter("records.waittime");
	public static final Counter PREGENERATED = metrics.counter("records.pregenerated");
	
	public static Timer SAVED_RECORD_TIMES;

	
	public static void main(String[] args) throws ParseException {
		
		String fileName = ( args.length == 0 ) ? "load.json" : args[0];
		File file = new File(fileName);
		if ( !file.canRead() ) {
			System.err.println("Unable to read " + file.getAbsolutePath());
			System.exit(0);
		}
		
		try {
			
			ObjectMapper objectMapper = new ObjectMapper();
			Config config = objectMapper.readValue(file, Config.class);
			
			ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
			System.out.println(objectWriter.writeValueAsString(config));
			
			final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS).build();
			reporter.start(config.reportInterval, TimeUnit.SECONDS);
			
			NumberFormat numberFormat = NumberFormat.getNumberInstance();
			numberFormat.setMaximumFractionDigits(3);

			PeriodFormatter formatter = new PeriodFormatterBuilder()
					.printZeroAlways()
				     .appendDays()
				     .appendSuffix("d ")
				     
				     .printZeroAlways()
				     .appendHours()
				     .appendSuffix("h ")
				     
				     .printZeroAlways()
				     .appendMinutes()
				     .appendSuffix("m ")
				     
				     .printZeroAlways()
				     .appendSeconds()
				     .appendSuffix("s")
				     .toFormatter();
			
			
			Semaphore completed = new Semaphore(0);
			ArrayList<Writer> writers = new ArrayList<Writer>();
				
			if ( config.write != null ) {				
				KeyGenerator keyGenerator =  (KeyGenerator)(Class.forName(config.write.keyGenerator).newInstance());

				for ( int i = 0, stop = Math.max(1, config.write.threads); i < stop; i++ ) {
					writers.add(new Writer(keyGenerator, config, completed));
				}
			}

			
			ThreadGroup threadGroup = new ThreadGroup("writers");
			long start = System.currentTimeMillis();
			
			SAVED_RECORD_TIMES = metrics.timer("records.saved.times");
			for (Writer w : writers) {
				new Thread(threadGroup, w).start();	
			}

			Snapshot savedTimes = null;
			double writeRate = 0;

			
			try {
				completed.tryAcquire(writers.size(),config.duration, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				savedTimes = GenericDataCreator.SAVED_RECORD_TIMES.getSnapshot();
				writeRate = GenericDataCreator.SAVED_RECORD_TIMES.getMeanRate();
				reporter.close();
	        	for ( Writer w : writers ) {
	        		w.cancel();
	        	}
			}

			reporter.report();
			Period elapsed = new Period(System.currentTimeMillis()-start);
			
			System.out.println(
				String.format("\n\n********** Complete **********\n"
					+ "Duration: %s\n"
					+ "Transfered: %s (%s)\n"
					+ "Stored: %s (%s)\n"
					+ "Count: %s\n"
					+ "Avg. Latency: %s ms\n"
					+ "Min Latency: %s ms\n"
					+ "Max Latency: %s ms\n"
					+ "Records/Second: %s"
					, formatter.print(elapsed)
					, SizeHelper.getSize(GenericDataCreator.NETWORK.getCount())
					, GenericDataCreator.NETWORK.getCount()
					, SizeHelper.getSize(GenericDataCreator.STORAGE.getCount())
					, GenericDataCreator.STORAGE.getCount()
					, GenericDataCreator.SAVED_RECORD_TIMES.getCount()
					, numberFormat.format(savedTimes.getMean()/1000000f)
					, numberFormat.format(savedTimes.getMin()/1000000f)
					, numberFormat.format(savedTimes.getMax()/1000000f)
					, numberFormat.format(writeRate)
				));
				
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
