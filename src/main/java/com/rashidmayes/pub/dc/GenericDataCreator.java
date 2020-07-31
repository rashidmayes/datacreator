package com.rashidmayes.pub.dc;

import java.io.File;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.ParseException;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.rashidmayes.pub.dc.keygen.KeyGenerator;
import com.rashidmayes.pub.dc.util.SizeHelper;

public class GenericDataCreator {

	public static final MetricRegistry metrics = new MetricRegistry();
	
	public static final Counter NETWORK = metrics.counter("records.network");
	public static final Counter STORAGE = metrics.counter("records.storage");
	public static final Counter WAIT = metrics.counter("records.waittime");
	public static final Counter PREGENERATED = metrics.counter("records.pregenerated");
	
	public static final Counter WITES_SAVED = metrics.counter("writes.saved");
	public static final Counter TRUNCATES_SAVED = metrics.counter("truncates.saved");
	
	public static Timer UPDATE_TIMES;
	public static Timer SAVED_RECORD_TIMES;

	
	public static void main(String[] args) throws ParseException {
		
		String fileName = ( args.length == 0 ) ? "sample.json" : args[0];
		String sPrefix = ( args.length == 2 ) ? args[1] : "a";
		
	
		File file = new File(fileName);
		if ( !file.canRead() ) {
			System.err.println("Unable to read " + file.getPath());
			System.exit(0);
		}
		
		try {			
			ObjectMapper objectMapper = new ObjectMapper();
			abstract class MixIn {@JsonIgnore TlsPolicy	tlsPolicy;}
			objectMapper.addMixIn(ClientPolicy.class, MixIn.class);
			objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
			
			ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
			
			Config config = objectMapper.readValue(file, Config.class);
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
				KeyGenerator keyGenerator =  (KeyGenerator)(Class.forName(config.write.keyGenerator).getConstructor().newInstance());

				for ( int i = 0, stop = Math.max(1, config.write.threads); i < stop; i++ ) {
					writers.add(new Writer(keyGenerator, config, completed));
				}
			}

			ThreadGroup threadGroup = new ThreadGroup("writers");
			long start = System.currentTimeMillis();
			
			SAVED_RECORD_TIMES = new Timer(new com.codahale.metrics.UniformReservoir());
			metrics.register(sPrefix + ".records.saved.times", SAVED_RECORD_TIMES);
			
			UPDATE_TIMES = new Timer(new com.codahale.metrics.UniformReservoir());
			metrics.register(sPrefix + ".users.update.times", UPDATE_TIMES);			
			
			for (Writer w : writers) {
				new Thread(threadGroup, w).start();	
			}

			Snapshot savedTimes = null;
			Snapshot updateTimes = null;
			double writeRate = 0;
			double updateRate = 0;

			
			ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
			try {
				scheduledExecutorService.scheduleWithFixedDelay(new TimerReporter(), 0, config.reportInterval, TimeUnit.SECONDS);
				completed.tryAcquire(writers.size(),config.duration, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				savedTimes = GenericDataCreator.SAVED_RECORD_TIMES.getSnapshot();
				updateTimes = GenericDataCreator.UPDATE_TIMES.getSnapshot();
				updateRate = GenericDataCreator.UPDATE_TIMES.getMeanRate();				
				writeRate = GenericDataCreator.SAVED_RECORD_TIMES.getMeanRate();
				reporter.close();
	        	for ( Writer w : writers ) {
	        		w.cancel();
	        	}
	        	
	        	scheduledExecutorService.isShutdown();
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
						+ "Records/Second: %s\n"
						
						+ "Update Count: %s\n"
						+ "Update Avg. Latency: %s ms\n"
						+ "Update Min Latency: %s ms\n"
						+ "Update Max Latency: %s ms\n"
						+ "Update Records/Second: %s"
						
						
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
						
						, GenericDataCreator.UPDATE_TIMES.getCount()
						, numberFormat.format(updateTimes.getMean()/1000000f)
						, numberFormat.format(updateTimes.getMin()/1000000f)
						, numberFormat.format(updateTimes.getMax()/1000000f)
						, numberFormat.format(updateRate)
					));
				
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.exit(0);
	}
}
