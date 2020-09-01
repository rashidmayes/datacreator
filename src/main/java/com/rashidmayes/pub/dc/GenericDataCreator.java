package com.rashidmayes.pub.dc;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
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
import com.rashidmayes.pub.dc.Config.Write;
import com.rashidmayes.pub.dc.keygen.KeyGenerator;
import com.rashidmayes.pub.dc.keygen.PipedKeyGenerator;
import com.rashidmayes.pub.dc.util.SizeHelper;

public class GenericDataCreator implements Listener {

	public static final MetricRegistry metrics = new MetricRegistry();
	
	//change to meters
	public static final Counter NETWORK_READ = metrics.counter("network.read");
	public static final Counter NETWORK_WRITE = metrics.counter("network.write");
	public static final Counter STORAGE_WRITE = metrics.counter("storage.write");
	public static final Counter WITES = metrics.counter("writes");
	public static final Counter READS = metrics.counter("reads");
	public static final Counter READ_HITS = metrics.counter("reads.hits");
	
	
	public static final Counter WAIT = metrics.counter("thread.waittime");
	public static final Counter PREGENERATED = metrics.counter("records.pregenerated");
	

	
	public static Timer SAVED_RECORD_TIMES;
	public static Timer READ_RECORD_TIMES;
	
	private Logger mLogger = Logger.getLogger(getClass().getSimpleName());
	private String mId;
	private KeyQueue mKeyQueue;
	
	GenericDataCreator() {
	}
	
	public void setId(String id) {
		this.mId = id;
	}
	
	public String getId() {
		return this.mId;
	}
	
	@Override
	public void recordWritten(Key key) {
		mKeyQueue.put(key);
	}


	@Override
	public void recordRead(Key key, Record record) {
		mKeyQueue.put(key);
	}
	
	@Override
	public void recycle(Key key) {
		
	}

	@Override
	public void recordDeleted(Key key) {

	}
	
	public void execute(File configFile) throws ParseException {
		

		
		try {			
			ObjectMapper objectMapper = new ObjectMapper();
			abstract class MixIn {@JsonIgnore TlsPolicy	tlsPolicy;}
			objectMapper.addMixIn(ClientPolicy.class, MixIn.class);
			objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
			
			ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
			
			Config config = objectMapper.readValue(configFile, Config.class);
			mLogger.info(objectWriter.writeValueAsString(config));
		
			final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS).build();
			if (config.reportInterval > 0 ) reporter.start(config.reportInterval, TimeUnit.SECONDS);

			NumberFormat percentFormat = NumberFormat.getPercentInstance();
			percentFormat.setMaximumFractionDigits(3);
			
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
			
			
			
			mKeyQueue = new KeyQueue(config.maxKeyQueueSize);
			PipedKeyGenerator.setKeyQueue(mKeyQueue);
			
			
			Semaphore completed = new Semaphore(0);
			ArrayList<Writer> writers = new ArrayList<Writer>();
			ArrayList<Reader> readers = new ArrayList<Reader>();
				
			if ( config.writes != null ) {
				for (Write write : config.writes) {
					KeyGenerator keyGenerator = (KeyGenerator)(Class.forName(write.keyGenerator).getConstructor().newInstance());

					for ( int i = 0, stop = Math.max(1, write.threads); i < stop; i++ ) {
						writers.add(new Writer(keyGenerator, config, write, GenericDataCreator.this, completed));
					}
				}
			}
		
			
			if ( config.reads != null ) {
				Arrays.stream(config.reads).forEach(r -> {
					try {
						KeyGenerator keyGenerator = (KeyGenerator)(Class.forName(r.keyGenerator).getConstructor().newInstance());

						for ( int i = 0, stop = Math.max(1, r.threads); i < stop; i++ ) {
							readers.add(new Reader(keyGenerator, config, r, GenericDataCreator.this, completed));
						}
					} catch (Exception e) {
						mLogger.log(Level.ALL, e.getMessage(), e);
					}
				});
			}
			
			
			
			
			
			ThreadGroup writerThreadGroup = new ThreadGroup("writers");
			ThreadGroup readersThreadGroup = new ThreadGroup("readers");
			long start = System.currentTimeMillis();
			
			SAVED_RECORD_TIMES = new Timer(new com.codahale.metrics.UniformReservoir());
			metrics.register("\u2211 " + mId + ".writes", SAVED_RECORD_TIMES);
			
			READ_RECORD_TIMES = new Timer(new com.codahale.metrics.UniformReservoir());
			metrics.register("\u2211 " + mId + ".reads", READ_RECORD_TIMES);			

			
			writers.forEach(r -> new Thread(writerThreadGroup, r).start());
			readers.forEach(r -> new Thread(readersThreadGroup, r).start());
		

			Snapshot savedTimes = null;
			Snapshot readTimes = null;
			double writeRate = 0;
			double readRate = 0;

			
			TimerReporter timerReporter = new TimerReporter();
			ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
			try {
				scheduledExecutorService.scheduleWithFixedDelay(timerReporter, 0, config.infoInterval, TimeUnit.SECONDS);
				completed.tryAcquire(writers.size() + readers.size(),config.duration, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				mLogger.log(Level.ALL, e.getMessage(), e);
			} finally {
				savedTimes = GenericDataCreator.SAVED_RECORD_TIMES.getSnapshot();
				readTimes = GenericDataCreator.READ_RECORD_TIMES.getSnapshot();
				readRate = GenericDataCreator.READ_RECORD_TIMES.getMeanRate();				
				writeRate = GenericDataCreator.SAVED_RECORD_TIMES.getMeanRate();
				reporter.close();
				readers.forEach(r -> r.cancel());
	        	
	        	for ( Writer w : writers ) {
	        		w.cancel();
	        	}
	        	
	        	scheduledExecutorService.shutdown();
	        	scheduledExecutorService.awaitTermination(1, TimeUnit.MINUTES);
			}

			if (config.reportInterval > 0) reporter.report();
			timerReporter.run();
			Period elapsed = new Period(System.currentTimeMillis()-start);
			
			System.out.println(
					String.format("\n\n********** Complete **********\n"
						+ "Duration: %s\n"
						+ "Network Out: %s (%s)\n"
						+ "Stored: %s (%s)\n"
						+ "Write Count: %s\n"
						+ "Avg. Latency: %s ms\n"
						+ "Min Latency: %s ms\n"
						+ "Max Latency: %s ms\n"
						+ "Records/Second: %s\n"
						
						+ "Network In: %s (%s)\n"
						+ "Read Count: %s\n"
						+ "Read Hit Ratio: %s\n"
						+ "Read Avg. Latency: %s ms\n"
						+ "Read Min Latency: %s ms\n"
						+ "Read Max Latency: %s ms\n"
						+ "Read Records/Second: %s"
						
						
						, formatter.print(elapsed)
						, SizeHelper.getSize(GenericDataCreator.NETWORK_WRITE.getCount())
						, GenericDataCreator.NETWORK_WRITE.getCount()
						, SizeHelper.getSize(GenericDataCreator.STORAGE_WRITE.getCount())
						, GenericDataCreator.STORAGE_WRITE.getCount()
						
						, GenericDataCreator.SAVED_RECORD_TIMES.getCount()
						, numberFormat.format(savedTimes.getMean()/1000000f)
						, numberFormat.format(savedTimes.getMin()/1000000f)
						, numberFormat.format(savedTimes.getMax()/1000000f)
						, numberFormat.format(writeRate)
						
						, SizeHelper.getSize(GenericDataCreator.NETWORK_READ.getCount())
						, GenericDataCreator.NETWORK_READ.getCount()
						, GenericDataCreator.READ_RECORD_TIMES.getCount()
						, percentFormat.format(((float) READ_HITS.getCount())/GenericDataCreator.READ_RECORD_TIMES.getCount())
						, numberFormat.format(readTimes.getMean()/1000000f)
						, numberFormat.format(readTimes.getMin()/1000000f)
						, numberFormat.format(readTimes.getMax()/1000000f)
						, numberFormat.format(readRate)
					));
				
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	


	public static void main(String[] args) throws ParseException {
		
		String fileName = ( args.length == 0 ) ? "examples/sample.json" : args[0];
		
		File configFile = new File(fileName);
		if ( configFile.canRead() ) {

			GenericDataCreator dataCreator = new GenericDataCreator();

			if ( args.length == 2 ) {
				dataCreator.setId(args[1]);
			} else {
			
				try {
					NetworkInterface.networkInterfaces()
					.takeWhile(i ->  dataCreator.getId() == null)
					.forEach(i -> {
						InetAddress ia;
						for (Enumeration<InetAddress> e = i.getInetAddresses(); e.hasMoreElements();) {
							ia = e.nextElement();
							if ( !ia.isLoopbackAddress() && ia instanceof Inet4Address) {
								dataCreator.setId(ia.getHostAddress());
								break;
							}
						}
					});
				} catch (IOException ioe) {
					ioe.printStackTrace();
					dataCreator.setId(RandomStringUtils.randomAlphabetic(2));
				}
			}
			
			
			dataCreator.execute(configFile);
			
		} else {
			System.err.println("Unable to read " + configFile.getPath());
			System.exit(1);
		}
		
		System.exit(0);
	}
}
