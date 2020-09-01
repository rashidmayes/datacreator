package com.rashidmayes.pub.dc;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteMode;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.RateLimiter;
import com.rashidmayes.pub.dc.Config.BinSpec;
import com.rashidmayes.pub.dc.Config.Write;
import com.rashidmayes.pub.dc.keygen.KeyGenerator;

public class Writer implements Runnable {

	private Config mConfig;
	private Write mWrite;
	private boolean mCancel;
	private KeyGenerator mKeyGenerator; 
	private Semaphore mCompleted;
	private AerospikeClient mClient;
	private LinkedList<RecordTemplate> mBuffer;
	private RateLimiter mRateLimiter;
	private RecordTemplate recordTemplate;
	private Listener mListener;
	private MapPolicy mMapPolicy;
	
	private Timer OP_TIMES;
	private Timer PUT_TIMES;

	
	
	public Writer(KeyGenerator keyGenerator, Config config, Write write, Listener listener, Semaphore completed) {
		this.mConfig = config;
		this.mWrite = write;
		this.mKeyGenerator = keyGenerator;
		this.mCompleted = completed;
		this.mClient = config.getClient();
		this.mListener = listener;
		this.mMapPolicy = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE);

		if ( StringUtils.isEmpty(write.set) ) {
			SimpleDateFormat sdf = new SimpleDateFormat("'GEN'yyyy-dd-MM_HH_mm_ss");
			write.set = sdf.format(new Date());
		}
		
		if ( write.rateLimit > 0 ) {
			this.mRateLimiter = RateLimiter.create(write.rateLimit);
		}
		
		if ( mWrite.preGenerate > 0 ) {
			mBuffer = new LinkedList<RecordTemplate>();
			for ( int i = 0; i < mWrite.preGenerate; i++ ) {
				mBuffer.add(generate());
				GenericDataCreator.PREGENERATED.inc();
			}
		}
	}
	
	public void run() {

		try {
			RecordTemplate template;
			String id = String.format("%s.%s", Thread.currentThread().getName(),mWrite.id);
			
			OP_TIMES = new Timer(new com.codahale.metrics.UniformReservoir());
			GenericDataCreator.metrics.register(id + ".ops", OP_TIMES);
			
			PUT_TIMES = new Timer(new com.codahale.metrics.UniformReservoir());
			GenericDataCreator.metrics.register(id  + ".puts", PUT_TIMES);

			while (!mCancel && mWrite.counter.longValue() < mWrite.limit) {
				mWrite.counter.increment();
				if ( mRateLimiter != null ) {
					 GenericDataCreator.WAIT.inc((long)mRateLimiter.acquire());
				}
				
				if ( mBuffer == null || mBuffer.isEmpty() ) {
					template = generate();
				} else {
					template = mBuffer.removeLast();
					if ( mBuffer.isEmpty() ) {
						mBuffer = null;
					}
				}
				
				try (Context c = GenericDataCreator.SAVED_RECORD_TIMES.time()) {					
					if ( mWrite.useOperations ) {
						
						Operation[] ops = template.getOps(mMapPolicy);					
						try (Context context = OP_TIMES.time()) {
							mClient.operate(null, template.key, ops);
						}
						
					} else {
						
						try (Context context = PUT_TIMES.time()) {
							mClient.put(null, template.key, template.bins);
						}
					}
					
					mListener.recordWritten(template.key);
					
				} catch (AerospikeException ae) {
					ae.printStackTrace();
					if ( ae.getResultCode() != 8 ) {
						break;
					}
					
				} finally {
					GenericDataCreator.NETWORK_WRITE.inc(template.estimatedSize);
					GenericDataCreator.STORAGE_WRITE.inc(template.dataSize);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			mCompleted.release();
		}
	}
	
	
	private RecordTemplate generate() {
		
		int ord = RandomUtils.nextInt(0, mWrite.binSpecs.length);
		BinSpec binSpec = mWrite.binSpecs[ord];
		
		if ( mWrite.sameRecordDifferentKey ) {
			
			if ( recordTemplate != null ) {
				recordTemplate.key = mKeyGenerator.generate(mConfig.name, mWrite.keyLength, mWrite.namespace, mWrite.set);
				return recordTemplate;
			}
		}
		
		recordTemplate = new RecordTemplate();
		
		recordTemplate.bins = new Bin[binSpec.bins.size()];
		recordTemplate.estimatedSize += 28 * recordTemplate.bins.length;
		
		if ( mWrite.set != null ) {
			recordTemplate.estimatedSize += 9 + mWrite.set.length();
		}
		
		Config.Bin binConfig;
		DataType type;
		int i = 0;
		
		for (Entry<String, Config.Bin> entry : binSpec.bins.entrySet() ) {
			binConfig = entry.getValue();
			type = binConfig.type;
			recordTemplate.bins[i] = type.create(entry.getKey(), binConfig);
			recordTemplate.estimatedSize += recordTemplate.bins[i].value.estimateSize();
			
			if ( entry.getKey().equals(binSpec.key) ) {
				
				
				
				recordTemplate.key = new Key(mWrite.namespace, mWrite.set, recordTemplate.bins[i].value );
			}
			
			i++;
		}
		
		if ( recordTemplate.key == null ) {
			recordTemplate.key = mKeyGenerator.generate(mConfig.name, mWrite.keyLength, mWrite.namespace, mWrite.set);
			recordTemplate.bins = Arrays.copyOf(recordTemplate.bins, recordTemplate.bins.length+1);
			recordTemplate.bins[i] = new Bin("_id", Value.get(recordTemplate.key.userKey));
		} 	
		
		recordTemplate.dataSize = ( recordTemplate.estimatedSize/128 ) * 128 + ((recordTemplate.estimatedSize % 128 > 0) ? 128 : 0);
		
		return recordTemplate;
	}
	
	public void cancel() {
		mCancel = true;
	}
}