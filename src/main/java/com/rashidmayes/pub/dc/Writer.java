package com.rashidmayes.pub.dc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteMode;
import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rashidmayes.pub.dc.Config.BinSpec;
import com.rashidmayes.pub.dc.Config.Write;
import com.rashidmayes.pub.dc.keygen.KeyGenerator;

public class Writer implements Runnable {

	Config mConfig;
	Write mWrite;
	boolean mCancel;
	KeyGenerator mKeyGenerator; 
	Semaphore mCompleted;
	AerospikeClient mClient;
	public LinkedList<RecordTemplate> mBuffer;
	RateLimiter mRateLimiter;
	RecordTemplate recordTemplate;
	
	//change to LongAdder if exact count is needed
	//private static long count = 0;
	static LongAdder counter = new LongAdder();
	Gson gson = new GsonBuilder().create();
	Random mRandom = new Random();
	MapPolicy mMapPolicy;
	boolean mOrderedMaps;
	
	public Writer(KeyGenerator keyGenerator, Config config, Semaphore completed) {
		this.mConfig = config;
		this.mWrite = config.write;
		this.mKeyGenerator = keyGenerator;
		this.mCompleted = completed;
		this.mClient = config.getClient();
		this.mMapPolicy = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE);
		this.mOrderedMaps = true;
		
		if ( StringUtils.isEmpty(config.write.set) ) {
			SimpleDateFormat sdf = new SimpleDateFormat("'GEN'yyyy-dd-MM_HH_mm_ss");
			config.write.set = sdf.format(new Date());
		}
		
		if ( config.write.rateLimit > 0 ) {
			this.mRateLimiter = RateLimiter.create(config.write.rateLimit);
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

			while (!mCancel && counter.longValue() < mWrite.limit) {
				counter.increment();
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
				
				try {					
					if ( mWrite.useOperations ) {
						
						Operation[] ops = template.getOps(mMapPolicy);					
						try (Context context = GenericDataCreator.SAVED_RECORD_TIMES.time()) {
							mClient.operate(null, template.key, ops);
						}
						
					} else {
						
						try (Context context = GenericDataCreator.SAVED_RECORD_TIMES.time()) {
							mClient.put(null, template.key, template.bins);
						}
					}
					
				} catch (AerospikeException ae) {
					ae.printStackTrace();
					if ( ae.getResultCode() != 8 ) {
						break;
					}
					
				} finally {
					GenericDataCreator.NETWORK.inc(template.estimatedSize);
					GenericDataCreator.STORAGE.inc(template.dataSize);
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
		
		if ( mConfig.write.sameRecordDifferentKey ) {
			
			if ( recordTemplate != null ) {
				recordTemplate.key = mKeyGenerator.generate(mConfig.name, mWrite.keyLength, mWrite.namespace, mWrite.set);
				return recordTemplate;
			}
		}
		
		recordTemplate = new RecordTemplate();
		recordTemplate.key = mKeyGenerator.generate(mConfig.name, mWrite.keyLength, mWrite.namespace, mWrite.set);
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
			
			i++;
		}
		
		recordTemplate.dataSize = ( recordTemplate.estimatedSize/128 ) * 128 + ((recordTemplate.estimatedSize % 128 > 0) ? 128 : 0);
		
		return recordTemplate;
	}
	
	public void cancel() {
		mCancel = true;
	}
}