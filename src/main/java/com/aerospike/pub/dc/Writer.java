package com.aerospike.pub.dc;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.pub.dc.Config.Write;
import com.aerospike.pub.dc.keygen.KeyGenerator;
import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

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
	private static LongAdder counter = new LongAdder();
	Gson gson = new GsonBuilder().create();
	
	public Writer(KeyGenerator keyGenerator, Config config, Semaphore completed) {
		this.mConfig = config;
		this.mWrite = config.write;
		this.mKeyGenerator = keyGenerator;
		this.mCompleted = completed;
		this.mClient = config.getClient();
		
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
			
			Context context = null;
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
					context = GenericDataCreator.SAVED_RECORD_TIMES.time();
					mClient.put(null, template.key, template.bins);
				} finally {
					if ( context != null ) {
						context.close();
					}
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
		
		if ( mConfig.write.sameRecordDifferentKey ) {
			
			if ( recordTemplate != null ) {
				recordTemplate.key = mKeyGenerator.generate(mConfig.name, mWrite.keyLength, mWrite.namespace, mWrite.set);
				return recordTemplate;
			}
		}
		
		recordTemplate = new RecordTemplate();
		recordTemplate.key = mKeyGenerator.generate(mConfig.name, mWrite.keyLength, mWrite.namespace, mWrite.set);
		recordTemplate.bins = new Bin[mWrite.bins.size()];
		recordTemplate.estimatedSize += 28 * recordTemplate.bins.length;
		
		if ( mWrite.set != null ) {
			recordTemplate.estimatedSize += 9 + mWrite.set.length();
		}
		
		Config.Bin binConfig;
		Config.Type type;
		int i = 0;
		for (Entry<String, Config.Bin> entry : mWrite.bins.entrySet() ) {
			binConfig = entry.getValue();
			type = binConfig.type;
			
			switch (type) {
			case INT:
				recordTemplate.bins[i] = new Bin(entry.getKey(), RandomUtils.nextLong(0, binConfig.size));
				recordTemplate.estimatedSize += 2 + 8;
				break;
			case DOUBLE:
				recordTemplate.bins[i] = new Bin(entry.getKey(), RandomUtils.nextDouble(0, binConfig.size));
				recordTemplate.estimatedSize += 5 + binConfig.size;
				break;
			case STRING:
				recordTemplate.bins[i] = new Bin(entry.getKey(), RandomStringUtils.randomAlphanumeric(binConfig.size));
				recordTemplate.estimatedSize += 5 + binConfig.size;
				break;
			case BLOB:
				recordTemplate.bins[i] = new Bin(entry.getKey(), RandomUtils.nextBytes(binConfig.size));
				recordTemplate.estimatedSize += 5 + binConfig.size;
				break;
				
			//list, maps, geo not in original design, needs testing, estimates not accurate
			case LIST: //change to msgpack
				if ( Config.Type.STRING == binConfig.elementType ) {
					ArrayList<String> list = new ArrayList<String>(binConfig.size);
					for ( int index = 0; index < binConfig.size ; index++ ) {
						list.add(RandomStringUtils.randomAlphanumeric(binConfig.elementLength));
					}
					recordTemplate.bins[i] = new Bin(entry.getKey(), list);
					recordTemplate.estimatedSize += 10 + (binConfig.size * binConfig.elementLength); //change to msg pack est.
				} else {
					ArrayList<Long> list = new ArrayList<Long>(binConfig.size);
					for ( int index = 0; index < binConfig.size ; index++ ) {
						list.add(RandomUtils.nextLong(0,binConfig.elementLength));
					}
					recordTemplate.bins[i] = new Bin(entry.getKey(), list);
					recordTemplate.estimatedSize += 10 + (binConfig.size * 8); //change to msg pack est.					
				}
				break;
			case MAP: //change to msgpack
				
				if ( binConfig.keyType == Config.Type.STRING ) {
					if ( binConfig.elementType == Config.Type.STRING ) {
						HashMap<String,String> map = new HashMap<String,String>();
						for ( int kp = 0; kp < binConfig.size; kp++ ) {
							map.put(RandomStringUtils.randomAlphanumeric(binConfig.keyLength), 
									RandomStringUtils.randomAlphanumeric(binConfig.elementLength));
						}
						recordTemplate.bins[i] = new Bin(entry.getKey(), map);
						recordTemplate.estimatedSize += 2 + (binConfig.keyLength + binConfig.elementLength) * binConfig.size;
					} else {
						HashMap<String,Long> map = new HashMap<String,Long>();
						for ( int kp = 0; kp < binConfig.size; kp++ ) {
							map.put(RandomStringUtils.randomAlphanumeric(binConfig.keyLength), 
									RandomUtils.nextLong(0, binConfig.elementLength));
						}
						recordTemplate.bins[i] = new Bin(entry.getKey(), map);
						recordTemplate.estimatedSize += 2 + (binConfig.keyLength + 8) * binConfig.size;
					}
				} else {
					if ( binConfig.elementType == Config.Type.INT ) {
						HashMap<Long,String> map = new HashMap<Long,String>();
						for ( int kp = 0; kp < binConfig.size; kp++ ) {
							map.put(RandomUtils.nextLong(0,binConfig.keyLength), 
									RandomStringUtils.randomAlphanumeric(binConfig.elementLength));
						}
						recordTemplate.bins[i] = new Bin(entry.getKey(), map);
						recordTemplate.estimatedSize += 2 + (binConfig.keyLength + binConfig.elementLength) * binConfig.size;
					} else {
						HashMap<Long,Long> map = new HashMap<Long,Long>();
						for ( int kp = 0; kp < binConfig.size; kp++ ) {
							map.put(RandomUtils.nextLong(0,binConfig.keyLength), 
									RandomUtils.nextLong(0, binConfig.elementLength));
						}
						recordTemplate.bins[i] = new Bin(entry.getKey(), map);
						recordTemplate.estimatedSize += 2 + (8 + 8) * binConfig.size;
					}
				}

				break;
			case GEOSPATIAL:
				JsonObject locobj = new JsonObject();
				locobj.addProperty("type", "Point");
				JsonArray coords = new JsonArray();

				coords.add(RandomUtils.nextDouble(0, binConfig.size) - RandomUtils.nextDouble(0, binConfig.size));
				coords.add(RandomUtils.nextDouble(0, binConfig.size) - RandomUtils.nextDouble(0, binConfig.size));
				locobj.add("coordinates", coords);
				
				String json = gson.toJson(locobj);
				
				recordTemplate.bins[i] = Bin.asGeoJSON(entry.getKey(), json);
				recordTemplate.estimatedSize += 12 + json.length();
				break;
				
			default:
				break;
			}
			
			i++;
		}
		
		recordTemplate.dataSize = ( recordTemplate.estimatedSize/128 ) * 128 + ((recordTemplate.estimatedSize % 128 > 0) ? 128 : 0);
		
		return recordTemplate;
	}
	
	public void cancel() {
		mCancel = true;
	}
}