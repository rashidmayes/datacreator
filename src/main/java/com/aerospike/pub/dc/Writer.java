package com.aerospike.pub.dc;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.pub.dc.Config.BinSpec;
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
		Config.Type type;
		int i = 0;
		for (Entry<String, Config.Bin> entry : binSpec.bins.entrySet() ) {
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
				recordTemplate.bins[i] = new Bin(entry.getKey(), RandomStringUtils.randomAlphanumeric((int)binConfig.size));
				recordTemplate.estimatedSize += 5 + binConfig.size;
				break;
			case BLOB:
				recordTemplate.bins[i] = new Bin(entry.getKey(), RandomUtils.nextBytes((int)binConfig.size));
				recordTemplate.estimatedSize += 5 + binConfig.size;
				break;
				
			//list, maps, geo not in original design, needs testing, estimates not accurate
			case LIST: //change to msgpack
				if ( Config.Type.STRING == binConfig.elementType ) {
					ArrayList<String> list = new ArrayList<String>((int)binConfig.size);
					for ( int index = 0; index < binConfig.size ; index++ ) {
						list.add(RandomStringUtils.randomAlphanumeric(binConfig.elementLength));
					}
					recordTemplate.bins[i] = new Bin(entry.getKey(), list);
					recordTemplate.estimatedSize += 10 + (binConfig.size * binConfig.elementLength); //change to msg pack est.
				} else if ( Config.Type.MAP == binConfig.elementType ) {
					
					HashMap<String,String> map;
					ArrayList<HashMap<String,String>> list = new ArrayList<HashMap<String,String>>((int)binConfig.size);
					for ( int index = 0; index < binConfig.size ; index++ ) {
						
						map = new HashMap<String,String>();
						map.put(RandomStringUtils.randomAlphanumeric(binConfig.keyLength), 
								RandomStringUtils.randomAlphanumeric(binConfig.elementLength));
						
						list.add(map);
					}
					recordTemplate.bins[i] = new Bin(entry.getKey(), list);
					recordTemplate.estimatedSize += 10 + (binConfig.size * binConfig.elementLength); //change to msg pack est.
					
					
				} else {
					ArrayList<Long> list = new ArrayList<Long>((int)binConfig.size);
					for ( int index = 0; index < binConfig.size ; index++ ) {
						list.add(RandomUtils.nextLong(0,binConfig.elementLength));
					}
					recordTemplate.bins[i] = new Bin(entry.getKey(), list);
					recordTemplate.estimatedSize += 10 + (binConfig.size * 8); //change to msg pack est.					
				}
				break;
			case MAP: //change to msgpack
				
				if ( binConfig.keyType == Config.Type.STRING ) {
					Map<Value,Value> map = new HashMap<Value,Value>();
					if ( binConfig.elementType == Config.Type.STRING ) {

						for ( int kp = 0; kp < binConfig.size; kp++ ) {
							map.put(Value.get(RandomStringUtils.randomAlphanumeric(binConfig.keyLength)), 
									Value.get(RandomStringUtils.randomAlphanumeric(binConfig.elementLength)));
						}
						recordTemplate.bins[i] = new Bin(entry.getKey(), map);
						recordTemplate.estimatedSize += 2 + (binConfig.keyLength + binConfig.elementLength) * binConfig.size;
					} else {

						for ( int kp = 0; kp < binConfig.size; kp++ ) {
							map.put(Value.get(RandomStringUtils.randomAlphanumeric(binConfig.keyLength)), 
									Value.get(RandomUtils.nextLong(0, binConfig.elementLength)));
						}
						recordTemplate.bins[i] = new Bin(entry.getKey(), map);
						recordTemplate.estimatedSize += 2 + (binConfig.keyLength + 8) * binConfig.size;
					}
				} else {
					Map<Value,Value> map = new HashMap<Value,Value>();
					if ( binConfig.elementType == Config.Type.STRING ) {

						for ( int kp = 0; kp < binConfig.size; kp++ ) {
							map.put(Value.get(RandomUtils.nextLong(0,binConfig.keyLength)), 
									Value.get(RandomStringUtils.randomAlphanumeric(binConfig.elementLength)));
						}
						recordTemplate.bins[i] = new Bin(entry.getKey(), map);
						recordTemplate.estimatedSize += 2 + (binConfig.keyLength + binConfig.elementLength) * binConfig.size;
					} else {

						for ( int kp = 0; kp < binConfig.size; kp++ ) {
							map.put(Value.get(RandomUtils.nextLong(0,binConfig.keyLength)), 
									Value.get(RandomUtils.nextLong(0, binConfig.elementLength)));
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
			case IP:
				long size = binConfig.size;
				if ( size < 0 ) {
					size = 4294967295L;
				}
				long numericIp = RandomUtils.nextLong(0, Math.min(size,4294967295L));
			    String ip = String.format("%d.%d.%d.%d", (numericIp >> 24) & 0xFF, (numericIp >> 16) & 0xFF, (numericIp >> 8) & 0xFF, numericIp & 0xFF );   
				
				recordTemplate.bins[i] = new Bin(entry.getKey(), ip);
				recordTemplate.estimatedSize += 5 + ip.length();
				break;
				
			case IPPORT:
				size = binConfig.size;
				if ( size < 0 ) {
					size = 4294967295L;
				}
				numericIp = RandomUtils.nextLong(0, Math.min(size,4294967295L));
			    ip = String.format("%d.%d.%d.%d:%d", (numericIp >> 24) & 0xFF, (numericIp >> 16) & 0xFF, (numericIp >> 8) & 0xFF, numericIp & 0xFF, RandomUtils.nextInt(0, 10) );   
				
				recordTemplate.bins[i] = new Bin(entry.getKey(), ip);
				recordTemplate.estimatedSize += 5 + ip.length();
				break;	

			case UUID:

			    String uid = UUID.randomUUID().toString();   
				
				recordTemplate.bins[i] = new Bin(entry.getKey(), uid);
				recordTemplate.estimatedSize += 5 + uid.length();
				break;
				
			case DELIMITED_STRING:
				String mask = ( binConfig.mask == null ) ? ",%s" : binConfig.mask;
				
			    StringBuilder buffer = new StringBuilder();
				
				if ( Config.Type.STRING == binConfig.elementType ) {
					for ( int index = 0; index < binConfig.size ; index++ ) {
						buffer.append(String.format(mask, RandomStringUtils.randomAlphabetic(binConfig.elementLength).toLowerCase()));
					}
				} else {
					for ( int index = 0; index < binConfig.size ; index++ ) {
						buffer.append(String.format(mask, RandomUtils.nextLong(0,binConfig.elementLength)));
					}
				}
				
				String data = buffer.substring(1);
				recordTemplate.bins[i] = new Bin(entry.getKey(), data);
				recordTemplate.estimatedSize += 5 + data.length();
				
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