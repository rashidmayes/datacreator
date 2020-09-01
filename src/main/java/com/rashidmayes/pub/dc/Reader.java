package com.rashidmayes.pub.dc;

import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.RateLimiter;
import com.rashidmayes.pub.dc.Config.Read;
import com.rashidmayes.pub.dc.keygen.KeyGenerator;

public class Reader implements Runnable {

	private Config mConfig;
	private Read mRead;
	private boolean mCancel;
	private KeyGenerator mKeyGenerator; 
	private Semaphore mCompleted;
	private AerospikeClient mClient;
	private RateLimiter mRateLimiter;
	private Listener mListener;

	private Timer mReadTimes;
	
	private Logger mLogger = Logger.getLogger(getClass().getSimpleName());
	
	public Reader(KeyGenerator keyGenerator, Config config, Read read, Listener listener, Semaphore completed) {
		this.mConfig = config;
		this.mRead = read;
		this.mKeyGenerator = keyGenerator;
		this.mCompleted = completed;
		this.mClient = config.getClient();
		this.mListener = listener;
		
		if ( mRead.rateLimit > 0 ) {
			this.mRateLimiter = RateLimiter.create(mRead.rateLimit);
		}
	}
	
	public void run() {

		try {
			String id = String.format("%s.%s", Thread.currentThread().getName(),mRead.id);
			

			mReadTimes = new Timer(new com.codahale.metrics.UniformReservoir());
			GenericDataCreator.metrics.register(id  + ".reads", mReadTimes);

			Key key;
			Record record;
			while (!mCancel && mRead.counter.longValue() < mRead.limit) {
				mRead.counter.increment();
				
				
				if ( mRateLimiter != null ) {
					 GenericDataCreator.WAIT.inc((long)mRateLimiter.acquire());
				}
				
				key = mKeyGenerator.generate(mConfig.name, mRead.keyLength, mRead.namespace, mRead.set);
				
				try (Context context = GenericDataCreator.READ_RECORD_TIMES.time()) {					
					try (Context c = mReadTimes.time()) {					
						record = mClient.get(null, key);
					}
					
					if ( record == null ) {
						
					} else {
						GenericDataCreator.READ_HITS.inc();
						
						mListener.recordRead(key, record);
						if ( mRead.recycleKeys ) {
							mListener.recycle(key);
						}
						if ( record.bins != null ) {
							GenericDataCreator.NETWORK_READ.inc(
									record.bins.values().stream().mapToInt((v) -> Value.get(v).estimateSize()).sum());
						}
					}
					
				} catch (AerospikeException ae) {
					mLogger.log(Level.ALL, ae.getMessage(), ae);
					if ( ae.getResultCode() != 8 ) {
						break;
					}
				}
			}
			
		} catch (Exception e) {
			mLogger.log(Level.ALL, e.getMessage(), e);
		} finally {
			mCompleted.release();
		}
	}
	
	

	public void cancel() {
		mCancel = true;
	}
}