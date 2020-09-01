package com.rashidmayes.pub.dc.keygen;

import java.util.logging.Logger;

import com.aerospike.client.Key;
import com.rashidmayes.pub.dc.KeyQueue;

public class PipedKeyGenerator implements KeyGenerator {

	private static final Logger mLogger = Logger.getLogger(PipedKeyGenerator.class.getSimpleName());
	private static KeyQueue KeyQueue;
	
	public static void setKeyQueue(KeyQueue keyQueue) {
		KeyQueue = keyQueue;
	}
	
	public static KeyQueue getKeyQueue() {
		return KeyQueue;
	}
	
	
	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		try {
			return KeyQueue.next(namespace, set, keyLength);
		} catch (Exception e) {
			mLogger.info(e.getMessage());
			return null;
		}
	}
}
