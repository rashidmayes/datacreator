package com.rashidmayes.pub.dc;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.Key;

public class KeyQueue {

	
	private int mQueueSize;
	
	private Map<String, TreeMap<String, LinkedBlockingQueue<Key>>> mBucket = new TreeMap<String, TreeMap<String, LinkedBlockingQueue<Key>>>();
	
	public KeyQueue(int size) {
		mQueueSize = size;
	}
	
	
	public boolean put(Key key) {
		var namespaceMap = mBucket.get(key.namespace);
		if ( namespaceMap != null ) {
			var keyQueue = namespaceMap.get(key.setName);
			if (keyQueue != null) {
				return keyQueue.add(key);
			}
		}

		return false;
	}

	
	public LinkedBlockingQueue<Key> getQueue(String namespace, String set) {
		var namespaceMap = mBucket.get(namespace);
		if ( namespaceMap == null ) {
			namespaceMap = new TreeMap<String, LinkedBlockingQueue<Key>>();
			mBucket.put(namespace, namespaceMap);
		}
		
		var keyQueue = namespaceMap.get(set);
		if (keyQueue == null) {
			keyQueue = new LinkedBlockingQueue<Key>(mQueueSize);
			namespaceMap.put(set, keyQueue);
		}
		
		return keyQueue;
	}

	public Key next(String namespace, String set, int timeout) throws InterruptedException {
		
		LinkedBlockingQueue<Key> q;
		if ( set == null ) {
			q = mBucket.get(namespace).values().stream().findAny().get();
		} else if ( namespace == null ) {
			q = mBucket.values().stream().findAny().get().values().stream().findAny().get();
		} else {
			q = getQueue(namespace,set);
		}
		
		return q.poll(timeout, TimeUnit.SECONDS);
	}
}