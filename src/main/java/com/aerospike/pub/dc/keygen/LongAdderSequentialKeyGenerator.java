package com.aerospike.pub.dc.keygen;

import java.util.concurrent.atomic.LongAdder;

import com.aerospike.client.Key;

public class LongAdderSequentialKeyGenerator implements KeyGenerator {
	public LongAdder count = new LongAdder();

	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		count.increment();
		return new Key(namespace, set, count.longValue());
	}
}
