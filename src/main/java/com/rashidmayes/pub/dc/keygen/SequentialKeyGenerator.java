package com.rashidmayes.pub.dc.keygen;

import com.aerospike.client.Key;

public class SequentialKeyGenerator implements KeyGenerator {
	private long count;

	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		String k = String.valueOf(count++ % keyLength);
		return new Key(namespace, set, k);
	}
}
