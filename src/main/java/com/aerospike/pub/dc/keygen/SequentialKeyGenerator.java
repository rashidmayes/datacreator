package com.aerospike.pub.dc.keygen;

import com.aerospike.client.Key;

public class SequentialKeyGenerator implements KeyGenerator {
	public long count;

	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		String k = String.valueOf(count++);
		return new Key(namespace, set, k);
	}
}
