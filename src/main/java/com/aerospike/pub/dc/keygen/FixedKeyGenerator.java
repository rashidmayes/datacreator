package com.aerospike.pub.dc.keygen;

import com.aerospike.client.Key;

public class FixedKeyGenerator implements KeyGenerator {

	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		
		return new Key(namespace, set, keyLength);
	}
}
