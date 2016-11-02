package com.aerospike.pub.dc.keygen;

import org.apache.commons.lang3.RandomUtils;

import com.aerospike.client.Key;

public class RandomKeyGenerator implements KeyGenerator {

	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		return new Key(namespace, set, RandomUtils.nextLong(0, keyLength));
	}
}
