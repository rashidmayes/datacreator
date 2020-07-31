package com.rashidmayes.pub.dc.keygen;

import org.apache.commons.lang3.RandomStringUtils;

import com.aerospike.client.Key;

public class RandomStringKeyGenerator implements KeyGenerator {

	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		return new Key(namespace, set, RandomStringUtils.randomAlphanumeric(keyLength));
	}

}
