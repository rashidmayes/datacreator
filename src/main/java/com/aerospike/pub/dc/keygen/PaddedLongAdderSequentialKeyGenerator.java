package com.aerospike.pub.dc.keygen;

import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.StringUtils;

import com.aerospike.client.Key;

public class PaddedLongAdderSequentialKeyGenerator implements KeyGenerator {
	public LongAdder count = new LongAdder();

	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		count.increment();
		String k = String.valueOf(count.longValue());
		StringUtils.leftPad(k, keyLength, '0');
		return new Key(namespace, set, k);
	}
}
