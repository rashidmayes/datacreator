package com.aerospike.pub.dc.keygen;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.StringUtils;

import com.aerospike.client.Key;

import gnu.crypto.util.Base64;

public class PrefixedSequentialKeyGenerator implements KeyGenerator {
	
	public static final String format = "%s-%s";
	
	String prefix;
	
	public PrefixedSequentialKeyGenerator() {
		
		try {
			NetworkInterface n;
			n = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
			for ( Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();  ) {
				n = en.nextElement();
				if ( n.getHardwareAddress() != null ) {
					
					prefix = String.format("%s-%s",
								Base64.encode(n.getHardwareAddress())
								,Long.toString(System.currentTimeMillis(),Character.MAX_RADIX)
							).toUpperCase();
					break;
				}
				
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if ( prefix == null ) {
			prefix = String.format("%s-%s",
					Long.toString(UUID.randomUUID().toString().hashCode(), Character.MAX_RADIX)
					 ,Long.toString(System.currentTimeMillis(),Character.MAX_RADIX)
					).toUpperCase();
		}
	}
	
	public LongAdder count = new LongAdder();

	@Override
	public Key generate(String name, int keyLength, String namespace, String set) {
		count.increment();
		String k = StringUtils.leftPad(String.valueOf(count.longValue())
				, keyLength - prefix.length(), '0');

		return new Key(namespace, set, String.format(format, prefix, k));
	}
}
