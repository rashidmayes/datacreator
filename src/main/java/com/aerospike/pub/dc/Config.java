package com.aerospike.pub.dc;

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.pub.dc.keygen.LongAdderSequentialKeyGenerator;

public class Config {
	
	public enum Type { GEOSPATIAL, STRING, INT, DOUBLE, BLOB, LIST, MAP, IP, IPPORT, UUID, DELIMITED_STRING, BOOLEAN }
	
	public static class Write {
		public String namespace = "test";
		public String set = "csd";
		public int threads = 1;	
		public int preGenerate = 0;
		public int keyLength = 32;
		public long limit = Long.MAX_VALUE;
		public String keyGenerator = LongAdderSequentialKeyGenerator.class.getName();
		public double rateLimit = 0;
		public boolean sameRecordDifferentKey;
		public BinSpec[] binSpecs;
	}
	
	
	public static class BinSpec {
		public Map<String, Bin> bins;
	}
		
	public static class Bin {
		public Type type;
		public long size;
		public Type keyType;
		public int keyLength;
		public Type elementType;
		public int elementLength;
		public String mask;
	}

	private AerospikeClient mClient = null;
	
	public ClientPolicy clientPolicy;
	public String host = "127.0.0.1";
	public String[] hosts = {};
	public int port = 3000;
	
	public long duration = Long.MAX_VALUE;
	public int reportInterval = 2;	
	public String name;
	public Write write;

	public AerospikeClient getClient() {
		if ( mClient == null || !mClient.isConnected() ) {
			mClient = new AerospikeClient(clientPolicy, host, port);
		}
		
		return mClient;
	}
}
