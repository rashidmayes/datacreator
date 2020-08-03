package com.rashidmayes.pub.dc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public enum DataType {
	
	INT (null){
		public Bin create(String name, Config.Bin binConfig) {
			Bin bin = new Bin(name, RandomUtils.nextLong(0, binConfig.size));
			return bin;
		}
	},
	
	DOUBLE (null){
		public Bin create(String name, Config.Bin binConfig) {
			Bin bin = new Bin(name, RandomUtils.nextDouble(0, binConfig.size));
			return bin;
		}
	},
	
	
	STRING (null){
		public Bin create(String name, Config.Bin binConfig) {
			Bin bin = new Bin(name, RandomStringUtils.randomAlphanumeric((int)binConfig.size));
			return bin;
		}
	},
	
	BLOB (null){
		public Bin create(String name, Config.Bin binConfig) {
			Bin bin = new Bin(name, RandomUtils.nextBytes((int)binConfig.size));
			return bin;
		}
	},
	
	LIST (null){
		public Bin create(String name, Config.Bin binConfig) {
			
			if ( STRING == binConfig.elementType ) {
				ArrayList<String> list = new ArrayList<String>((int)binConfig.size);
				for ( int index = 0; index < binConfig.size ; index++ ) {
					list.add(RandomStringUtils.randomAlphanumeric(binConfig.elementLength));
				}
				return new Bin(name, list);

			} else if ( MAP == binConfig.elementType ) {
				
				HashMap<String,String> map;
				ArrayList<HashMap<String,String>> list = new ArrayList<HashMap<String,String>>((int)binConfig.size);
				for ( int index = 0; index < binConfig.size ; index++ ) {
					
					map = new HashMap<String,String>();
					map.put(RandomStringUtils.randomAlphanumeric(binConfig.keyLength), 
							RandomStringUtils.randomAlphanumeric(binConfig.elementLength));
					
					list.add(map);
				}
				return new Bin(name, list);
				
				
			} else {
				ArrayList<Long> list = new ArrayList<Long>((int)binConfig.size);
				for ( int index = 0; index < binConfig.size ; index++ ) {
					list.add(RandomUtils.nextLong(0,binConfig.elementLength));
				}
				return new Bin(name, list);
				
			}
		}
	},
	
	MAP (null){
		public Bin create(String name, Config.Bin binConfig) {

			if ( binConfig.keyType == STRING ) {
				Map<Value,Value> map = new HashMap<Value,Value>();
				if ( binConfig.elementType == STRING ) {

					for ( int kp = 0; kp < binConfig.size; kp++ ) {
						map.put(Value.get(RandomStringUtils.randomAlphabetic(binConfig.keyLength)), 
								Value.get(RandomStringUtils.randomAlphanumeric(binConfig.elementLength)));
					}
					return new Bin(name, map);
				} else {

					for ( int kp = 0; kp < binConfig.size; kp++ ) {
						map.put(Value.get(RandomStringUtils.randomAlphabetic(binConfig.keyLength)), 
								Value.get(RandomUtils.nextLong(0, binConfig.elementLength)));
					}
					return new Bin(name, map);
				}
			} else {
				Map<Value,Value> map = new HashMap<Value,Value>();
				if ( binConfig.elementType == STRING ) {

					for ( int kp = 0; kp < binConfig.size; kp++ ) {
						map.put(Value.get(String.valueOf(kp)), 
								Value.get(RandomStringUtils.randomAlphanumeric(binConfig.elementLength)));
					}
					return new Bin(name, map);
				} else {

					for ( int kp = 0; kp < binConfig.size; kp++ ) {
						map.put(Value.get(String.valueOf(kp)), 
								Value.get(RandomUtils.nextLong(0, binConfig.elementLength)));
					}
					return new Bin(name, map);
				}
			}			
		}
	},
	
	GEOSPATIAL (null){
		
		public Bin create(String name, Config.Bin binConfig) {
			
			JsonObject locobj = new JsonObject();
			locobj.addProperty("type", "Point");
			JsonArray coords = new JsonArray();

			coords.add(RandomUtils.nextDouble(0, binConfig.size) - RandomUtils.nextDouble(0, binConfig.size));
			coords.add(RandomUtils.nextDouble(0, binConfig.size) - RandomUtils.nextDouble(0, binConfig.size));
			locobj.add("coordinates", coords);
			
			String json = gson.toJson(locobj);
			
			return Bin.asGeoJSON(name, json);	
		}
	},
	
	IP (null){
		public Bin create(String name, Config.Bin binConfig) {
			
			long size = binConfig.size;
			if ( size < 0 ) {
				size = 4294967295L;
			}
			long numericIp = RandomUtils.nextLong(0, Math.min(size,4294967295L));
		    String ip = String.format("%d.%d.%d.%d", (numericIp >> 24) & 0xFF, (numericIp >> 16) & 0xFF, (numericIp >> 8) & 0xFF, numericIp & 0xFF );   
			
			return new Bin(name, ip);			
		}
	},
	
	IPPORT (null){
		public Bin create(String name, Config.Bin binConfig) {

			long size = binConfig.size;
			if ( size < 0 ) {
				size = 4294967295L;
			}
			long numericIp = RandomUtils.nextLong(0, Math.min(size,4294967295L));
		    String ip = String.format("%d.%d.%d.%d:%d", (numericIp >> 24) & 0xFF, (numericIp >> 16) & 0xFF, (numericIp >> 8) & 0xFF, numericIp & 0xFF, RandomUtils.nextInt(0, 10) );   
			
		    return new Bin(name, ip);		
		}
	},
	
	
	UUID (null){
		public Bin create(String name, Config.Bin binConfig) {
		    String uid = java.util.UUID.randomUUID().toString();   
			
		    return new Bin(name, uid);		
		}
	},
	
	
	DELIMITED_STRING (null){
		public Bin create(String name, Config.Bin binConfig) {
			String mask = ( binConfig.mask == null ) ? ",%s" : binConfig.mask;
			
		    StringBuilder buffer = new StringBuilder();
			
			if ( STRING == binConfig.elementType ) {
				for ( int index = 0; index < binConfig.size ; index++ ) {
					buffer.append(String.format(mask, RandomStringUtils.randomAlphabetic(binConfig.elementLength).toLowerCase()));
				}
			} else {
				for ( int index = 0; index < binConfig.size ; index++ ) {
					buffer.append(String.format(mask, RandomUtils.nextLong(0,binConfig.elementLength)));
				}
			}
			
			String data = buffer.substring(1);
			return new Bin(name, data);	
		}
	},
	
	
	US_FIRST_NAME ("/data/firstnames.txt"){
		public Bin create(String name, Config.Bin binConfig) {
			Bin bin = new Bin(name, getRandomDictionaryValue());
			return bin;
		}
	},
	
	US_LAST_NAME ("/data/lastnames.txt") {
		public Bin create(String name, Config.Bin binConfig) {
			Bin bin = new Bin(name, getRandomDictionaryValue());
			return bin;
		}
	},
	
	US_STATE ("/data/states.txt") {
		public Bin create(String name, Config.Bin binConfig) {
			var state = getRandomDictionaryValue();
			if ( binConfig.size > 2 ) {
				return new Bin(name, state.substring(2));
			} else {
				return new Bin(name, state.substring(0,2));
			}
		}
	},
	
	
	US_CITY ("/data/cities.txt") {
		public Bin create(String name, Config.Bin binConfig) {
			Bin bin = new Bin(name, getRandomDictionaryValue());
			return bin;
		}
	},
	
	US_ZIP (null) {
		public Bin create(String name, Config.Bin binConfig) {
			Bin bin = new Bin(name, RandomUtils.nextInt(10000,100000));
			return bin;
		}
	},
	
	US_PHONE_NUMBER (null) {
		public Bin create(String name, Config.Bin binConfig) {
			
			if ( binConfig.size > 10 ) {
				return new Bin(name,
						String.format("+1 (%d) %d-%d", 
								RandomUtils.nextInt(100,1000),
								RandomUtils.nextInt(100,1000),
								RandomUtils.nextInt(1000,10000)
				));
			} else {
				return new Bin(name,
						String.format("(%d) %d-%d", 
								RandomUtils.nextInt(100,1000),
								RandomUtils.nextInt(100,1000),
								RandomUtils.nextInt(1000,10000)
				));
			}
		}
	},
	
	
	US_STREET ("/data/streets.txt") {
		String[] suffixes = { "Way", "LN", "ST", "Ave", "Cir", "PL" };
		public Bin create(String name, Config.Bin binConfig) {
			
			Bin bin = new Bin(name,
			String.format("%d %s %s", RandomUtils.nextLong(1,binConfig.size),getRandomDictionaryValue(), suffixes[ RandomUtils.nextInt(0,suffixes.length) ])
			);
			return bin;
		}
	};
	
	
	private static final Gson gson = new Gson();
	private String[] dictionary;
	
	private DataType(String dictionaryPath) {
		if ( dictionaryPath != null ) {
			try {
				var lines = Files.readAllLines(Path.of(DataType.class.getResource(dictionaryPath).getPath()));
				dictionary = lines.toArray(new String[lines.size()]);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	
	public String getRandomDictionaryValue() {
		if (dictionary == null) {
			return null;
		} else {
			 return dictionary[RandomUtils.nextInt(0, dictionary.length)];
		}
	}
	
	public abstract Bin create(String name, Config.Bin config);

	
}
