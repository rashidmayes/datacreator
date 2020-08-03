#Generic Data Creator

Creates records in Aerospike according to the specified JSON 


##Example

java -jar DataCreator-1.0.0-jar-with-dependencies.jar ../sample.json

Config JSON sample.json

```json

{
	"host" : "127.0.0.1",
	"port" : 3000,
	
	"clientPolicy" : {
	  	"writePolicyDefault" : {
	  		"commitLevel" : "COMMIT_ALL"
	  	}
	},	
	
	"reportInterval": 5,
	"duration" : 60,

	"write" : {
		  "sameRecordDifferentKey" : false,
		  "rateLimit" : 0,
		  "namespace" : "test",
		  "set" : "myset",
		  "threads" : 1,
		  "preGenerate" : 10,
		  "limit" : 1,	  
		  "keyGenerator" : "com.aerospike.pub.dc.keygen.LongAdderSequentialKeyGenerator",
		  "keyLength" : 15,
		  "useOperations": true,
		  
		  "binSpecs" : [
		 		{
		 		  "bins" : {
		 		  
		 		  	"string" : { "type" : "STRING", "size" : 8 },
				  	"blob" : { "type" : "BLOB", "size" : 15 },
				  	"geo" : { "type" : "GEOSPATIAL", "size" : 10 },
				  	"ip" : { "type" : "IP", "size" : 20 },
				  	"ipport" : { "type" : "IPPORT", "size" : 100 },
				  	"uuid" : { "type" : "UUID", "size" : 300 },
				  	"delemited" : { 
				  		"type" : "DELIMITED_STRING", 
				  		"size" : 5,
				  		"elementType" : "STRING",
				  		"elementLength" : 3,
				  		"mask" : "|%s"
				  	},		 		  
		 		  
		 		  
		 		  	"list" : { 
		 		  		"type" : "LIST", 
		 		  		"size" : 5,
		 		  		"keyLength": 10,
		 		  		"elementType" : "INT",
		 		  		"elementLength": 10
		 		 	},
		 		  
		 		  	"listofmaps" : { 
		 		  		"type" : "LIST", 
		 		  		"size" : 5,
		 		  		"keyLength": 10,
		 		  		"elementType" : "MAP",
		 		  		"elementLength": 10
		 		 	},		 		  
		 		  
		 		  
		 		  	"map" : { 
		 		  		"type" : "MAP", 
		 		  		"size" : 5, 
		 		  		"keyType": "STRING",
		 		  		"keyLength": 5,
		 		  		"elementType" : "INT",
		 		  		"elementLength": 5
		 		 	}
				  }
				} 
		  ] 
		 
	}
}
```

Outputs:

```
records.saved.times
             count = 10
         mean rate = 577.49 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.00 calls/second
    15-minute rate = 0.00 calls/second
               min = 0.23 milliseconds
               max = 2.37 milliseconds
              mean = 0.47 milliseconds
            stddev = 0.63 milliseconds
            median = 0.26 milliseconds
              75% <= 0.29 milliseconds
              95% <= 2.37 milliseconds
              98% <= 2.37 milliseconds
              99% <= 2.37 milliseconds
            99.9% <= 2.37 milliseconds


********** Complete **********
Duration: 0d 0h 0m 0s
Transfered: 1.93K (1980)
Stored: 2.5K (2560)
Count: 10
Avg. Latency: 0.473 ms
Min Latency: 0.231 ms
Max Latency: 2.375 ms
Records/Second: 860.062

```
And creates:

```
aql> set output json
OUTPUT = JSON
aql> select * from test.datatypes

[
    [
        {
          "string": "FBO1c7eI",
          "blob": "CA F2 B9 E7 38 AE 72 0B 9A D9 B4 90 3A DD 55",
          "geo": "\"{\"type\":\"Point\",\"coordinates\":[-4.758521540930496,-7.720478886124124]}\"",
          "ip": "0.0.0.6",
          "ipport": "0.0.0.63:9",
          "uuid": "949b54ff-8e76-412b-af41-a2b9b9194fa8",
          "delemited": "iap|cyu|lni|swt|eyc",
          "list": [
            1,
            5,
            9,
            9,
            4
          ],
          "listofmaps": [
            {
              "YNU2bGBXXx": "bDfpM0ON0W"
            },
            {
              "QMM8yL08Sz": "NGCHLWIQK3"
            },
            {
              "pWmdX6dxuE": "ZI5Tac2IwP"
            },
            {
              "ctIL8AYQzF": "eZuEOvkTLW"
            },
            {
              "cLlFo7Rs2d": "LZ87TKhxMM"
            }
          ],
          "map": {
            "vzGbB": 1,
            "n2HQq": 4,
            "TTdos": 1,
            "kWu00": 1,
            "XDfWx": 1
          }
        }
    ],
    [
        {
          "Status": 0
        }
    ]
]

```

Please see the examples folder for more sample configurations.

##Key Generators


Key Generators generate the keys which are used for each record. 

| Name | Description | Examples |
|------|-------------|----------|
| FixedKeyGenerator | Single static key | 1 |
| LongAdderSequentialKeyGenerator | Thread safe sequential numeric key | 0 1 2 3 4 5..n |
| PaddedLongAdderSequentialKeyGenerator | Sequential numeric key with padding | 000 001 002 003..099..999 |
| PrefixedSequentialKeyGenerator | Base 64 hardware address of the network adapter prepended to the system time followed by the padded key counter value | xf-az-0001 |
| RandomKeyGenerator | Random number between 0 and key length | 73 |
| RandomStringKeyGenerator | Random alpha-numeric string of key length size| sdf3nfg56df4liu333 |
| SequentialKeyGenerator | non-thread safe numeric key | 1 2 3 4 5..n |




