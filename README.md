#Generic Data Creator

Creates records in Aerospike according to the specified JSON 


##Example

java -jar DataCreator-1.0.0-jar-with-dependencies.jar ../sample.json

Config JSON sample.json

``json

{
	"host" : "127.0.0.1",
	"port" : 3000,
	"duration" : 1,

	"write" : {
		  "namespace" : "test",
		  "set" : "sample",
		  "threads" : 1,
		  "limit" : 10,
		  
		  "bins" :  {
		  	"firstname" : { "type" : "STRING", "size" : 5 },
		  	"lastname" : { "type" : "STRING", "size" : 10 },
		  	"age" : { "type" : "INT", "size" : 84 }
		  }  
	}
}

``

Outputs:

``
-- Counters --------------------------------------------------------------------
records.network
             count = 1980
records.pregenerated
             count = 0
records.storage
             count = 2560
records.waittime
             count = 0

-- Timers ----------------------------------------------------------------------
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

``
And creates:

``
aql> select * From test.sample
+-----------+--------------+-----+
| firstname | lastname     | age |
+-----------+--------------+-----+
| "2BmDz"   | "RTbUTWsz55" | 45  |
| "jpXg4"   | "JARb16mO68" | 60  |
| "jiTw0"   | "Qjax1RvAf3" | 7   |
| "u0wLI"   | "BMENi3T4ej" | 33  |
| "Wy6aF"   | "hHgMZgdFID" | 36  |
| "KyBKy"   | "B7nHCMIADn" | 41  |
| "hdnYr"   | "KHDjuEfVxC" | 34  |
| "6WBht"   | "Bb1gxlioMA" | 72  |
| "XFWgM"   | "FRP0yrYe1T" | 41  |
| "UMXRd"   | "B0Pm8HQmF8" | 73  |
+-----------+--------------+-----+
10 rows in set (0.068 secs)
``