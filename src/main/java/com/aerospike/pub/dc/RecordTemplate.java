package com.aerospike.pub.dc;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class RecordTemplate {
	Key key;
	Bin[] bins;
	long estimatedSize = 64;
	long dataSize;
}
