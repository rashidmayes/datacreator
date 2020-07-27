package com.aerospike.pub.dc;

import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;

public class RecordTemplate {
	Key key;
	Bin[] bins;
	long estimatedSize = 64;
	long dataSize;
	
	private Operation[] ops = null;
	@SuppressWarnings("unchecked")
	protected Operation[] getOps(MapPolicy mapPolicy) {
		if ( ops == null ) {
			ops = new Operation[bins.length];
			int index = 0;
			for ( Bin bin : bins ) {

				if ( bin.value.getType() == 19 ) {
					ops[index++] = (MapOperation.putItems(mapPolicy, bin.name, (Map<Value, Value>)(bin.value.getObject())));
				} else {
					ops[index++] = (Operation.put(bin));
				}
			}
		}
		return ops;
	}
}
