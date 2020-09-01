package com.rashidmayes.pub.dc;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

public interface Listener {
	public void recordWritten(Key key);
	public void recordRead(Key key, Record record);
	public void recycle(Key key);
	public void recordDeleted(Key key);
}
