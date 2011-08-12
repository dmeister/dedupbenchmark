package de.pc2.dedup.traffic.runner.data;

import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.base.Preconditions;

public class UniqueData implements Data {
	public void getBulkData(ByteBuffer buf, int length, Random random_state) {
		Preconditions.checkArgument(length > 1);
		byte[] ba = new byte[length];
		random_state.nextBytes(ba);

		buf.put(ba, 0, length);
	}
}
