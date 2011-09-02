package de.pc2.dedup.traffic.runner.data;

import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.base.Preconditions;

import de.pc2.dedup.traffic.runner.util.RandomUtil;

public class UniqueData implements Data {
	public void getBulkData(ByteBuffer buf, int length, Random random_state) {
		Preconditions.checkArgument(length > 1);
		Preconditions.checkArgument(buf.remaining() >= length);
		
		RandomUtil.nextBytes(random_state, buf, length);
	}
}
