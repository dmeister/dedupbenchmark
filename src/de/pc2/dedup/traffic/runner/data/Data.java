package de.pc2.dedup.traffic.runner.data;

import java.nio.ByteBuffer;
import java.util.Random;

public interface Data {

	public abstract void getBulkData(ByteBuffer buf, int length,
			Random random_state) throws Exception;

}