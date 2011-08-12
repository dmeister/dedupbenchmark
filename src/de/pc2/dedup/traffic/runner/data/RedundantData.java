package de.pc2.dedup.traffic.runner.data;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.uncommons.maths.random.ExponentialGenerator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class RedundantData implements Data {
	private final List<ByteBuffer> buffers;

	public RedundantData(int bufferedBlocks, int bufferedBlockSizes,
			Random initial_random) {

		buffers = Lists.newArrayList();

		for (int i = 0; i < bufferedBlocks; i++) {
			ByteBuffer buf = ByteBuffer.allocate(bufferedBlockSizes);
			generateUniqueBuffer(buf, bufferedBlockSizes, initial_random);
			buf.flip();
			buffers.add(buf);
		}
	}

	private void generateUniqueBuffer(ByteBuffer buf, int length,
			Random random_state) {
		Preconditions.checkArgument(length > 1);
		byte[] ba = new byte[length];
		random_state.nextBytes(ba);

		buf.put(ba, 0, length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.pc2.dedup.traffic.runner.data.Data#getBulkData(java.nio.ByteBuffer,
	 * int, java.util.Random)
	 */
	public synchronized void getBulkData(ByteBuffer buf, int length,
			Random random_state) {

		ExponentialGenerator expGenerator = new ExponentialGenerator(
				1.0 / (buffers.size() / 8), random_state);
		while (length > 0) {
			int i;
			do {
				double d = expGenerator.nextValue();
				i = (int) d;
			} while (i >= buffers.size());

			ByteBuffer src = buffers.get(i).slice();

			int l = length;
			if (src.remaining() < l) {
				l = src.remaining();
			}
			buf.put(src.array(), src.arrayOffset(), l);
			length -= l;
			src.flip();
		}
	}
}
