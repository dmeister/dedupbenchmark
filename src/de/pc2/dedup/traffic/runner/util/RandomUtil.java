package de.pc2.dedup.traffic.runner.util;

import java.nio.ByteBuffer;
import java.util.Random;

public class RandomUtil {
	
	private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

	public static void nextBytes(Random rng, ByteBuffer buf) {
		for (int i = 0, len = buf.remaining(); i < len;) {
			int rnd = rng.nextInt();
			if (len - i >= INT_SIZE) {
				buf.putInt(rnd);
			} else {
				for (int n = len - i; n-- > 0; rnd >>= Byte.SIZE) {
					buf.put((byte) rnd);
				}
			}
		}
	}
}
