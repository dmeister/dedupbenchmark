package de.pc2.dedup.traffic.runner.dist;

import java.util.Random;

public interface Distribution {
	public long getNext(Random random);
}
