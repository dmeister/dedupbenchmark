package de.pc2.dedup.traffic.runner.dist;

import java.util.Random;

public class BlockAlignedDistribtion implements Distribution {
	private final int blockSize;
	private final Distribution rootDistribution;

	public BlockAlignedDistribtion(int blockSize, Distribution rootDistribution) {
		this.blockSize = blockSize;
		this.rootDistribution = rootDistribution;
	}

	public long getNext(Random random) {
		long bl = rootDistribution.getNext(random);
		if (bl % blockSize == 0) {
			return bl;
		}
		return ((bl / blockSize) + 1) * blockSize;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("BlockAlignedDistribtion [blockSize=").append(blockSize)
				.append(", rootDistribution=").append(rootDistribution)
				.append("]");
		return builder.toString();
	}

}
