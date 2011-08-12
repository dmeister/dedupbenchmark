package de.pc2.dedup.traffic.runner;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.log4j.Logger;
import org.uncommons.maths.random.DevRandomSeedGenerator;
import org.uncommons.maths.random.SeedGenerator;

import com.google.common.base.Preconditions;

import de.pc2.dedup.traffic.runner.data.Data;
import de.pc2.dedup.traffic.runner.data.RedundantData;
import de.pc2.dedup.traffic.runner.data.UniqueData;
import de.pc2.dedup.traffic.runner.dist.Distribution;
import de.pc2.dedup.traffic.runner.random.XORShiftRandomGenerator;

public class FirstGenerationTraffic extends Traffic {

	class OnlineGeneratedTrafficSession implements TrafficSession {
		class GenerateJob implements Callable<Boolean> {
			private final int index;

			GenerateJob(int index) {
				this.index = index;
			}

			public Boolean call() throws Exception {
				return generateData(index, index + getLoadBlocks());
			}

			@Override
			public String toString() {
				StringBuilder builder = new StringBuilder();
				builder.append("GenerateJob [index=").append(index).append("]");
				return builder.toString();
			}
		}

		private final int endIndex;
		private final ExecutorService executor = Executors
				.newFixedThreadPool(1);
		private XORShiftRandomGenerator redundantRandom;
		private XORShiftRandomGenerator redundantRandomData;
		private final SeedGenerator sg = new DevRandomSeedGenerator();
		private final int startIndex;
		private XORShiftRandomGenerator uniqueRandom;

		private XORShiftRandomGenerator uniqueRandomData;

		long assignedSize;
		Type type;

		Type switchType() {
			if (type == Type.REDUNDANT) {
				type = Type.UNIQUE;
			} else if (type == Type.UNIQUE) {
				type = Type.REDUNDANT;
			} else {
				throw new IllegalStateException("type");
			}
			return type;
		}

		OnlineGeneratedTrafficSession(int startIndex, int endIndex)
				throws Exception {
			this.startIndex = startIndex;
			this.endIndex = endIndex;
			uniqueRandom = new XORShiftRandomGenerator(sg);
			uniqueRandomData = new XORShiftRandomGenerator(sg);
			redundantRandom = new XORShiftRandomGenerator(sg);
			redundantRandomData = new XORShiftRandomGenerator(sg);

			// init first block
			type = Type.UNIQUE;
			assignedSize = 0;
		}

		public void clearBuffer(int i) {
			if (i >= blocks.length()) {
				return;
			}
			logger.debug("Clear block " + i);

			ByteBuffer b = blocks.get(i);
			if (b != null) {
				b.flip();
				bufferQueue.offer(b);
			}
			blocks.set(i, null);
		}

		public void close() {
			this.executor.shutdown();
		}

		private boolean generateData(int si, int ei) throws Exception {
			for (int i = si; i < ei; i++) {
				if (i >= endIndex) {
					break;
				}
				logger.debug("Generating block " + i);

				ByteBuffer block = bufferQueue.poll();
				if (block == null) {
					block = ByteBuffer.allocate((int) BLOCK_SIZE);
				}

				// generate
				if (assignedSize > 0) {
					if (type == Type.UNIQUE) {
						long bulkLenght = assignedSize;
						if (bulkLenght > block.remaining()) {
							assignedSize = (bulkLenght - block.remaining());
							bulkLenght = block.remaining();
						}
						if (bulkLenght > 0) {
							uniqueDataGenerator.getBulkData(block, (int) bulkLenght,
									uniqueRandomData);
						}
					} else if (type == Type.REDUNDANT) {
						long bulkLenght = assignedSize;
						if (bulkLenght > block.remaining()) {
							assignedSize = (bulkLenght - block.remaining());
							bulkLenght = block.remaining();
						}
						if (bulkLenght > 0) {
							// Generate real data later
							redundantDataGenerator.getBulkData(block,
									(int) bulkLenght, redundantRandomData);
						}
					}
					assignedSize = 0;
				}
				while (block.remaining() > 0) {
					Type nextType = switchType();
					if (nextType == Type.UNIQUE) {
						long bulkLenght = uniqueDistribution
								.getNext(uniqueRandom);
						Preconditions.checkState(bulkLenght > 0);
						if (bulkLenght > block.remaining()) {

							assignedSize = (bulkLenght - block.remaining());

							bulkLenght = block.remaining();
						}
						if (bulkLenght > 0) {
							uniqueDataGenerator.getBulkData(block, (int) bulkLenght,
									uniqueRandomData);
						}
					} else if (type == Type.REDUNDANT) {
						long bulkLenght = redundantDistribution
								.getNext(redundantRandom);
						if (bulkLenght > block.remaining()) {
							assignedSize = (bulkLenght - block.remaining());
							bulkLenght = block.remaining();
						}
						if (bulkLenght > 0) {
							redundantDataGenerator.getBulkData(block,
									(int) bulkLenght, redundantRandomData);
						}
					}
				}
				// finished generate that block

				block.flip();
				blocks.set(i, block);
				logger.debug("Generated block " + i);
			}
			return true;

		}

		public int getBlockCount() {
			return blocks.length();
		}

		public ByteBuffer getBuffer(int i) throws Exception {

			if (directGeneration) {
				generateData(i, i + 1);
				ByteBuffer b = blocks.get(i);
				Preconditions.checkState(b != null);
				return b;
			}
			logger.debug("Get buffer " + i);

			ByteBuffer b = blocks.get(i);
			if (b == null) {
				Future<Boolean> f = block_state.get(i);
				if (f == null) {
					f = loadBuffer(i);
					f.get();
					logger.warn("Waiting for block " + i);
				} else if (f.isDone() == false) {
					f.get();
					logger.warn("Waiting for block " + i);
				}
				b = blocks.get(i);
			}
			if (i % getLoadBlocks() == 0) {
				int preloadIndex = i + (getLoadBlocks() * getPreloadWindow());
				if (preloadIndex < endIndex) {
					logger.debug(String.format(
							"Preloading block %s at block %s", preloadIndex, i));

					Future<Boolean> f2 = executor.submit(new GenerateJob(
							preloadIndex));
					block_state.set(preloadIndex, f2);
				}
			}
			return b;
		}

		public synchronized Future<Boolean> loadBuffer(int i) throws Exception {
			if (i >= blocks.length() && directGeneration) {
				return null;
			}

			logger.debug("Load buffer " + i);

			Future<Boolean> f = block_state.get(i);
			if (f != null) {
				return f;
			}
			f = executor.submit(new GenerateJob(i));

			for (int j = i; j < i + getLoadBlocks(); j++) {
				if (j >= blocks.length()) {
					break;
				}
				block_state.set(j, f);
			}
			return f;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("OnlineGeneratedTrafficSession [startIndex=")
					.append(startIndex).append(", endIndex=").append(endIndex)
					.append(", executor=").append(executor).append("]");
			return builder.toString();
		}

	}

	enum Type {
		REDUNDANT, UNIQUE
	}

	private static final Logger logger = Logger
			.getLogger(FirstGenerationTraffic.class);

	private final AtomicReferenceArray<Future<Boolean>> block_state;
	private final int blockCount;
	private final AtomicReferenceArray<ByteBuffer> blocks;
	private final ConcurrentLinkedQueue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();

	private final Data redundantDataGenerator;
	private final Distribution redundantDistribution;
	private final UniqueData uniqueDataGenerator;
	private final Distribution uniqueDistribution;
	private final boolean directGeneration;

	public FirstGenerationTraffic(long size, int loadBlocks, int preloadWindow,
			Distribution uniqueDistribution, Distribution redundantDistribution, boolean directGeneration) {
		super(loadBlocks, preloadWindow);
		this.uniqueDistribution = uniqueDistribution;
		this.redundantDistribution = redundantDistribution;
		this.directGeneration = directGeneration;
		uniqueDataGenerator = new UniqueData();

		Random initialRedundantRandom = new XORShiftRandomGenerator();
		redundantDataGenerator = new RedundantData(2 * 1024, 32 * 1024,
				initialRedundantRandom);

		long blockCount = (long) Math.ceil(1.0 * size / (BLOCK_SIZE));
		if (blockCount > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("file too large");
		}
		this.blockCount = (int) blockCount;

		logger.info(String.format("Size %s, block count %s", size, blockCount));

		blocks = new AtomicReferenceArray<ByteBuffer>((int) blockCount);
		block_state = new AtomicReferenceArray<Future<Boolean>>(
				(int) blockCount);
	}

	public void close() {
	}

	public int getBlockCount() {
		return blockCount;
	}

	public TrafficSession getSession(int startindex, int endindex)
			throws Exception {
		return new OnlineGeneratedTrafficSession(startindex, endindex);
	}
}