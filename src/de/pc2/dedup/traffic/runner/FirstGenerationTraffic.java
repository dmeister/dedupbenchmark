package de.pc2.dedup.traffic.runner;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.log4j.Logger;
import org.uncommons.maths.random.MersenneTwisterRNG;
import org.uncommons.maths.random.SeedGenerator;

import com.google.common.base.Preconditions;

import de.pc2.dedup.traffic.runner.data.Data;
import de.pc2.dedup.traffic.runner.data.RedundantData;
import de.pc2.dedup.traffic.runner.data.UniqueData;
import de.pc2.dedup.traffic.runner.dist.Distribution;
import de.pc2.dedup.traffic.runner.util.StorageUnit;

public class FirstGenerationTraffic extends Traffic {

	class OnlineGeneratedTrafficSession implements TrafficSession {
		class GenerateJob implements Callable<Boolean> {
			private final int index;

			GenerateJob(int index) {
				this.index = index;
			}

			public Boolean call() throws Exception {
				return generateData(index);
			}

			@Override
			public String toString() {
				StringBuilder builder = new StringBuilder();
				builder.append("GenerateJob [index=").append(index).append("]");
				return builder.toString();
			}
		}

		private final int endIndex;
		private final ExecutorService executor = Executors.newSingleThreadExecutor();
		private Random redundantRandom;
		private Random redundantRandomData;
		private final int startIndex;
		private Random uniqueRandom;

		private Random uniqueRandomData;

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
			uniqueRandom = new MersenneTwisterRNG(seedGenerator);
			uniqueRandomData = new MersenneTwisterRNG(seedGenerator);
			redundantRandom = new MersenneTwisterRNG(seedGenerator);
			redundantRandomData = new MersenneTwisterRNG(seedGenerator);

			// init first block
			type = Type.UNIQUE;
			assignedSize = 0;
		}

		public void clearBuffer(int i) {
			if (i >= blocks.length()) {
				return;
			}

                        if (logger.isDebugEnabled()) {
			    logger.debug("Clear block " + i);
                        }
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

		private boolean generateData(int i) throws Exception {
			if (i >= endIndex) {
				return true;
			}
                        if (logger.isDebugEnabled()) {
			    logger.debug("Generating block " + i);
                        }
			ByteBuffer block = bufferQueue.poll();
			if (block == null) {
				block = ByteBuffer.allocate((int) blockSize);
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
						uniqueDataSize.addAndGet(bulkLenght);
						uniqueBulks.incrementAndGet();
						assignedSize -= bulkLenght;
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
						redundantDataSize.addAndGet(bulkLenght);
						redundantBulks.incrementAndGet();
						assignedSize -= bulkLenght;
					}
				}
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
						uniqueDataSize.addAndGet(bulkLenght);
						uniqueBulks.incrementAndGet();                                                
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
						redundantDataSize.addAndGet(bulkLenght);
						redundantBulks.incrementAndGet();
					}
				}
			}
			// finished generate that block

			block.flip();
			blocks.set(i, block);

                        if (logger.isDebugEnabled()) {
			    logger.debug("Generated block " + i);
                        }
			return true;

		}

		public int getBlockCount() {
			return blocks.length();
		}

		public ByteBuffer getBuffer(int i) throws Exception {
			if (directGeneration) {
				generateData(i);
				ByteBuffer b = blocks.get(i);
				Preconditions.checkState(b != null);
				return b;
			}

                        if (logger.isDebugEnabled()) {
			    logger.debug("Get buffer " + i);
                        }
			ByteBuffer b = blocks.get(i);
			if (b == null) {
				Future<Boolean> f = block_state.get(i);
				if (f == null) {
					f = loadBuffer(i);
					f.get();
                                        if (logger.isDebugEnabled()) {
					    logger.debug("Waiting for block " + i);
                                        }
                                        waitingBlocks.incrementAndGet();
				} else if (f.isDone() == false) {
					f.get();
					logger.debug("Waiting for block " + i);
                                        waitingBlocks.incrementAndGet();
				}
				b = blocks.get(i);
			}

			int preloadIndex = i + getPreloadWindow();
			if (preloadIndex < endIndex) {
                                if (logger.isDebugEnabled()) {
				logger.debug(String.format(
						"Preloading block %s at block %s", preloadIndex, i));
                                }

				Future<Boolean> f2 = executor.submit(new GenerateJob(
						preloadIndex));
				block_state.set(preloadIndex, f2);
			}

			return b;
		}

		public synchronized Future<Boolean> loadBuffer(int i) throws Exception {
			if (i >= blocks.length() && directGeneration) {
				return null;
			}

                        if (logger.isDebugEnabled()) {
			    logger.debug("Load buffer " + i);
                        }
			Future<Boolean> f = block_state.get(i);
			if (f != null) {
				return f;
			}
			f = executor.submit(new GenerateJob(i));
			block_state.set(i, f);
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
	private final int blockSize;
	private final AtomicReferenceArray<ByteBuffer> blocks;
	private final ConcurrentLinkedQueue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();

	private final Data redundantDataGenerator;
	private final Distribution redundantDistribution;
	private final UniqueData uniqueDataGenerator;
	private final Distribution uniqueDistribution;
	private final boolean directGeneration;
	private final SeedGenerator seedGenerator;

	private final AtomicLong redundantBulks = new AtomicLong();
	private final AtomicLong redundantDataSize = new AtomicLong();
	private final AtomicLong uniqueBulks = new AtomicLong();
	private final AtomicLong uniqueDataSize = new AtomicLong();
        private final AtomicLong waitingBlocks = new AtomicLong();

	public FirstGenerationTraffic(long size, int blockSize,
			int preloadWindow,
			Distribution uniqueDistribution, Distribution redundantDistribution, 
			SeedGenerator seedGenerator,
			boolean directGeneration) throws Exception {
		super(preloadWindow);
		this.uniqueDistribution = uniqueDistribution;
		this.redundantDistribution = redundantDistribution;
		this.directGeneration = directGeneration;
		this.seedGenerator = seedGenerator;
		this.blockSize = blockSize;
		
		uniqueDataGenerator = new UniqueData();
		Random initialRedundantRandom = new MersenneTwisterRNG(seedGenerator);
		redundantDataGenerator = new RedundantData(2 * 1024, 32 * 1024,
				initialRedundantRandom);

		long blockCount = (long) Math.ceil(1.0 * size / (blockSize));
		if (blockCount > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("file too large");
		}
		this.blockCount = (int) blockCount;

		blocks = new AtomicReferenceArray<ByteBuffer>((int) blockCount);
		block_state = new AtomicReferenceArray<Future<Boolean>>(
				(int) blockCount);
	}

	public void close() {
		logger.info(String.format("Unique %s, %s Bytes", uniqueBulks.get(), StorageUnit.formatUnit(uniqueDataSize.get())));
		logger.info(String.format("Redundant %s, %s Bytes", redundantBulks.get(), StorageUnit.formatUnit(redundantDataSize.get())));
                logger.info(String.format("Waited for %s blocks", StorageUnit.formatUnit(waitingBlocks.get())));
	}

	public int getBlockCount() {
		return blockCount;
	}

	public TrafficSession getSession(int startindex, int endindex)
			throws Exception {
		return new OnlineGeneratedTrafficSession(startindex, endindex);
	}
}

