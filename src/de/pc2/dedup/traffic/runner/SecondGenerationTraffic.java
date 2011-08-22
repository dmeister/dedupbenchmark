package de.pc2.dedup.traffic.runner;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.log4j.Logger;
import org.uncommons.maths.random.MersenneTwisterRNG;
import org.uncommons.maths.random.SeedGenerator;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;

import de.pc2.dedup.traffic.runner.data.RedundantData;
import de.pc2.dedup.traffic.runner.data.UniqueData;
import de.pc2.dedup.traffic.runner.dist.Distribution;
import de.pc2.dedup.traffic.runner.dist.SwitchDistribution;
import de.pc2.dedup.traffic.runner.util.StorageUnit;

public class SecondGenerationTraffic extends Traffic {

	class Generation2RedundantData implements SecondGenerationData {
		private RedundantData redundantData;

		public Generation2RedundantData(RedundantData redundantData) {
			super();
			this.redundantData = redundantData;
		}

		public void getBulkData(ByteBuffer buf, int length,
				Random random_state,
				ByteBuffer firstGenerationBlock)
				throws Exception {
			redundantData.getBulkData(buf, length, random_state);
			
			logger.debug(String.format("Skip %s bytes from %s", length, firstGenerationBlock));
			firstGenerationBlock.position(firstGenerationBlock.position() + length);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Generation2RedundantData [redundantData=")
					.append(redundantData).append("]");
			return builder.toString();
		}
	}

	class Generation2UniqueData implements SecondGenerationData {
		private UniqueData uniqueData;

		public Generation2UniqueData(UniqueData uniqueData) {
			this.uniqueData = uniqueData;
		}

		public void getBulkData(ByteBuffer buf, int length,
				Random random_state,
				ByteBuffer firstGenerationBlock) throws Exception {
			uniqueData.getBulkData(buf, length, random_state);
			
			logger.debug(String.format("Skip %s bytes from %s", length, firstGenerationBlock));
			firstGenerationBlock.position(firstGenerationBlock.position() + length);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Generation2UniqueData [uniqueData=")
					.append(uniqueData).append("]");
			return builder.toString();
		}
	}

	class GenerationState {
		private SecondGenerationData data;
		private Distribution dist;

		public GenerationState(Distribution dist, SecondGenerationData data) {
			Preconditions.checkNotNull(dist);
			Preconditions.checkNotNull(data);

			this.dist = dist;
			this.data = data;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("GenerationState [dist=").append(dist)
					.append(", data=").append(data).append("]");
			return builder.toString();
		}
	}

	class OnlineGeneratedTrafficSession implements TrafficSession {
		class GenerateJob implements Callable<Boolean> {
			private final int index;

			GenerateJob(int index) {
				this.index = index;
			}

			public Boolean call() throws Exception {
				return generateData(index);
			}
		}

		private final int endIndex;
		private final ExecutorService executor = Executors
				.newFixedThreadPool(1);
		private final TrafficSession firstGenerationTrafficSession;
		private Random internalRedundantRandom;
		private Random internalRedundantRandomData;
		private final int startIndex;
		private Random switchRandom;
		private Random temporalRedundantRandom;
		private Random temporalRedundantRandomData;
		private Random uniqueRandom;

		private Random uniqueRandomData;
		long assignedSize;
		Type type;

		OnlineGeneratedTrafficSession(int startIndex, int endIndex)
				throws Exception {
			this.startIndex = startIndex;
			this.endIndex = endIndex;
			uniqueRandom = new MersenneTwisterRNG(seedGenerator);
			uniqueRandomData = new MersenneTwisterRNG(seedGenerator);
			internalRedundantRandom = new MersenneTwisterRNG(seedGenerator);
			internalRedundantRandomData = new MersenneTwisterRNG(seedGenerator);
			temporalRedundantRandom = new MersenneTwisterRNG(seedGenerator);
			temporalRedundantRandomData = new MersenneTwisterRNG(seedGenerator);
			switchRandom = new MersenneTwisterRNG(seedGenerator);
			firstGenerationTrafficSession = firstGenerationTraffic.getSession(
					startIndex, endIndex);

			// init first block
			type = Type.TEMPORAL_REDUNDANT;
		}

		public void clearBuffer(int i) {
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
				
				logger.debug("Generating block " + i);

				ByteBuffer block = bufferQueue.poll();
				if (block == null) {
					block = ByteBuffer.allocate((int) BLOCK_SIZE);
				}
				ByteBuffer firstGenerationBlock = this.firstGenerationTrafficSession.getBuffer(i);
	
				Preconditions.checkState(block != null);
				Preconditions.checkState(firstGenerationBlock != null);
				Preconditions.checkState(type != null);

				if (assignedSize > 0) {
					GenerationState typeState = states.get(type);
					Random dataRandom = null;
					if (type == Type.UNIQUE) {
						dataRandom = uniqueRandomData;
					} else if (type == Type.INTERNALREDUNDANT) {
						dataRandom = internalRedundantRandomData;
					} else if (type == Type.TEMPORAL_REDUNDANT) {
						dataRandom = temporalRedundantRandom;
					}

					long bulkLenght = assignedSize;
					if (bulkLenght > block.remaining()) {
						assignedSize = (bulkLenght - block.remaining());
						bulkLenght = block.remaining();
					}
					if (bulkLenght > 0) {
						typeState.data.getBulkData(block, (int) bulkLenght,
								dataRandom, firstGenerationBlock);
					}

					assignedSize = 0;
				}
				while (block.remaining() > 0) {
					type = getNextType(type);
					Preconditions.checkNotNull(type);

					GenerationState typeState = states.get(type);
					Preconditions.checkNotNull(typeState);

					Random dataRandom = null;
					Random lengthRandom = null;
					if (type == Type.UNIQUE) {
						dataRandom = uniqueRandomData;
						lengthRandom = uniqueRandom;
					} else if (type == Type.INTERNALREDUNDANT) {
						dataRandom = internalRedundantRandomData;
						lengthRandom = internalRedundantRandom;
					} else if (type == Type.TEMPORAL_REDUNDANT) {
						dataRandom = temporalRedundantRandomData;
						lengthRandom = temporalRedundantRandom;
					}
					
					logger.debug(String.format("%s: %s byte", type, StorageUnit.formatUnit(bulk)))

					long bulkLenght = typeState.dist.getNext(lengthRandom);
					if (bulkLenght > block.remaining()) {
						assignedSize = (bulkLenght - block.remaining());
						bulkLenght = block.remaining();
					}
					if (bulkLenght > 0) {
						typeState.data.getBulkData(block, (int) bulkLenght,
								dataRandom, firstGenerationBlock);
					}

				}
				firstGenerationTrafficSession.clearBuffer(i);
				// finished generate that block
				block.flip();
				blocks.set(i, block);
				logger.debug("Generated block " + i);
		
			return true;
		}

		public int getBlockCount() {
			return blocks.length();
		}

		public ByteBuffer getBuffer(int i) throws Exception {
			
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
			Preconditions.checkState(b != null);
			
				int preloadIndex = i + getPreloadWindow();
				if (preloadIndex < endIndex) {
					logger.debug(String.format(
							"Preloading block %s at block %s", preloadIndex, i));

					Future<Boolean> f2 = executor.submit(new GenerateJob(
							preloadIndex));
					block_state.set(preloadIndex, f2);
				}
			
			return b;
		}

		private Type getNextType(Type currentType) {
			return switchDistribution.getNextState(currentType, switchRandom);
		}

		public synchronized Future<Boolean> loadBuffer(int i) throws Exception {
			if (i >= blocks.length()) {
				return null;
			}

			logger.debug("Load second generation block " + i);

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
					.append(", firstGenerationTrafficSession=")
					.append(firstGenerationTrafficSession)
					.append("]");
			return builder.toString();
		}

	}

	public interface SecondGenerationData {

		public abstract void getBulkData(ByteBuffer buf, int length,
				Random random_state,
				ByteBuffer firstGenerationBlock)
				throws Exception;

	}

	class TemporalRedundantData implements SecondGenerationData {

		public TemporalRedundantData() {
		}

		public void getBulkData(ByteBuffer buf, int length,
				Random random_state,
				ByteBuffer firstGenerationBlock)
				throws Exception {
			
			logger.debug(String.format("Get %s bytes from %s", length, firstGenerationBlock));
			
			buf.put(firstGenerationBlock.array(), firstGenerationBlock.arrayOffset()
					+ firstGenerationBlock.position(), length);
			firstGenerationBlock.position(firstGenerationBlock.position() + length);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("TemporalRedundantData []");
			return builder.toString();
		}
	}

	public static enum Type {
		INTERNALREDUNDANT, TEMPORAL_REDUNDANT, UNIQUE;

		public static Type fromTag(char tag) {
			if (tag == 'U') {
				return UNIQUE;
			} else if (tag == 'I') {
				return INTERNALREDUNDANT;
			} else if (tag == 'T') {
				return TEMPORAL_REDUNDANT;
			} else {
				throw new IllegalArgumentException(String.format("tag %s", tag));
			}
		}
	}

	private static final Logger logger = Logger
			.getLogger(SecondGenerationTraffic.class);

	private final AtomicReferenceArray<Future<Boolean>> block_state;
	private final int blockCount;
	private final AtomicReferenceArray<ByteBuffer> blocks;
	private final ConcurrentLinkedQueue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();

	private final FirstGenerationTraffic firstGenerationTraffic;
	private final ConcurrentMap<Type, GenerationState> states;
	private final SwitchDistribution switchDistribution;
	private final SeedGenerator seedGenerator;
	
	private final AtomicLong internalRedundantBulks = new AtomicLong();
	private final AtomicLong internalRedundantDataSize = new AtomicLong();
	private final AtomicLong temporalRedundantBulks = new AtomicLong();
	private final AtomicLong temporalRedundantDataSize = new AtomicLong();
	private final AtomicLong uniqueBulks = new AtomicLong();
	private final AtomicLong uniqueDataSize = new AtomicLong();
	
	public SecondGenerationTraffic(long size,
			int preloadWindow, Distribution uniqueDistribution,
			Distribution internalRedundantDistribution,
			Distribution temporalRedundantDistribution,
			SwitchDistribution switchDistribution,
			FirstGenerationTraffic firstGenerationTraffic,
			SeedGenerator seedGenerator) throws Exception {
		super(preloadWindow);
		Preconditions.checkNotNull(switchDistribution);
		Preconditions.checkNotNull(firstGenerationTraffic);
		
		this.seedGenerator = seedGenerator;

		GenerationState uniqueState = new GenerationState(uniqueDistribution,
				new Generation2UniqueData(new UniqueData()));

		Random initialRedundantRandom = new MersenneTwisterRNG(seedGenerator);
		RedundantData redundantDataGenerator = new RedundantData(2 * 1024,
				32 * 1024, initialRedundantRandom);

		GenerationState internalRedundant = new GenerationState(
				internalRedundantDistribution, new Generation2RedundantData(
						redundantDataGenerator));
		GenerationState temporalRedundant = new GenerationState(
				temporalRedundantDistribution, new TemporalRedundantData());

		states = new MapMaker().makeMap();
		states.put(Type.UNIQUE, uniqueState);
		states.put(Type.INTERNALREDUNDANT, internalRedundant);
		states.put(Type.TEMPORAL_REDUNDANT, temporalRedundant);
		this.switchDistribution = switchDistribution;
		this.firstGenerationTraffic = firstGenerationTraffic;

		long blockCount = (long) Math.ceil(1.0 * size / (BLOCK_SIZE));
		if (blockCount > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("file too large");
		}
		this.blockCount = (int) blockCount;

		blocks = new AtomicReferenceArray<ByteBuffer>((int) blockCount);
		block_state = new AtomicReferenceArray<Future<Boolean>>(
				(int) blockCount);
	}

	public void close() {
		logger.info(String.format("Unique %s, %s Bytes", uniqueBulks.get(), uniqueDataSize.get()));
		logger.info(String.format("Internal Redundant %s, %s Bytes", internalRedundantBulks.get(), internalRedundantDataSize.get()));
		logger.info(String.format("Temporal Redundant %s, %s Bytes", temporalRedundantBulks.get(), temporalRedundantDataSize.get()));
	}

	public int getBlockCount() {
		return blockCount;
	}

	public TrafficSession getSession(int startindex, int endindex)
			throws Exception {
		return new OnlineGeneratedTrafficSession(startindex, endindex);
	}
}
