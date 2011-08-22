package de.pc2.dedup.traffic.runner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.uncommons.maths.random.DevRandomSeedGenerator;
import org.uncommons.maths.random.SeedGenerator;

import com.hazelcast.core.Hazelcast;

import de.pc2.dedup.traffic.runner.dist.BlockAlignedDistribtion;
import de.pc2.dedup.traffic.runner.dist.DistUtil;
import de.pc2.dedup.traffic.runner.dist.Distribution;
import de.pc2.dedup.traffic.runner.dist.SwitchDistribution;
import de.pc2.dedup.traffic.runner.util.FileSeedGenerator;
import de.pc2.dedup.traffic.runner.util.StorageUnit;

/**
 * Application class of the traffic runner.
 * 
 * @author dirkmeister
 * 
 */
public class OnlineTrafficRunner {

	static class StartSignal {
		public static StartSignal newClusterStart() {
			Lock l = Hazelcast.getLock("startlock");
			int t = Hazelcast.getCluster().getMembers().size();
			return new StartSignal(t, l);
		}
		public static StartSignal newLocalStart() {
			return new StartSignal(1, new ReentrantLock());
		}
		private final Condition cond;
		private int counter;

		private final Lock lock;

		private final int target;

		public StartSignal(int target, Lock lock) {
			this.counter = 0;
			this.lock = lock;
			this.cond = lock.newCondition();
			this.target = target;
		}

		public void add() {
			lock.lock();
			if (++counter == target) {
				cond.signalAll();
			}
			lock.unlock();
		}

		public void await() throws InterruptedException {
			lock.lock();

			while (counter != target) {
				cond.await();
			}
			lock.unlock();
		}
	}

	private static final Logger logger = Logger
			.getLogger(OnlineTrafficRunner.class);

	/**
	 * Factory method for the traffic runnable.
	 * 
	 * @param randomIO
	 * @param traffic
	 * @param channel
	 * @param start
	 * @param end
	 * @return
	 * @throws Exception
	 */
	protected static TrafficRunnable getTrafficRunnable(Traffic traffic,
			FileChannel channel, int start, int end, StartSignal signal)
			throws Exception {
		return new SequentialTrafficRunnable(traffic, channel, start, end,
				signal);
	}

	private static SeedGenerator getSeedGenerator(String filename) throws Exception {
		File f = new File(filename);
		if (f.exists()) {
			logger.info("Using seed file " + filename);
			return new FileSeedGenerator(filename);
		}
		return new DevRandomSeedGenerator();
	}
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		if (args.length < 1) {
			logger.error("Illegal arguments");
			System.exit(1);
		}
		String destination = args[args.length - 1];
		logger.info(String.format("Destination: %s", destination));
		int threadCount = 1;
		String backgroundProcess = null;
		int preloadWindow = 8;
		long size = 0;
		boolean clusterDiscovery = false;
		boolean firstGenerationData = true;

		for (int i = 0; i < args.length - 1; i++) {
			if (args[i].startsWith("-t=")) {
				threadCount = Integer
						.parseInt(args[i].substring("-t=".length()));
			}
			if (args[i].startsWith("-p=")) {
				preloadWindow = Integer.parseInt(args[i].substring("-p="
						.length()));
			}
			if (args[i].startsWith("-s=")) {
				size = StorageUnit.parseUnit(args[i].substring("-s=".length()));
			}
			if (args[i].startsWith("-b=")) {
				backgroundProcess = args[i].substring("-b=".length());
			}
			if (args[i].equals("-c")) {
				clusterDiscovery = true;
			}
			if (args[i].equals("-g")) {
				firstGenerationData = false;
			}
		}
		
		logger.info(String.format("Size: %s", size));
		logger.info(String.format("Threads: %s", threadCount));

		SeedGenerator firstGenerationSeed = getSeedGenerator("seeds/first_generation.seed");
		Distribution redundantDistribution = new BlockAlignedDistribtion(
				4 * 1024, DistUtil.loadEmpiricalData(
						"data/upb-6-rabin8-redundant-bulk-list.csv", 0.999));
		Distribution uniqueDistribution = new BlockAlignedDistribtion(4 * 1024,
				DistUtil.loadEmpiricalData(
						"data/upb-6-rabin8-unique-bulk-list.csv", 0.999));
		FirstGenerationTraffic firstGenerationTraffic = new FirstGenerationTraffic(
				size, preloadWindow, uniqueDistribution,
				redundantDistribution, firstGenerationSeed, !firstGenerationData);

		Traffic traffic = null;
		if (firstGenerationData) {
			traffic = firstGenerationTraffic;
		} else {
			SeedGenerator secondGenerationSeed = getSeedGenerator("seeds/second_generation.seed");
			Distribution secondGenerationUniqueDistribution = new BlockAlignedDistribtion(
					4 * 1024, DistUtil.loadEmpiricalData(
							"data/upb-6-rabin8-temporal-unique-bulk-list.csv", 0.999));
			Distribution internalRedundantDistribution = new BlockAlignedDistribtion(
					4 * 1024,
					DistUtil.loadEmpiricalData(
							"data/upb-6-rabin8-internal-redundant-bulk-list.csv",
							0.999));
			Distribution temporalRedundantDistribution = new BlockAlignedDistribtion(
					4 * 1024,
					DistUtil.loadEmpiricalData(
							"data/upb-6-rabin8-temporal-redundant-bulk-list.csv",
							0.999));
			SwitchDistribution switchDistribution = SwitchDistribution
					.loadSwitchDistribution("data/upb-6-rabin8-switch-stats.csv");
			SecondGenerationTraffic secondGenerationTraffic = new SecondGenerationTraffic(
					size, preloadWindow,
					secondGenerationUniqueDistribution,
					internalRedundantDistribution,
					temporalRedundantDistribution, switchDistribution,
					firstGenerationTraffic, secondGenerationSeed);
			traffic = secondGenerationTraffic;
		}

		List<TrafficRunnable> threads = new ArrayList<TrafficRunnable>();
		int blockCount = traffic.getBlockCount();
		int blockShare = blockCount / threadCount;
		FileOutputStream outputStream = new FileOutputStream(destination);
		FileChannel channel = outputStream.getChannel();

		StartSignal signal = null;
		if (clusterDiscovery) {
			signal = StartSignal.newClusterStart();
		} else {
			signal = StartSignal.newLocalStart();
		}

		for (int i = 0; i < threadCount; i++) {
			int start = i * blockShare;
			int end = (i + 1) * blockShare;
			if (end > blockCount) {
				end = blockCount;
			}
			threads.add(getTrafficRunnable(traffic, channel, start, end, signal));
		}

		Process process = null;
		if (backgroundProcess != null) {
			process = new ProcessBuilder().command(
					backgroundProcess.split("\\w")).start();
		}
		for (TrafficRunnable t : threads) {
			t.preload();
		}
		long startTime = System.currentTimeMillis();
		for (TrafficRunnable t : threads) {
			t.start();
		}

		Thread.sleep(1000);

		logger.info("Start");
		signal.add();

		for (TrafficRunnable t : threads) {
			t.join();
			t.close();
		}

		try {
			channel.force(true);
		} catch (IOException e) {
			// ignore
		}
		outputStream.close();
		if (process != null) {
			process.destroy();
		}
		long endTime = System.currentTimeMillis();

		long runMillis = endTime - startTime;
		int blocks = 0;
		for (TrafficRunnable t : threads) {
			blocks += t.getBlocksWritten();
			if (!t.isSuccessful() || !t.isFinished()) {
				logger.warn("Illegal run");
				return;
			}
		}
		long mb = 1L * blocks * Traffic.BLOCK_SIZE / (1024 * 1024);
		double mbs = 1000.0 * mb / runMillis;
		logger.info(String.format("Time: %s ms%n", runMillis));
		logger.info(String.format("MB: %s MB%n", mb));
		logger.info(String.format("MB/s: %s MB/s%n", mbs));
		logger.info(String.format("MB/s: %s MB/s%n", mbs));
		LinkedHashMap<Long, Integer> statistics = new LinkedHashMap<Long, Integer>();
		for (TrafficRunnable t : threads) {
			LinkedHashMap<Long, Integer> thread_statistics = t.getStatistics();
			for (Map.Entry<Long, Integer> e : thread_statistics.entrySet()) {
				if (statistics.containsKey(e.getKey()) == false) {
					statistics.put(e.getKey(), 0);
				}
				statistics.put(e.getKey(),
						statistics.get(e.getKey()) + e.getValue());
			}
		}
		for (Map.Entry<Long, Integer> e : statistics.entrySet()) {
			double stat_mb = 1L * e.getValue() * Traffic.BLOCK_SIZE;
			double stat_mbs = stat_mb / (1024 * 1024 * 10);
			logger.info(String.format("Second %s s: %s MB/s",
					(e.getKey() * 10), stat_mbs));
		}
		traffic.close();
	}

}
