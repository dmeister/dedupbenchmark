package de.pc2.dedup.traffic.runner;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import de.pc2.dedup.traffic.runner.OnlineTrafficRunner.StartSignal;

public class SequentialTrafficRunnable extends TrafficRunnable {
	private static final Logger logger = Logger
			.getLogger(SequentialTrafficRunnable.class);
	private volatile int blocksWritten = 0;
	private final int endindex;
	private volatile boolean finished = false;
	private TrafficSession session;
	private StartSignal signal;
	private final int startindex;
	private long startTime = 0;
	private LinkedHashMap<Long, Integer> statistics = new LinkedHashMap<Long, Integer>();

	private volatile boolean successful = false;

	public SequentialTrafficRunnable(Traffic traffic, FileChannel channel,
			int startindex, int endindex, StartSignal signal) throws Exception {
		super(traffic, channel);
		session = traffic.getSession(startindex, endindex);
		this.startindex = startindex;
		this.endindex = endindex;
		this.signal = signal;
	}

	public void close() {
		session.close();
	}

	public int getBlocksWritten() {
		return blocksWritten;
	}

	public LinkedHashMap<Long, Integer> getStatistics() {
		return this.statistics;
	}

	public boolean isFinished() {
		return finished;
	}

	public boolean isSuccessful() {
		return successful;
	}

	public void preload() throws Exception {
		int preloadEndIndex = startindex + (getTraffic().getPreloadWindow());

		logger.debug(String
				.format("Preload %s:%s", startindex, preloadEndIndex));

		for (int i = startindex; i < preloadEndIndex; i ++) {
			if (i >= getTraffic().getBlockCount()) {
				break;
			}
			Future<Boolean> f = session.loadBuffer(i);
			f.get();
		}
	}

	public void run() {
		int i = 0;
		try {

			signal.await();

			this.startTime = System.currentTimeMillis();
			for (i = startindex; i < endindex; i++) {
				logger.debug(String.format("Writing block %s", i));
				ByteBuffer block = session.getBuffer(i);
				Preconditions.checkState(block != null);
				long pos = ((long) i) * Traffic.BLOCK_SIZE;
				getChannel().write(block, pos);
				blocksWritten++;

				long t = (System.currentTimeMillis() - this.startTime)
						/ (1000 * 10);
				if (statistics.containsKey(t) == false) {
					statistics.put(t, 0);
				}
				statistics.put(t, statistics.get(t) + 1);

				session.clearBuffer(i);
			}
			successful = true;
		} catch (Exception e) {
			successful = false;
			logger.error("Run failed at index " + i, e);
		}
		finished = true;
	}
}
