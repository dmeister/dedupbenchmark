package de.pc2.dedup.traffic.runner;

import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;

/**
 * Represents a writer thread. All traffic files currently write the the same
 * file channel. The file channel is the block device tested.
 * 
 * SequentialTrafficRunnable is the most common implementation.
 * RandomTrafficRunnable exists, but it fails to simulate realistic random
 * traffic.
 * 
 * @author dirkmeister
 */
public abstract class TrafficRunnable extends Thread {

	/**
	 * target file channel
	 */
	private final FileChannel channel;

	private final Traffic traffic;

	public TrafficRunnable(Traffic traffic, FileChannel channel) {
		super();
		this.traffic = traffic;
		this.channel = channel;
	}

	public void close() {
	}

	public abstract int getBlocksWritten();

	public FileChannel getChannel() {
		return channel;
	}

	public LinkedHashMap<Long, Integer> getStatistics() {
		return new LinkedHashMap<Long, Integer>();
	}

	public Traffic getTraffic() {
		return traffic;
	}

	public abstract boolean isFinished();

	public abstract boolean isSuccessful();

	public void preload() throws Exception {
	}

	public abstract void run();
}
