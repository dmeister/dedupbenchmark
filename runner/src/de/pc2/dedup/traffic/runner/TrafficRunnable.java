package de.pc2.dedup.traffic.runner;

import java.nio.channels.FileChannel;

public abstract class TrafficRunnable extends Thread {

	private final Traffic traffic;
	private final FileChannel channel;

	public TrafficRunnable(Traffic traffic, FileChannel channel) {
		super();
		this.traffic = traffic;
		this.channel = channel;
	}

	public abstract void run();
	public abstract boolean isFinished();
	public abstract boolean isSuccessful();
	public abstract int getBlocksWritten();
	
	public Traffic getTraffic() {
		return traffic;
	}

	public FileChannel getChannel() {
		return channel;
	}
}
