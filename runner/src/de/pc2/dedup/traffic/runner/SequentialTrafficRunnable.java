package de.pc2.dedup.traffic.runner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SequentialTrafficRunnable extends TrafficRunnable {
	private final int startindex;
	private final int endindex;
	private volatile boolean finished = false;
	private volatile boolean successful = false;
	private volatile int blocksWritten = 0;

	public SequentialTrafficRunnable(Traffic traffic, FileChannel channel,
			int startindex, int endindex) {
		super(traffic,channel);
		this.startindex = startindex;
		this.endindex = endindex;
	}

	public void run() {
		try {

			for (int i = startindex; i < endindex; i++) {
				ByteBuffer block = getTraffic().getBuffer(i);
				long pos = i * Traffic.BLOCK_SIZE;
				getChannel().write(block, pos);
				getTraffic().clearBuffer(i);
				blocksWritten++;
			}
			successful = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		finished = true;
	}

	public boolean isFinished() {
		return finished;
	}

	public boolean isSuccessful() {
		return successful;
	}

	public int getBlocksWritten() {
		return blocksWritten;
	}
}
