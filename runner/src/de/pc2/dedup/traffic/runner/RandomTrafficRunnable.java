package de.pc2.dedup.traffic.runner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomTrafficRunnable extends TrafficRunnable {
	private final int startindex;
	private final int endindex;
	private volatile boolean finished = false;
	private volatile boolean successful = false;
	private volatile int blocksWritten = 0;
	private final List<Integer> blocksToWrite = new ArrayList<Integer>();
	private final Random r = new Random();

	public static void shuffle (List<Integer> array) 
    {
        Random rng = new Random();   // i.e., java.util.Random.
        int n = array.size();        // The number of items left to shuffle (loop invariant).
        while (n > 1) 
        {
            int k = rng.nextInt(n);  // 0 <= k < n.
            n--;                     // n is now the last pertinent index;
            int temp = array.get(n);     // swap array[n] with array[k] (does nothing if k == n).
            array.set(n,array.get(k));
            array.set(k,temp);
        }
    }
	
	public RandomTrafficRunnable(Traffic traffic, FileChannel channel,
			int startindex, int endindex) {
		super(traffic,channel);
		this.startindex = startindex;
		this.endindex = endindex;
		for(int i = startindex; i < endindex; i++) {
			blocksToWrite.add(i);
		}
		shuffle(blocksToWrite);
	}

	public void run() {
		try {
			for(int i : blocksToWrite) {
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
