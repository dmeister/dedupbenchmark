package de.pc2.dedup.traffic.runner;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class TrafficRunner {

	protected static TrafficRunnable getTrafficRunnable(boolean randomIO, Traffic traffic, FileChannel channel,int start,int end) {
		if(randomIO) {
			return new RandomTrafficRunnable(traffic, channel, start, end);
		} else {
			return new SequentialTrafficRunnable(traffic, channel, start, end);
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		String trafficFile = args[0];
		String destination = args[1];
		int threadCount = Integer.parseInt(args[2]);
		boolean randomIO = (args.length == 4 && args[3].equals("-random"));
		
		Traffic traffic = new  Traffic(trafficFile);
		traffic.load();
		
		List<TrafficRunnable> threads = new ArrayList<TrafficRunnable>();
		int blockCount = traffic.getBlockCount();
		int blockShare = blockCount / threadCount;
		FileOutputStream outputStream = new FileOutputStream(destination);
		FileChannel channel = outputStream.getChannel();
		for(int i = 0; i < threadCount;i++) {
			int start = i * blockShare;
			int end = (i+1) * blockShare;
			if(end > blockCount) {
				end = blockCount;
			}
			threads.add(getTrafficRunnable(randomIO,traffic,channel,start,end));
		}
			
		
		long startTime = System.currentTimeMillis();
		for(Thread t : threads) {
			t.start();
		}
		for(Thread t : threads) {
			t.join();
		}
                channel.force(true);
                outputStream.close();
		long endTime = System.currentTimeMillis();
		
		long runMillis = endTime - startTime;
		int blocks = 0;
		for(TrafficRunnable t : threads) {
			blocks += t.getBlocksWritten();
			if(!t.isSuccessful() || !t.isFinished()) {
				System.out.println("Illegal Run");
				return;
			}
		}
		long mb = 1L * blocks * Traffic.BLOCK_SIZE / (1024*1024);
		double mbs = 1000.0 * mb / runMillis;
		System.out.printf("Time: %s ms%n", runMillis);
		System.out.printf("MB: %s MB%n", mb);
		System.out.printf("MB/s: %s MB/s%n", mbs);
	}

}
