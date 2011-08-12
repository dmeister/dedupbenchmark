package de.pc2.dedup.traffic.runner;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public interface TrafficSession {
	/**
	 * buffer of block i is not used anymore
	 * 
	 * @param i
	 */
	void clearBuffer(int i);

	void close();

	ByteBuffer getBuffer(int i) throws Exception;

	Future<Boolean> loadBuffer(int i) throws Exception;

}