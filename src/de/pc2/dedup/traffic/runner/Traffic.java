package de.pc2.dedup.traffic.runner;

/**
 * A shared traffic instance of all traffic runnables
 * 
 * @author dirkmeister
 * 
 */
public abstract class Traffic {
	public static final int BLOCK_SIZE = 1024 * 1024;

	private final int loadBlocks;
	private final int preloadWindow;

	public Traffic(int loadBlocks, int preloadWindow) {
		this.loadBlocks = loadBlocks;
		this.preloadWindow = preloadWindow;
	}

	public void close() {
	}

	public abstract int getBlockCount();

	public int getLoadBlocks() {
		return loadBlocks;
	}

	public int getPreloadWindow() {
		return preloadWindow;
	}

	public abstract TrafficSession getSession(int startindex, int endindex)
			throws Exception;
}