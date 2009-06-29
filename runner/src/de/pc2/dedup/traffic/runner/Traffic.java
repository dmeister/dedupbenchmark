package de.pc2.dedup.traffic.runner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Traffic {
	public static final int BLOCK_SIZE = 1024 * 1024;
	private final File file;
	private final long size;
	private ByteBuffer[] blocks;
	
	public Traffic(String filename) {
		this.file = new File(filename);
		this.size = file.length();
	}
	
	public void load() throws IOException {
		int blockCount = (int)Math.ceil(1.0 * size / (BLOCK_SIZE));
		blocks = new ByteBuffer[blockCount];
		FileInputStream s = new FileInputStream(file);
		FileChannel c = s.getChannel();
		
		for(int i = 0; i < blocks.length; i++) {
			int pos = i * BLOCK_SIZE;
			ByteBuffer block = ByteBuffer.allocate(BLOCK_SIZE);
			c.read(block, pos);
			block.flip();
			blocks[i] = block;
		}	
	}
	
	public int getBlockCount() {
		return blocks.length;
	}

	public ByteBuffer getBuffer(int i) {
		return blocks[i];
	}
	
	public void clearBuffer(int i) {
		blocks[i] = null;
	}
} 
