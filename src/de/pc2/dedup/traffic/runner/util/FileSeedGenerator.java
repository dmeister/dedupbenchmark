package de.pc2.dedup.traffic.runner.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.uncommons.maths.random.SeedException;
import org.uncommons.maths.random.SeedGenerator;

public class FileSeedGenerator implements SeedGenerator {
	private InputStream is;

	public FileSeedGenerator(String filename) throws FileNotFoundException {
		is = new BufferedInputStream(new FileInputStream(new File(filename)));
	}

	public byte[] generateSeed(int length) throws SeedException {
		try {
			byte[] buf = new byte[length];
			is.read(buf);
			return buf;
		} catch (IOException e) {
			throw new SeedException("Failed to generate seed", e);
		}
	}

}
