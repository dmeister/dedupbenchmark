package de.pc2.dedup.traffic.runner.dist;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

public class DistUtil {
	static class IntegerLineProcessor implements LineProcessor<List<Long>> {
		private List<Long> list = Lists.newArrayList();

		public List<Long> getResult() {
			return list;
		}

		public boolean processLine(String line) throws IOException {
			long l = Long.valueOf(line);
			Preconditions.checkArgument(l > 0);
			list.add(l);
			return true;
		}

	}

	public static Distribution loadEmpiricalData(String filename)
			throws IOException {
		return loadEmpiricalData(filename, 1.00);
	}

	public static Distribution loadEmpiricalData(String filename,
			double quantileValue) throws IOException {
		List<Long> data = loadFile(filename);
		if (quantileValue != 1.00) {
			final long q = QuantileUtil.Quantile(data, quantileValue);
			Predicate<Long> quantileFilter = new Predicate<Long>() {
				public boolean apply(Long input) {
					return input < q;
				}
			};
			data = Lists.newArrayList(Iterables.filter(data, quantileFilter));
		}
		return new EmpriricalDistribution(data);
	}

	private static List<Long> loadFile(String filename) throws IOException {
		return Files.readLines(new File(filename), Charset.defaultCharset(),
				new IntegerLineProcessor());
	}

	private DistUtil() {
		// do not allow instances
	}
}
