package de.pc2.dedup.traffic.runner.dist;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class QuantileUtil {
	public static long Quantile(List<Long> data, double quantileValue) {
		Preconditions.checkArgument(data.size() > 0);
		Preconditions
				.checkArgument(quantileValue > 0.0 && quantileValue <= 1.0);

		List<Long> samples = Lists.newArrayList(data);
		Collections.sort(samples);

		double i = samples.size() * quantileValue;
		if (samples.size() % 2 == 1) {
			return samples.get((int) Math.ceil(i) - 1);
		}
		int i0 = (int) Math.floor(i) - 1;
		return (samples.get(i0) + samples.get(i0 + 1)) / 2;

	}

	private QuantileUtil() {
		// do not allow instances
	}
}
