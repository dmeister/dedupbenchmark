package de.pc2.dedup.traffic.runner.dist;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;

public class EmpriricalDistribution implements Distribution {
	private final List<Long> data;

	public EmpriricalDistribution(List<Long> dataList) {
		this.data = Lists.newArrayList(dataList);
		Collections.sort(data);
	}

	public long getNext(Random random) {
		double r = random.nextDouble();
		int k = (int) Math.floor(data.size() * r);
		double offset = r * data.size() - (k - 1);
		if (k + 1 < data.size()) {
			return (long) (data.get(k) * (1 - offset) + data.get(k + 1)
				* offset);
		} else {
			return data.get(k).longValue();
		}
	}

	@Override
	public String toString() {
		final int maxLen = 10;
		StringBuilder builder = new StringBuilder();
		builder.append("EmpriricalDistribution [data=")
				.append(data != null ? data.subList(0,
						Math.min(data.size(), maxLen)) : null).append("]");
		return builder.toString();
	}
}
