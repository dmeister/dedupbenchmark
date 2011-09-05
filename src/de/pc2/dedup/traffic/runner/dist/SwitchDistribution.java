package de.pc2.dedup.traffic.runner.dist;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import de.pc2.dedup.traffic.runner.SecondGenerationTraffic;

public class SwitchDistribution {

    	private static final Logger logger = Logger
			.getLogger(SwitchDistribution.class);

	class SwitchTypeDistribution {
		int count = 0;
		SortedMap<SecondGenerationTraffic.Type, Integer> count_map;

		public SwitchTypeDistribution() {
			count_map = Maps.newTreeMap();
		}
	}

	static class TypePair {
		public final SecondGenerationTraffic.Type first;
		public final SecondGenerationTraffic.Type second;
		
		public TypePair(SecondGenerationTraffic.Type first,
				SecondGenerationTraffic.Type second) {
			this.first = first;
			this.second = second;
		}
	}

	static class TypePairLineProcessor implements LineProcessor<List<TypePair>> {
		private List<TypePair> list = Lists.newArrayList();

		public List<TypePair> getResult() {
			return list;
		}

		public boolean processLine(String line) throws IOException {
			char oldTag = line.charAt(0);
			if (oldTag == '-') {
				return true; // ignore first line
			}
			char newTag = line.charAt(1);
			SecondGenerationTraffic.Type oldType = SecondGenerationTraffic.Type
					.fromTag(oldTag);
			SecondGenerationTraffic.Type newType = SecondGenerationTraffic.Type
					.fromTag(newTag);
			Preconditions.checkNotNull(oldType);
			Preconditions.checkNotNull(newType);

			list.add(new TypePair(oldType, newType));
			return true;
		}

	}

	public static SwitchDistribution loadSwitchDistribution(String filename)
			throws IOException {
		List<TypePair> typePairs = Files.readLines(new File(filename),
				Charset.defaultCharset(), new TypePairLineProcessor());
		Map<TypePair, Integer> countMap = Maps.newHashMap();

		for (TypePair pair : typePairs) {
			if (!countMap.containsKey(pair)) {
				countMap.put(pair, 0);
			}
			countMap.put(pair, countMap.get(pair) + 1);
		}

		SwitchDistribution sd = new SwitchDistribution(countMap);
		return sd;
	}

	private final Map<SecondGenerationTraffic.Type, SwitchTypeDistribution> map;

	private SwitchDistribution(Map<TypePair, Integer> countMap) {
		map = Maps.newHashMap();
		map.put(SecondGenerationTraffic.Type.UNIQUE,
				new SwitchTypeDistribution());
		map.put(SecondGenerationTraffic.Type.INTERNALREDUNDANT,
				new SwitchTypeDistribution());
		map.put(SecondGenerationTraffic.Type.TEMPORAL_REDUNDANT,
				new SwitchTypeDistribution());

		for (Map.Entry<TypePair, Integer> entry : countMap.entrySet()) {
			SwitchTypeDistribution std = map.get(entry.getKey().first);
			if (std != null) {
				std.count += entry.getValue();
				std.count_map.put(entry.getKey().second, std.count);
			} else {
                logger.warn("Strange count map entry: " + entry.getKey().first + ", " + entry.getKey().second);
            }
		}
	}

	public SecondGenerationTraffic.Type getNextState(
			SecondGenerationTraffic.Type oldType, Random rng) {
		SwitchTypeDistribution std = map.get(oldType);
		int i = rng.nextInt(std.count);
		for (Map.Entry<SecondGenerationTraffic.Type, Integer> entry : std.count_map
				.entrySet()) {
			if (i <= entry.getValue()) {
				return entry.getKey();
			}
		}
		// here are dragons
		throw new IllegalStateException();
	}

	@Override
	public String toString() {
		final int maxLen = 10;
		StringBuilder builder = new StringBuilder();
		builder.append("SwitchDistribution [map=")
				.append(map != null ? toString(map.entrySet(), maxLen) : null)
				.append("]");
		return builder.toString();
	}

	private String toString(Collection<?> collection, int maxLen) {
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		int i = 0;
		for (Iterator<?> iterator = collection.iterator(); iterator.hasNext()
				&& i < maxLen; i++) {
			if (i > 0)
				builder.append(", ");
			builder.append(iterator.next());
		}
		builder.append("]");
		return builder.toString();
	}

}
