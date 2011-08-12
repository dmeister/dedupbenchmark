package de.pc2.dedup.traffic.runner.util;

public class StorageUnit {
	private final static double GB = 1024.0 * 1024.0 * 1024.0;
	private final static double KB = 1024.0;
	private final static double MB = 1024.0 * 1024.0;
	private final static double TB = 1024.0 * 1024.0 * 1024.0 * 1024.0;

	public static String formatUnit(long bytes) {
		if (bytes > TB) {
			return String.format("%.2fG", bytes / TB);
		}
		if (bytes > GB) {
			return String.format("%.2fG", bytes / GB);
		}
		if (bytes > MB) {
			return String.format("%.2fM", bytes / MB);
		}
		if (bytes > KB) {
			return String.format("%.2fK", bytes / KB);
		}
		return Long.toString(bytes);
	}

	public static long parseUnit(String input) {
		long multi = 1;

		char last = input.charAt(input.length() - 1);
		if (last == 'K' || last == 'k') {
			multi = 1024L;
		} else if (last == 'M' || last == 'm') {
			multi = 1024 * 1024L;
		} else if (last == 'G' || last == 'g') {
			multi = 1024L * 1024 * 1024L;
		} else if (last == 'T' || last == 't') {
			multi = 1024L * 1024L * 1024 * 1024L;
		}

		if (multi == 1) {
			return Long.parseLong(input);
		} else {
			return Long.parseLong(input.substring(0, input.length() - 1))
					* multi;
		}
	}

	private StorageUnit() {
	}
}
