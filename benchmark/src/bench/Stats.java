/*******************************************************************************
 * Copyright (c) 2026 Michael OReilly
 *
 * This program and the accompanying materials are made available under
 * the terms of the Apache License, Version 2.0, which accompanies this
 * distribution (see LICENSE).
 *
 * The Apache License is available at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Contributors:
 *    Michael OReilly - initial API and implementation
 *******************************************************************************/

package bench;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Arrays;

/**
 * Percentiles and JVM resource readings for the benchmark results.
 */
final class Stats {

	private Stats() {
	}

	/**
	 * Returns a percentile of the first {@code n} values, which are sorted in place.
	 *
	 * @param values the samples
	 * @param n      how many samples are valid
	 * @param p      the percentile, 0 to 100
	 * @return the value at that percentile, or 0 with no samples
	 */
	static long percentile(long[] values, int n, double p) {
		if (n == 0) {
			return 0;
		}
		Arrays.sort(values, 0, n);
		int index = (int) Math.ceil(p / 100.0 * n) - 1;
		return values[Math.max(0, Math.min(n - 1, index))];
	}

	/**
	 * Returns the number of live threads in the JVM.
	 *
	 * @return the thread count
	 */
	static int threads() {
		return ManagementFactory.getThreadMXBean().getThreadCount();
	}

	/**
	 * Returns the number of live threads whose names start with a prefix.
	 *
	 * @param prefix the name prefix
	 * @return the thread count
	 */
	static int threads(String prefix) {
		int n = 0;
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			if (t.isAlive() && t.getName().startsWith(prefix)) {
				n++;
			}
		}
		return n;
	}

	/**
	 * Returns the heap in use after a full collection, in MB.
	 *
	 * @return heap used, MB
	 */
	static long heapUsedMb() {
		MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
		for (int i = 0; i < 3; i++) {
			System.gc();
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		return memory.getHeapMemoryUsage().getUsed() / (1024 * 1024);
	}

	/**
	 * Returns the direct (off-heap) buffer memory in use, in MB, as reported by the JVM's
	 * buffer pools. Netty's pooled direct memory appears here.
	 *
	 * @return direct memory used, MB
	 */
	static long directUsedMb() {
		long used = 0;
		for (java.lang.management.BufferPoolMXBean pool
				: ManagementFactory.getPlatformMXBeans(java.lang.management.BufferPoolMXBean.class)) {
			if ("direct".equals(pool.getName())) {
				used += pool.getMemoryUsed();
			}
		}
		return used / (1024 * 1024);
	}

	/**
	 * Returns the CPU time used by the process so far, in milliseconds, or -1 if unavailable.
	 *
	 * @return process CPU time, ms
	 */
	static long processCpuMillis() {
		java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
		if (os instanceof com.sun.management.OperatingSystemMXBean) {
			return ((com.sun.management.OperatingSystemMXBean) os).getProcessCpuTime() / 1_000_000;
		}
		return -1;
	}
}
