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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Runs one benchmark scenario against one client and prints a single {@code RESULT} line of
 * {@code key=value} pairs. Each run starts its own mosquitto; run each (client, scenario) pair
 * in a fresh JVM, as {@code run.sh} does, so JIT and GC state does not carry over.
 * <p>
 * Usage: {@code bench.Bench --client paho|netty --scenario connections|throughput|latency|soak
 * [key=value ...]}; see {@code benchmark/README.md} for the options of each scenario.
 */
public final class Bench {

	private final String clientName;
	private final Map<String, String> options;
	private final Map<String, Object> result = new LinkedHashMap<>();

	private Bench(String clientName, Map<String, String> options) {
		this.clientName = clientName;
		this.options = options;
	}

	/**
	 * Entry point.
	 *
	 * @param args see the class description
	 * @throws Exception if the scenario fails
	 */
	public static void main(String[] args) throws Exception {
		String client = null;
		String scenario = null;
		Map<String, String> options = new LinkedHashMap<>();
		for (int i = 0; i < args.length; i++) {
			if ("--client".equals(args[i])) {
				client = args[++i];
			} else if ("--scenario".equals(args[i])) {
				scenario = args[++i];
			} else if (args[i].contains("=")) {
				String[] kv = args[i].split("=", 2);
				options.put(kv[0], kv[1]);
			} else {
				throw new IllegalArgumentException("unexpected argument: " + args[i]);
			}
		}
		if (client == null || scenario == null) {
			throw new IllegalArgumentException("usage: --client paho|netty --scenario connections|throughput|latency|soak [key=value ...]");
		}
		Bench bench = new Bench(client, options);
		bench.result.put("client", client);
		bench.result.put("scenario", scenario);
		if (WINDOWS) {
			String timer = options.getOrDefault("timer", "1ms");
			if ("1ms".equals(timer)) {
				holdTimerAt1ms();
			}
			bench.result.put("timer", timer);
		}
		try (Broker broker = new Broker()) {
			switch (scenario) {
				case "connections":
					bench.connections(broker);
					break;
				case "throughput":
					bench.throughput(broker);
					break;
				case "latency":
					bench.latency(broker);
					break;
				case "soak":
					bench.soak(broker);
					break;
				default:
					throw new IllegalArgumentException("unknown scenario: " + scenario);
			}
		}
		StringBuilder line = new StringBuilder("RESULT");
		for (Map.Entry<String, Object> e : bench.result.entrySet()) {
			line.append(' ').append(e.getKey()).append('=').append(e.getValue());
		}
		System.out.println(line);
		System.exit(0);   // Paho's and Netty's non-daemon threads must not keep the JVM alive
	}

	private static final boolean WINDOWS = System.getProperty("os.name", "").startsWith("Windows");

	/**
	 * Keeps Windows' system timer at 1 ms for the rest of the run. By default Windows wakes
	 * threads from timed waits only on its 15.6 ms clock tick, and a thread woken by another
	 * may also wait for it: Netty's event loop then sometimes starts a publish up to 15.6 ms
	 * after it was handed over, which shows as a tail of about 10 ms in the latency results.
	 * Programs often raise the timer (browsers, media players, IDEs), so without this the
	 * results depend on what else is running. The JVM raises it to 1 ms for the duration of
	 * any {@code Thread.sleep} that is not a multiple of 10 ms, so a daemon thread sleeping
	 * 1 ms at a time holds it there. Both clients get it; {@code timer=default} leaves the
	 * timer alone. Other systems have no such tick, so this is done on Windows only.
	 */
	private static void holdTimerAt1ms() {
		Thread t = new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					return;
				}
			}
		}, "bench-timer-1ms");
		t.setDaemon(true);
		t.start();
	}

	// ---------------------------------------------------------------- connections

	/**
	 * Times {@code sequential} (20) connects one at a time for the single-connect time, then
	 * opens the rest of {@code clients} connections with {@code parallel} (50) connects at once,
	 * holds them, then closes them. Reports connect times, the threads, heap and direct memory
	 * the connections cost, the CPU used while idle, and how many stayed connected.
	 */
	private void connections(Broker broker) throws Exception {
		int n = intOption("clients", 1000);
		int holdSeconds = intOption("hold", 10);
		int sequential = Math.min(n, intOption("sequential", 20));
		int parallel = intOption("parallel", 50);
		result.put("clients", n);
		result.put("parallel", parallel);

		int threadsBefore = Stats.threads();
		long heapBefore = Stats.heapUsedMb();
		List<BenchClient> clients = java.util.Collections.synchronizedList(new ArrayList<>(n));
		try {
			long[] single = new long[sequential];
			for (int i = 0; i < sequential; i++) {
				BenchClient c = newClient();
				clients.add(c);
				long t = System.nanoTime();
				c.connect(settings(broker.uri(), "conn-" + i));
				single[i] = (System.nanoTime() - t) / 1000;
			}
			result.put("single_connect_ms", String.format("%.1f", Stats.percentile(single, sequential, 50) / 1000.0));

			long start = System.nanoTime();
			java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(parallel);
			List<java.util.concurrent.Future<?>> connects = new ArrayList<>();
			for (int i = sequential; i < n; i++) {
				String id = "conn-" + i;
				connects.add(pool.submit(() -> {
					BenchClient c = newClient();
					clients.add(c);
					c.connect(settings(broker.uri(), id));
					return null;
				}));
			}
			long connectFailures = 0;
			for (java.util.concurrent.Future<?> f : connects) {
				try {
					f.get();
				} catch (java.util.concurrent.ExecutionException e) {
					connectFailures++;
				}
			}
			pool.shutdown();
			long rampNanos = System.nanoTime() - start;
			result.put("connect_failures", connectFailures);
			n = n - sequential;   // the parallel ramp's rate
			result.put("ramp_ms", rampNanos / 1_000_000);
			result.put("connects_per_s", n == 0 ? 0 : (long) (n / (rampNanos / 1e9)));
			int total = clients.size();
			result.put("threads_per_client", String.format("%.2f", (Stats.threads() - threadsBefore) / (double) total));
			result.put("threads_total", Stats.threads());
			result.put("heap_mb", Stats.heapUsedMb() - heapBefore);
			result.put("direct_mb", Stats.directUsedMb());

			long cpuBefore = Stats.processCpuMillis();
			Thread.sleep(holdSeconds * 1000L);
			result.put("idle_cpu_ms_per_s", (Stats.processCpuMillis() - cpuBefore) / holdSeconds);
			result.put("still_connected", clients.stream().filter(BenchClient::isConnected).count());
		} finally {
			long closeStart = System.nanoTime();
			for (BenchClient c : new ArrayList<>(clients)) {
				closeQuietly(c);
			}
			result.put("close_ms", (System.nanoTime() - closeStart) / 1_000_000);
		}
	}

	// ---------------------------------------------------------------- throughput

	/**
	 * One publisher and one subscriber on a topic. The publisher keeps up to {@code window}
	 * publishes outstanding and sends {@code count} messages of {@code size} bytes. Reports the
	 * rate at which messages arrive at the subscriber, CPU per message, and losses.
	 */
	private void throughput(Broker broker) throws Exception {
		int qos = intOption("qos", 1);
		int size = intOption("size", 64);
		int count = intOption("count", 100_000);
		int warmup = intOption("warmup", Math.min(count / 5, 20_000));
		int inflight = intOption("inflight", 0);
		int window = window(qos, inflight);
		result.put("qos", qos);
		result.put("size", size);
		result.put("count", count);
		result.put("inflight", inflight == 0 ? "default" : String.valueOf(inflight));
		result.put("window", window);

		String topic = "bench/tp/" + UUID.randomUUID();
		BenchClient sub = newClient();
		BenchClient pub = newClient();
		try {
			BenchClient.Settings subSettings = settings(broker.uri(), "tp-sub");
			sub.connect(subSettings);
			BenchClient.Settings pubSettings = settings(broker.uri(), "tp-pub");
			pubSettings.maxInflight = inflight;
			pub.connect(pubSettings);

			AtomicInteger received = new AtomicInteger();
			CountDownLatch warmedUp = new CountDownLatch(warmup);
			CountDownLatch done = new CountDownLatch(warmup + count);
			sub.subscribe(topic, qos, (t, p) -> {
				received.incrementAndGet();
				warmedUp.countDown();
				done.countDown();
			});

			byte[] payload = new byte[size];
			Semaphore permits = new Semaphore(window);
			AtomicLong failures = new AtomicLong();
			AtomicLong windowFull = new AtomicLong();

			publish(pub, topic, payload, qos, warmup, permits, failures, windowFull);
			if (!warmedUp.await(120, TimeUnit.SECONDS)) {
				throw new IllegalStateException("warm-up did not complete: " + received.get() + " of " + warmup);
			}

			long cpuBefore = Stats.processCpuMillis();
			long start = System.nanoTime();
			publish(pub, topic, payload, qos, count, permits, failures, windowFull);
			long published = System.nanoTime();
			boolean complete = done.await(120, TimeUnit.SECONDS);
			long end = System.nanoTime();
			long cpu = Stats.processCpuMillis() - cpuBefore;

			int got = received.get() - warmup;
			double seconds = (end - start) / 1e9;
			result.put("msgs_per_s", (long) (got / seconds));
			result.put("mb_per_s", String.format("%.1f", got * (double) size / seconds / (1024 * 1024)));
			result.put("publish_msgs_per_s", (long) (count / ((published - start) / 1e9)));
			result.put("cpu_us_per_msg", String.format("%.2f", cpu * 1000.0 / Math.max(1, got)));
			result.put("lost", complete ? 0 : count - got);
			result.put("publish_failures", failures.get());
			result.put("window_full_retries", windowFull.get());
		} finally {
			closeQuietly(pub);
			closeQuietly(sub);
		}
	}

	/**
	 * Publishes {@code n} messages, keeping at most as many outstanding as {@code permits} allows.
	 */
	private static void publish(BenchClient pub, String topic, byte[] payload, int qos, int n, Semaphore permits,
								AtomicLong failures, AtomicLong windowFull) throws Exception {
		for (int i = 0; i < n; i++) {
			permits.acquire();
			while (!pub.publish(topic, payload, qos, permits::release, e -> {
				failures.incrementAndGet();
				permits.release();
			})) {
				windowFull.incrementAndGet();
				LockSupport.parkNanos(20_000);
			}
		}
	}

	// ---------------------------------------------------------------- latency

	/**
	 * Publishes at a fixed {@code rate} for {@code duration} seconds after a warm-up; each
	 * payload carries its send time. Reports percentiles of the publish-to-delivery time.
	 */
	private void latency(Broker broker) throws Exception {
		int qos = intOption("qos", 1);
		int rate = intOption("rate", 2000);
		int duration = intOption("duration", 20);
		int warmupSeconds = intOption("warmup", 5);
		int size = Math.max(12, intOption("size", 64));
		int inflight = intOption("inflight", 0);
		result.put("qos", qos);
		result.put("rate", rate);
		result.put("duration_s", duration);
		result.put("size", size);
		result.put("inflight", inflight == 0 ? "default" : String.valueOf(inflight));

		int warmupCount = rate * warmupSeconds;
		int measured = rate * duration;
		long[] latencies = new long[measured];
		AtomicInteger recorded = new AtomicInteger();
		AtomicInteger received = new AtomicInteger();

		String topic = "bench/lat/" + UUID.randomUUID();
		BenchClient sub = newClient();
		BenchClient pub = newClient();
		try {
			sub.connect(settings(broker.uri(), "lat-sub"));
			BenchClient.Settings pubSettings = settings(broker.uri(), "lat-pub");
			pubSettings.maxInflight = inflight;
			pub.connect(pubSettings);
			CountDownLatch done = new CountDownLatch(warmupCount + measured);
			sub.subscribe(topic, qos, (t, p) -> {
				long now = System.nanoTime();
				ByteBuffer b = ByteBuffer.wrap(p);
				int seq = b.getInt();
				long sent = b.getLong();
				received.incrementAndGet();
				if (seq >= warmupCount) {
					int i = recorded.getAndIncrement();
					if (i < latencies.length) {
						latencies[i] = (now - sent) / 1000;   // microseconds
					}
				}
				done.countDown();
			});

			long interval = 1_000_000_000L / rate;
			AtomicLong failures = new AtomicLong();
			long late = 0;
			long start = System.nanoTime();
			for (int seq = 0; seq < warmupCount + measured; seq++) {
				long due = start + seq * interval;
				if (due - System.nanoTime() < -interval) {
					late++;
				}
				waitUntil(due);
				byte[] payload = new byte[size];
				ByteBuffer.wrap(payload).putInt(seq).putLong(System.nanoTime());
				while (!pub.publish(topic, payload, qos, () -> { }, e -> failures.incrementAndGet())) {
					LockSupport.parkNanos(10_000);
					ByteBuffer.wrap(payload).putInt(seq).putLong(System.nanoTime());
				}
			}
			done.await(60, TimeUnit.SECONDS);

			int n = Math.min(recorded.get(), latencies.length);
			result.put("p50_us", Stats.percentile(latencies, n, 50));
			result.put("p90_us", Stats.percentile(latencies, n, 90));
			result.put("p99_us", Stats.percentile(latencies, n, 99));
			result.put("p999_us", Stats.percentile(latencies, n, 99.9));
			result.put("max_us", n == 0 ? 0 : latencies[n - 1]);
			result.put("received", received.get() + "/" + (warmupCount + measured));
			result.put("late_sends", late);
			result.put("publish_failures", failures.get());
		} finally {
			closeQuietly(pub);
			closeQuietly(sub);
		}
	}

	// ---------------------------------------------------------------- soak

	/**
	 * {@code clients} publishers connect through a proxy that drops every connection each
	 * {@code drop} seconds; they reconnect automatically, keep their sessions, and each publish
	 * QoS 1 messages carrying a sequence number at {@code rate} per second. A subscriber
	 * connected straight to the broker records what arrives. After {@code duration} seconds and
	 * a grace period, reports acknowledged messages that never arrived (should be 0),
	 * duplicates, publishes refused or failed, unresolved publishes, reconnects, clients not
	 * connected at the end, thread growth and deadlocks.
	 */
	private void soak(Broker broker) throws Exception {
		int n = intOption("clients", 20);
		int duration = intOption("duration", 120);
		int drop = intOption("drop", 15);
		int rate = intOption("rate", 20);
		int grace = intOption("grace", 20);
		int qos = intOption("qos", 1);
		result.put("clients", n);
		result.put("duration_s", duration);
		result.put("drop_every_s", drop);
		result.put("rate_per_client", rate);
		result.put("qos", qos);

		String prefix = "bench/soak/" + UUID.randomUUID() + "/";
		Map<Integer, Set<Integer>> arrived = new ConcurrentHashMap<>();
		AtomicLong duplicates = new AtomicLong();
		BenchClient sub = newClient();
		List<BenchClient> pubs = new ArrayList<>();
		List<Thread> threads = new ArrayList<>();
		AtomicLong acked = new AtomicLong();
		AtomicLong failed = new AtomicLong();
		AtomicLong refused = new AtomicLong();
		AtomicLong sent = new AtomicLong();
		AtomicLong reconnects = new AtomicLong();
		AtomicLong lostEvents = new AtomicLong();
		AtomicLong doubleOutcomes = new AtomicLong();
		List<Set<Integer>> ackedSeqs = new ArrayList<>();
		Map<String, AtomicLong> refusalReasons = new ConcurrentHashMap<>();

		try (DropProxy proxy = new DropProxy(broker.port)) {
			sub.connect(settings(broker.uri(), "soak-sub"));
			sub.subscribe(prefix + "#", qos, (t, p) -> {
				int client = Integer.parseInt(t.substring(prefix.length()));
				int seq = ByteBuffer.wrap(p).getInt();
				if (!arrived.computeIfAbsent(client, k -> ConcurrentHashMap.newKeySet()).add(seq)) {
					duplicates.incrementAndGet();
				}
			});

			int threadsBefore = Stats.threads();
			for (int i = 0; i < n; i++) {
				BenchClient c = newClient();
				BenchClient.Settings s = settings(proxy.uri(), "soak-pub-" + i);
				s.automaticReconnect = true;
				s.cleanSession = false;
				s.connectionTimeout = 10;
				s.keepAlive = 10;
				s.onReconnect = reconnects::incrementAndGet;
				s.onConnectionLost = lostEvents::incrementAndGet;
				c.connect(s);
				pubs.add(c);
				ackedSeqs.add(ConcurrentHashMap.newKeySet());
			}

			int threadsConnected = Stats.threads() - Stats.threads(SharedLoopThreads.PREFIX);
			long end = System.currentTimeMillis() + duration * 1000L;
			for (int i = 0; i < n; i++) {
				int id = i;
				BenchClient c = pubs.get(i);
				Set<Integer> mine = ackedSeqs.get(i);
				Thread t = new Thread(() -> {
					long interval = 1_000_000_000L / rate;
					long next = System.nanoTime();
					for (int seq = 0; System.currentTimeMillis() < end; seq++) {
						next += interval;
						waitUntil(next);
						byte[] payload = ByteBuffer.allocate(8).putInt(seq).putInt(id).array();
						int s = seq;
						sent.incrementAndGet();
						// Each publish should have exactly one outcome; count the first, and any
						// further one as a client bug.
						java.util.concurrent.atomic.AtomicBoolean resolved = new java.util.concurrent.atomic.AtomicBoolean();
						try {
							if (!c.publish(prefix + id, payload, qos, () -> {
								if (resolved.compareAndSet(false, true)) {
									acked.incrementAndGet();
									mine.add(s);
								} else {
									doubleOutcomes.incrementAndGet();
								}
							}, e -> {
								if (resolved.compareAndSet(false, true)) {
									failed.incrementAndGet();
								} else {
									doubleOutcomes.incrementAndGet();
								}
							})) {
								if (resolved.compareAndSet(false, true)) {
									refused.incrementAndGet();
									refusalReasons.computeIfAbsent("window full", k -> new AtomicLong()).incrementAndGet();
								}
							}
						} catch (Exception e) {
							if (resolved.compareAndSet(false, true)) {
								refused.incrementAndGet();
								refusalReasons.computeIfAbsent(e.getClass().getSimpleName() + ":" + e.getMessage(),
										k -> new AtomicLong()).incrementAndGet();
							} else {
								doubleOutcomes.incrementAndGet();
							}
						}
					}
				}, "soak-publisher-" + i);
				t.setDaemon(true);
				threads.add(t);
				t.start();
			}

			int drops = 0;
			while (System.currentTimeMillis() < end) {
				Thread.sleep(Math.min(drop * 1000L, Math.max(1, end - System.currentTimeMillis())));
				if (System.currentTimeMillis() < end) {
					proxy.dropAll();
					drops++;
				}
			}
			for (Thread t : threads) {
				t.join(5000);
			}
			Thread.sleep(grace * 1000L);   // let reconnects finish and in-flight messages drain

			long lostAfterAck = 0;
			for (int i = 0; i < n; i++) {
				Set<Integer> got = arrived.getOrDefault(i, ConcurrentHashMap.newKeySet());
				for (Integer seq : ackedSeqs.get(i)) {
					if (!got.contains(seq)) {
						lostAfterAck++;
					}
				}
			}
			long delivered = arrived.values().stream().mapToLong(Set::size).sum();
			long unresolved = sent.get() - acked.get() - failed.get() - refused.get();
			long[] deadlocked = ManagementFactory.getThreadMXBean().findDeadlockedThreads();

			result.put("drops", drops);
			result.put("sent", sent.get());
			result.put("acked", acked.get());
			result.put("delivered_unique", delivered);
			result.put("acked_but_lost", lostAfterAck);
			result.put("duplicates", duplicates.get());
			result.put("refused", refused.get());
			result.put("failed", failed.get());
			result.put("unresolved", unresolved);
			result.put("double_outcomes", doubleOutcomes.get());
			result.put("connection_lost_events", lostEvents.get());
			result.put("reconnects", reconnects.get());
			result.put("proxy_connections", proxy.accepted.get());
			result.put("not_connected_at_end", pubs.stream().filter(c -> !c.isConnected()).count());
			result.put("threads_per_client", String.format("%.2f", (Stats.threads() - threadsBefore) / (double) n));
			result.put("thread_growth", Stats.threads() - Stats.threads(SharedLoopThreads.PREFIX) - threadsConnected
					- threads.stream().filter(Thread::isAlive).count());
			result.put("event_loop_threads", Stats.threads(SharedLoopThreads.PREFIX));
			result.put("deadlocked_threads", deadlocked == null ? 0 : deadlocked.length);
			StringBuilder reasons = new StringBuilder();
			refusalReasons.forEach((k, v) -> reasons.append(k.replace(' ', '_')).append(':').append(v.get()).append(';'));
			result.put("refusal_reasons", reasons.length() == 0 ? "-" : reasons.toString());
		} finally {
			for (BenchClient c : pubs) {
				closeQuietly(c);
			}
			closeQuietly(sub);
		}
	}

	// ---------------------------------------------------------------- helpers

	/**
	 * Waits until {@code System.nanoTime()} reaches {@code due}: parks while more than 2 ms
	 * remain, then spins, as timed parking is only accurate to a millisecond or more (about
	 * 1-15 ms on Windows).
	 */
	private static void waitUntil(long due) {
		long remaining;
		while ((remaining = due - System.nanoTime()) > 2_000_000) {
			LockSupport.parkNanos(remaining - 2_000_000);
		}
		while (due - System.nanoTime() > 0) {
			Thread.yield();
		}
	}

	/**
	 * Returns the publisher's window: for QoS 1 and 2 the in-flight limit (the client's own
	 * default when {@code inflight} is 0: Paho 10, this client 1000); for QoS 0, which the
	 * clients do not limit by default, {@code qos0window} (default 1000) so the publisher cannot queue without end.
	 */
	private int window(int qos, int inflight) {
		if (qos == 0) {
			return intOption("qos0window", 1000);
		}
		if (inflight > 0) {
			return inflight;
		}
		return "paho".equals(clientName) ? 10 : 1000;
	}

	private BenchClient newClient() {
		switch (clientName) {
			case "paho":
				return new PahoBenchClient();
			case "netty":
				return new NettyBenchClient();
			default:
				throw new IllegalArgumentException("unknown client: " + clientName);
		}
	}

	private static BenchClient.Settings settings(String uri, String clientId) {
		BenchClient.Settings s = new BenchClient.Settings();
		s.serverURI = uri;
		s.clientId = clientId;
		return s;
	}

	private int intOption(String name, int defaultValue) {
		String v = options.get(name);
		return v == null ? defaultValue : Integer.parseInt(v);
	}

	private static void closeQuietly(BenchClient c) {
		try {
			c.close();
		} catch (Exception ignored) {
			// best effort
		}
	}
}
