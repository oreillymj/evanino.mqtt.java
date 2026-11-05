# Benchmarks: this client vs Eclipse Paho

A harness that runs the same scenarios against Eclipse Paho 1.2.5 (`org.eclipse.paho.client.mqttv3`) and this
client, each in a fresh JVM against its own private mosquitto. It is not part of the Maven build, so Paho never
becomes a dependency of the project.

```sh
benchmark/run.sh                    # every scenario, both clients (about 20 minutes)
benchmark/run.sh throughput latency # scenarios whose names start with these
QUICK=1 benchmark/run.sh            # small counts, to check that everything works
JAVA_OPTS="-Dbench.flushConsolidation=true -Dbench.sharedGroupSizing=true" benchmark/run.sh
                                    # with this client's optional tuning turned on
```

`JAVA_OPTS` adds options to every benchmark JVM. `bench.flushConsolidation` and `bench.sharedGroupSizing`
turn on this client's `MqttConnectOptions.enableFlushConsolidation` and `MqttAsyncClient.enableSharedGroupSizing`,
which are off by default; Paho ignores them.

Requirements: a JDK 8 or later, mosquitto (found at `-Dmosquitto.path`, the usual install locations or the
`PATH`), and these jars in `~/.m2/repository`: Netty 4.1.137, Guava 21, the Android stub jar (all fetched by a
Maven build of the project) and Paho 1.2.5:

```sh
mvn dependency:get -Dartifact=org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5
```

Each run prints one `RESULT key=value ...` line, and the lines are appended to `benchmark/results/<timestamp>.txt`.
The JVMs run with `-Xms1g -Xmx2g`. A single run can also be started by hand:

```sh
java -cp <classpath> bench.Bench --client paho|netty --scenario <scenario> [key=value ...]
```

## Scenarios

| Scenario | What it does | Main results |
|---|---|---|
| `connections` | Times `sequential` (20) connects one at a time, then opens the rest of `clients` (1000) with `parallel` (50) connects at once, holds them for `hold` (10) s, then closes them. | `single_connect_ms` (median), `connects_per_s` (parallel ramp), `connect_failures`, `threads_per_client`, `heap_mb`, `direct_mb`, `idle_cpu_ms_per_s`, `still_connected`, `close_ms` |
| `throughput` | One publisher, one subscriber on a topic, in the same JVM. After `warmup` messages, sends `count` messages of `size` (64) bytes at `qos`, keeping up to a window of publishes outstanding. | `msgs_per_s` (delivered to the subscriber), `mb_per_s`, `publish_msgs_per_s`, `cpu_us_per_msg`, `lost` |
| `latency` | Publishes at a fixed `rate` (2000/s) for `duration` (30) s after `warmup` (5) s. Each payload carries its send time; the subscriber records publish-to-delivery time. | `p50_us`, `p90_us`, `p99_us`, `p999_us`, `max_us`, `received` |
| `soak` | `clients` (20) publishers connect through a proxy that drops every connection every `drop` (20) s. They reconnect automatically with persistent sessions and each publish QoS 1 at `rate` (20)/s with sequence numbers. A subscriber connected directly to the broker checks what arrives. Runs `duration` (300) s plus `grace` (30) s. | `acked_but_lost` (must be 0), `duplicates`, `refused`, `failed`, `unresolved`, `double_outcomes`, `reconnects`, `not_connected_at_end`, `thread_growth`, `event_loop_threads`, `deadlocked_threads` |

Options common to `throughput` and `latency`: `inflight=N` sets both clients' in-flight limit (`setMaxInflight`);
without it each client keeps its default (Paho 10, this client 1000), and the publisher's window follows it. The
runner includes `throughput-qos1-inflight1000`, which gives both clients the same window. At QoS 0, which the
in-flight limit does not cover, the publisher keeps at most `qos0window` publishes outstanding (default 1000, so that
a client with no QoS 0 limit, such as Paho, cannot queue without end). `run.sh` passes `qos0window=65535`, this
client's default QoS 0 limit (`setMaxUnwrittenQos0`), to both clients.

On Windows, every scenario holds the system timer at 1 ms for both clients (`timer=1ms`, the default there, reported
in the `RESULT` line); `timer=default` leaves Windows' 15.6 ms tick alone. See "Latency and the Windows timer" below.

## Reading the results fairly

- **The broker is not the limit by design.** mosquitto runs with no cap on queued or in-flight messages per client
  (`max_queued_messages 0`, `max_inflight_messages 0`) and `set_tcp_nodelay true`. Throughput and latency include
  mosquitto's own cost, the same for both clients.
- **Everything is on one machine.** Loopback round trips are tens of microseconds, so the in-flight window matters
  far less than on a real network: with 20 ms of latency, Paho's window of 10 caps QoS 1 at about 500 messages a
  second. `throughput` at QoS 1 and 2 on loopback understates that difference.
- **Publisher and subscriber share a JVM**, so CPU per message covers both sides, and GC pauses affect both.
- **Paho uses `MemoryPersistence`.** Its default file persistence writes every QoS 1 and 2 message to disk and would
  measure the disk rather than the client.
- **This client uses its defaults**, including the shared event loop. Paho's defaults are also kept, except for the
  options the scenario sets the same way on both (clean session, timeouts, in-flight limit when given).
- **Latency is measured at the send time**, not at the scheduled time, so a publisher that falls behind shows up in
  `late_sends` rather than as extra latency. Treat runs with many late sends as overloaded.
- **Soak differences in `refused` are expected:** while disconnected, Paho refuses publishes (no
  `DisconnectedBufferOptions` is set), and this client holds QoS 1 publishes and sends them after reconnecting.
- **Stability signals:** `acked_but_lost`, `unresolved` (publishes whose callback never came), `double_outcomes` (publishes reported both
  as completed and as failed, or twice), `not_connected_at_end`, `thread_growth` and `deadlocked_threads`
  should all be 0. `event_loop_threads` is reported apart from `thread_growth`: this client's
  shared event loop starts its threads lazily as reconnects spread connections over its loops, up to twice the
  number of processors.
- **Latency pacing parks, then spins** for the last 2 ms before each send, because timed parking oversleeps by a
  millisecond or more (up to about 15 ms on Windows). Expect one core busy during latency runs.
- **On Windows the timer tick decides the latency tail**, so the harness holds the timer at 1 ms (see below).
  Results made with `timer=default`, or before the harness did this, depend on what else was running.
- Run several times and compare medians; a single run on a laptop varies by 10 % or more.

## Results: this client vs Paho (`run.sh`)

Indicative figures from one machine, collected on 3 November 2026, with the default in-flight window of 1000.
**Performance may vary**: expect different numbers on other hardware, operating systems, JVMs and broker versions,
and from run to run (see above). **These results come from clients and a broker running on the same system, over
loopback, with no network latency.** On a real network, round trips dominate QoS 1 and 2 throughput, and the
in-flight window then matters far more than the client's own cost.

Setup: AMD Ryzen 5 5500U (6 cores, 12 threads), 32 GB, Windows 11 Pro, Temurin JDK 1.8.0_432, Mosquitto 2.0.18
started by the harness, 64-byte payloads, the full `run.sh` (not `QUICK`). It was run twice, back to back: once
with this client's defaults, and once with both optional settings on:

```
JAVA_OPTS="-Dbench.flushConsolidation=true -Dbench.sharedGroupSizing=true" benchmark/run.sh
```

Paho ignores these options, so its two columns show how much the same run varies.

| Scenario | Paho (1st run) | This client, defaults | Paho (2nd run) | This client, both options on |
|---|---|---|---|---|
| 1000 connections: ramp time / threads | 6.5 s / 4,048 | 0.35 s / 1,070 | 6.5 s / 4,048 | 0.35 s / 1,050 |
| Idle CPU, 1000 connections | 151 ms/s | 0 ms/s | 114 ms/s | 1 ms/s |
| QoS 0 throughput, harness window 1000 (CPU per message) | 71,000 msg/s (28.4 µs) | 58,000 msg/s (26.7 µs) | 55,000 msg/s (35.0 µs) | 51,000 msg/s (28.6 µs) |
| QoS 1 throughput, default windows (Paho 10, this client 1000) | 20,700 msg/s | 30,900 msg/s | 21,800 msg/s | 28,900 msg/s |
| QoS 1 throughput, both windows 1000 | 24,400 msg/s | 28,800 msg/s | 22,600 msg/s | 30,800 msg/s |
| QoS 1 CPU per message, default windows | 137 µs | 78 µs | 137 µs | 90 µs |
| QoS 2 throughput, default windows | 11,700 msg/s | 14,400 msg/s | 11,600 msg/s | 13,800 msg/s |
| QoS 2 CPU per message | 244 µs | 148 µs | 255 µs | 149 µs |
| QoS 0 latency at 2000 msg/s: p50 / p99 / p99.9 | 123 / 354 / 572 µs | 137 / 573 / 10,708 µs | 128 / 379 / 2,770 µs | 138 / 3,023 / 12,329 µs |
| QoS 1 latency at 2000 msg/s: p50 / p99 / p99.9 | 109 / 311 / 1,040 µs | 126 / 1,671 / 11,777 µs | 108 / 407 / 4,330 µs | 132 / 452 / 9,206 µs |
| Soak, 20 clients, connections dropped every 20 s: refused / failed / lost | 7,441 / 14 / 0 | 0 / 0 / 0 | 7,458 / 14 / 0 | 0 / 0 / 0 |

Every throughput and latency run delivered every message.

Notes:

- QoS 1 and 2: this client is faster than Paho and uses 35–45% less CPU per message, with or without the options.
  With the same window of 1000 for both, the gap at QoS 1 narrows but remains.
- QoS 0: these runs were made before `run.sh` passed `qos0window=65535`, so both clients had a window of 1000,
  not this client's own limit of 65535 (see below). Paho was ahead in both runs, but its own figure moved from
  71,000 to 55,000 msg/s between two identical runs, so a single run says little at this level. The figures for this client with both options on were lower
  than with its defaults here; on the 2-core Linux machine where the options were tuned, they gave about 13% more
  QoS 0 throughput. With 12 hardware threads here, the halved shared event loop group may matter more than the
  saved flushes. Measure on your own hardware before turning them on.
- Latency: medians are close (10–25 µs behind Paho), but this client's tails (p99.9 and above) are longer, at
  around 10 ms, in all four runs. Paho's tails varied between its two runs too. These runs were made before the
  harness held Windows' timer at 1 ms; the tail comes from the timer tick, not from the client (see "Latency and
  the Windows timer" below).
- Soak: Paho refuses publishes while it is disconnected (`Client is not connected`), and 14 of its in-flight
  publishes failed, one per drop; this client queued and delivered all 120,000 messages through every reconnect.
  Neither lost an acknowledged message.
- `run.sh` writes the raw `RESULT` lines to `results/<timestamp>.txt`; that folder is not kept in git.

### QoS 0 with a window of 65535

The `throughput` scenario at QoS 0 run on its own, outside `run.sh`, with `qos0window=65535` so that each client can
have as many QoS 0 publishes outstanding as this client's default limit (`setMaxUnwrittenQos0`, 65535) allows. Paho
has no QoS 0 limit, so it gets the same window. The optional settings were off. This client was built twice: with
logging compiled in (as `run.sh` builds it, with logging off at run time) and with it compiled out (`MQTT_LOGGING=false`,
as `-Pno-logging` does). The three builds were run in turn, three times, on the same machine as above:

```
java ... bench.Bench --client paho|netty --scenario throughput qos=0 count=500000 qos0window=65535
```

| Run | Paho | This client, logging compiled in | This client, logging compiled out |
|---|---|---|---|
| 1 | 41,600 msg/s (58.6 µs) | 65,100 msg/s (28.9 µs) | 66,500 msg/s (29.7 µs) |
| 2 | 36,200 msg/s (73.7 µs) | 44,500 msg/s (35.9 µs) | 48,300 msg/s (33.3 µs) |
| 3 | 32,600 msg/s (81.5 µs) | 52,000 msg/s (32.0 µs) | 48,500 msg/s (31.6 µs) |

Figures are messages delivered per second, with CPU per message in brackets. Every run delivered every message.

- With the larger window, this client was ahead of Paho in every run, by 23–60%, with less than half its CPU per
  message. Paho was slower with the larger window than with 1000 (55,000–71,000 msg/s above) and its CPU per message
  rose with each run.
- Compiling logging out made no difference that stands out from the run-to-run variation (up to 20,000 msg/s
  between runs of the same build here).
- This client's runs show 126–191 `window_full_retries`: publishes refused for a moment because 65535 QoS 0
  publishes were already waiting to be written, and retried by the harness. That is the QoS 0 limit doing its job.

### All scenarios: QoS 0 window 65535, logging compiled out, both options on

A full `run.sh` run with this client set up for speed: logging compiled out (`MQTT_LOGGING=false`), both optional
settings on, and the QoS 0 throughput scenario given `qos0window=65535` (for both clients) so that this client's
default QoS 0 limit applies, as `run.sh` now does by default. QoS 1 and 2 use each client's default window (Paho 10, this client 1000) except in the
row where both are 1000. Same machine as above, 3 November 2026:

```
MQTT_LOGGING=false JAVA_OPTS="-Dbench.flushConsolidation=true -Dbench.sharedGroupSizing=true" benchmark/run.sh
```

| Scenario | Paho | This client |
|---|---|---|
| 1000 connections: ramp time / threads / idle CPU | 6.5 s / 4,044 / 40 ms/s | 0.34 s / 1,056 / 0 ms/s |
| QoS 0 throughput, window 65535 (CPU per message) | 33,700 msg/s (70.9 µs) | 50,500 msg/s (31.0 µs) |
| QoS 1 throughput, default windows (CPU per message) | 21,700 msg/s (154 µs) | 31,300 msg/s (84 µs) |
| QoS 1 throughput, both windows 1000 (CPU per message) | 22,700 msg/s (145 µs) | 30,700 msg/s (80 µs) |
| QoS 2 throughput, default windows (CPU per message) | 11,700 msg/s (255 µs) | 14,700 msg/s (145 µs) |
| QoS 0 latency at 2000 msg/s: p50 / p99 / p99.9 | 124 / 365 / 1,335 µs | 130 / 3,137 / 12,160 µs |
| QoS 1 latency at 2000 msg/s: p50 / p99 / p99.9 | 104 / 294 / 399 µs | 136 / 442 / 10,070 µs |
| Soak: refused / failed / lost | 7,433 / 15 / 0 | 0 / 0 / 0 |

Every throughput and latency run delivered every message.

- Throughput: this client was ahead at every QoS, by 50% at QoS 0, 35–44% at QoS 1 and 26% at QoS 2, with
  43–56% less CPU per message. The QoS 0 figure is within the range of the separate runs above (44,500–66,500 msg/s).
- Latency: as in the other runs, medians are close but this client's slowest 1 in 1000 messages take about 10 ms.
  This run was also made before the harness held the timer at 1 ms; see the next section.
- The figures are in line with the runs with logging compiled in and the options off, so neither setting is
  needed for these results on this machine.

### All scenarios on Linux, with the defaults

A full `run.sh` run with nothing changed: logging compiled in, both optional settings off, `qos0window=65535` for
QoS 0 throughput, and each client's default window for QoS 1 and 2 (Paho 10, this client 1000) except in the row
where both are 1000. Intel Core i5-7300U (2 cores, 4 threads), 32 GB, Ubuntu 24.04.5, OpenJDK 21.0.12, Mosquitto
2.0.18 (the system package, started by the harness), 4 November 2026:

```
benchmark/run.sh
```

| Scenario | Paho | This client |
|---|---|---|
| 1000 connections: ramp time / threads / idle CPU | 6.6 s / 4,044 / 132 ms/s | 0.85 s / 1,015 / 9 ms/s |
| First connection | 309 ms | 1.9 ms |
| QoS 0 throughput, window 65535 (CPU per message) | 41,500 msg/s (58.7 µs) | 39,400 msg/s (44.8 µs) |
| QoS 1 throughput, default windows (CPU per message) | 17,600 msg/s (115 µs) | 19,200 msg/s (114 µs) |
| QoS 1 throughput, both windows 1000 (CPU per message) | 18,800 msg/s (92 µs) | 18,900 msg/s (115 µs) |
| QoS 2 throughput, default windows (CPU per message) | 9,350 msg/s (172 µs) | 9,160 msg/s (167 µs) |
| QoS 0 latency at 2000 msg/s: p50 / p99 / p99.9 | 159 / 263 / 1,164 µs | 147 / 1,736 / 4,828 µs |
| QoS 1 latency at 2000 msg/s: p50 / p99 / p99.9 | 160 / 1,025 / 2,921 µs | 147 / 331 / 2,041 µs |
| Soak: refused / failed / lost | 7,468 / 23 / 0 | 0 / 0 / 0 |

Every throughput and latency run delivered every message.

- Throughput: with 2 cores shared by the broker, the publisher and the subscriber, the two clients are close. This
  client was 9% ahead at QoS 1 with the default windows, level with both windows at 1000, and 2–5% behind at QoS 0
  and QoS 2, which is within the run-to-run variation. It used 24% less CPU per message at QoS 0 and about the same
  at QoS 1 and 2. The larger leads on the 12-thread Windows machine come from having cores to spare.
- QoS 0: this client's run shows 3,080 `window_full_retries`, against 126–191 on Windows: on 2 cores, the event loop
  falls behind the publisher more often, and the 65535 limit holds it back.
- Connections: the same pattern as on Windows, with about a quarter of the threads and a fraction of the idle CPU.
- Latency: medians are 12–13 µs ahead of Paho. The tails varied: this client's QoS 0 p99 was longer in this run but
  not in the two `run.sh latency` runs below (317 and 237 µs), and Paho's QoS 1 p99 was longer here. On a machine
  this small, the tail depends mostly on what else is scheduled.
- Soak: as on Windows, Paho refused publishes while disconnected and 23 in-flight publishes failed; this client
  delivered all 120,000. Neither lost an acknowledged message.

### Latency and the Windows timer

The p99.9 latency of about 10 ms for this client in the Windows runs above comes from Windows' timer, not from the
client. By default Windows wakes threads from timed waits only on its clock tick, every 15.6 ms. A program can ask
for a 1 ms timer (browsers, media players and IDEs often do), and the tick is then 1 ms for the whole system while it
runs. So on Windows the tail depends on what else is running, and runs of the same build varied a lot.

How this was found (scratch instrumentation, not part of the harness): in the messages slower than 5 ms, nearly all
the extra time was between `publish()` returning, which took under a microsecond, and Netty's event loop starting to
send the message: a median of 7.5 ms and at most 15.9 ms, half a tick and one tick. Writing, the broker round trip
and the hand-off to the message callback were all fast. There were no GC or JVM pauses over 5 ms, and a separate
process that woke every 100 µs never saw a gap longer than one tick, so the machine as a whole did not stall. With a
daemon thread sleeping 1 ms in a loop, which makes the JVM hold the timer at 1 ms, the tail went away (QoS 1, same
scratch program, two runs each):

| | p99.9 | Max | Messages over 5 ms (of 60,000) |
|---|---|---|---|
| This client, 15.6 ms tick | 8.4–10.2 ms | 13–14 ms | 156–255 |
| This client, 1 ms timer | 0.8–1.4 ms | 5–8 ms | 0–1 |
| Paho, 1 ms timer | 1.3 ms | 9 ms | 17 |

Paho showed the tail too in some runs, but less often. Why the event loop is hit more than Paho's sender thread has
not been proven: Paho wakes its thread with `wait`/`notify`, and Netty wakes its event loop with
`Selector.wakeup()`, which on Windows writes to a loopback socket.

The harness now holds the timer at 1 ms on Windows for both clients. `run.sh latency`, twice, on the same Windows
machine as above (3 November 2026):

| QoS | Paho: p50 / p99 / p99.9 | This client: p50 / p99 / p99.9 |
|---|---|---|
| 0, run 1 | 138 / 305 / 414 µs | 129 / 369 / 1,976 µs |
| 0, run 2 | 142 / 340 / 4,101 µs | 137 / 312 / 411 µs |
| 1, run 1 | 104 / 299 / 507 µs | 121 / 349 / 1,182 µs |
| 1, run 2 | 104 / 257 / 350 µs | 121 / 313 / 613 µs |

And `run.sh latency`, twice, on Linux, which has no such tick: Intel Core i5-7300U (2 cores, 4 threads), 32 GB,
Ubuntu 24.04.5, OpenJDK 21.0.12, Mosquitto 2.0.18:

| QoS | Paho: p50 / p99 / p99.9 | This client: p50 / p99 / p99.9 |
|---|---|---|
| 0, run 1 | 150 / 226 / 940 µs | 124 / 317 / 2,107 µs |
| 0, run 2 | 130 / 239 / 1,298 µs | 139 / 237 / 1,388 µs |
| 1, run 1 | 152 / 287 / 1,484 µs | 139 / 234 / 1,429 µs |
| 1, run 2 | 151 / 324 / 1,892 µs | 146 / 263 / 1,754 µs |

Every run delivered all 70,000 messages. Without the tick, the two clients' tails are of the same order, and which
is ahead changes from run to run; medians are within about 20 µs.

**If your application needs low latency on Windows**, hold the timer at 1 ms while it runs, in the same way:

```java
Thread timer = new Thread(() -> {
	while (true) {
		try {
			Thread.sleep(1);   // the JVM raises Windows' timer to 1 ms for sleeps that are not a multiple of 10 ms
		} catch (InterruptedException e) {
			return;
		}
	}
}, "timer-1ms");
timer.setDaemon(true);
timer.start();
```

A 1 ms timer costs a little power, which matters most on battery. This client does not do it itself: the timer is
a setting for the whole system, so it is the application's choice.

## Results: this client vs Mosquitto's own C client

Indicative figures from one machine, collected on 31 October 2026. **Performance may vary**: expect different
numbers on other hardware, operating systems, JVMs and broker versions, and from run to run (see above). **These
results come from a client and a broker running on the same system, over loopback, with no network latency.** On a
real network, round trips dominate QoS 1 and 2 throughput, and the in-flight window then matters far more than the
client's own cost (see "Everything is on one machine" above).

Setup: AMD Ryzen 5 5500U (6 cores, 12 threads), 32 GB, Windows 11 Pro, Temurin JDK 1.8.0_432, Mosquitto 2.0.18
configured as the harness configures it (no queue or in-flight caps, `set_tcp_nodelay true`), 64-byte payloads.
The receiver in every run is `mosquitto_sub` at the same QoS, so only the publisher differs. The rate is messages
received divided by the time from the start of publishing until `mosquitto_sub` has all of them; every run below
delivered every message.

| QoS | In-flight window | This client | `mosquitto_pub` 2.1.2 (`-l`, `-M`) |
|---|---|---|---|
| 0 | 1000 (this client) | 72,000–75,000 msg/s | 46,000–50,000 msg/s |
| 1 | 20 | 34,000–35,000 msg/s | 25,000–25,500 msg/s |
| 1 | 1000 | 27,000–36,000 msg/s | 25,300–25,500 msg/s |
| 2 | 20 | 15,400–19,000 msg/s | 12,000–13,100 msg/s |
| 2 | 1000 | 17,000 msg/s | 10,900–11,900 msg/s |

Notes:

- These runs are not part of `run.sh`. This client published through the harness's `NettyBenchClient`, after a
  warm-up on a topic with no subscriber, so the JIT had compiled the hot paths before timing started.
- `mosquitto_pub` reads one message per line from stdin (`-l`). The Windows build used drops QoS 1 and 2 messages
  that are still unacknowledged when stdin reaches end of file, so stdin was held open until `mosquitto_sub` had
  received everything.
- `mosquitto_pub` stays at about 25,000 msg/s at QoS 1 whatever its window, while this client delivers more
  into the same `mosquitto_sub`, so the subscriber was not the limit there.
- The harness's own `throughput` scenario, with this client as the subscriber, gave similar figures for this client
  (QoS 0 about 71,500, QoS 1 with a window of 1000 about 38,300, QoS 2 with a window of 1000 14,000–17,000 msg/s).
- Paho Java was also run into `mosquitto_sub`, but the results are not reliable. The broker acknowledged
  every message, yet `mosquitto_sub` often received fewer and lagged well behind. The cause was not found and it was
  not seen with this client, so use `run.sh` to compare with Paho.
