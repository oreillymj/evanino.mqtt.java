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

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * A private mosquitto for one benchmark run, on a free loopback port. The broker is configured
 * not to be the limit: no cap on queued or in-flight messages per client (so QoS 1 and 2
 * messages are never dropped), and TCP_NODELAY on its sockets.
 */
final class Broker implements AutoCloseable {

	private final Process process;
	private final File config;
	final int port;

	/**
	 * Starts mosquitto and waits until it accepts connections.
	 *
	 * @throws Exception if mosquitto cannot be found or does not start
	 */
	Broker() throws Exception {
		File exe = findMosquitto();
		if (exe == null) {
			throw new IllegalStateException("mosquitto not found; set -Dmosquitto.path=/path/to/mosquitto");
		}
		try (ServerSocket s = new ServerSocket(0)) {
			port = s.getLocalPort();
		}
		config = File.createTempFile("mosquitto-bench-", ".conf");
		String text = "listener " + port + " 127.0.0.1\n"
				+ "allow_anonymous true\n"
				+ "persistence false\n"
				+ "max_queued_messages 0\n"
				+ "max_inflight_messages 0\n"
				+ "set_tcp_nodelay true\n";
		Files.write(config.toPath(), text.getBytes(StandardCharsets.UTF_8));
		process = new ProcessBuilder(exe.getAbsolutePath(), "-c", config.getAbsolutePath())
				.redirectErrorStream(true)
				.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(config.getAbsolutePath() + ".log")))
				.start();
		long deadline = System.currentTimeMillis() + 10_000;
		while (System.currentTimeMillis() < deadline) {
			if (!process.isAlive()) {
				throw new IllegalStateException("mosquitto exited; see " + config.getAbsolutePath() + ".log");
			}
			try (Socket s = new Socket()) {
				s.connect(new InetSocketAddress("127.0.0.1", port), 200);
				return;
			} catch (Exception e) {
				Thread.sleep(100);
			}
		}
		close();
		throw new IllegalStateException("mosquitto did not start listening on " + port);
	}

	/**
	 * Returns the broker URI.
	 *
	 * @return {@code tcp://127.0.0.1:port}
	 */
	String uri() {
		return "tcp://127.0.0.1:" + port;
	}

	@Override
	public void close() {
		process.destroy();
		try {
			if (!process.waitFor(5, TimeUnit.SECONDS)) {
				process.destroyForcibly();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		config.delete();
	}

	/**
	 * Finds the mosquitto executable: {@code -Dmosquitto.path}, the usual install locations,
	 * then the {@code PATH}.
	 *
	 * @return the executable, or {@code null}
	 */
	private static File findMosquitto() {
		String configured = System.getProperty("mosquitto.path");
		if (configured != null) {
			File f = new File(configured);
			return f.isFile() ? f : null;
		}
		String[] candidates = {
				"C:\\Program Files\\mosquitto\\mosquitto.exe",
				"/usr/sbin/mosquitto",
				"/usr/local/sbin/mosquitto",
				"/opt/homebrew/sbin/mosquitto",
		};
		for (String c : candidates) {
			File f = new File(c);
			if (f.isFile()) {
				return f;
			}
		}
		String path = System.getenv("PATH");
		if (path != null) {
			for (String dir : path.split(File.pathSeparator)) {
				for (String name : new String[]{"mosquitto", "mosquitto.exe"}) {
					File f = new File(dir, name);
					if (f.isFile()) {
						return f;
					}
				}
			}
		}
		return null;
	}
}
