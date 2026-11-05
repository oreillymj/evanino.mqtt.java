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

package io.github.oreillymj.evanino.mqtt;

import org.junit.Assume;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * A private mosquitto with no limit on queued or in-flight messages per client, for load tests.
 * With mosquitto's defaults (1000 queued, 20 in flight) a subscriber that falls behind loses
 * messages in the broker, whatever the client does. Optionally with a fixed number of retained
 * messages loaded by the test.
 */
public final class UnlimitedMosquitto extends ExternalResource {

	private Process process;
	private File config;
	private int port;

	/**
	 * Returns the broker URI.
	 *
	 * @return {@code tcp://127.0.0.1:port}
	 */
	public URI getServerURI() {
		return URI.create("tcp://127.0.0.1:" + port);
	}

	@Override
	protected void before() throws Throwable {
		File exe = new File(System.getProperty("mosquitto.path", "C:\\Program Files\\mosquitto\\mosquitto.exe"));
		if (!exe.isFile()) {
			exe = new File("/usr/sbin/mosquitto");
		}
		Assume.assumeTrue("mosquitto not found", exe.isFile());
		try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			port = s.getLocalPort();
		}
		config = File.createTempFile("mosquitto-unlimited-", ".conf");
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
			try (Socket s = new Socket()) {
				s.connect(new InetSocketAddress("127.0.0.1", port), 200);
				return;
			} catch (Exception e) {
				Thread.sleep(100);
			}
		}
		throw new IllegalStateException("mosquitto did not start on " + port);
	}

	@Override
	protected void after() {
		if (process != null) {
			process.destroy();
			try {
				if (!process.waitFor(5, TimeUnit.SECONDS)) {
					process.destroyForcibly();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		if (config != null) {
			config.delete();
		}
	}
}
