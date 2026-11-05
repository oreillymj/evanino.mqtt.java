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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JUnit class rule that provides an MQTT broker for integration tests, taking the place of
 * Paho's {@code TestProperties.getServerURI()}.
 * <p>
 * If the system property {@code mqtt.test.serverURI} is set, that broker is used as-is (and
 * {@code mqtt.test.webSocketServerURI}, if set, for WebSocket tests). Otherwise a private
 * mosquitto instance is started with a TCP listener and a WebSocket listener on free local
 * ports, plus a TLS listener (requiring a client certificate) and a secure WebSocket listener
 * using the test certificates in {@code src/test/resources/tls}, and stopped after the test
 * class. The mosquitto executable is taken from the {@code mosquitto.path} system
 * property, or looked up in the usual install locations and on the {@code PATH}. When no broker
 * is available the tests are skipped rather than failed.
 */
public final class MosquittoBroker extends ExternalResource {

	private static final String[] DEFAULT_LOCATIONS = {
			"C:\\Program Files\\mosquitto\\mosquitto.exe",
			"/usr/sbin/mosquitto",
			"/usr/local/sbin/mosquitto",
			"/opt/homebrew/sbin/mosquitto",
	};

	private URI serverURI;
	private URI webSocketServerURI;
	private URI sslServerURI;
	private URI secureWebSocketServerURI;
	private Process process;
	private File configFile;
	private int port;
	private int webSocketPort;
	private int sslPort;
	private int secureWebSocketPort;

	/**
	 * Returns the URI of the broker, for example {@code tcp://127.0.0.1:52345}.
	 *
	 * @return the broker URI
	 */
	public URI getServerURI() {
		return serverURI;
	}

	/**
	 * Returns the URI of the broker's WebSocket listener, for example
	 * {@code ws://127.0.0.1:52346}, skipping the calling test if there is none.
	 *
	 * @return the WebSocket broker URI
	 */
	public URI getWebSocketServerURI() {
		Assume.assumeTrue("no WebSocket broker; set -Dmqtt.test.webSocketServerURI", webSocketServerURI != null);
		return webSocketServerURI;
	}

	/**
	 * Returns the URI of the broker's TLS listener, for example {@code mqtts://localhost:52347},
	 * skipping the calling test if there is none. The listener requires a client certificate.
	 *
	 * @return the TLS broker URI
	 */
	public URI getSslServerURI() {
		Assume.assumeTrue("no TLS broker; set -Dmqtt.test.sslServerURI", sslServerURI != null);
		return sslServerURI;
	}

	/**
	 * Returns the URI of the broker's secure WebSocket listener, for example
	 * {@code wss://localhost:52348}, skipping the calling test if there is none.
	 *
	 * @return the secure WebSocket broker URI
	 */
	public URI getSecureWebSocketServerURI() {
		Assume.assumeTrue("no secure WebSocket broker; set -Dmqtt.test.secureWebSocketServerURI", secureWebSocketServerURI != null);
		return secureWebSocketServerURI;
	}

	/**
	 * Returns whether this rule started its own mosquitto process, so it can be stopped and
	 * restarted by tests that need to simulate a broker outage.
	 *
	 * @return {@code true} for a private mosquitto instance
	 */
	public boolean isManaged() {
		return configFile != null;
	}

	/**
	 * Starts the broker, or skips the test class if none is available.
	 *
	 * @throws Exception if the broker cannot be started
	 */
	@Override
	protected void before() throws Exception {
		String external = System.getProperty("mqtt.test.serverURI");
		if (external != null && !external.trim().isEmpty()) {
			serverURI = new URI(external.trim());
			String externalWs = System.getProperty("mqtt.test.webSocketServerURI");
			if (externalWs != null && !externalWs.trim().isEmpty()) {
				webSocketServerURI = new URI(externalWs.trim());
			}
			String externalSsl = System.getProperty("mqtt.test.sslServerURI");
			if (externalSsl != null && !externalSsl.trim().isEmpty()) {
				sslServerURI = new URI(externalSsl.trim());
			}
			String externalWss = System.getProperty("mqtt.test.secureWebSocketServerURI");
			if (externalWss != null && !externalWss.trim().isEmpty()) {
				secureWebSocketServerURI = new URI(externalWss.trim());
			}
			return;
		}

		File exe = findMosquitto();
		Assume.assumeTrue("mosquitto not found; set -Dmosquitto.path or -Dmqtt.test.serverURI", exe != null);

		port = freePort();
		webSocketPort = freePort();
		sslPort = freePort();
		secureWebSocketPort = freePort();
		configFile = File.createTempFile("mosquitto-test-", ".conf", configDirectory());
		configFile.deleteOnExit();
		String config = "listener " + port + " 127.0.0.1\n"
				+ "allow_anonymous true\n"
				+ "persistence false\n"
				+ "listener " + webSocketPort + " 127.0.0.1\n"
				+ "protocol websockets\n";
		String tls = TestTls.mosquittoTlsSettings();
		if (tls != null) {
			config += "listener " + sslPort + " 127.0.0.1\n" + tls + "require_certificate true\n"
					+ "listener " + secureWebSocketPort + " 127.0.0.1\n" + "protocol websockets\n" + tls;
		}
		Files.write(configFile.toPath(), config.getBytes(StandardCharsets.UTF_8));
		serverURI = new URI("tcp://127.0.0.1:" + port);
		webSocketServerURI = new URI("ws://127.0.0.1:" + webSocketPort);
		if (tls != null) {
			// "localhost", not 127.0.0.1: the test server certificate is only valid for localhost
			sslServerURI = new URI("mqtts://localhost:" + sslPort);
			secureWebSocketServerURI = new URI("wss://localhost:" + secureWebSocketPort);
		}
		start(exe);
	}

	/**
	 * Stops the broker if this rule started it.
	 */
	@Override
	protected void after() {
		stop();
		if (configFile != null) {
			configFile.delete();
		}
	}

	/**
	 * Stops the private mosquitto process, if running.
	 */
	public void stop() {
		if (process != null) {
			process.destroy();
			try {
				if (!process.waitFor(5, TimeUnit.SECONDS)) {
					process.destroyForcibly();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			process = null;
		}
	}

	/**
	 * Starts the private mosquitto process again on the same port, after {@link #stop()}.
	 *
	 * @throws Exception if the broker cannot be started
	 */
	public void restart() throws Exception {
		stop();
		start(findMosquitto());
	}

	/**
	 * Launches mosquitto with this rule's config file and waits until it accepts connections.
	 *
	 * @param exe the mosquitto executable
	 * @throws Exception if the broker does not start listening within 10 seconds
	 */
	private void start(File exe) throws Exception {
		ProcessBuilder pb = new ProcessBuilder(exe.getAbsolutePath(), "-c", configFile.getAbsolutePath());
		pb.redirectErrorStream(true);
		File log = new File(configFile.getAbsolutePath() + ".log");
		pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
		process = pb.start();

		long deadline = System.currentTimeMillis() + 10_000;
		while (System.currentTimeMillis() < deadline) {
			if (!process.isAlive()) {
				throw new IllegalStateException("mosquitto exited with code " + process.exitValue()
						+ "; see " + log.getAbsolutePath() + describe(log));
			}
			try (Socket s = new Socket()) {
				s.connect(new InetSocketAddress("127.0.0.1", port), 200);
				return;
			} catch (IOException notYet) {
				Thread.sleep(50);
			}
		}
		throw new IllegalStateException("mosquitto did not start listening on port " + port + describe(log));
	}

	/**
	 * Returns the config file and the last lines of mosquitto's log, for an error message, so
	 * the reason mosquitto failed shows where the log file itself is not kept, such as in CI.
	 *
	 * @param log mosquitto's log file
	 * @return the text, starting with a line break
	 */
	private String describe(File log) {
		StringBuilder text = new StringBuilder();
		try {
			text.append("\n--- ").append(configFile.getName()).append(" ---\n")
					.append(new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8));
		} catch (IOException e) {
			text.append("\n(config file not readable: ").append(e).append(')');
		}
		try {
			List<String> lines = Files.readAllLines(log.toPath(), StandardCharsets.UTF_8);
			text.append("--- ").append(log.getName()).append(" ---");
			for (String line : lines.subList(Math.max(0, lines.size() - 30), lines.size())) {
				text.append('\n').append(line);
			}
		} catch (IOException e) {
			text.append("\n(log not readable: ").append(e).append(')');
		}
		return text.toString();
	}

	/**
	 * Locates the mosquitto executable.
	 *
	 * @return the executable, or {@code null} if it cannot be found
	 */
	private static File findMosquitto() {
		String configured = System.getProperty("mosquitto.path");
		if (configured != null) {
			File f = new File(configured);
			return f.isFile() ? f : null;
		}
		for (String location : DEFAULT_LOCATIONS) {
			File f = new File(location);
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

	/**
	 * Returns the directory for mosquitto's config and log: {@code target/mosquitto} in a Maven
	 * build, next to the test certificates, or {@code null} for the system temporary directory.
	 * On GitHub's Ubuntu runners mosquitto could not open a config file in {@code /tmp}.
	 *
	 * @return the directory, or {@code null}
	 */
	private static File configDirectory() {
		File target = new File("target");
		if (!target.isDirectory()) {
			return null;
		}
		File dir = new File(target, "mosquitto");
		return dir.isDirectory() || dir.mkdirs() ? dir : null;
	}

	/**
	 * Finds an unused local TCP port.
	 *
	 * @return a free port number
	 * @throws IOException if no port can be allocated
	 */
	private static int freePort() throws IOException {
		try (ServerSocket s = new ServerSocket(0)) {
			return s.getLocalPort();
		}
	}
}
