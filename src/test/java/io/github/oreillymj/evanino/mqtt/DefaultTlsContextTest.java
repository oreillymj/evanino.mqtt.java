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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A TLS scheme ({@code mqtts://}, {@code ssl://}, {@code wss://}, {@code https://}) without
 * {@code setSslContext} uses the JVM's default TLS context, as Paho does with no socket factory,
 * so the {@code javax.net.ssl.*} system properties apply. Before, such a connection was made
 * without TLS. Requires a broker with TLS listeners for the last two tests; see
 * {@link MosquittoBroker}.
 */
public class DefaultTlsContextTest {

	/** The broker used by the tests that need one. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** The first byte of a TLS handshake record. */
	private static final int TLS_HANDSHAKE = 0x16;

	/** The first byte of an MQTT CONNECT packet. */
	private static final int MQTT_CONNECT = 0x10;

	private static int sslPort;
	private static int websocketPort;

	/**
	 * Picks up the TLS and secure WebSocket ports.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		sslPort = broker.getSslServerURI().getPort();
		websocketPort = broker.getSecureWebSocketServerURI().getPort();
	}

	/**
	 * {@code mqtts://} and its alias {@code ssl://} start with a TLS handshake when no context is
	 * set.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	public void testTlsSchemesUseTlsWithoutContext() throws Exception {
		Assert.assertEquals(TLS_HANDSHAKE, firstByteSent("mqtts"));
		Assert.assertEquals(TLS_HANDSHAKE, firstByteSent("ssl"));
	}

	/**
	 * {@code wss://} and its alias {@code https://} start with a TLS handshake when no context
	 * is set, rather than a clear-text upgrade request.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	public void testSecureWebSocketSchemesUseTlsWithoutContext() throws Exception {
		Assert.assertEquals(TLS_HANDSHAKE, firstByteSent("wss"));
		Assert.assertEquals(TLS_HANDSHAKE, firstByteSent("https"));
	}

	/**
	 * Plain schemes are unchanged: {@code tcp://} sends CONNECT in the clear and {@code ws://}
	 * sends a clear-text upgrade request.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	public void testPlainSchemesStayPlain() throws Exception {
		Assert.assertEquals(MQTT_CONNECT, firstByteSent("tcp"));
		Assert.assertEquals('G', firstByteSent("ws"));   // GET /mqtt HTTP/1.1
	}

	/**
	 * Without a context and without the test CA in the JVM's trust store, the handshake with
	 * the broker fails on the certificate, which shows that the default context is used.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testDefaultContextRejectsUntrustedBroker() throws Exception {
		Assume.assumeTrue("javax.net.ssl.trustStore is set for this JVM", System.getProperty("javax.net.ssl.trustStore") == null);
		MqttClient mqttClient = new MqttClient(TestClients.options(broker.getSslServerURI(), "DefaultTlsContextTest-" + UUID.randomUUID()));
		mqttClient.setTimeToWait(5000);
		try {
			mqttClient.connect(mqttClient.getOptions());
			Assert.fail("The JVM's default trust store should not trust the test CA");
		} catch (MqttException ex) {
			for (Throwable t = ex; t != null && t.getCause() != t; t = t.getCause()) {
				if (t instanceof SSLHandshakeException) {
					return;
				}
			}
			throw new AssertionError("Expected an SSLHandshakeException in the cause chain of " + ex, ex);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * In a JVM started with Paho's usual {@code javax.net.ssl.keyStore} and
	 * {@code javax.net.ssl.trustStore} system properties, a client with no context connects to
	 * the broker over TLS and secure WebSockets, presenting the client certificate the broker
	 * requires. Runs {@link Connect} in a child JVM so the properties do not affect other tests.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testSystemPropertiesApplyWithoutContext() throws Exception {
		File keystore = TestTls.file("client-keystore.jks");
		File truststore = TestTls.file("client-truststore.jks");
		Assume.assumeTrue("test TLS keystores not on the classpath", keystore != null && truststore != null);

		List<String> command = new ArrayList<>();
		command.add(new File(System.getProperty("java.home"), "bin" + File.separator + "java").getPath());
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add("-Djavax.net.ssl.keyStore=" + keystore.getAbsolutePath());
		command.add("-Djavax.net.ssl.keyStorePassword=" + TestTls.PASSWORD);
		command.add("-Djavax.net.ssl.trustStore=" + truststore.getAbsolutePath());
		command.add("-Djavax.net.ssl.trustStorePassword=" + TestTls.PASSWORD);
		command.add(Connect.class.getName());
		command.add("ssl://localhost:" + sslPort);
		command.add("wss://localhost:" + websocketPort);

		Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try (InputStream in = process.getInputStream()) {
			byte[] buffer = new byte[4096];
			for (int n; (n = in.read(buffer)) != -1; ) {
				output.write(buffer, 0, n);
			}
		}
		Assert.assertTrue("child JVM did not finish", process.waitFor(30, TimeUnit.SECONDS));
		Assert.assertEquals(new String(output.toByteArray(), StandardCharsets.UTF_8), 0, process.exitValue());
	}

	/**
	 * Connects a client with no TLS context to a local socket and returns the first byte the
	 * client sends.
	 *
	 * @param scheme the URI scheme
	 * @return the first byte, or -1 if the client sent nothing
	 * @throws Exception if the test fails unexpectedly
	 */
	private static int firstByteSent(String scheme) throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
			server.setSoTimeout(5000);
			Future<Integer> firstByte = executor.submit(() -> {
				try (Socket socket = server.accept()) {
					return socket.getInputStream().read();
				}
			});
			MqttConnectOptions options = TestClients.options(
					new URI(scheme + "://127.0.0.1:" + server.getLocalPort()), "DefaultTlsContextTest-" + UUID.randomUUID());
			MqttClient mqttClient = new MqttClient(options);
			mqttClient.setTimeToWait(5000);
			try {
				mqttClient.connect(options);
				Assert.fail("The local socket is not a broker");
			} catch (MqttException expected) {
				// the socket closes after the first byte
			} finally {
				TestClients.closeQuietly(mqttClient);
			}
			return firstByte.get(5, TimeUnit.SECONDS);
		} finally {
			executor.shutdownNow();
		}
	}

	/**
	 * Child JVM entry point for {@link #testSystemPropertiesApplyWithoutContext()}: connects to
	 * each URI given, without a TLS context, and disconnects. Exits with 0 if every connection
	 * succeeds and 1 otherwise.
	 */
	public static final class Connect {

		/**
		 * Prevents instantiation; only {@link #main(String[])} is used.
		 */
		private Connect() {
		}

		/**
		 * Connects to each broker URI in turn.
		 *
		 * @param args the broker URIs
		 */
		public static void main(String[] args) {
			int status = 0;
			for (String uri : args) {
				MqttClient client = null;
				try {
					client = new MqttClient(uri, MqttClient.generateClientId());
					client.setTimeToWait(10_000);
					client.connect(client.getOptions());
					client.disconnect();
					System.out.println("connected: " + uri);
				} catch (Exception e) {
					System.out.println("failed: " + uri);
					e.printStackTrace(System.out);
					status = 1;
				} finally {
					TestClients.closeQuietly(client);
				}
			}
			System.exit(status);
		}
	}
}
