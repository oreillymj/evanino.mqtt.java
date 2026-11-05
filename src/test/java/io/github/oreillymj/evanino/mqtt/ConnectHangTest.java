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
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Connects that must end. The connection timeout covers the whole connect, to CONNACK: a server
 * that accepts TCP and then does not answer the TLS handshake, or never sends CONNACK, used to
 * hold the connect until keep-alive gave up (Paho issues 1071, 856, 711, 511; 1028 is the
 * same without a timeout). Also: disconnect() and close() during a hung connect (1071), a
 * refused connect (1020), and servers that close the connection quickly at different points
 * (1013). Fake brokers on loopback; needs no real broker.
 */
public class ConnectHangTest {

	/**
	 * 1071 and 1028: the server accepts the TCP connection and reads CONNECT but never answers.
	 * The connect fails after the connection timeout, and the client can then be closed.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testNoConnackFailsAfterConnectionTimeout() throws Exception {
		try (Silent server = new Silent(false)) {
			MqttAsyncClient client = newClient(server.port(), 2);
			long start = System.nanoTime();
			IMqttToken token = client.connect(options(server.port(), 2));
			MqttException failure = waitForFailure(token, 10_000);
			long ms = (System.nanoTime() - start) / 1_000_000;
			Assert.assertTrue("failed after " + ms + " ms", ms >= 1500 && ms < 6000);
			Assert.assertTrue(failure.toString(), hasCode(failure, MqttException.REASON_CODE_CLIENT_TIMEOUT));
			Assert.assertFalse(client.isConnected());
			Assert.assertFalse(client.isConnecting());
			client.close();
		}
	}

	/**
	 * 1028: with no connection timeout of the client's own, keep-alive still ends a connection
	 * whose CONNACK never comes: MQTT's keep-alive starts once CONNECT is sent, so with no
	 * answer within 1.5 times the interval the connection is dead.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testNoConnackEndsByKeepAlive() throws Exception {
		try (Silent server = new Silent(false)) {
			MqttAsyncClient client = newClient(server.port(), -1);
			MqttConnectOptions o = options(server.port(), -1);
			o.setKeepAliveInterval(2);
			long start = System.nanoTime();
			MqttException failure = waitForFailure(client.connect(o), 15_000);
			client.close();
		}
	}

	/**
	 * 1071 with TLS: the server accepts TCP but never answers the TLS handshake.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testStuckTlsHandshakeFailsAfterConnectionTimeout() throws Exception {
		try (Silent server = new Silent(true)) {
			String uri = "ssl://127.0.0.1:" + server.port();
			MqttAsyncClient client = new MqttAsyncClient(uri, "ConnectHang-tls");
			MqttConnectOptions o = TestClients.options(URI.create(uri), "ConnectHang-tls");
			o.setConnectionTimeout(2);
			long start = System.nanoTime();
			IMqttToken token = client.connect(o);
			MqttException failure = waitForFailure(token, 10_000);
			long ms = (System.nanoTime() - start) / 1_000_000;
			Assert.assertTrue("failed after " + ms + " ms", ms >= 1500 && ms < 6000);
			Assert.assertTrue(failure.toString(), hasCode(failure, MqttException.REASON_CODE_CLIENT_TIMEOUT));
			client.close();
		}
	}

	/**
	 * 1071: disconnect() and close() while the connect hangs work, and the token ends.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testDisconnectAndCloseDuringHungConnect() throws Exception {
		try (Silent server = new Silent(false)) {
			MqttAsyncClient client = newClient(server.port(), 60);
			IMqttToken connect = client.connect(options(server.port(), 60));
			Thread.sleep(500);
			Assert.assertTrue(client.isConnecting());
			client.disconnect().waitForCompletion(5000);
			waitForFailure(connect, 5000);
			Assert.assertFalse(client.isConnecting());

			IMqttToken again = client.connect(options(server.port(), 60));
			Thread.sleep(500);
			client.close();
			waitForFailure(again, 5000);
		}
	}

	/**
	 * 1020: connecting to a port where nothing listens fails promptly.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testRefusedConnectFails() throws Exception {
		int port;
		try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			port = s.getLocalPort();
		}
		MqttAsyncClient client = newClient(port, 2);
		try {
			long start = System.nanoTime();
			MqttException failure = waitForFailure(client.connect(options(port, 2)), 5000);
			try {
				new MqttClient("tcp://127.0.0.1:" + port, "ConnectHang-sync").connect(options(port, 2));
				Assert.fail("the synchronous connect did not throw");
			} catch (MqttException expected) {
			}
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * 1013: the server closes each connection quickly, at varying points (before reading
	 * CONNECT, after CONNECT, right after CONNACK, a few milliseconds after CONNACK). Every
	 * connect token completes, and when it succeeded the connection loss is reported.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testQuickCloseNeverLeavesConnectHanging() throws Exception {
		try (QuickClose server = new QuickClose()) {
			List<String> outcomes = new ArrayList<>();
			for (int i = 0; i < 80; i++) {
				server.mode = i % 4;
				server.delayMillis = (i / 4) % 5;
				MqttAsyncClient client = newClient(server.port(), 5);
				try {
					IMqttToken token = client.connect(options(server.port(), 5));
					try {
						token.waitForCompletion(10_000);
					} catch (MqttException e) {
						// a failed connect is fine; a hang is not
					}
					Assert.assertTrue("connect " + i + " (mode " + server.mode + ", delay " + server.delayMillis
							+ " ms) never completed", token.isComplete());
					outcomes.add(server.mode + ":" + (token.getException() == null ? "ok" : "failed"));
				} finally {
					TestClients.shutdownQuietly(client);
				}
			}
		}
	}

	private static MqttAsyncClient newClient(int port, int timeout) throws MqttException {
		return new MqttAsyncClient("tcp://127.0.0.1:" + port, "ConnectHang");
	}

	private static MqttConnectOptions options(int port, int timeout) {
		MqttConnectOptions o = TestClients.options(URI.create("tcp://127.0.0.1:" + port), "ConnectHang");
		o.setConnectionTimeout(timeout);
		return o;
	}

	private static boolean hasCode(Throwable t, int code) {
		for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
			if (c instanceof MqttException && ((MqttException) c).getReasonCode() == code) {
				return true;
			}
		}
		return false;
	}

	/** Waits for a token that must fail, and returns its exception. */
	private static MqttException waitForFailure(IMqttToken token, long millis) throws InterruptedException {
		long deadline = System.currentTimeMillis() + millis;
		while (!token.isComplete()) {
			if (System.currentTimeMillis() > deadline) {
				Assert.fail("the connect never completed");
			}
			Thread.sleep(20);
		}
		Assert.assertNotNull("the connect succeeded", token.getException());
		return token.getException();
	}

	/** Accepts connections and never answers; with {@code tls}, never answers the handshake. */
	private static final class Silent implements AutoCloseable {
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();

		Silent(boolean tls) throws IOException {
			server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
			Thread t = new Thread(() -> {
				try {
					while (true) {
						Socket s = server.accept();
						synchronized (sockets) {
							sockets.add(s);
						}
					}
				} catch (IOException closed) {
					// done
				}
			}, "silent-broker");
			t.setDaemon(true);
			t.start();
		}

		int port() {
			return server.getLocalPort();
		}

		@Override
		public void close() throws IOException {
			server.close();
			synchronized (sockets) {
				for (Socket s : sockets) {
					s.close();
				}
			}
		}
	}

	/**
	 * Closes each connection quickly. Mode 0: at once; 1: after reading CONNECT; 2: right
	 * after CONNACK; 3: {@code delayMillis} after CONNACK.
	 */
	private static final class QuickClose implements AutoCloseable {
		private final ServerSocket server;
		volatile int mode;
		volatile int delayMillis;

		QuickClose() throws IOException {
			server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
			Thread t = new Thread(() -> {
				try {
					while (true) {
						Socket s = server.accept();
						try {
							if (mode >= 1) {
								DataInputStream in = new DataInputStream(s.getInputStream());
								in.readUnsignedByte();
								int length = in.readUnsignedByte();
								in.readFully(new byte[length]);
							}
							if (mode >= 2) {
								s.getOutputStream().write(new byte[]{0x20, 0x02, 0x00, 0x00});
								s.getOutputStream().flush();
							}
							if (mode == 3) {
								Thread.sleep(delayMillis);
							}
						} catch (IOException | InterruptedException ignored) {
							// the client went away
						} finally {
							s.close();
						}
					}
				} catch (IOException closed) {
					// done
				}
			}, "quick-close-broker");
			t.setDaemon(true);
			t.start();
		}

		int port() {
			return server.getLocalPort();
		}

		@Override
		public void close() throws IOException {
			server.close();
		}
	}
}
