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
import java.net.SocketException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The state of a client that is reconnecting automatically, and {@code disconnect()} while it
 * does (Paho issue 1079: Paho's {@code disconnect()} then fails with "client is connecting",
 * and nothing shows that reconnect attempts are going on). Uses a fake broker that accepts a
 * number of connections with a CONNACK and closes the others at once. Not part of Paho's suite;
 * needs no broker.
 */
public class ReconnectingStateTest {

	/**
	 * Between two reconnect attempts {@link MqttAsyncClient#isReconnecting()} is {@code true}
	 * while {@link MqttAsyncClient#isConnecting()} is not; {@code disconnect()} then does not
	 * throw, completes, stops the attempts and clears the state.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testDisconnectWhileReconnecting() throws Exception {
		try (FakeBroker broker = new FakeBroker()) {
			MqttAsyncClient client = connect(broker, -1);
			try {
				Assert.assertFalse(client.isReconnecting());
				broker.dropAccepted();
				waitFor(() -> broker.refused.get() >= 2, 15_000);
				waitFor(() -> !client.isConnecting(), 5000);   // between attempts
				Assert.assertTrue("isReconnecting() between attempts", client.isReconnecting());
				Assert.assertFalse(client.isConnected());

				IMqttToken disconnect = client.disconnect();
				disconnect.waitForCompletion(5000);
				Assert.assertTrue(disconnect.isComplete());
				Assert.assertFalse(client.isReconnecting());
				int attempts = broker.refused.get();
				Thread.sleep(3000);
				Assert.assertEquals("reconnect attempts after disconnect()", attempts, broker.refused.get());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * Once a reconnect succeeds, isReconnecting() is {@code false} again.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testReconnectingEndsWithTheReconnect() throws Exception {
		try (FakeBroker broker = new FakeBroker()) {
			MqttAsyncClient client = connect(broker, -1);
			try {
				broker.dropAccepted();
				broker.acceptAfterRefusals(1);
				waitFor(client::isReconnecting, 5000);
				waitFor(client::isConnected, 15_000);
				Assert.assertFalse(client.isReconnecting());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * When the retries run out, isReconnecting() is {@code false}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testReconnectingEndsWhenRetriesRunOut() throws Exception {
		try (FakeBroker broker = new FakeBroker()) {
			MqttAsyncClient client = connect(broker, 2);
			try {
				broker.dropAccepted();
				waitFor(client::isReconnecting, 5000);
				waitFor(() -> !client.isReconnecting(), 20_000);
				Assert.assertFalse(client.isConnected());
				int attempts = broker.refused.get();
				Thread.sleep(3000);
				Assert.assertEquals(attempts, broker.refused.get());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	private static MqttAsyncClient connect(FakeBroker broker, int maxRetries) throws Exception {
		URI uri = URI.create("tcp://127.0.0.1:" + broker.port());
		MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "ReconnectingState");
		MqttConnectOptions o = TestClients.options(uri, "ReconnectingState");
		o.setAutomaticReconnect(true);
		o.setRetryInterval(1);
		o.setMaxReconnectDelay(1000);
		o.setMaxRetries(maxRetries);
		o.setConnectionTimeout(2);
		client.setCallback(new MqttCallbackAdapter());
		client.connect(o).waitForCompletion(5000);
		Assert.assertTrue(client.isConnected());
		return client;
	}

	private static void waitFor(java.util.function.BooleanSupplier condition, long millis) throws InterruptedException {
		long deadline = System.currentTimeMillis() + millis;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				Assert.fail("timed out");
			}
			Thread.sleep(20);
		}
	}

	/**
	 * Answers the first connection with a CONNACK and closes later ones at once, until told to
	 * accept again after a number of refusals.
	 */
	private static final class FakeBroker implements AutoCloseable {
		private final ServerSocket server;
		private volatile Socket accepted;
		private volatile int acceptAfter = Integer.MAX_VALUE;
		final AtomicInteger refused = new AtomicInteger();

		FakeBroker() throws IOException {
			server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
			Thread t = new Thread(this::run, "fake-broker");
			t.setDaemon(true);
			t.start();
		}

		int port() {
			return server.getLocalPort();
		}

		void dropAccepted() throws IOException {
			accepted.close();
		}

		/** Accepts the next connection after this many more refusals. */
		void acceptAfterRefusals(int n) {
			acceptAfter = refused.get() + n;
		}

		private void run() {
			try {
				boolean first = true;
				while (true) {
					Socket s = server.accept();
					if (first || refused.get() >= acceptAfter) {
						first = false;
						acceptAfter = Integer.MAX_VALUE;
						DataInputStream in = new DataInputStream(s.getInputStream());
						in.readUnsignedByte();
						int length = in.readUnsignedByte();   // a short CONNECT: one length byte
						in.readFully(new byte[length]);
						// Before the CONNACK: once it is sent, the client can connect and the
						// test can call dropAccepted() before this thread runs again.
						accepted = s;
						s.getOutputStream().write(new byte[]{0x20, 0x02, 0x00, 0x00});
					} else {
						refused.incrementAndGet();
						s.close();
					}
				}
			} catch (SocketException closed) {
				// the test is over
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void close() throws IOException {
			server.close();
		}
	}
}
