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
import org.junit.ClassRule;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Paho issues 1098 and 1099, with several clients in one JVM and the broker restarted:
 * <ul>
 *   <li>1098: the first client reconnects, but the others open the TCP connection and never send
 *   CONNECT, so the broker closes them after its timeout.</li>
 *   <li>1099: on the next attempt those clients send CONNECT, receive CONNACK and a queued
 *   message, then send a second CONNECT on the same connection (a protocol violation), and the
 *   broker closes the connection.</li>
 * </ul>
 * Here: ten clients with persistent sessions and automatic reconnect go through a relay that
 * records what each client sends on every connection. The relay goes down three times, a QoS 1
 * message is queued for every client while it is down, and the relay comes back. Every client
 * must reconnect and receive its message; on every connection the first packet must be CONNECT
 * and there must be only one CONNECT; and no connection may stay open without a CONNECT. Uses
 * {@link MosquittoBroker}.
 */
public class PahoIssue1098ReconnectManyClientsTest {

	/** The broker. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	private static final int CLIENTS = 10;
	private static final int OUTAGES = 3;

	/**
	 * Runs the outages and checks every connection the clients made.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testEveryClientReconnectsWithOneConnectPerConnection() throws Exception {
		try (InspectingRelay relay = new InspectingRelay(BROKER.getServerURI().getPort())) {
			java.net.URI relayUri = java.net.URI.create("tcp://127.0.0.1:" + relay.port());
			List<MqttAsyncClient> clients = new ArrayList<>();
			List<List<String>> received = new ArrayList<>();
			MqttAsyncClient direct = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue1098-direct");
			try {
				direct.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue1098-direct")).waitForCompletion(10_000);
				for (int i = 0; i < CLIENTS; i++) {
					String id = "PahoIssue1098-" + i;
					MqttConnectOptions o = TestClients.options(relayUri, id);
					o.setCleanSession(false);
					o.setAutomaticReconnect(true);
					o.setRetryInterval(1);
					o.setMaxReconnectDelay(2000);
					o.setConnectionTimeout(10);
					MqttAsyncClient c = TestClients.createMqttAsyncClient(relayUri, id);
					List<String> got = new CopyOnWriteArrayList<>();
					received.add(got);
					// A client-wide callback, not a per-subscription listener: those are dropped on
					// connection loss (see PersistentSessionListenerTest), which is a separate issue.
					c.setCallback(new MqttCallbackAdapter() {
						@Override
						public void messageArrived(String topic, MqttMessage message) {
							got.add(new String(message.getPayload(), StandardCharsets.UTF_8));
						}
					});
					c.connect(o).waitForCompletion(10_000);
					c.subscribe("PahoIssue1098/" + i, 1).waitForCompletion(10_000);
					clients.add(c);
				}

				for (int outage = 1; outage <= OUTAGES; outage++) {
					relay.down();
					waitFor("all clients to see the outage " + outage, 15_000, () -> clients.stream().noneMatch(MqttAsyncClient::isConnected));
					for (int i = 0; i < CLIENTS; i++) {
						direct.publish("PahoIssue1098/" + i, ("outage-" + outage).getBytes(StandardCharsets.UTF_8), 1, false)
								.waitForCompletion(10_000);
					}
					Thread.sleep(2000);   // let reconnect attempts fail while down
					relay.up();
					final int o = outage;
					try {
						waitFor("every client to reconnect and get its message after outage " + outage, 30_000, () -> {
							for (int i = 0; i < CLIENTS; i++) {
								if (!clients.get(i).isConnected() || !received.get(i).contains("outage-" + o)) {
									return false;
								}
							}
							return true;
						});
					} catch (AssertionError e) {
						StringBuilder state = new StringBuilder(e.getMessage());
						for (int i = 0; i < CLIENTS; i++) {
							state.append("\n  client ").append(i).append(": connected=").append(clients.get(i).isConnected())
									.append(" connecting=").append(clients.get(i).isConnecting())
									.append(" received=").append(received.get(i));
						}
						for (InspectingRelay.Connection c : relay.connections()) {
							state.append("\n  connection ").append(c.id).append(" types=").append(c.types())
									.append(" open_ms=").append(c.openWhileUpMillis());
						}
						throw new AssertionError(state.toString());
					}
				}
				Thread.sleep(1000);

				List<String> problems = new ArrayList<>();
				for (InspectingRelay.Connection c : relay.connections()) {
					List<Integer> types = c.types();
					if (types.isEmpty()) {
						if (c.openWhileUpMillis() > 3000) {
							problems.add("connection " + c.id + " stayed open " + c.openWhileUpMillis() + " ms without sending anything (1098)");
						}
						continue;
					}
					if (types.get(0) != 1) {
						problems.add("connection " + c.id + " started with packet type " + types.get(0));
					}
					long connects = types.stream().filter(t -> t == 1).count();
					if (connects > 1) {
						problems.add("connection " + c.id + " sent " + connects + " CONNECTs: " + types + " (1099)");
					}
				}
				System.out.println(relay.connections().size() + " connections relayed; problems: " + problems);
				Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
			} finally {
				for (MqttAsyncClient c : clients) {
					TestClients.shutdownQuietly(c);
				}
				TestClients.shutdownQuietly(direct);
			}
		}
	}

	private static void waitFor(String what, long millis, java.util.function.BooleanSupplier condition) throws InterruptedException {
		long deadline = System.currentTimeMillis() + millis;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				Assert.fail("timed out waiting for " + what);
			}
			Thread.sleep(50);
		}
	}

	/**
	 * A TCP relay to the broker that records the MQTT packet types each client sends on each
	 * connection. {@link #down()} closes the listener and every connection, as a broker restart
	 * does; {@link #up()} listens again on the same port.
	 */
	static final class InspectingRelay implements AutoCloseable {
		private final int brokerPort;
		private final int port;
		private final List<Connection> connections = new CopyOnWriteArrayList<>();
		private final AtomicInteger ids = new AtomicInteger();
		private volatile ServerSocket server;
		private volatile boolean isUp;

		InspectingRelay(int brokerPort) throws IOException {
			this.brokerPort = brokerPort;
			try (ServerSocket s = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
				port = s.getLocalPort();
			}
			up();
		}

		int port() {
			return port;
		}

		List<Connection> connections() {
			return Collections.unmodifiableList(connections);
		}

		synchronized void up() throws IOException {
			ServerSocket s = new ServerSocket();
			s.setReuseAddress(true);
			s.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 50);
			server = s;
			isUp = true;
			Thread t = new Thread(() -> accept(s), "relay-accept");
			t.setDaemon(true);
			t.start();
		}

		synchronized void down() {
			isUp = false;
			closeQuietly(server);
			for (Connection c : connections) {
				c.close();
			}
		}

		private void accept(ServerSocket s) {
			while (!s.isClosed()) {
				try {
					Socket client = s.accept();
					Connection c = new Connection(ids.incrementAndGet(), client);
					connections.add(c);
					Socket broker = new Socket("127.0.0.1", brokerPort);
					c.broker = broker;
					start(() -> c.relayFromClient(broker.getOutputStream()));
					start(() -> pipe(broker.getInputStream(), client.getOutputStream(), c));
				} catch (IOException e) {
					return;
				}
			}
		}

		private static void start(IoRunnable r) {
			Thread t = new Thread(() -> {
				try {
					r.run();
				} catch (IOException ignored) {
					// the connection was closed
				}
			}, "relay-pipe");
			t.setDaemon(true);
			t.start();
		}

		private static void pipe(InputStream in, OutputStream out, Connection c) throws IOException {
			byte[] buf = new byte[8192];
			try {
				int n;
				while ((n = in.read(buf)) >= 0) {
					out.write(buf, 0, n);
					out.flush();
				}
			} finally {
				c.close();
			}
		}

		@Override
		public void close() {
			down();
		}

		private static void closeQuietly(AutoCloseable c) {
			try {
				if (c != null) {
					c.close();
				}
			} catch (Exception ignored) {
				// closing
			}
		}

		interface IoRunnable {
			void run() throws IOException;
		}

		/** One relayed connection and what the client sent on it. */
		final class Connection {
			final int id;
			private final Socket client;
			private volatile Socket broker;
			private final List<Integer> types = new CopyOnWriteArrayList<>();
			private final long opened = System.currentTimeMillis();
			private volatile long closed;

			Connection(int id, Socket client) {
				this.id = id;
				this.client = client;
			}

			List<Integer> types() {
				return types;
			}

			/** How long the connection was open while the relay was up. */
			long openWhileUpMillis() {
				long end = closed != 0 ? closed : System.currentTimeMillis();
				return end - opened;
			}

			void relayFromClient(OutputStream toBroker) throws IOException {
				DataInputStream in = new DataInputStream(client.getInputStream());
				try {
					while (true) {
						int first = in.readUnsignedByte();
						int length = 0;
						int multiplier = 1;
						java.io.ByteArrayOutputStream header = new java.io.ByteArrayOutputStream();
						header.write(first);
						int b;
						do {
							b = in.readUnsignedByte();
							header.write(b);
							length += (b & 0x7f) * multiplier;
							multiplier *= 128;
						} while ((b & 0x80) != 0);
						byte[] body = new byte[length];
						in.readFully(body);
						types.add(first >> 4);
						toBroker.write(header.toByteArray());
						toBroker.write(body);
						toBroker.flush();
					}
				} finally {
					close();
				}
			}

			void close() {
				if (closed == 0) {
					closed = System.currentTimeMillis();
				}
				closeQuietly(client);
				closeQuietly(broker);
			}
		}
	}
}
