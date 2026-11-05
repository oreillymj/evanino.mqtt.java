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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Paho issues about sessions, subscriptions and connection handling:
 * <ul>
 *   <li>1036: close() while reconnecting gives a NullPointerException on the reconnect
 *   thread.</li>
 *   <li>1025: with file persistence and QoS 2 publishes made while offline, the client
 *   reconnects, publishes, disconnects again, in a loop, and never clears its messages.</li>
 *   <li>999 and 985: subscribing in connectComplete deadlocks, with messages queued for the
 *   session or with more than five topics.</li>
 *   <li>986: unsubscribe with per-subscription listeners throws
 *   ConcurrentModificationException.</li>
 *   <li>983: subscribe(String[], int[], IMqttMessageListener[]) calls itself until the stack
 *   overflows.</li>
 *   <li>1061: a long client ID makes MqttDefaultFilePersistence fail, as the directory name
 *   is longer than the file system allows.</li>
 *   <li>1059: with a keep-alive of 5 s, the next PINGREQ is scheduled later than 5 s.</li>
 *   <li>996: a PUBLISH whose topic contains U+0000 kills the connection
 *   (IllegalArgumentException: Invalid UTF-8 char).</li>
 * </ul>
 * Uses {@link UnlimitedMosquitto}, and fake brokers on loopback.
 */
public class PahoIssueSessionTest {

	/** The broker. */
	@ClassRule
	public static final UnlimitedMosquitto BROKER = new UnlimitedMosquitto();

	/** For file persistence. */
	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	/**
	 * 1036: close() while automatic reconnect is under way throws nothing, on any thread, and
	 * stops the attempts.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testCloseWhileReconnecting() throws Exception {
		List<Throwable> uncaught = new CopyOnWriteArrayList<>();
		Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> uncaught.add(e));
		ConnectionManipulationProxyServer proxy = new ConnectionManipulationProxyServer("127.0.0.1", BROKER.getServerURI().getPort(), 0);
		proxy.startProxy();
		URI uri = URI.create("tcp://127.0.0.1:" + proxy.getLocalPort());
		try {
			for (int i = 0; i < 10; i++) {
				proxy.enableProxy();
				MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "PahoIssue1036");
				MqttConnectOptions o = TestClients.options(uri, "PahoIssue1036");
				o.setAutomaticReconnect(true);
				o.setRetryInterval(1);
				o.setMaxReconnectDelay(200);
				o.setMaxRetries(-1);
				client.setCallback(new MqttCallbackAdapter());
				client.connect(o).waitForCompletion(10_000);
				proxy.disableProxy();
				long deadline = System.currentTimeMillis() + 10_000;
				while (!client.isReconnecting() && System.currentTimeMillis() < deadline) {
					Thread.sleep(10);
				}
				Thread.sleep(i * 37 % 300);   // close at different points of the cycle
				client.close();
				Assert.assertFalse(client.isReconnecting());
			}
			Thread.sleep(2000);
			Assert.assertTrue("uncaught: " + uncaught, uncaught.isEmpty());
		} finally {
			Thread.setDefaultUncaughtExceptionHandler(previous);
			proxy.stopProxy();
		}
	}

	/**
	 * 1025: QoS 2 publishes made while offline, with MqttDefaultFilePersistence and
	 * cleanSession=false, are all delivered once the broker can be reached, without the
	 * client dropping the connection again, and nothing is left in the store.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testOfflineQos2WithFilePersistence() throws Exception {
		ConnectionManipulationProxyServer proxy = new ConnectionManipulationProxyServer("127.0.0.1", BROKER.getServerURI().getPort(), 0);
		proxy.startProxy();
		URI uri = URI.create("tcp://127.0.0.1:" + proxy.getLocalPort());
		File dir = folder.newFolder("store");
		MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence(dir.getAbsolutePath());
		MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "PahoIssue1025", persistence);
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue1025-sub");
		AtomicInteger lost = new AtomicInteger();
		Set<String> received = ConcurrentHashMap.newKeySet();
		try {
			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void messageArrived(String topic, MqttMessage message) {
					received.add(new String(message.getPayload(), StandardCharsets.UTF_8));
				}
			});
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue1025-sub")).waitForCompletion(10_000);
			sub.subscribe("PahoIssue1025/t", 2).waitForCompletion(10_000);

			MqttConnectOptions o = TestClients.options(uri, "PahoIssue1025");
			o.setCleanSession(false);
			o.setAutomaticReconnect(true);
			o.setRetryInterval(1);
			o.setMaxReconnectDelay(1000);
			o.setMaxRetries(-1);
			client.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					lost.incrementAndGet();
				}
			});
			client.connect(o).waitForCompletion(10_000);
			proxy.disableProxy();
			while (client.isConnected()) {
				Thread.sleep(20);
			}
			List<IMqttDeliveryToken> tokens = new ArrayList<>();
			for (int i = 0; i < 200; i++) {
				tokens.add(client.publish("PahoIssue1025/t", ("m" + i).getBytes(StandardCharsets.UTF_8), 2, false));
			}
			int lostBefore = lost.get();
			proxy.enableProxy();
			for (IMqttDeliveryToken t : tokens) {
				t.waitForCompletion(30_000);
			}
			long deadline = System.currentTimeMillis() + 10_000;
			while (received.size() < 200 && System.currentTimeMillis() < deadline) {
				Thread.sleep(20);
			}
			Assert.assertEquals(200, received.size());
			Thread.sleep(2000);
			Assert.assertEquals("connection lost again after reconnecting", lostBefore, lost.get());
			Assert.assertTrue(client.isConnected());
			Assert.assertEquals(0, client.getInFlightMessageCount());
		} finally {
			TestClients.shutdownQuietly(client);
			TestClients.shutdownQuietly(sub);
			proxy.stopProxy();
		}
	}

	/**
	 * 999 and 985: a synchronous MqttClient subscribes to ten topics, one at a time, from
	 * connectComplete, while the broker has messages queued for its session. Nothing
	 * deadlocks, and the queued messages arrive.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSubscribeFromConnectComplete() throws Exception {
		String id = "PahoIssue999";
		MqttClient client = TestClients.createMqttClient(BROKER.getServerURI(), id);
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue999-pub");
		AtomicInteger subscribed = new AtomicInteger();
		AtomicReference<Throwable> error = new AtomicReference<>();
		Set<String> received = ConcurrentHashMap.newKeySet();
		try {
			MqttConnectOptions o = TestClients.options(BROKER.getServerURI(), id);
			o.setCleanSession(false);
			// a first session with the subscriptions, then messages queued while it is away
			client.connect(o);
			for (int i = 0; i < 10; i++) {
				client.subscribe("PahoIssue999/" + i, 1);
			}
			client.disconnect();
			pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue999-pub")).waitForCompletion(10_000);
			for (int i = 0; i < 50; i++) {
				pub.publish("PahoIssue999/" + (i % 10), ("q" + i).getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(10_000);
			}

			CountDownLatch done = new CountDownLatch(1);
			client.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectComplete(boolean reconnect, String serverURI) {
					try {
						for (int i = 0; i < 10; i++) {
							client.subscribe("PahoIssue999/" + i, 1);
							subscribed.incrementAndGet();
						}
					} catch (Throwable e) {
						error.set(e);
					} finally {
						done.countDown();
					}
				}

				@Override
				public void messageArrived(String topic, MqttMessage message) {
					received.add(new String(message.getPayload(), StandardCharsets.UTF_8));
				}
			});
			client.connect(o);
			Assert.assertTrue("subscribing from connectComplete hung after " + subscribed.get() + " topics",
					done.await(30, TimeUnit.SECONDS));
			Assert.assertNull(error.get());
			long deadline = System.currentTimeMillis() + 10_000;
			while (received.size() < 50 && System.currentTimeMillis() < deadline) {
				Thread.sleep(20);
			}
			Assert.assertEquals(50, received.size());
		} finally {
			TestClients.closeQuietly(client);
			TestClients.shutdownQuietly(pub);
		}
	}

	/**
	 * 986: unsubscribing, while messages for per-subscription listeners keep arriving, throws
	 * nothing.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testUnsubscribeWhileMessagesArrive() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue986");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue986-pub");
		AtomicBoolean running = new AtomicBoolean(true);
		AtomicReference<Throwable> error = new AtomicReference<>();
		try {
			client.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue986")).waitForCompletion(10_000);
			MqttConnectOptions po = TestClients.options(BROKER.getServerURI(), "PahoIssue986-pub");
			po.setMaxInflight(1000);
			pub.connect(po).waitForCompletion(10_000);
			Thread publisher = new Thread(() -> {
				int i = 0;
				while (running.get()) {
					try {
						pub.publish("PahoIssue986/" + (i++ % 20), new byte[]{1}, 0, false);
					} catch (MqttException e) {
						Thread.yield();
					}
				}
			});
			publisher.start();
			for (int round = 0; round < 50; round++) {
				List<IMqttToken> subs = new ArrayList<>();
				for (int t = 0; t < 20; t++) {
					subs.add(client.subscribe("PahoIssue986/" + t, 0, (topic, m) -> { }));
				}
				subs.add(client.subscribe("PahoIssue986/#", 0, (topic, m) -> { }));
				for (IMqttToken s : subs) {
					s.waitForCompletion(10_000);
				}
				try {
					client.unsubscribe("PahoIssue986/#").waitForCompletion(10_000);
					for (int t = 0; t < 20; t++) {
						client.unsubscribe("PahoIssue986/" + t).waitForCompletion(10_000);
					}
				} catch (Throwable e) {
					error.set(e);
					break;
				}
			}
			running.set(false);
			publisher.join(5000);
			Assert.assertNull("unsubscribe failed: " + error.get(), error.get());
			Assert.assertTrue(client.isConnected());
		} finally {
			running.set(false);
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * 983: every subscribe overload that takes arrays of listeners returns, without calling
	 * itself.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testListenerArraySubscribesDoNotRecurse() throws Exception {
		MqttAsyncClient async = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue983");
		MqttClient sync = TestClients.createMqttClient(BROKER.getServerURI(), "PahoIssue983-sync");
		try {
			async.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue983")).waitForCompletion(10_000);
			sync.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue983-sync"));
			IMqttMessageListener l = (t, m) -> { };
			String[] topics = {"PahoIssue983/a", "PahoIssue983/b"};
			int[] qos = {0, 1};
			IMqttMessageListener[] ls = {l, l};
			async.subscribe(topics, qos, ls).waitForCompletion(10_000);
			async.subscribe(topics, qos, null, null, ls).waitForCompletion(10_000);
			sync.subscribe(topics, qos, ls);
			sync.subscribe(topics, ls);
			sync.subscribe(topics[0], l);
			sync.subscribe(topics[0], 1, l);
		} catch (StackOverflowError e) {
			Assert.fail("subscribe recursed");
		} finally {
			TestClients.shutdownQuietly(async);
			TestClients.closeQuietly(sync);
		}
	}

	/**
	 * 1061: MqttDefaultFilePersistence works with a 1000-character client ID and a long URI.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testFilePersistenceWithLongClientId() throws Exception {
		char[] chars = new char[1000];
		Arrays.fill(chars, 'c');
		String id = new String(chars);
		File dir = folder.newFolder("long");
		MqttDefaultFilePersistence p = new MqttDefaultFilePersistence(dir.getAbsolutePath());
		p.open(id, "tcp://a-rather-long-broker-host-name.example.com:1883");
		p.put("s-1", new MqttPersistableImpl(new byte[]{1, 2}, new byte[]{3}));
		Assert.assertEquals(java.util.Collections.singletonList("s-1"), java.util.Collections.list(p.keys()));
		Assert.assertEquals(2, p.get("s-1").getHeaderLength());
		p.close();

		// and through the client, with a broker that accepts client IDs of any length
		MqttDefaultFilePersistence p2 = new MqttDefaultFilePersistence(folder.newFolder("long2").getAbsolutePath());
		MqttAsyncClient client = new MqttAsyncClient(BROKER.getServerURI().toString(), id, p2);
		try {
			MqttConnectOptions o = TestClients.options(BROKER.getServerURI(), id);
			o.setCleanSession(false);
			client.connect(o).waitForCompletion(10_000);
			client.publish("PahoIssue1061/t", new byte[]{1}, 1, false).waitForCompletion(10_000);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * 1059: with a keep-alive of 2 s and nothing else to send, PINGREQs go out no more than
	 * 2 s apart (and the first no more than 2 s after CONNECT).
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPingInterval() throws Exception {
		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			server.setSoTimeout(10_000);
			URI uri = URI.create("tcp://127.0.0.1:" + server.getLocalPort());
			MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "PahoIssue1059");
			try {
				MqttConnectOptions o = TestClients.options(uri, "PahoIssue1059");
				o.setKeepAliveInterval(2);
				client.connect(o);
				try (Socket s = server.accept()) {
					s.setSoTimeout(5000);
					DataInputStream in = new DataInputStream(s.getInputStream());
					OutputStream out = s.getOutputStream();
					readPacket(in);
					long last = System.nanoTime();
					out.write(new byte[]{0x20, 0x02, 0x00, 0x00});
					List<Long> gaps = new ArrayList<>();
					for (int i = 0; i < 4; i++) {
						int type = readPacket(in);
						long now = System.nanoTime();
						Assert.assertEquals("expected PINGREQ", 12, type);
						gaps.add((now - last) / 1_000_000);
						last = now;
						out.write(new byte[]{(byte) 0xd0, 0x00});   // PINGRESP
					}
					System.out.println("PINGREQ gaps (ms): " + gaps);
					for (long g : gaps) {
						Assert.assertTrue("PINGREQ gap " + g + " ms with keep-alive 2 s", g <= 2100);
					}
				}
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 996: a PUBLISH from the broker whose topic contains U+0000 (which the broker must not
	 * send) does not leave the connection silently broken: either the message is delivered, or
	 * the connection is closed at once and connectionLost is called. Netty's decoder rejects
	 * the topic; the client ignores the failed decode, and the decoder then discards everything
	 * after it, so the connection stays open but deaf until keep-alive closes it.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testTopicWithNulCharacter() throws Exception {
		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			server.setSoTimeout(10_000);
			URI uri = URI.create("tcp://127.0.0.1:" + server.getLocalPort());
			MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "PahoIssue996");
			List<String> events = new CopyOnWriteArrayList<>();
			try {
				client.setCallback(new MqttCallbackAdapter() {
					@Override
					public void messageArrived(String topic, MqttMessage message) {
						events.add("message on " + topic.replace("\0", "\\0"));
					}

					@Override
					public void connectionLost(Throwable cause) {
						events.add("connection lost: " + cause);
					}
				});
				MqttConnectOptions o = TestClients.options(uri, "PahoIssue996");
				o.setKeepAliveInterval(2);
				IMqttToken connect = client.connect(o);
				try (Socket s = server.accept()) {
					DataInputStream in = new DataInputStream(s.getInputStream());
					readPacket(in);
					OutputStream out = s.getOutputStream();
					out.write(new byte[]{0x20, 0x02, 0x00, 0x00});
					connect.waitForCompletion(5000);
					IMqttToken subscribe = client.subscribe("#", 0);
					byte[] sub = readBody(in);                       // SUBSCRIBE: answer with SUBACK
					out.write(new byte[]{(byte) 0x90, 0x03, sub[0], sub[1], 0x00});
					subscribe.waitForCompletion(5000);
					publish(out, "before");
					publish(out, "a\0b");
					publish(out, "after");
					out.flush();
					long sent = System.nanoTime();
					long deadline = System.currentTimeMillis() + 10_000;
					while (events.size() < 2 && System.currentTimeMillis() < deadline) {
						Thread.sleep(20);
					}
					long ms = (System.nanoTime() - sent) / 1_000_000;
					System.out.println("topic with U+0000: " + events + " after " + ms + " ms (keep-alive 2 s)");
					Assert.assertTrue("the control message did not arrive: " + events, events.contains("message on before"));
					Assert.assertTrue("nothing reported after the topic with U+0000: " + events, events.size() >= 2);
					// Either the message is delivered (and the next one too), or the connection is
					// closed at once; not left open but deaf until keep-alive gives up.
					Assert.assertTrue("only reported after " + ms + " ms: " + events, ms < 1000);
				}
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 1030: the broker sends a packet the client cannot decode (here reserved type 0). Paho
	 * closes the connection with REASON_CODE_INVALID_MESSAGE. The connection must not stay
	 * open but deaf: it is closed at once and connectionLost is called.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testUndecodablePacket() throws Exception {
		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			server.setSoTimeout(10_000);
			URI uri = URI.create("tcp://127.0.0.1:" + server.getLocalPort());
			MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "PahoIssue1030");
			List<String> events = new CopyOnWriteArrayList<>();
			try {
				client.setCallback(new MqttCallbackAdapter() {
					@Override
					public void connectionLost(Throwable cause) {
						events.add("connection lost: " + cause);
					}
				});
				MqttConnectOptions o = TestClients.options(uri, "PahoIssue1030");
				o.setKeepAliveInterval(2);
				IMqttToken connect = client.connect(o);
				try (Socket s = server.accept()) {
					DataInputStream in = new DataInputStream(s.getInputStream());
					readPacket(in);
					OutputStream out = s.getOutputStream();
					out.write(new byte[]{0x20, 0x02, 0x00, 0x00});
					connect.waitForCompletion(5000);
					out.write(new byte[]{0x00, 0x00});   // reserved packet type 0
					out.flush();
					long sent = System.nanoTime();
					long deadline = System.currentTimeMillis() + 10_000;
					while (events.isEmpty() && System.currentTimeMillis() < deadline) {
						Thread.sleep(20);
					}
					long ms = (System.nanoTime() - sent) / 1_000_000;
					System.out.println("undecodable packet: " + events + " after " + ms + " ms (keep-alive 2 s)");
					Assert.assertFalse("nothing reported", events.isEmpty());
					Assert.assertTrue("only reported after " + ms + " ms", ms < 1000);
				}
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/** Writes a QoS 0 PUBLISH with a one-byte payload. */
	private static void publish(OutputStream out, String topic) throws java.io.IOException {
		byte[] t = topic.getBytes(StandardCharsets.UTF_8);
		out.write(new byte[]{0x30, (byte) (2 + t.length + 1), 0x00, (byte) t.length});
		out.write(t);
		out.write('x');
	}

	/** Reads one MQTT packet; returns its variable header and payload. */
	private static byte[] readBody(DataInputStream in) throws java.io.IOException {
		in.readUnsignedByte();
		int length = 0;
		int multiplier = 1;
		int b;
		do {
			b = in.readUnsignedByte();
			length += (b & 0x7f) * multiplier;
			multiplier *= 128;
		} while ((b & 0x80) != 0);
		byte[] body = new byte[length];
		in.readFully(body);
		return body;
	}

	/** Reads one MQTT packet; returns its type. */
	private static int readPacket(DataInputStream in) throws java.io.IOException {
		int first = in.readUnsignedByte();
		int length = 0;
		int multiplier = 1;
		int b;
		do {
			b = in.readUnsignedByte();
			length += (b & 0x7f) * multiplier;
			multiplier *= 128;
		} while ((b & 0x80) != 0);
		in.readFully(new byte[length]);
		return first >> 4;
	}
}
