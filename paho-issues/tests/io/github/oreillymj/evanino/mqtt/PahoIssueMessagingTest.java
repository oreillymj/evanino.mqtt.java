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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Older Paho issues about subscriptions, topics and messages. Each test names the issues it
 * covers. Uses {@link MosquittoBroker} (which has a WebSocket listener) and {@link RawBroker}.
 */
public class PahoIssueMessagingTest {

	/** The broker. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/**
	 * 965, 827, 367: a shared subscription ($share/group/topic, which mosquitto also offers to
	 * MQTT 3.1.1 clients) delivers to the listener given with it, though messages arrive on the
	 * plain topic.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSharedSubscriptionListener() throws Exception {
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueShare-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueShare-pub");
		CountDownLatch got = new CountDownLatch(1);
		AtomicInteger fallback = new AtomicInteger();
		try {
			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void messageArrived(String topic, MqttMessage message) {
					fallback.incrementAndGet();
				}
			});
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssueShare-sub")).waitForCompletion(10_000);
			sub.subscribe("$share/group/PahoIssueShare/t", 1, (t, m) -> got.countDown()).waitForCompletion(10_000);
			pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssueShare-pub")).waitForCompletion(10_000);
			pub.publish("PahoIssueShare/t", new byte[]{1}, 1, false).waitForCompletion(10_000);
			boolean ok = got.await(5, TimeUnit.SECONDS);
			System.out.println("shared subscription: listener=" + ok + " client-wide callback=" + fallback.get());
			Assert.assertTrue("the shared subscription's listener was not called (client-wide callback: "
					+ fallback.get() + ")", ok);
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}

	/**
	 * 773, 378: two listeners, on hello/1/world and hello/+/world, and two listeners on the same
	 * topic. One message: each listener is called exactly once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testOverlappingSubscriptions() throws Exception {
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue773-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue773-pub");
		AtomicInteger a = new AtomicInteger();
		AtomicInteger b = new AtomicInteger();
		AtomicInteger c1 = new AtomicInteger();
		AtomicInteger c2 = new AtomicInteger();
		try {
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue773-sub")).waitForCompletion(10_000);
			sub.subscribe("PahoIssue773/1/world", 1, (t, m) -> a.incrementAndGet()).waitForCompletion(10_000);
			sub.subscribe("PahoIssue773/+/world", 1, (t, m) -> b.incrementAndGet()).waitForCompletion(10_000);
			sub.subscribe("PahoIssue773/same", 1, (t, m) -> c1.incrementAndGet()).waitForCompletion(10_000);
			sub.subscribe("PahoIssue773/same", 1, (t, m) -> c2.incrementAndGet()).waitForCompletion(10_000);
			pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue773-pub")).waitForCompletion(10_000);
			pub.publish("PahoIssue773/1/world", new byte[]{1}, 1, false).waitForCompletion(10_000);
			pub.publish("PahoIssue773/same", new byte[]{1}, 1, false).waitForCompletion(10_000);
			Thread.sleep(1500);
			System.out.println("overlapping: exact=" + a + " wildcard=" + b + " same-topic listeners=" + c1 + "," + c2);
			Assert.assertEquals("exact-topic listener calls", 1, a.get());
			Assert.assertEquals("wildcard listener calls", 1, b.get());
			Assert.assertEquals("first listener on the same topic", 1, c1.get());
			Assert.assertEquals("second listener on the same topic", 1, c2.get());
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}

	/**
	 * 918, 882: a listener on foo/+/bar/# gets messages on foo/x/bar (# also matches the
	 * parent level) and on foo/x/bar/baz.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testWildcardListenerMatching() throws Exception {
		Assert.assertTrue(MqttTopic.isMatched("foo/+/bar/#", "foo/x/bar"));
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue918-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue918-pub");
		List<String> got = new CopyOnWriteArrayList<>();
		try {
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue918-sub")).waitForCompletion(10_000);
			sub.subscribe("PahoIssue918/+/bar/#", 1, (t, m) -> got.add(t)).waitForCompletion(10_000);
			pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue918-pub")).waitForCompletion(10_000);
			pub.publish("PahoIssue918/x/bar", new byte[]{1}, 1, false).waitForCompletion(10_000);
			pub.publish("PahoIssue918/x/bar/baz", new byte[]{1}, 1, false).waitForCompletion(10_000);
			Thread.sleep(1500);
			Assert.assertEquals(java.util.Arrays.asList("PahoIssue918/x/bar", "PahoIssue918/x/bar/baz"), got);
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}

	/**
	 * 818, 836, 724: topics with characters outside the Basic Multilingual Plane (surrogate
	 * pairs in Java), such as emoji, round-trip.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSurrogatePairsInTopic() throws Exception {
		String topic = "PahoIssue818/😀/𠜎/é";
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue818-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue818-pub");
		List<String> got = new CopyOnWriteArrayList<>();
		try {
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue818-sub")).waitForCompletion(10_000);
			sub.subscribe("PahoIssue818/#", 1, (t, m) -> got.add(t)).waitForCompletion(10_000);
			pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue818-pub")).waitForCompletion(10_000);
			pub.publish(topic, "x".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(10_000);
			Thread.sleep(1500);
			Assert.assertEquals(java.util.Collections.singletonList(topic), got);
			Assert.assertTrue(sub.isConnected());
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}

	/**
	 * 880, 481: a CONNACK refusing bad credentials (4) or authorisation (5) makes the
	 * synchronous connect throw MqttSecurityException, with that reason code.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testRefusedCredentialsGiveSecurityException() throws Exception {
		for (int rc : new int[]{4, 5}) {
			try (RawBroker broker = new RawBroker()) {
				MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue880");
				try {
					IMqttToken t = client.connect(TestClients.options(broker.uri(), "PahoIssue880"));
					broker.acceptAndConnack(rc);
					try {
						t.waitForCompletion(5000);
						Assert.fail("connect succeeded");
					} catch (MqttException e) {
						System.out.println("CONNACK " + rc + ": " + e.getClass().getSimpleName() + " " + e);
						Assert.assertTrue("CONNACK " + rc + " gave " + e.getClass().getName(), e instanceof MqttSecurityException);
						Assert.assertEquals(rc, e.getReasonCode());
					}
				} finally {
					TestClients.shutdownQuietly(client);
				}
			}
		}
	}

	/**
	 * 595: a QoS 1 message that arrives before the client has subscribed to anything (for
	 * example from its session) is still acknowledged.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testUnsolicitedQos1IsAcknowledged() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue595");
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "PahoIssue595");
				o.setCleanSession(false);
				IMqttToken t = client.connect(o);
				broker.acceptAndConnack(0);
				t.waitForCompletion(5000);
				broker.publish("PahoIssue595/t", 1, 7, "x");
				RawBroker.Packet ack = broker.readUntil(4);
				Assert.assertEquals(7, ack.packetId());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 344: a broken broker sends a QoS 1 PUBLISH with packet identifier 0 (not allowed). The
	 * client is not left unable to work: it either handles the message or closes the
	 * connection at once, and a well-formed message afterwards (on a new connection if need
	 * be) still arrives.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPacketIdZero() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue344");
			List<String> events = new CopyOnWriteArrayList<>();
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "PahoIssue344");
				o.setKeepAliveInterval(2);
				client.setCallback(new MqttCallbackAdapter() {
					@Override
					public void messageArrived(String topic, MqttMessage message) {
						events.add("message " + new String(message.getPayload(), StandardCharsets.UTF_8));
					}

					@Override
					public void connectionLost(Throwable cause) {
						events.add("lost");
					}
				});
				IMqttToken t = client.connect(o);
				broker.acceptAndConnack(0);
				t.waitForCompletion(5000);
				IMqttToken s = client.subscribe("#", 1);
				broker.suback(broker.readUntil(8));
				s.waitForCompletion(5000);
				broker.publish("PahoIssue344/t", 1, 0, "zero");
				broker.publish("PahoIssue344/t", 1, 8, "next");
				long start = System.nanoTime();
				long deadline = System.currentTimeMillis() + 10_000;
				while (!(events.contains("message next") || events.contains("lost")) && System.currentTimeMillis() < deadline) {
					Thread.sleep(20);
				}
				long ms = (System.nanoTime() - start) / 1_000_000;
				System.out.println("packet id 0: " + events + " after " + ms + " ms (keep-alive 2 s)");
				Assert.assertTrue("only reported after " + ms + " ms: " + events, ms < 1000);
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 394: a broken broker sends a PUBLISH with QoS 3 (not allowed). The connection is closed
	 * at once rather than left open and deaf.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testQos3Publish() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue394");
			List<String> events = new CopyOnWriteArrayList<>();
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "PahoIssue394");
				o.setKeepAliveInterval(2);
				client.setCallback(new MqttCallbackAdapter() {
					@Override
					public void connectionLost(Throwable cause) {
						events.add("lost");
					}
				});
				IMqttToken t = client.connect(o);
				broker.acceptAndConnack(0);
				t.waitForCompletion(5000);
				broker.write(0x36, 0x05, 0x00, 0x01, 'a', 0x00, 0x01);   // QoS 3
				long start = System.nanoTime();
				long deadline = System.currentTimeMillis() + 10_000;
				while (events.isEmpty() && System.currentTimeMillis() < deadline) {
					Thread.sleep(20);
				}
				long ms = (System.nanoTime() - start) / 1_000_000;
				System.out.println("QoS 3 publish: " + events + " after " + ms + " ms (keep-alive 2 s)");
				Assert.assertTrue("only reported after " + ms + " ms", ms < 1000);
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 501: messages of 100 KB arrive over TCP and over WebSocket with the default options.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testLargeMessages() throws Exception {
		for (URI uri : new URI[]{BROKER.getServerURI(), BROKER.getWebSocketServerURI()}) {
			MqttAsyncClient sub = new MqttAsyncClient(uri.toString(), "PahoIssue501-sub");
			MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue501-pub");
			CountDownLatch got = new CountDownLatch(1);
			AtomicInteger lost = new AtomicInteger();
			try {
				sub.setCallback(new MqttCallbackAdapter() {
					@Override
					public void connectionLost(Throwable cause) {
						lost.incrementAndGet();
					}
				});
				sub.connect(TestClients.options(uri, "PahoIssue501-sub")).waitForCompletion(10_000);
				sub.subscribe("PahoIssue501/t", 1, (t, m) -> {
					if (m.getPayload().length == 100_000) {
						got.countDown();
					}
				}).waitForCompletion(10_000);
				pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue501-pub")).waitForCompletion(10_000);
				pub.publish("PahoIssue501/t", new byte[100_000], 1, false).waitForCompletion(10_000);
				boolean ok = got.await(10, TimeUnit.SECONDS);
				System.out.println("100 KB message over " + uri.getScheme() + ": received=" + ok + " connection lost=" + lost.get());
				Assert.assertTrue("100 KB message not received over " + uri.getScheme(), ok);
			} finally {
				TestClients.shutdownQuietly(pub);
				TestClients.shutdownQuietly(sub);
			}
		}
	}

	/**
	 * 501, continued: what the same 100 KB message does with setMaxBytesInMessage raised to
	 * 1 MB, over TCP and over WebSocket (which also has a 64 KB frame limit of its own).
	 * Prints the outcomes; fails if any is lost.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testLargeMessagesWithRaisedLimit() throws Exception {
		StringBuilder failures = new StringBuilder();
		for (URI uri : new URI[]{BROKER.getServerURI(), BROKER.getWebSocketServerURI()}) {
			for (int max : new int[]{65536, 1 << 20}) {
				MqttAsyncClient sub = new MqttAsyncClient(uri.toString(), "PahoIssue501b-sub");
				MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue501b-pub");
				CountDownLatch got = new CountDownLatch(1);
				long[] lostAt = {0};
				try {
					sub.setCallback(new MqttCallbackAdapter() {
						@Override
						public void connectionLost(Throwable cause) {
							lostAt[0] = System.nanoTime();
						}
					});
					MqttConnectOptions o = TestClients.options(uri, "PahoIssue501b-sub");
					o.setMaxBytesInMessage(max);
					sub.connect(o).waitForCompletion(10_000);
					sub.subscribe("PahoIssue501b/t", 1, (t, m) -> got.countDown()).waitForCompletion(10_000);
					pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue501b-pub")).waitForCompletion(10_000);
					long sent = System.nanoTime();
					pub.publish("PahoIssue501b/t", new byte[100_000], 1, false).waitForCompletion(10_000);
					boolean ok = got.await(5, TimeUnit.SECONDS);
					String outcome = uri.getScheme() + " max=" + max + ": received=" + ok
							+ (lostAt[0] != 0 ? " lost after " + (lostAt[0] - sent) / 1_000_000 + " ms" : " no connectionLost")
							+ " connected=" + sub.isConnected();
					System.out.println("100 KB " + outcome);
					if (!ok) {
						failures.append(outcome).append("; ");
					}
				} finally {
					TestClients.shutdownQuietly(pub);
					TestClients.shutdownQuietly(sub);
				}
			}
		}
		Assert.assertEquals("", failures.toString());
	}

	/**
	 * 704, 617: subscribing to an invalid filter (a/#/b) fails at once with an exception the
	 * caller sees, rather than reaching the broker.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testInvalidFilter() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue704");
		AtomicInteger lost = new AtomicInteger();
		try {
			client.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					lost.incrementAndGet();
				}
			});
			client.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue704")).waitForCompletion(10_000);
			Throwable thrown = null;
			try {
				client.subscribe("PahoIssue704/#/b", 1).waitForCompletion(10_000);
			} catch (Throwable e) {
				thrown = e;
			}
			Thread.sleep(500);
			System.out.println("invalid filter: thrown=" + thrown + " connected=" + client.isConnected() + " lost=" + lost.get());
			Assert.assertTrue("an invalid filter was sent: thrown=" + thrown + ", connection lost=" + lost.get(),
					thrown instanceof IllegalArgumentException);
			Assert.assertTrue(client.isConnected());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * 461: when another client connects with the same client ID, the broker closes the first
	 * client's connection, and its connectionLost is called.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testTakeOverCallsConnectionLost() throws Exception {
		MqttAsyncClient first = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue461");
		MqttAsyncClient second = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue461");
		CountDownLatch lost = new CountDownLatch(1);
		try {
			first.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					lost.countDown();
				}
			});
			first.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue461")).waitForCompletion(10_000);
			second.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue461")).waitForCompletion(10_000);
			Assert.assertTrue("connectionLost not called on take-over", lost.await(10, TimeUnit.SECONDS));
		} finally {
			TestClients.shutdownQuietly(second);
			TestClients.shutdownQuietly(first);
		}
	}

	/**
	 * 869: credentials that expire (a JWT as password) can be replaced before an automatic
	 * reconnect: a new password set on the options in connectionLost is sent in the next
	 * CONNECT.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPasswordChangedBeforeReconnect() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue869");
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "PahoIssue869");
				o.setUserName("user");
				o.setPassword("token-1");
				o.setAutomaticReconnect(true);
				o.setRetryInterval(1);
				o.setMaxRetries(-1);
				client.setCallback(new MqttCallbackAdapter() {
					@Override
					public void connectionLost(Throwable cause) {
						client.getOptions().setPassword("token-2");
					}
				});
				IMqttToken t = client.connect(o);
				broker.accept();
				String first = new String(broker.read().body, StandardCharsets.UTF_8);
				broker.write(0x20, 0x02, 0x00, 0x00);
				t.waitForCompletion(5000);
				broker.accept();   // drops the first connection; the client reconnects
				String second = new String(broker.read().body, StandardCharsets.UTF_8);
				Assert.assertTrue(first.endsWith("token-1"));
				Assert.assertTrue("second CONNECT did not carry the new password", second.endsWith("token-2"));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 643: after MqttClient.publish(topic, message) returns, the message's ID is the packet
	 * identifier it was sent with.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testMessageIdAfterSyncPublish() throws Exception {
		MqttClient client = TestClients.createMqttClient(BROKER.getServerURI(), "PahoIssue643");
		try {
			client.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue643"));
			MqttMessage m = new MqttMessage(new byte[]{1});
			m.setQos(1);
			client.publish("PahoIssue643/t", m);
			System.out.println("message id after sync publish: " + m.getId());
			Assert.assertTrue("message id " + m.getId(), m.getId() > 0);
		} finally {
			TestClients.closeQuietly(client);
		}
	}
}
