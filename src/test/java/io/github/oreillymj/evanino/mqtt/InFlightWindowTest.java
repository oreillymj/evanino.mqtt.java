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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The in-flight window set with {@link MqttConnectOptions#setMaxInflight(int)}, and packet
 * identifier allocation. Publishes made before connecting are held as in flight, which lets the
 * tests fill the window without a broker holding back acknowledgements. Not part of Paho's
 * suite. The tests that connect require a broker; see {@link MosquittoBroker}.
 */
public class InFlightWindowTest {

	/** The broker used by the tests that connect. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "InFlightWindowTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * The window defaults to 1000 and must be between 1 and 65535, the number of packet
	 * identifiers. The QoS 0 window defaults to 65535 and must be 1 or more.
	 */
	@Test
	public void testSetMaxInflightRange() {
		MqttConnectOptions options = new MqttConnectOptions();
		Assert.assertEquals(1000, options.getMaxInflight());
		Assert.assertEquals(65535, options.getMaxUnwrittenQos0());
		for (int bad : new int[]{0, -1, 65536, Integer.MAX_VALUE}) {
			try {
				options.setMaxInflight(bad);
				Assert.fail("accepted " + bad);
			} catch (IllegalArgumentException expected) {
				// out of range
			}
		}
		options.setMaxInflight(1);
		Assert.assertEquals(1, options.getMaxInflight());
		options.setMaxInflight(65535);
		Assert.assertEquals(65535, options.getMaxInflight());
		try {
			options.setMaxUnwrittenQos0(0);
			Assert.fail("accepted a QoS 0 window of 0");
		} catch (IllegalArgumentException expected) {
			// out of range
		}
	}

	/**
	 * A QoS 1 or 2 publish beyond the window throws {@code REASON_CODE_MAX_INFLIGHT}; QoS 0
	 * publishes are not counted. Once the held publishes are delivered, the window is free
	 * again.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPublishBeyondWindowIsRefused() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttConnectOptions options = TestClients.options(serverURI, name.getMethodName());
		options.setMaxInflight(3);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			List<IMqttDeliveryToken> held = new ArrayList<>();
			held.add(TestClients.publish(client, topic, new byte[]{1}, 1, false));
			held.add(TestClients.publish(client, topic, new byte[]{2}, 2, false));
			TestClients.publish(client, topic, new byte[]{0}, 0, false);   // QoS 0 is not held
			held.add(TestClients.publish(client, topic, new byte[]{3}, 1, false));
			Assert.assertEquals(3, client.getInFlightMessageCount());

			try {
				TestClients.publish(client, topic, new byte[]{4}, 1, false);
				Assert.fail("a fourth publish should exceed the window of 3");
			} catch (MqttException e) {
				Assert.assertEquals(MqttException.REASON_CODE_MAX_INFLIGHT, e.getReasonCode());
			}
			Assert.assertEquals(3, client.getInFlightMessageCount());

			client.connect().waitForCompletion(5000);   // the held publishes are sent and acknowledged
			for (IMqttDeliveryToken token : held) {
				token.waitForCompletion(5000);
			}
			Assert.assertEquals(0, client.getInFlightMessageCount());
			TestClients.publish(client, topic, new byte[]{5}, 1, false).waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Publishes from many threads at once never exceed the window: exactly as many succeed as
	 * the window allows.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testConcurrentPublishesRespectWindow() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttConnectOptions options = TestClients.options(serverURI, name.getMethodName());
		options.setMaxInflight(50);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			int threads = 8;
			int perThread = 20;
			AtomicInteger accepted = new AtomicInteger();
			AtomicInteger refused = new AtomicInteger();
			CountDownLatch start = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(threads);
			for (int t = 0; t < threads; t++) {
				new Thread(() -> {
					try {
						start.await();
						for (int i = 0; i < perThread; i++) {
							try {
								TestClients.publish(client, topic, new byte[]{1}, 1, false);
								accepted.incrementAndGet();
							} catch (MqttException e) {
								if (e.getReasonCode() == MqttException.REASON_CODE_MAX_INFLIGHT) {
									refused.incrementAndGet();
								}
							}
						}
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} finally {
						done.countDown();
					}
				}).start();
			}
			start.countDown();
			done.await();
			Assert.assertEquals(50, accepted.get());
			Assert.assertEquals(threads * perThread - 50, refused.get());
			Assert.assertEquals(50, client.getInFlightMessageCount());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * When the identifiers wrap from 65535 back to 1, identifiers still awaiting
	 * acknowledgement are skipped rather than reused.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testWrappedIdentifiersSkipThoseInUse() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttAsyncClient client = new MqttAsyncClient(TestClients.options(serverURI, name.getMethodName()));
		try {
			int first = TestClients.publish(client, topic, new byte[]{1}, 1, false).getMessageId();
			int second = TestClients.publish(client, topic, new byte[]{2}, 1, false).getMessageId();
			Assert.assertEquals(1, first);
			Assert.assertEquals(2, second);

			setNextMessageId(client, 65535);
			Assert.assertEquals(65535, TestClients.publish(client, topic, new byte[]{3}, 1, false).getMessageId());
			Assert.assertEquals("1 and 2 are in use, so the wrapped counter goes on to 3", 3,
					TestClients.publish(client, topic, new byte[]{4}, 1, false).getMessageId());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * With the largest window, all 65535 packet identifiers can be in flight at once, each
	 * different; the next publish is refused, and a subscribe finds no identifier free.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 120_000)
	public void testEveryIdentifierCanBeInFlightOnce() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttConnectOptions options = TestClients.options(serverURI, name.getMethodName());
		options.setMaxInflight(MqttConnectOptions.MAX_INFLIGHT_LIMIT);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			Set<Integer> ids = new HashSet<>();
			for (int i = 0; i < 65535; i++) {
				ids.add(TestClients.publish(client, topic, new byte[0], 1, false).getMessageId());
			}
			Assert.assertEquals("every identifier should be used once", 65535, ids.size());
			Assert.assertFalse(ids.contains(0));
			Assert.assertEquals(65535, client.getInFlightMessageCount());

			try {
				TestClients.publish(client, topic, new byte[0], 1, false);
				Assert.fail("the window of 65535 is full");
			} catch (MqttException e) {
				Assert.assertEquals(MqttException.REASON_CODE_MAX_INFLIGHT, e.getReasonCode());
			}
			try {
				client.subscribe(topic, 1);
				Assert.fail("no identifier should be free for a SUBSCRIBE");
			} catch (MqttException e) {
				Assert.assertEquals(MqttException.REASON_CODE_NO_MESSAGE_IDS_AVAILABLE, e.getReasonCode());
			}
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Sets the client's next packet identifier, to reach the wrap-around quickly.
	 *
	 * @param client the client
	 * @param next   the next identifier to allocate
	 * @throws Exception if the field cannot be set
	 */
	private static void setNextMessageId(MqttAsyncClient client, int next) throws Exception {
		Field field = MqttAsyncClient.class.getDeclaredField("nextMessageId");
		field.setAccessible(true);
		((AtomicInteger) field.get(client)).set(next);
	}
}
