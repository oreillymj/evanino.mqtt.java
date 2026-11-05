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

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Paho issues about the receiving side when the application is slow or the load is bursty:
 * <ul>
 *   <li>1065 and 982: a messageArrived callback that blocks for longer than the keep-alive
 *   ("Timed out as no write activity") ends the connection, and with Paho the client then
 *   never reconnects.</li>
 *   <li>998: 10,000 messages a second with a slow messageArrived end in a disconnect
 *   (EOFException) after about 100 messages.</li>
 *   <li>1075: after a burst of messages from many publishers, part of them only arrive once
 *   another message is published.</li>
 *   <li>1014: waiting for a QoS 0 publish with MqttClient.</li>
 * </ul>
 * Uses {@link UnlimitedMosquitto}: with mosquitto's default queue limit, a subscriber that falls
 * behind loses messages in the broker, whatever the client does.
 */
public class PahoIssueSlowCallbackTest {

	/** The broker. */
	@ClassRule
	public static final UnlimitedMosquitto BROKER = new UnlimitedMosquitto();

	/**
	 * 1065: messageArrived blocks for three times the keep-alive. The connection stays up (the
	 * keep-alive is handled apart from the callback thread) and later messages arrive.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testBlockedCallbackKeepsConnection() throws Exception {
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueSlow-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueSlow-pub");
		AtomicInteger lost = new AtomicInteger();
		CountDownLatch second = new CountDownLatch(1);
		try {
			MqttConnectOptions o = TestClients.options(BROKER.getServerURI(), "PahoIssueSlow-sub");
			o.setKeepAliveInterval(2);
			o.setAutomaticReconnect(true);
			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					lost.incrementAndGet();
				}

				@Override
				public void messageArrived(String topic, MqttMessage message) throws Exception {
					if ("block".equals(new String(message.getPayload(), StandardCharsets.UTF_8))) {
						Thread.sleep(6000);
					} else {
						second.countDown();
					}
				}
			});
			sub.connect(o).waitForCompletion(10_000);
			sub.subscribe("PahoIssueSlow/block", 1).waitForCompletion(10_000);
			pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssueSlow-pub")).waitForCompletion(10_000);
			pub.publish("PahoIssueSlow/block", "block".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(10_000);
			Thread.sleep(500);
			pub.publish("PahoIssueSlow/block", "next".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(10_000);
			Assert.assertTrue("the message after the blocking one never arrived", second.await(20, TimeUnit.SECONDS));
			Assert.assertTrue(sub.isConnected());
			Assert.assertEquals("connection lost while the callback blocked", 0, lost.get());
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}

	/**
	 * 998: 10,000 messages as fast as possible to a subscriber whose callback takes 0.2 ms each.
	 * All arrive and the connection stays up.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSlowCallbackUnderLoad() throws Exception {
		int n = 10_000;
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueLoad-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueLoad-pub");
		AtomicInteger lost = new AtomicInteger();
		CountDownLatch all = new CountDownLatch(n);
		try {
			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					lost.incrementAndGet();
				}

				@Override
				public void messageArrived(String topic, MqttMessage message) {
					long until = System.nanoTime() + 200_000;
					while (System.nanoTime() < until) {
						Thread.yield();
					}
					all.countDown();
				}
			});
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssueLoad-sub")).waitForCompletion(10_000);
			sub.subscribe("PahoIssueLoad/t", 1).waitForCompletion(10_000);
			MqttConnectOptions po = TestClients.options(BROKER.getServerURI(), "PahoIssueLoad-pub");
			po.setMaxInflight(1000);
			pub.connect(po).waitForCompletion(10_000);
			byte[] payload = new byte[100];
			for (int i = 0; i < n; i++) {
				while (true) {
					try {
						pub.publish("PahoIssueLoad/t", payload, 1, false);
						break;
					} catch (MqttException e) {
						if (e.getReasonCode() != MqttException.REASON_CODE_MAX_INFLIGHT) {
							throw e;
						}
						Thread.sleep(1);
					}
				}
			}
			Assert.assertTrue("received " + (n - all.getCount()) + " of " + n, all.await(60, TimeUnit.SECONDS));
			Assert.assertEquals(0, lost.get());
			Assert.assertTrue(sub.isConnected());
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}

	/**
	 * 1075: 20 publishers send 500 messages each at once, then stop. Every message arrives
	 * without anything being published afterwards.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testBurstFromManyPublishersArrivesCompletely() throws Exception {
		int publishers = 20;
		int each = 500;
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueBurst-sub");
		Set<String> seen = ConcurrentHashMap.newKeySet();
		ExecutorService pool = Executors.newFixedThreadPool(publishers);
		try {
			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void messageArrived(String topic, MqttMessage message) {
					seen.add(new String(message.getPayload(), StandardCharsets.UTF_8));
				}
			});
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssueBurst-sub")).waitForCompletion(10_000);
			sub.subscribe("PahoIssueBurst/#", 1).waitForCompletion(10_000);
			CountDownLatch start = new CountDownLatch(1);
			java.util.List<java.util.concurrent.Future<Object>> results = new java.util.ArrayList<>();
			AtomicInteger acked = new AtomicInteger();
			for (int p = 0; p < publishers; p++) {
				final int id = p;
				results.add(pool.submit(() -> {
					MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueBurst-pub" + id);
					try {
						MqttConnectOptions o = TestClients.options(BROKER.getServerURI(), "PahoIssueBurst-pub" + id);
						o.setMaxInflight(each);
						pub.connect(o).waitForCompletion(10_000);
						start.await();
						IMqttDeliveryToken last = null;
						for (int i = 0; i < each; i++) {
							last = pub.publish("PahoIssueBurst/" + id, (id + "-" + i).getBytes(StandardCharsets.UTF_8), 1, false,
									null, new IMqttActionListener() {
										@Override
										public void onSuccess(IMqttToken asyncActionToken) {
											acked.incrementAndGet();
										}

										@Override
										public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
										}
									});
						}
						last.waitForCompletion(30_000);
					} finally {
						TestClients.shutdownQuietly(pub);
					}
					return null;
				}));
			}
			start.countDown();
			StringBuilder errors = new StringBuilder();
			for (java.util.concurrent.Future<Object> f : results) {
				try {
					f.get(60, TimeUnit.SECONDS);
				} catch (java.util.concurrent.ExecutionException e) {
					errors.append(' ').append(e.getCause());
				}
			}
			long deadline = System.currentTimeMillis() + 30_000;
			while (seen.size() < publishers * each && System.currentTimeMillis() < deadline) {
				Thread.sleep(50);
			}
			System.out.println("burst: acked=" + acked.get() + " received=" + seen.size() + " publisher errors:" + errors);
			Assert.assertEquals("publisher errors:" + errors, "", errors.toString());
			Assert.assertEquals(publishers * each, seen.size());
		} finally {
			pool.shutdownNow();
			TestClients.shutdownQuietly(sub);
		}
	}

	/**
	 * 1014: MqttClient.publish at QoS 0 returns once the message is written; it does not wait
	 * for a reply that never comes.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSyncQos0PublishReturns() throws Exception {
		MqttClient client = TestClients.createMqttClient(BROKER.getServerURI(), "PahoIssue1014");
		try {
			client.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue1014"));
			long start = System.nanoTime();
			for (int i = 0; i < 100; i++) {
				client.publish("PahoIssue1014/t", new byte[]{1}, 0, false);
			}
			long ms = (System.nanoTime() - start) / 1_000_000;
			Assert.assertTrue("100 QoS 0 publishes took " + ms + " ms", ms < 5000);
		} finally {
			TestClients.closeQuietly(client);
		}
	}
}
