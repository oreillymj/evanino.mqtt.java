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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Paho issues under load:
 * <ul>
 *   <li>1044: subscribing (with a wildcard) to a tree with thousands of retained messages stops
 *   the client after some of them (under 40,000 on localhost).</li>
 *   <li>1047: a new client with several wildcard subscriptions does not get all the retained
 *   messages.</li>
 *   <li>1035 and 1026: connecting many clients at once sometimes fails with "Connect already in
 *   progress", or hangs.</li>
 * </ul>
 * Uses {@link UnlimitedMosquitto}.
 */
public class PahoIssueLoadTest {

	/** The broker. */
	@ClassRule
	public static final UnlimitedMosquitto BROKER = new UnlimitedMosquitto();

	/**
	 * 50,000 retained messages with long topics and 200-byte payloads, under three subtrees. A
	 * new client subscribes to the three subtrees with wildcards and gets every one, then still
	 * works.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testManyRetainedMessages() throws Exception {
		String[] trees = {"a", "b", "c"};
		int perTree = 50_000 / trees.length;
		String padding = "some/rather/long/topic/levels/to/make/the/topics/longer";
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueRetained-pub");
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueRetained-sub");
		Set<String> seen = ConcurrentHashMap.newKeySet();
		try {
			MqttConnectOptions po = TestClients.options(BROKER.getServerURI(), "PahoIssueRetained-pub");
			po.setMaxInflight(2000);
			pub.connect(po).waitForCompletion(10_000);
			byte[] payload = new byte[200];
			IMqttDeliveryToken last = null;
			for (String tree : trees) {
				for (int i = 0; i < perTree; i++) {
					String topic = "PahoIssueRetained/" + tree + "/" + padding + "/" + i;
					while (true) {
						try {
							last = pub.publish(topic, payload, 1, true);
							break;
						} catch (MqttException e) {
							if (e.getReasonCode() != MqttException.REASON_CODE_MAX_INFLIGHT) {
								throw e;
							}
							Thread.sleep(1);
						}
					}
				}
			}
			last.waitForCompletion(60_000);
			while (pub.getInFlightMessageCount() > 0) {
				Thread.sleep(20);
			}

			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void messageArrived(String topic, MqttMessage message) {
					seen.add(topic);
				}
			});
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssueRetained-sub")).waitForCompletion(10_000);
			sub.subscribe(new String[]{"PahoIssueRetained/a/#", "PahoIssueRetained/b/#", "PahoIssueRetained/c/+/rather/#"},
					new int[]{1, 0, 1}).waitForCompletion(10_000);
			long deadline = System.currentTimeMillis() + 60_000;
			while (seen.size() < perTree * trees.length && System.currentTimeMillis() < deadline) {
				Thread.sleep(50);
			}
			Assert.assertEquals("retained messages received", perTree * trees.length, seen.size());
			Assert.assertTrue(sub.isConnected());

			CountDownLatch after = new CountDownLatch(1);
			sub.subscribe("PahoIssueRetained/after", 1, (t, m) -> after.countDown()).waitForCompletion(10_000);
			pub.publish("PahoIssueRetained/after", new byte[]{1}, 1, false);
			Assert.assertTrue("the client stopped working", after.await(10, TimeUnit.SECONDS));
		} finally {
			// clear the retained messages
			TestClients.shutdownQuietly(sub);
			TestClients.shutdownQuietly(pub);
		}
	}

	/**
	 * 500 clients connect at the same time, from 50 threads. Every connect succeeds; none fails
	 * with "Connect already in progress" or hangs.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testManyClientsConnectAtOnce() throws Exception {
		int n = 500;
		ExecutorService pool = Executors.newFixedThreadPool(50);
		List<MqttAsyncClient> clients = java.util.Collections.synchronizedList(new ArrayList<>());
		try {
			CountDownLatch start = new CountDownLatch(1);
			List<Future<String>> results = new ArrayList<>();
			for (int i = 0; i < n; i++) {
				final String id = "PahoIssueMany-" + i;
				results.add(pool.submit(() -> {
					MqttAsyncClient c = TestClients.createMqttAsyncClient(BROKER.getServerURI(), id);
					clients.add(c);
					start.await();
					try {
						c.connect(TestClients.options(BROKER.getServerURI(), id)).waitForCompletion(30_000);
						c.publish("PahoIssueMany/" + id, id.getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(30_000);
						return c.isConnected() ? null : id + " not connected";
					} catch (MqttException e) {
						return id + ": " + e;
					}
				}));
			}
			start.countDown();
			List<String> failures = new ArrayList<>();
			for (Future<String> f : results) {
				String r = f.get(120, TimeUnit.SECONDS);
				if (r != null) {
					failures.add(r);
				}
			}
			Assert.assertTrue(failures.size() + " failures: " + failures, failures.isEmpty());
		} finally {
			pool.shutdownNow();
			for (MqttAsyncClient c : clients) {
				TestClients.shutdownQuietly(c);
			}
		}
	}
}
