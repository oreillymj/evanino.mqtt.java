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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared subscriptions, {@code $share/{group}/{filter}}: the broker delivers messages on the
 * plain topic, so the listener given with the subscription is matched on {@code {filter}}. It
 * used to be matched on the whole {@code $share/...} string, so it was never called (Paho issues
 * 965, 827 and 367 report the same). Not part of Paho's suite; uses {@link MosquittoBroker},
 * which accepts shared subscriptions from MQTT 3.1.1 clients.
 */
public class SharedSubscriptionTest {

	/** The broker. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/**
	 * The filter that topic names are matched against.
	 */
	@Test
	public void testMatchingFilter() {
		Assert.assertEquals("a/b", MqttSubscription.matchingFilter("$share/group/a/b"));
		Assert.assertEquals("a/#", MqttSubscription.matchingFilter("$share/group/a/#"));
		Assert.assertEquals("a/+", MqttSubscription.matchingFilter("$queue/a/+"));
		Assert.assertEquals("$share/group", MqttSubscription.matchingFilter("$share/group"));   // no filter: left alone
		Assert.assertEquals("$SYS/#", MqttSubscription.matchingFilter("$SYS/#"));
		Assert.assertEquals("a/$share/g/b", MqttSubscription.matchingFilter("a/$share/g/b"));
		Assert.assertTrue(MqttSubscription.matches("$share/g/a/+/c", "a/b/c"));
		Assert.assertFalse(MqttSubscription.matches("$share/g/a/+/c", "$share/g/a/b/c"));
	}

	/**
	 * Messages on a shared subscription, exact and with a wildcard, reach its listener and not
	 * the client-wide callback.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testListenerIsCalled() throws Exception {
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "SharedSubscription-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "SharedSubscription-pub");
		List<String> exact = new CopyOnWriteArrayList<>();
		List<String> wildcard = new CopyOnWriteArrayList<>();
		AtomicInteger clientWide = new AtomicInteger();
		try {
			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void messageArrived(String topic, MqttMessage message) {
					clientWide.incrementAndGet();
				}
			});
			sub.connect(TestClients.options(BROKER.getServerURI(), "SharedSubscription-sub")).waitForCompletion(10_000);
			sub.subscribe("$share/group/SharedSubscription/exact", 1, (t, m) -> exact.add(t)).waitForCompletion(10_000);
			sub.subscribe("$share/group/SharedSubscription/wild/#", 1, (t, m) -> wildcard.add(t)).waitForCompletion(10_000);
			pub.connect(TestClients.options(BROKER.getServerURI(), "SharedSubscription-pub")).waitForCompletion(10_000);
			pub.publish("SharedSubscription/exact", new byte[]{1}, 1, false).waitForCompletion(10_000);
			pub.publish("SharedSubscription/wild/a/b", new byte[]{1}, 1, false).waitForCompletion(10_000);
			long deadline = System.currentTimeMillis() + 5000;
			while ((exact.isEmpty() || wildcard.isEmpty()) && System.currentTimeMillis() < deadline) {
				Thread.sleep(20);
			}
			Thread.sleep(300);
			Assert.assertEquals(java.util.Collections.singletonList("SharedSubscription/exact"), exact);
			Assert.assertEquals(java.util.Collections.singletonList("SharedSubscription/wild/a/b"), wildcard);
			Assert.assertEquals("client-wide callback calls", 0, clientWide.get());
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}
}
