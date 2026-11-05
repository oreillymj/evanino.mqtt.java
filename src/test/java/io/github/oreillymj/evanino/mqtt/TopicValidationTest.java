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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Topic names and filters are checked before they are sent: a broker closes the connection on
 * an invalid one (Paho issues 704 and 617 are about Paho's partial checks). Not part of Paho's
 * suite; uses {@link MosquittoBroker}.
 */
public class TopicValidationTest {

	/** The broker. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/**
	 * Invalid filters are refused at once, for subscribe and unsubscribe, single and array
	 * forms; valid ones, including unusual ones, are accepted by the client and the broker; and
	 * the connection is never lost.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testFilters() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "TopicValidation");
		AtomicInteger lost = new AtomicInteger();
		try {
			client.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					lost.incrementAndGet();
				}
			});
			client.connect(TestClients.options(BROKER.getServerURI(), "TopicValidation")).waitForCompletion(10_000);
			for (String bad : new String[]{"a/#/b", "a#", "#/a", "a/b+", "+a/b", "a/\0", ""}) {
				assertRefused(() -> client.subscribe(bad, 1));
				assertRefused(() -> client.subscribe(bad, 1, (t, m) -> { }));
				assertRefused(() -> client.subscribe(new String[]{"ok", bad}, new int[]{0, 1}));
				assertRefused(() -> client.unsubscribe(bad));
				assertRefused(() -> client.unsubscribe(new String[]{"ok", bad}));
			}
			for (String good : new String[]{"#", "+", "a/+/b/#", "/", "a//b", "+/+", "$share/g/a/#", "$SYS/#", "a/b c/é"}) {
				client.subscribe(good, 1).waitForCompletion(10_000);
				client.unsubscribe(good).waitForCompletion(10_000);
			}
			Assert.assertTrue(client.isConnected());
			Assert.assertEquals(0, lost.get());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Publishing to a topic with a wildcard or U+0000 is refused at once; other topics are
	 * published.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testTopicNames() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "TopicValidation-pub");
		try {
			client.connect(TestClients.options(BROKER.getServerURI(), "TopicValidation-pub")).waitForCompletion(10_000);
			for (String bad : new String[]{"a/#", "a/+/b", "+", "#", "a\0b"}) {
				for (int qos = 0; qos <= 2; qos++) {
					final int q = qos;
					assertRefused(() -> client.publish(bad, new byte[]{1}, q, false));
				}
			}
			for (String good : new String[]{"a", "/", "a//b", "$share-not-a-filter", "a b/é"}) {
				client.publish(good, new byte[]{1}, 1, false).waitForCompletion(10_000);
			}
			Assert.assertTrue(client.isConnected());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	private interface Action {
		void run() throws Exception;
	}

	private static void assertRefused(Action action) {
		try {
			action.run();
		} catch (IllegalArgumentException expected) {
			return;
		} catch (Exception e) {
			throw new AssertionError("expected IllegalArgumentException, got " + e, e);
		}
		Assert.fail("accepted");
	}
}
