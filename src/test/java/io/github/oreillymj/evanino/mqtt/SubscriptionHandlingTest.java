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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Duplicate subscriptions, re-subscribing to a topic at a different QoS, and duplicate
 * unsubscribes. Not part of Paho's suite. Requires a broker; see {@link MosquittoBroker}.
 * <p>
 * Each test checks what the application sees (how many times, and at what QoS, a message is
 * delivered) and, where it matters, the client's own bookkeeping (whether a packet was sent).
 */
public class SubscriptionHandlingTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	/** How long to wait for a message that is expected not to arrive. */
	private static final long NO_MESSAGE_WAIT_MILLIS = 1500;

	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "SubscriptionHandlingTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * Subscribing twice to the same topic at the same QoS succeeds both times and does not
	 * duplicate delivery: a message published once arrives once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDuplicateSubscriptionSameQos() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient client = connect(name.getMethodName(), receiver);
		try {
			IMqttToken first = client.subscribe(topic, 1);
			first.waitForCompletion(5000);
			IMqttToken second = client.subscribe(topic, 1);
			second.waitForCompletion(5000);
			Assert.assertTrue(first.isComplete());
			Assert.assertTrue(second.isComplete());
			Assert.assertEquals(Integer.valueOf(1), client.getServerSubscriptions().get(topic));

			byte[] payload = "once".getBytes(StandardCharsets.UTF_8);
			TestClients.publish(client, topic, payload, 1, false).waitForCompletion(5000);

			Assert.assertTrue("message not received", receiver.validateReceipt(topic, 1, payload));
			Assert.assertNull("duplicate subscription delivered the message twice", receiver.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Two different listeners subscribed to the same topic each receive the message once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDuplicateSubscriptionDifferentListeners() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttAsyncClient client = connect(name.getMethodName(), new MqttV3Receiver());
		try {
			PerSubscriptionMessageHandlerTest.Listener listenerA = new PerSubscriptionMessageHandlerTest.Listener();
			PerSubscriptionMessageHandlerTest.Listener listenerB = new PerSubscriptionMessageHandlerTest.Listener();
			client.subscribe(topic, 1, null, null, listenerA).waitForCompletion(5000);
			client.subscribe(topic, 1, null, null, listenerB).waitForCompletion(5000);

			TestClients.publish(client, topic, "both".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(5000);

			MqttMessage a = listenerA.getNextMessage();
			MqttMessage b = listenerB.getNextMessage();
			Assert.assertNotNull("first listener did not receive the message", a);
			Assert.assertNotNull("second listener did not receive the message", b);
			Assert.assertEquals("both", a.getPayloadString());
			Assert.assertEquals("both", b.getPayloadString());
			Thread.sleep(NO_MESSAGE_WAIT_MILLIS);
			Assert.assertNull("first listener received the message twice", listenerA.getNextMessageNow());
			Assert.assertNull("second listener received the message twice", listenerB.getNextMessageNow());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Re-subscribing to a topic at a higher QoS replaces the subscription: the broker grants
	 * the new QoS, and a message published once arrives once, at the new QoS.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testResubscribeAtHigherQos() throws Exception {
		resubscribeAtDifferentQos(0, 2);
	}

	/**
	 * Re-subscribing to a topic at a lower QoS replaces the subscription: the broker grants the
	 * new QoS, and a QoS 2 message published once arrives once, downgraded to the new QoS.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testResubscribeAtLowerQos() throws Exception {
		resubscribeAtDifferentQos(2, 0);
	}

	/**
	 * Unsubscribing twice from the same topic: the first unsubscribe sends UNSUBSCRIBE and
	 * completes when the broker acknowledges it; the second is a no-op that completes
	 * immediately without sending anything, and no message arrives afterwards.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDuplicateUnsubscribeIsNoOp() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient client = connect(name.getMethodName(), receiver);
		try {
			client.subscribe(topic, 1).waitForCompletion(5000);
			Assert.assertTrue(client.getServerSubscriptions().containsKey(topic));

			IMqttToken first = client.unsubscribe(topic);
			first.waitForCompletion(5000);
			Assert.assertFalse("topic still subscribed on the broker", client.getServerSubscriptions().containsKey(topic));
			Assert.assertTrue(client.getPendingServerUnsubscribes().isEmpty());

			IMqttToken second = client.unsubscribe(topic);
			Assert.assertTrue("second unsubscribe should complete immediately", second.isComplete());
			Assert.assertNull("second unsubscribe should succeed", second.getException());
			Assert.assertTrue("second unsubscribe should not send UNSUBSCRIBE", client.getPendingServerUnsubscribes().isEmpty());

			TestClients.publish(client, topic, "after unsubscribe".getBytes(StandardCharsets.UTF_8), 1, false)
					.waitForCompletion(5000);
			Assert.assertNull("message received after unsubscribing", receiver.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Two unsubscribes for the same topic issued back to back, before the broker has
	 * acknowledged the first: only one UNSUBSCRIBE is sent, and both tokens complete.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testConcurrentDuplicateUnsubscribe() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient client = connect(name.getMethodName(), receiver);
		try {
			client.subscribe(topic, 1).waitForCompletion(5000);

			IMqttToken first = client.unsubscribe(topic);
			IMqttToken second = client.unsubscribe(topic);
			Assert.assertTrue("at most one UNSUBSCRIBE should be in flight", client.getPendingServerUnsubscribes().size() <= 1);

			first.waitForCompletion(5000);
			second.waitForCompletion(5000);
			Assert.assertNull(first.getException());
			Assert.assertNull(second.getException());
			Assert.assertFalse(client.getServerSubscriptions().containsKey(topic));

			TestClients.publish(client, topic, "after unsubscribe".getBytes(StandardCharsets.UTF_8), 1, false)
					.waitForCompletion(5000);
			Assert.assertNull("message received after unsubscribing", receiver.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	// -------------------------------------------------------------
	// Helper methods
	// -------------------------------------------------------------

	/**
	 * Subscribes at one QoS, re-subscribes at another, and checks that a QoS 2 message is
	 * delivered exactly once at the second QoS.
	 *
	 * @param firstQos  the QoS of the first subscription
	 * @param secondQos the QoS of the replacement subscription
	 * @throws Exception if the test fails unexpectedly
	 */
	private void resubscribeAtDifferentQos(int firstQos, int secondQos) throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient client = connect(name.getMethodName(), receiver);
		try {
			IMqttToken first = client.subscribe(topic, firstQos);
			first.waitForCompletion(5000);
			Assert.assertEquals(firstQos, first.getGrantedQos()[0]);

			IMqttToken second = client.subscribe(topic, secondQos);
			second.waitForCompletion(5000);
			Assert.assertNotNull("re-subscribe should send a new SUBSCRIBE and report the granted QoS", second.getGrantedQos());
			Assert.assertEquals(secondQos, second.getGrantedQos()[0]);
			Assert.assertEquals(Integer.valueOf(secondQos), client.getServerSubscriptions().get(topic));

			byte[] payload = "resubscribed".getBytes(StandardCharsets.UTF_8);
			TestClients.publish(client, topic, payload, 2, false).waitForCompletion(5000);

			Assert.assertTrue("message not received once at the new QoS", receiver.validateReceipt(topic, secondQos, payload));
			Assert.assertNull("message delivered more than once after re-subscribing", receiver.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Connects an asynchronous client with a clean session.
	 *
	 * @param clientId the client identifier
	 * @param receiver the client-wide callback
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private static MqttAsyncClient connect(String clientId, MqttV3Receiver receiver) throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, clientId);
		client.setCallback(receiver);
		client.connect().waitForCompletion(5000);
		return client;
	}
}
