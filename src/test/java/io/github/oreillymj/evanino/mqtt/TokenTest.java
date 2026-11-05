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
import java.util.UUID;

/**
 * The details a token reports about its action, for connect, subscribe and unsubscribe tokens.
 * Not part of Paho's suite. Requires a broker; see {@link MosquittoBroker}.
 */
public class TokenTest {

	/** The broker used by every test in this class. */
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
		topicPrefix = "TokenTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * {@code getActionCallback()} returns the listener passed to the action, or set on the
	 * token, and {@code null} when there is none.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testGetActionCallback() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		try {
			IMqttActionListener listener = new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
				}
			};
			String topic = topicPrefix + name.getMethodName();

			IMqttToken connect = client.connect(null, listener);
			connect.waitForCompletion(5000);
			Assert.assertSame(listener, connect.getActionCallback());

			IMqttToken subscribe = client.subscribe(topic, 1, null, listener);
			subscribe.waitForCompletion(5000);
			Assert.assertSame(listener, subscribe.getActionCallback());

			IMqttToken unsubscribe = client.unsubscribe(topic, null, listener);
			unsubscribe.waitForCompletion(5000);
			Assert.assertSame(listener, unsubscribe.getActionCallback());

			IMqttToken noListener = client.subscribe(topic, 1);
			Assert.assertNull(noListener.getActionCallback());
			noListener.setActionCallback(listener);
			Assert.assertSame(listener, noListener.getActionCallback());
			noListener.waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code getClient()} returns the asynchronous client that started the action, for every
	 * kind of token, including those of the blocking client, which returns its underlying
	 * asynchronous client as Paho does.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testGetClient() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		MqttClient blocking = TestClients.createMqttClient(serverURI, name.getMethodName() + "-sync");
		try {
			String topic = topicPrefix + name.getMethodName();
			IMqttToken connect = client.connect();
			connect.waitForCompletion(5000);
			Assert.assertSame(client, connect.getClient());

			IMqttToken subscribe = client.subscribe(topic, 1);
			subscribe.waitForCompletion(5000);
			Assert.assertSame(client, subscribe.getClient());

			IMqttDeliveryToken publish = TestClients.publish(client, topic, new byte[]{1}, 1, false);
			publish.waitForCompletion(5000);
			Assert.assertSame(client, publish.getClient());

			IMqttToken unsubscribe = client.unsubscribe(topic);
			unsubscribe.waitForCompletion(5000);
			Assert.assertSame(client, unsubscribe.getClient());

			IMqttToken disconnect = client.disconnect();
			disconnect.waitForCompletion(5000);
			Assert.assertSame(client, disconnect.getClient());

			IMqttToken syncConnect = blocking.connectWithResult(blocking.getOptions());
			Assert.assertSame(blocking.getAsyncClient(), syncConnect.getClient());
		} finally {
			TestClients.closeQuietly(blocking);
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code getMessageId()} reports the packet identifier of the SUBSCRIBE, UNSUBSCRIBE or
	 * PUBLISH sent: non-zero and different for each packet, the same for a token that follows
	 * an in-flight SUBSCRIBE, and {@code 0} for connect tokens, QoS 0 publishes and actions that
	 * needed no packet.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testGetMessageId() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		try {
			String topicA = topicPrefix + name.getMethodName() + "/a";
			String topicB = topicPrefix + name.getMethodName() + "/b";
			IMqttToken connect = client.connect();
			connect.waitForCompletion(5000);
			Assert.assertEquals(0, connect.getMessageId());

			IMqttToken subscribe = client.subscribe(topicA, 1);
			IMqttToken follower = client.subscribe(topicA, 1);   // same topic and QoS, still in flight
			subscribe.waitForCompletion(5000);
			follower.waitForCompletion(5000);
			Assert.assertTrue("subscribe token has no packet ID", subscribe.getMessageId() > 0);
			Assert.assertEquals("a follower reports the SUBSCRIBE it follows", subscribe.getMessageId(), follower.getMessageId());

			IMqttToken already = client.subscribe(topicA, 1);    // no packet needed
			already.waitForCompletion(5000);
			Assert.assertEquals(0, already.getMessageId());

			IMqttToken arrays = client.subscribe(new String[]{topicB}, new int[]{1});
			arrays.waitForCompletion(5000);
			Assert.assertTrue(arrays.getMessageId() > 0);
			Assert.assertNotEquals(subscribe.getMessageId(), arrays.getMessageId());

			IMqttDeliveryToken qos0 = TestClients.publish(client, topicA, new byte[]{0}, 0, false);
			qos0.waitForCompletion(5000);
			Assert.assertEquals("a QoS 0 publish has no packet ID", 0, qos0.getMessageId());
			IMqttDeliveryToken qos1 = TestClients.publish(client, topicA, new byte[]{1}, 1, false);
			qos1.waitForCompletion(5000);
			Assert.assertTrue(qos1.getMessageId() > 0);

			IMqttToken unsubscribe = client.unsubscribe(new String[]{topicA, topicB});
			unsubscribe.waitForCompletion(5000);
			Assert.assertTrue("unsubscribe token has no packet ID", unsubscribe.getMessageId() > 0);

			IMqttToken noop = client.unsubscribe(topicA);           // already unsubscribed
			noop.waitForCompletion(5000);
			Assert.assertEquals(0, noop.getMessageId());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}
}
