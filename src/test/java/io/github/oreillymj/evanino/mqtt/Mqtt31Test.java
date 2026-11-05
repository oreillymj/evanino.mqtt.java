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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 3.1: client identifiers are truncated to the 23 characters 3.1 allows, a connection and
 * message round trip works, and a CONNECT that cannot be sent fails the connect instead of
 * leaving it waiting. Not part of Paho's suite. The connection tests require a broker; see
 * {@link MosquittoBroker}.
 */
public class Mqtt31Test {

	/** The broker used by the connection tests. */
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
		topicPrefix = "Mqtt31Test-" + UUID.randomUUID() + "-";
	}

	/**
	 * With MQTT 3.1, a client identifier longer than 23 characters is sent as its first 23;
	 * shorter ones, and every identifier under 3.1.1, are sent unchanged.
	 */
	@Test
	public void testClientIdTruncatedForMqtt31() {
		String longId = "abcdefghijklmnopqrstuvwxyz0123";   // 30 characters
		MqttConnectOptions options = new MqttConnectOptions();
		options.setClientId(longId);

		options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
		Assert.assertEquals("abcdefghijklmnopqrstuvw", options.getConnectClientId());
		Assert.assertEquals("the configured ID is kept", longId, options.getClientId());

		options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
		Assert.assertEquals(longId, options.getConnectClientId());

		options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
		options.setClientId("exactly-twenty-three-ch");
		Assert.assertEquals("exactly-twenty-three-ch", options.getConnectClientId());

		options.setClientId(null);   // a generated ID already fits
		String generated = options.getConnectClientId();
		Assert.assertTrue(generated, generated.length() <= MqttConnectOptions.MQTT_3_1_MAX_CLIENT_ID_LENGTH);
	}

	/**
	 * Truncation never splits a surrogate pair: if the 23rd character is the first half of
	 * one, the identifier is cut to 22.
	 */
	@Test
	public void testTruncationKeepsSurrogatePairsWhole() {
		String emoji = new String(Character.toChars(0x1F600));    // two chars
		String id = "0123456789012345678901" + emoji + "tail";   // pair at positions 22-23
		MqttConnectOptions options = new MqttConnectOptions();
		options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
		options.setClientId(id);
		Assert.assertEquals("0123456789012345678901", options.getConnectClientId());
	}

	/**
	 * A 3.1 client with a long identifier connects, and a QoS 2 message makes the round trip.
	 * Before truncation, Netty's encoder rejected the CONNECT and the connect hung.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testMqtt31ConnectsWithLongClientId() throws Exception {
		String longId = "Mqtt31Test-long-client-identifier-" + UUID.randomUUID();
		MqttConnectOptions options = TestClients.options(serverURI, longId);
		options.setMqttVersion(3);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			MqttV3Receiver receiver = new MqttV3Receiver();
			client.setCallback(receiver);
			client.connect().waitForCompletion(5000);
			Assert.assertTrue(client.isConnected());

			String topic = topicPrefix + name.getMethodName();
			client.subscribe(topic, 2).waitForCompletion(5000);
			byte[] payload = "over MQTT 3.1".getBytes(StandardCharsets.UTF_8);
			TestClients.publish(client, topic, payload, 2, false).waitForCompletion(5000);
			Assert.assertTrue("message not received", receiver.validateReceipt(topic, 2, payload));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * The broker sees the truncated identifier: a 3.1.1 client connecting with that shorter
	 * identifier takes over the 3.1 client's session, so the 3.1 client loses its connection.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testBrokerSeesTruncatedClientId() throws Exception {
		String longId = "M31-" + UUID.randomUUID().toString().replace("-", "");
		String truncated = longId.substring(0, MqttConnectOptions.MQTT_3_1_MAX_CLIENT_ID_LENGTH);

		MqttConnectOptions options = TestClients.options(serverURI, longId);
		options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
		MqttAsyncClient mqtt31 = new MqttAsyncClient(options);
		MqttAsyncClient usurper = TestClients.createMqttAsyncClient(serverURI, truncated);
		try {
			MqttV3Receiver receiver = new MqttV3Receiver();
			mqtt31.setCallback(receiver);
			mqtt31.connect().waitForCompletion(5000);
			Assert.assertTrue(mqtt31.isConnected());

			usurper.connect().waitForCompletion(5000);

			long deadline = System.currentTimeMillis() + 5000;
			while (mqtt31.isConnected() && System.currentTimeMillis() < deadline) {
				Thread.sleep(50);
			}
			Assert.assertFalse("the broker did not see the truncated client ID", mqtt31.isConnected());
		} finally {
			TestClients.shutdownQuietly(usurper);
			TestClients.shutdownQuietly(mqtt31);
		}
	}

	/**
	 * If the CONNECT packet cannot be written, for example because the encoder rejects it, the
	 * connect fails straight away with that cause, rather than waiting for a CONNACK that
	 * cannot come.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testConnectFailsWhenConnectCannotBeSent() throws Exception {
		EncoderException rejected = new EncoderException("rejected by the test");
		MqttAsyncClient client = new MqttAsyncClient(TestClients.options(serverURI, name.getMethodName()));
		Promise<MqttConnectResult> connectFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
		EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
			@Override
			public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
				promise.setFailure(rejected);
			}
		}, new MqttChannelHandler(client, connectFuture));
		try {
			Assert.assertTrue("connect did not fail", connectFuture.await(5, TimeUnit.SECONDS));
			Assert.assertFalse(connectFuture.isSuccess());
			Throwable cause = connectFuture.cause();
			while (cause != null && cause != rejected) {
				cause = cause.getCause();
			}
			Assert.assertSame("the write failure should be the cause", rejected, cause);
			channel.runPendingTasks();
			Assert.assertFalse("the channel should be closed", channel.isOpen());
		} finally {
			channel.finishAndReleaseAll();
		}
	}
}
