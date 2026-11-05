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

import io.netty.handler.codec.mqtt.MqttQoS;
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
 * The last will: Paho's {@code getWillDestination()} and {@code getWillMessage()}, and the will
 * payload reaching subscribers byte for byte. Not part of Paho's suite. The delivery test
 * requires a broker; see {@link MosquittoBroker}.
 */
public class LastWillTest {

	/** The broker used by the delivery test. */
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
		topicPrefix = "LastWillTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * With no will set, both getters return {@code null}.
	 */
	@Test
	public void testNoWill() {
		MqttConnectOptions options = new MqttConnectOptions();
		Assert.assertNull(options.getWillDestination());
		Assert.assertNull(options.getWillMessage());
	}

	/**
	 * The getters return the topic, payload, QoS and retained flag passed to {@code setWill}.
	 */
	@Test
	public void testGettersReturnTheWill() {
		MqttConnectOptions options = new MqttConnectOptions();
		byte[] payload = {1, 2, 3};
		options.setWill("status/client", payload, 2, true);

		Assert.assertEquals("status/client", options.getWillDestination());
		MqttMessage will = options.getWillMessage();
		Assert.assertArrayEquals(payload, will.getPayload());
		Assert.assertEquals(2, will.getQos());
		Assert.assertTrue(will.isRetained());

		options.setWill("status/other", new byte[]{4}, 0, false);
		Assert.assertEquals("status/other", options.getWillDestination());
		Assert.assertEquals(0, options.getWillMessage().getQos());
		Assert.assertFalse(options.getWillMessage().isRetained());
	}

	/**
	 * The getters also reflect a will set with {@code setLastWill}, and clearing it with
	 * {@code null} clears them.
	 */
	@Test
	public void testGettersReflectSetLastWill() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setLastWill(new MqttLastWill("status/text", "bye", false, MqttQoS.AT_LEAST_ONCE));
		Assert.assertEquals("status/text", options.getWillDestination());
		Assert.assertArrayEquals("bye".getBytes(StandardCharsets.UTF_8), options.getWillMessage().getPayload());
		Assert.assertEquals(1, options.getWillMessage().getQos());

		options.setLastWill(null);
		Assert.assertNull(options.getWillDestination());
		Assert.assertNull(options.getWillMessage());
	}

	/**
	 * The will cannot be changed through the arrays and messages it was set with or returns.
	 */
	@Test
	public void testWillIsCopied() {
		MqttConnectOptions options = new MqttConnectOptions();
		byte[] payload = {1, 2, 3};
		options.setWill("status/client", payload, 1, false);

		payload[0] = 9;                                     // the caller's array
		options.getWillMessage().getPayload()[1] = 9;       // a returned message
		options.getWillMessage().setQos(0);
		options.getLastWill().getPayload()[2] = 9;          // the will's own accessor

		MqttMessage will = options.getWillMessage();
		Assert.assertArrayEquals(new byte[]{1, 2, 3}, will.getPayload());
		Assert.assertEquals(1, will.getQos());
	}

	/**
	 * A text will is encoded as UTF-8, whatever the platform's default charset.
	 */
	@Test
	public void testTextWillIsUtf8() {
		String text = "déconnecté – 断开";   // é, en dash, Chinese
		MqttLastWill will = new MqttLastWill("status/text", text, false, MqttQoS.AT_MOST_ONCE);
		Assert.assertArrayEquals(text.getBytes(StandardCharsets.UTF_8), will.getPayload());
		Assert.assertEquals(text, will.getMessage());
	}

	/**
	 * A binary will payload, including bytes that are not valid text, reaches a subscriber
	 * exactly as given. It used to be decoded with the platform charset and re-encoded as
	 * UTF-8, which changed such bytes.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testWillPayloadIsDeliveredExactly() throws Exception {
		String willTopic = topicPrefix + name.getMethodName();
		byte[] payload = {0, (byte) 0xFF, (byte) 0xC3, (byte) 0xA9, (byte) 0x80, 'o', 'k'};
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient watcher = TestClients.createMqttAsyncClient(serverURI, name.getMethodName() + "-watcher");
		MqttConnectOptions options = TestClients.options(serverURI, name.getMethodName());
		options.setWill(willTopic, payload, 1, false);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			watcher.setCallback(receiver);
			watcher.connect().waitForCompletion(5000);
			watcher.subscribe(willTopic, 1).waitForCompletion(5000);

			client.connect().waitForCompletion(5000);
			client.disconnectForcibly();   // no DISCONNECT, so the broker publishes the will

			Assert.assertTrue("will not received byte for byte", receiver.validateReceipt(willTopic, 1, payload));
		} finally {
			TestClients.shutdownQuietly(client);
			TestClients.shutdownQuietly(watcher);
		}
	}
}
