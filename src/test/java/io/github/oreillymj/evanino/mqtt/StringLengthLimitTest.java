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
import org.junit.ClassRule;
import org.junit.Test;

import java.io.DataInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Strings and binary fields that MQTT prefixes with a 16-bit length must not be longer than
 * 65535 bytes. Netty's encoder writes the length modulo 65536 and then the whole value, so a
 * longer topic would spill into the PUBLISH payload: the problem of CVE-2025-10543 in Paho Go,
 * reported against Paho Java as Paho issue 1101. The client refuses such values before encoding
 * them. Not part of Paho's suite; one test uses {@link MosquittoBroker}.
 */
public class StringLengthLimitTest {

	/** The broker for the test that publishes. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/** 65536 bytes in UTF-8: one more than MQTT allows. */
	private static final String TOO_LONG = repeat('t', 65536);

	/** 21846 three-byte characters, 65538 bytes in UTF-8, though only 21846 chars long. */
	private static final String TOO_LONG_UTF8 = repeat('€', 21846);

	/**
	 * Publishing to a too-long topic fails at once, at QoS 0 and 1, and leaves the connection
	 * usable.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPublishRefusesTooLongTopic() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "StringLengthLimit");
		try {
			client.connect(TestClients.options(BROKER.getServerURI(), "StringLengthLimit")).waitForCompletion(10_000);
			for (int qos = 0; qos <= 1; qos++) {
				final int q = qos;
				assertRefused(() -> client.publish(TOO_LONG, new byte[]{1}, q, false));
				assertRefused(() -> client.publish(TOO_LONG_UTF8, new byte[]{1}, q, false));
			}
			client.publish("StringLengthLimit/ok", new byte[]{1}, 1, false).waitForCompletion(10_000);
			Assert.assertTrue(client.isConnected());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Subscribing to, or unsubscribing from, a too-long topic filter fails at once, in the
	 * single and array forms.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSubscribeAndUnsubscribeRefuseTooLongFilter() throws Exception {
		MqttAsyncClient client = new MqttAsyncClient("tcp://127.0.0.1:1", "StringLengthLimit-sub");
		try {
			assertRefused(() -> client.subscribe(TOO_LONG, 1));
			assertRefused(() -> client.subscribe(TOO_LONG_UTF8, 1, (t, m) -> { }));
			assertRefused(() -> client.subscribe(new String[]{"ok", TOO_LONG}, new int[]{0, 1}));
			assertRefused(() -> client.subscribe(new String[]{TOO_LONG}, new int[]{1}, null, null,
					new IMqttMessageListener[]{(t, m) -> { }}));
			assertRefused(() -> client.unsubscribe(TOO_LONG));
			assertRefused(() -> client.unsubscribe(new String[]{"ok", TOO_LONG}));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * The CONNECT fields with a 16-bit length (user name, password, will topic and will payload)
	 * are refused when set.
	 */
	@Test
	public void testConnectFieldsRefuseTooLongValues() {
		MqttConnectOptions o = new MqttConnectOptions();
		assertRefused(() -> o.setUserName(TOO_LONG));
		assertRefused(() -> o.setPassword(TOO_LONG_UTF8));
		assertRefused(() -> o.setPassword(TOO_LONG.toCharArray()));
		assertRefused(() -> o.setWill(TOO_LONG, new byte[]{1}, 1, false));
		assertRefused(() -> o.setWill("will", new byte[65536], 1, false));
		assertRefused(() -> new MqttLastWill(TOO_LONG_UTF8, "w", false, MqttQoS.AT_MOST_ONCE));
		Assert.assertNull(o.getUserName());
		Assert.assertNull(o.getPasswordString());
		Assert.assertNull(o.getLastWill());
	}

	/**
	 * Values of exactly 65535 bytes are allowed, and a 65535-byte topic, made of multi-byte
	 * characters, is written with its full length: the fake broker reads it back intact.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testLongestAllowedTopicIsWrittenIntact() throws Exception {
		String longest = repeat('€', 21845);   // 65535 bytes
		MqttConnectOptions o = new MqttConnectOptions();
		o.setUserName(repeat('u', 65535));
		o.setWill(longest, new byte[65535], 0, false);

		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			server.setSoTimeout(10_000);
			URI uri = URI.create("tcp://127.0.0.1:" + server.getLocalPort());
			MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "StringLengthLimit-wire");
			try {
				IMqttToken connect = client.connect(TestClients.options(uri, "StringLengthLimit-wire"));
				try (Socket s = server.accept()) {
					s.setSoTimeout(5000);
					DataInputStream in = new DataInputStream(s.getInputStream());
					readPacket(in);                                                // CONNECT
					s.getOutputStream().write(new byte[]{0x20, 0x02, 0x00, 0x00});    // CONNACK
					connect.waitForCompletion(5000);
					client.publish(longest, "payload".getBytes(StandardCharsets.UTF_8), 0, false);
					byte[] publish = readPacket(in);
					int topicLength = ((publish[0] & 0xff) << 8) | (publish[1] & 0xff);
					Assert.assertEquals(65535, topicLength);
					Assert.assertEquals(longest, new String(publish, 2, topicLength, StandardCharsets.UTF_8));
					Assert.assertEquals("payload", new String(publish, 2 + topicLength,
							publish.length - 2 - topicLength, StandardCharsets.UTF_8));
				}
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	private interface Action {
		void run() throws Exception;
	}

	private static void assertRefused(Action action) {
		try {
			action.run();
		} catch (IllegalArgumentException e) {
			Assert.assertTrue(e.getMessage(), e.getMessage().contains("65535"));
			return;
		} catch (Exception e) {
			throw new AssertionError("expected IllegalArgumentException, got " + e, e);
		}
		Assert.fail("a value over 65535 bytes was accepted");
	}

	/** Reads one MQTT packet and returns its variable header and payload. */
	private static byte[] readPacket(DataInputStream in) throws java.io.IOException {
		in.readUnsignedByte();
		int length = 0;
		int multiplier = 1;
		int b;
		do {
			b = in.readUnsignedByte();
			length += (b & 0x7f) * multiplier;
			multiplier *= 128;
		} while ((b & 0x80) != 0);
		byte[] body = new byte[length];
		in.readFully(body);
		return body;
	}

	private static String repeat(char c, int n) {
		char[] chars = new char[n];
		Arrays.fill(chars, c);
		return new String(chars);
	}
}
