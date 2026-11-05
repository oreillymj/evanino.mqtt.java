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

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Messages larger than 64 KB, over TCP and WebSocket. They used to fail: over TCP the default
 * {@link MqttConnectOptions#setMaxBytesInMessage(int)} was 65536, and over WebSocket the frame
 * limit was fixed at 65536 whatever that option said (Paho issue 501 is about Paho's WebSocket
 * client failing on large messages). Not part of Paho's suite; uses {@link MosquittoBroker}.
 */
public class LargeMessageTest {

	/** The broker, with TCP and WebSocket listeners. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/**
	 * With the default options, 100 KB and 1 MB messages arrive over TCP and over WebSocket.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testLargeMessagesWithDefaults() throws Exception {
		for (URI uri : new URI[]{BROKER.getServerURI(), BROKER.getWebSocketServerURI()}) {
			for (int size : new int[]{100_000, 1_000_000}) {
				Assert.assertNull(uri.getScheme() + ", " + size + " bytes", receive(uri, size, -1));
			}
		}
	}

	/**
	 * With the limit lowered below the message size, the connection is closed with
	 * REASON_CODE_INVALID_MESSAGE, over TCP and over WebSocket.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testLimitClosesTheConnection() throws Exception {
		for (URI uri : new URI[]{BROKER.getServerURI(), BROKER.getWebSocketServerURI()}) {
			Throwable lost = receive(uri, 100_000, 50_000);
			Assert.assertNotNull(uri.getScheme() + ": the message arrived", lost);
			Assert.assertTrue(uri.getScheme() + ": " + lost + " / " + lost.getCause(),
					hasCode(lost, MqttException.REASON_CODE_INVALID_MESSAGE));
		}
	}

	/**
	 * The limit must be 1 to MQTT's maximum.
	 */
	@Test
	public void testLimitRange() {
		MqttConnectOptions o = new MqttConnectOptions();
		Assert.assertEquals(MqttConnectOptions.MAX_PACKET_SIZE, o.getMaxBytesInMessage());
		for (int bad : new int[]{0, -1, MqttConnectOptions.MAX_PACKET_SIZE + 1}) {
			try {
				o.setMaxBytesInMessage(bad);
				Assert.fail("accepted " + bad);
			} catch (IllegalArgumentException expected) {
				// refused
			}
		}
	}

	/**
	 * Subscribes over {@code uri} (with the given limit, or the default for -1), publishes one
	 * message of {@code size} bytes over TCP, and returns null if it arrived, or the cause of
	 * the connection loss.
	 */
	private static Throwable receive(URI uri, int size, int limit) throws Exception {
		MqttAsyncClient sub = new MqttAsyncClient(uri.toString(), "LargeMessage-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "LargeMessage-pub");
		CountDownLatch done = new CountDownLatch(1);
		AtomicReference<Throwable> lost = new AtomicReference<>();
		AtomicReference<Integer> got = new AtomicReference<>();
		try {
			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					lost.set(cause);
					done.countDown();
				}
			});
			MqttConnectOptions o = TestClients.options(uri, "LargeMessage-sub");
			if (limit > 0) {
				o.setMaxBytesInMessage(limit);
			}
			sub.connect(o).waitForCompletion(10_000);
			sub.subscribe("LargeMessage/t", 1, (t, m) -> {
				got.set(m.getPayload().length);
				done.countDown();
			}).waitForCompletion(10_000);
			pub.connect(TestClients.options(BROKER.getServerURI(), "LargeMessage-pub")).waitForCompletion(10_000);
			pub.publish("LargeMessage/t", new byte[size], 1, false).waitForCompletion(10_000);
			Assert.assertTrue(uri.getScheme() + ": neither the message nor connectionLost came", done.await(10, TimeUnit.SECONDS));
			if (got.get() != null) {
				Assert.assertEquals(size, (int) got.get());
				return null;
			}
			return lost.get();
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}

	private static boolean hasCode(Throwable t, int code) {
		for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
			if (c instanceof MqttException && ((MqttException) c).getReasonCode() == code) {
				return true;
			}
		}
		return false;
	}
}
