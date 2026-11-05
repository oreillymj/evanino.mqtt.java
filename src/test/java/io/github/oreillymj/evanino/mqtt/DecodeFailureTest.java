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
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A packet from the broker that Netty's decoder rejects closes the connection at once, and the
 * loss is reported with {@link MqttException#REASON_CODE_INVALID_MESSAGE}. The decoder discards
 * everything after such a packet, so the connection used to stay open but deaf, losing later
 * messages, until keep-alive gave up (Paho issues 996, 1030, 344 and 394 are about Paho's
 * handling of the same packets). Uses {@link RawBroker}; needs no real broker.
 */
public class DecodeFailureTest {

	/**
	 * A topic containing U+0000, which MQTT forbids.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testTopicWithNulCharacter() throws Exception {
		assertClosedPromptly(broker -> broker.publish("a\0b", 0, 0, "x"));
	}

	/**
	 * A PUBLISH whose topic contains a wildcard.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPublishTopicWithWildcard() throws Exception {
		assertClosedPromptly(broker -> broker.publish("a/+", 0, 0, "x"));
	}

	/**
	 * A packet of the reserved type 0.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testReservedPacketType() throws Exception {
		assertClosedPromptly(broker -> broker.write(0x00, 0x00));
	}

	/**
	 * A PUBLISH with both QoS bits set: QoS 3 does not exist in any MQTT version.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testQos3Publish() throws Exception {
		assertClosedPromptly(broker -> broker.write(0x36, 0x05, 0x00, 0x01, 'a', 0x00, 0x01));
	}

	/**
	 * A QoS 1 PUBLISH with packet identifier 0, which MQTT forbids.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPacketIdentifierZero() throws Exception {
		assertClosedPromptly(broker -> broker.publish("a", 1, 0, "x"));
	}

	/**
	 * A packet larger than {@link MqttConnectOptions#setMaxBytesInMessage(int)}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPacketOverTheSizeLimit() throws Exception {
		assertClosedPromptly(broker -> {
			byte[] packet = new byte[3 + 2000];
			packet[0] = 0x30;                      // PUBLISH, QoS 0
			packet[1] = (byte) (0x80 | (2002 & 0x7f));
			packet[2] = (byte) (2002 >> 7);
			packet[3] = 0x00;
			packet[4] = 0x01;
			packet[5] = 'a';
			broker.write(packet);
		});
	}

	/**
	 * A malformed CONNACK fails the connect at once, with the decoder's exception as the cause.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testMalformedConnack() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "DecodeFailure-connack");
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "DecodeFailure-connack");
				o.setKeepAliveInterval(30);
				IMqttToken t = client.connect(o);
				broker.accept();
				broker.read();
				broker.write(0x00, 0x00);   // reserved type 0 instead of CONNACK
				long start = System.nanoTime();
				try {
					t.waitForCompletion(10_000);
					Assert.fail("connected");
				} catch (MqttException e) {
					long ms = (System.nanoTime() - start) / 1_000_000;
					Assert.assertTrue("failed after " + ms + " ms", ms < 2000);
					Assert.assertEquals(MqttException.REASON_CODE_INVALID_MESSAGE, invalidMessageCode(e));
				}
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	private interface BadPacket {
		void send(RawBroker broker) throws Exception;
	}

	/**
	 * Connects with a 30 s keep-alive and a 1000-byte size limit, sends a well-formed message,
	 * then the bad packet: the good message arrives, and connectionLost follows within a
	 * second, with REASON_CODE_INVALID_MESSAGE.
	 */
	private static void assertClosedPromptly(BadPacket bad) throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "DecodeFailure");
			List<Object> events = new CopyOnWriteArrayList<>();
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "DecodeFailure");
				o.setKeepAliveInterval(30);
				o.setMaxBytesInMessage(1000);
				client.setCallback(new MqttCallbackAdapter() {
					@Override
					public void messageArrived(String topic, MqttMessage message) {
						events.add("message on " + topic);
					}

					@Override
					public void connectionLost(Throwable cause) {
						events.add(cause);
					}
				});
				IMqttToken t = client.connect(o);
				broker.acceptAndConnack(0);
				t.waitForCompletion(5000);
				IMqttToken s = client.subscribe("#", 1);
				broker.suback(broker.readUntil(8));
				s.waitForCompletion(5000);
				broker.publish("good", 0, 0, "x");
				bad.send(broker);
				long start = System.nanoTime();
				long deadline = System.currentTimeMillis() + 10_000;
				while (events.size() < 2 && System.currentTimeMillis() < deadline) {
					Thread.sleep(10);
				}
				long ms = (System.nanoTime() - start) / 1_000_000;
				Assert.assertEquals("message on good", events.get(0));
				Assert.assertEquals("events: " + events, 2, events.size());
				Assert.assertTrue("connectionLost after " + ms + " ms", ms < 1000);
				Assert.assertEquals(MqttException.REASON_CODE_INVALID_MESSAGE, invalidMessageCode((Throwable) events.get(1)));
				Assert.assertFalse(client.isConnected());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/** Finds the reason code of the MqttException in a chain of causes, or -1. */
	private static int invalidMessageCode(Throwable t) {
		for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
			if (c instanceof MqttException && ((MqttException) c).getReasonCode() == MqttException.REASON_CODE_INVALID_MESSAGE) {
				return MqttException.REASON_CODE_INVALID_MESSAGE;
			}
		}
		return -1;
	}
}
