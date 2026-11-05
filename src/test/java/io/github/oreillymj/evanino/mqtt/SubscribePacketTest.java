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

import java.nio.charset.StandardCharsets;

/**
 * Which subscribes send a SUBSCRIBE packet, checked on the wire with {@link RawBroker}:
 * <ul>
 *   <li>subscribing again to a topic at the QoS it is already subscribed at (or already being
 *   subscribed at) sends nothing: the broker already has the subscription;</li>
 *   <li>subscribing to the same topic at a different QoS sends a SUBSCRIBE with the new QoS,
 *   which replaces the subscription on the broker (MQTT 3.1.1 section 3.8.4).</li>
 * </ul>
 * To show that no SUBSCRIBE was sent, each test publishes a marker message afterwards and checks
 * that it is the next packet the broker reads. {@link SubscriptionHandlingTest} checks the same
 * cases against a real broker, by what is delivered. Not part of Paho's suite; needs no broker.
 */
public class SubscribePacketTest {

	/**
	 * After a subscription to a topic at QoS 1 is acknowledged, subscribing to it again at QoS 1
	 * (with no listener, with a listener, with an MqttCallback, or with a token listener) sends
	 * no SUBSCRIBE, and each subscribe's token completes at once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSameQosSendsNoSubscribe() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = connect(broker, "SubscribePacket-same");
			try {
				IMqttToken first = client.subscribe("a/b", 1);
				RawBroker.Packet subscribe = broker.readUntil(8);
				Assert.assertEquals("a/b", topicOf(subscribe));
				Assert.assertEquals(1, qosOf(subscribe));
				broker.suback(subscribe);
				first.waitForCompletion(5000);

				IMqttToken[] again = {
						client.subscribe("a/b", 1),
						client.subscribe("a/b", 1, (t, m) -> { }),
						client.subscribe("a/b", new MqttCallbackAdapter(), 1),
						client.subscribe("a/b", 1, null, null),
				};
				for (IMqttToken t : again) {
					t.waitForCompletion(5000);
					Assert.assertTrue(t.isComplete());
					Assert.assertNull(t.getException());
				}
				assertNextIsMarker(client, broker);
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * Two subscribes to a topic at the same QoS, the second made before the broker has
	 * acknowledged the first, send one SUBSCRIBE, and its SUBACK completes both tokens.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSameQosWhileFirstIsPendingSendsOneSubscribe() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = connect(broker, "SubscribePacket-pending");
			try {
				IMqttToken first = client.subscribe("a/b", 1);
				IMqttToken second = client.subscribe("a/b", 1);
				RawBroker.Packet subscribe = broker.readUntil(8);
				Assert.assertEquals("a/b", topicOf(subscribe));
				Assert.assertFalse("completed before SUBACK", second.isComplete());
				broker.suback(subscribe);
				first.waitForCompletion(5000);
				second.waitForCompletion(5000);
				Assert.assertNull(second.getException());
				assertNextIsMarker(client, broker);
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * Subscribing to a topic already subscribed at QoS 1 at QoS 2, and then at QoS 0, sends a
	 * SUBSCRIBE each time, with the new QoS; subscribing again at QoS 0 then sends nothing.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDifferentQosSendsSubscribe() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = connect(broker, "SubscribePacket-different");
			try {
				subscribeAndAck(client, broker, "a/b", 1);
				subscribeAndAck(client, broker, "a/b", 2);   // higher
				subscribeAndAck(client, broker, "a/b", 0);   // lower

				client.subscribe("a/b", 0).waitForCompletion(5000);   // now the same again
				assertNextIsMarker(client, broker);
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * A subscribe at a different QoS made while the first is still waiting for its SUBACK is
	 * not folded into it: a second SUBSCRIBE is sent, with the new QoS.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDifferentQosWhileFirstIsPendingSendsSubscribe() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = connect(broker, "SubscribePacket-different-pending");
			try {
				IMqttToken first = client.subscribe("a/b", 1);
				IMqttToken second = client.subscribe("a/b", 2);
				RawBroker.Packet s1 = broker.readUntil(8);
				RawBroker.Packet s2 = broker.readUntil(8);
				Assert.assertEquals(1, qosOf(s1));
				Assert.assertEquals(2, qosOf(s2));
				Assert.assertNotEquals(s1.packetId(), s2.packetId());
				broker.write(0x90, 0x03, s1.body[0] & 0xff, s1.body[1] & 0xff, 0x01);
				broker.write(0x90, 0x03, s2.body[0] & 0xff, s2.body[1] & 0xff, 0x02);
				first.waitForCompletion(5000);
				second.waitForCompletion(5000);
				Assert.assertEquals(2, second.getGrantedQos()[0]);
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * The array form, subscribe(String[], int[]): a filter already subscribed at the same QoS
	 * sends no SUBSCRIBE, as with the single-topic forms.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testArrayFormSameQosSendsNoSubscribe() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = connect(broker, "SubscribePacket-array");
			try {
				subscribeAndAck(client, broker, "a/b", 1);
				IMqttToken again = client.subscribe(new String[]{"a/b"}, new int[]{1});
				again.waitForCompletion(5000);
				assertNextIsMarker(client, broker);
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * A subscribe made the moment connect completes sends one SUBSCRIBE. The connect token
	 * completes inside the CONNACK handler, which then sends any subscribes not sent yet; the
	 * subscribing thread could send its SUBSCRIBE at the same moment, and both used to send it.
	 * Repeated, as it depends on timing.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testSubscribeRightAfterConnectSendsOneSubscribe() throws Exception {
		for (int i = 0; i < 30; i++) {
			try (RawBroker broker = new RawBroker()) {
				MqttAsyncClient client = connect(broker, "SubscribePacket-race-" + i);
				try {
					IMqttToken t = client.subscribe("a/b", 1);
					broker.suback(broker.readUntil(8));
					t.waitForCompletion(5000);
					assertNextIsMarker(client, broker);
				} finally {
					TestClients.shutdownQuietly(client);
				}
			}
		}
	}

	/** Connects to the fake broker. */
	private static MqttAsyncClient connect(RawBroker broker, String clientId) throws Exception {
		MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), clientId);
		IMqttToken t = client.connect(TestClients.options(broker.uri(), clientId));
		broker.acceptAndConnack(0);
		t.waitForCompletion(5000);
		return client;
	}

	/** Subscribes, checks the SUBSCRIBE on the wire, grants the QoS asked for, and waits. */
	private static void subscribeAndAck(MqttAsyncClient client, RawBroker broker, String topic, int qos) throws Exception {
		IMqttToken t = client.subscribe(topic, qos);
		RawBroker.Packet subscribe = broker.readUntil(8);
		Assert.assertEquals(topic, topicOf(subscribe));
		Assert.assertEquals("requested QoS", qos, qosOf(subscribe));
		broker.write(0x90, 0x03, subscribe.body[0] & 0xff, subscribe.body[1] & 0xff, qos);
		t.waitForCompletion(5000);
		Assert.assertEquals(qos, t.getGrantedQos()[0]);
	}

	/**
	 * Publishes a marker at QoS 0 and checks that it is the next packet the broker reads, so that
	 * nothing (no SUBSCRIBE) was sent before it.
	 */
	private static void assertNextIsMarker(MqttAsyncClient client, RawBroker broker) throws Exception {
		client.publish("marker", "m".getBytes(StandardCharsets.UTF_8), 0, false);
		RawBroker.Packet next = broker.readUntil(-1);
		Assert.assertEquals("a packet other than the marker PUBLISH was sent (type " + next.type()
				+ (next.type() == 8 ? ", SUBSCRIBE for " + topicOf(next) : "") + ")", 3, next.type());
	}

	/** The (first) topic filter of a SUBSCRIBE. */
	private static String topicOf(RawBroker.Packet subscribe) {
		int length = ((subscribe.body[2] & 0xff) << 8) | (subscribe.body[3] & 0xff);
		return new String(subscribe.body, 4, length, StandardCharsets.UTF_8);
	}

	/** The QoS requested for the (first) topic filter of a SUBSCRIBE. */
	private static int qosOf(RawBroker.Packet subscribe) {
		int length = ((subscribe.body[2] & 0xff) << 8) | (subscribe.body[3] & 0xff);
		return subscribe.body[4 + length] & 0x03;
	}
}
