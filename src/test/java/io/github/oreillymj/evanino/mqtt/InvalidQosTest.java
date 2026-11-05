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

/**
 * Wherever the client takes a QoS as an {@code int}, a value MQTT does not have (anything but
 * 0, 1 and 2) means QoS 0: no exception, and QoS 0 on the wire. Paho throws
 * {@code IllegalArgumentException} from {@code MqttMessage.setQos}. Also, a SUBACK granting a
 * QoS that does not exist is taken as QoS 0 rather than failing in the handler. Uses
 * {@link RawBroker}; needs no real broker.
 */
public class InvalidQosTest {

	/**
	 * MqttMessage's setter and constructor.
	 */
	@Test
	public void testMessage() {
		MqttMessage m = new MqttMessage(new byte[]{1});
		for (int bad : new int[]{-1, 3, 128, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
			m.setQos(2);
			m.setQos(bad);
			Assert.assertEquals("setQos(" + bad + ")", 0, m.getQos());
			Assert.assertEquals("new MqttMessage(..., " + bad + ", ...)", 0, new MqttMessage("x", bad, false, false).getQos());
		}
		m.setQos(1);
		Assert.assertEquals(1, m.getQos());
		MqttConnectOptions o = new MqttConnectOptions();
		o.setWill("will", new byte[]{1}, 7, false);
		Assert.assertEquals(0, o.getLastWill().getQos().value());
	}

	/**
	 * On the wire: publishes (int overloads, MqttMessage and MqttTopic) and subscribes (single
	 * and array) with an invalid QoS go out at QoS 0, and a SUBACK granting QoS 3 completes the
	 * subscribe.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testOnTheWire() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "InvalidQos");
			try {
				IMqttToken c = client.connect(TestClients.options(broker.uri(), "InvalidQos"));
				broker.acceptAndConnack(0);
				c.waitForCompletion(5000);

				client.publish("t", new byte[]{1}, 3, false);
				Assert.assertEquals(0, publishQos(broker.readUntil(3)));
				client.publish("t", new byte[]{1}, -1, false);
				Assert.assertEquals(0, publishQos(broker.readUntil(3)));
				MqttMessage m = new MqttMessage(new byte[]{1});
				m.setQos(9);
				client.publish("t", m);
				Assert.assertEquals(0, publishQos(broker.readUntil(3)));
				client.getTopic("t").publish(new byte[]{1}, 5, false);
				Assert.assertEquals(0, publishQos(broker.readUntil(3)));

				IMqttToken s = client.subscribe("a", 4);
				RawBroker.Packet sub = broker.readUntil(8);
				Assert.assertEquals("requested QoS", 0, sub.body[sub.body.length - 1]);
				broker.write(0x90, 0x03, sub.body[0] & 0xff, sub.body[1] & 0xff, 0x03);   // granted QoS 3
				s.waitForCompletion(5000);
				Assert.assertNull(s.getException());

				IMqttToken s2 = client.subscribe(new String[]{"b"}, new int[]{-7});
				RawBroker.Packet sub2 = broker.readUntil(8);
				Assert.assertEquals("requested QoS", 0, sub2.body[sub2.body.length - 1]);
				broker.write(0x90, 0x03, sub2.body[0] & 0xff, sub2.body[1] & 0xff, 0x00);
				s2.waitForCompletion(5000);
				Assert.assertTrue(client.isConnected());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	private static int publishQos(RawBroker.Packet p) {
		return (p.first >> 1) & 0x03;
	}
}
