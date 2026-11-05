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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * The payload a QoS 1 or 2 publish keeps for retransmission still holds the message after the
 * PUBLISH packet has been built. Netty's {@code MqttMessageBuilders} copies the payload with
 * {@code writeBytes}, which once drained the kept buffer, so every retransmitted PUBLISH was
 * sent with an empty payload. Not part of Paho's suite; needs no broker.
 */
public class RetransmissionPayloadTest {

	/**
	 * A publish held while disconnected keeps a readable copy of its payload, and its PUBLISH
	 * packet carries the payload too.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testKeptPayloadIsReadable() throws Exception {
		MqttAsyncClient client = new MqttAsyncClient("tcp://127.0.0.1:1", "RetransmissionPayloadTest");
		try {
			byte[] payload = {1, 2, 3};
			for (int qos = 1; qos <= 2; qos++) {
				IMqttDeliveryToken token = client.publish("RetransmissionPayloadTest", payload, qos, false);
				MqttPendingPublish pending = client.getPendingPublishes().get(token.getMessageId());
				Assert.assertNotNull("the publish should be held", pending);
				ByteBuf kept = pending.getPayload();
				Assert.assertArrayEquals("payload kept for retransmission (QoS " + qos + ")",
						payload, ByteBufUtil.getBytes(kept));
				Assert.assertArrayEquals("payload of the PUBLISH packet (QoS " + qos + ")",
						payload, ByteBufUtil.getBytes(pending.getMessage().payload()));
			}
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}
}
