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
 * A CONNACK that refuses the connection fails the connect with the matching reason code: an
 * {@link MqttSecurityException} for bad credentials (4) and for not authorised (5), as Paho
 * throws (Paho issue 880 is about getting it). Uses {@link RawBroker}; needs no real broker.
 */
public class ConnackRefusalTest {

	/**
	 * Each refusal code, through MqttAsyncClient's token and through MqttClient.connect.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testRefusals() throws Exception {
		int[][] cases = {
				{1, MqttException.REASON_CODE_INVALID_PROTOCOL_VERSION, 0},
				{2, MqttException.REASON_CODE_INVALID_CLIENT_ID, 0},
				{4, MqttException.REASON_CODE_FAILED_AUTHENTICATION, 1},
				{5, MqttException.REASON_CODE_NOT_AUTHORIZED, 1},
		};
		for (int[] c : cases) {
			try (RawBroker broker = new RawBroker()) {
				MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "ConnackRefusal");
				try {
					IMqttToken t = client.connect(TestClients.options(broker.uri(), "ConnackRefusal"));
					broker.acceptAndConnack(c[0]);
					try {
						t.waitForCompletion(5000);
						Assert.fail("connected despite CONNACK " + c[0]);
					} catch (MqttException e) {
						assertRefusal(c, e);
					}
				} finally {
					TestClients.shutdownQuietly(client);
				}
			}
			try (RawBroker broker = new RawBroker()) {
				MqttClient client = new MqttClient(broker.uri().toString(), "ConnackRefusal-sync");
				Thread answer = new Thread(() -> {
					try {
						broker.acceptAndConnack(c[0]);
					} catch (Exception ignored) {
						// the test fails on the connect
					}
				});
				answer.start();
				try {
					client.connect(TestClients.options(broker.uri(), "ConnackRefusal-sync"));
					Assert.fail("connected despite CONNACK " + c[0]);
				} catch (MqttException e) {
					assertRefusal(c, e);
				} finally {
					answer.join(5000);
					client.close(true);
				}
			}
		}
	}

	private static void assertRefusal(int[] c, MqttException e) {
		Assert.assertEquals("CONNACK " + c[0], c[1], e.getReasonCode());
		Assert.assertEquals("CONNACK " + c[0] + " gave " + e.getClass().getName(), c[2] == 1, e instanceof MqttSecurityException);
	}
}
