/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/internal/MqttPersistentDataTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c)  2014 IBM Corp.
 *
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v1.0
 *   and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 *   The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *   and the Eclipse Distribution License is available at
 *     http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.Assert;
import org.junit.Test;

/**
 * Port of Paho's {@code MqttPersistentDataTest}. Paho's {@code MqttPersistentData} is
 * {@link MqttPersistable} in this client.
 */
public class MqttPersistentDataTest {

	/**
	 * A persistable built with a {@code null} payload keeps a copy of the header, not the
	 * caller's array.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testConstructWithNullPayload() throws Exception {
		byte[] head = new byte[]{1, 2, 3};
		MqttPersistable data = new MqttPersistable(head, null);
		Assert.assertArrayEquals("Header data", head, data.getHeaderBytes());
		Assert.assertNotSame("Header cloned", head, data.getHeaderBytes());
		Assert.assertEquals(0, data.getPayloadLength());
	}
}
