/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/Issue370Test.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2017 Red Hat Inc.
 *
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v2.0
 *   and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 *   The Eclipse Public License is available at
 *      https://www.eclipse.org/legal/epl-2.0
 *   and the Eclipse Distribution License is available at
 *     https://www.eclipse.org/org/documents/edl-v10.php
 *
 *   Contributors:
 *       Red Hat Inc - initial API and implementation
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.Test;

/**
 * Port of Paho's {@code Issue370Test}: closing a client that was never connected must not
 * fail or hang. Paho's {@code close()} is {@link MqttAsyncClient#shutdown()} here.
 */
public class Issue370Test {

	/**
	 * A never-connected client can be closed.
	 *
	 * @throws MqttException if closing fails
	 */
	@Test(timeout = 60_000)
	public void noOpenClose() throws MqttException {
		final MqttAsyncClient client = new MqttAsyncClient("tcp://localhost", "foo-bar");
		client.shutdown();
	}
}
