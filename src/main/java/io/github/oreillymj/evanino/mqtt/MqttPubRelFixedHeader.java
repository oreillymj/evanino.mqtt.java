/*******************************************************************************
 * Copyright (c) 2017, 2026 Michael OReilly
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

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Builds the fixed header for PUBREL packets, the third step of the QoS 2 handshake.
 */
public class MqttPubRelFixedHeader implements IMqttPublishFixedHeader {


	/*
	Netty's MqttFixedHeader packs those four bits as (dup << 3) | (qos << 1) | retain — so dup=false, qos=AT_MOST_ONCE (0), retain=false produces 0000, not 0010.
	To get the required 0010 pattern, the qos field specifically needs to be AT_LEAST_ONCE (value 1) — that's the only one of these three flags that can produce the 0010 bit pattern Netty's encoder will write.
	It's a quirk of how the spec overloads the qos bits specifically for PUBREL, not an actual QoS level in the usual sense.
	 */

	/**
	 * Creates a PUBREL fixed header. The QoS is set to {@link MqttQoS#AT_LEAST_ONCE} so the
	 * flag bits encode as the {@code 0010} pattern the spec requires.
	 *
	 * @return a new PUBREL fixed header
	 */
	public static MqttFixedHeader getFixedHeader() {
		// Create a new MqttFixedHeader
		return new MqttFixedHeader(
				MqttMessageType.PUBREL, // Message Type
				false, // DUP flag - Oasis spec says this must be false - https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349261
				MqttQoS.AT_LEAST_ONCE, // Quality of Service Level must 1 as per spec - https://stanford-clark.com/MQTT/#publish / https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037
				false, // Retain
				0); // Remaining Length
	}

}
