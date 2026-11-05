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

import java.nio.charset.StandardCharsets;

/**
 * Length checks for the strings and binary fields MQTT encodes with a 16-bit length prefix
 * (topics, topic filters, client ID, user name, will topic, password, will payload in 3.1.1).
 * <p>
 * Netty's {@code MqttEncoder} does not check these lengths: it writes the length modulo 65536
 * followed by the whole value, so a longer value spills into the next field (for example a
 * topic into the PUBLISH payload), the problem of CVE-2025-10543 in Paho Go. Every such value is
 * checked here before it is encoded.
 */
final class MqttStrings {

	/** The longest value a 16-bit length prefix can describe. */
	static final int MAX_LENGTH = 65535;

	private MqttStrings() {
	}

	/**
	 * Returns the number of bytes a string takes in UTF-8.
	 *
	 * @param value the string
	 * @return its UTF-8 length
	 */
	static int utf8Length(String value) {
		return value.getBytes(StandardCharsets.UTF_8).length;
	}

	/**
	 * Checks that a string fits MQTT's 16-bit length prefix when encoded in UTF-8.
	 *
	 * @param value the string, or {@code null} (not checked)
	 * @param what  what the string is, for the message
	 * @throws IllegalArgumentException if it encodes to more than 65535 bytes
	 */
	static void checkLength(String value, String what) {
		if (value != null) {
			// A UTF-8 character takes at most 3 bytes per UTF-16 char, so short strings need no encoding.
			if (value.length() * 3 > MAX_LENGTH) {
				checkLength(utf8Length(value), what);
			}
		}
	}

	/**
	 * Checks that a binary field fits MQTT's 16-bit length prefix.
	 *
	 * @param value the bytes, or {@code null} (not checked)
	 * @param what  what the field is, for the message
	 * @throws IllegalArgumentException if it is longer than 65535 bytes
	 */
	static void checkLength(byte[] value, String what) {
		if (value != null) {
			checkLength(value.length, what);
		}
	}

	/**
	 * Checks a topic name for a PUBLISH: 65535 bytes at most, and no wildcard ({@code +},
	 * {@code #}) or U+0000, which MQTT forbids in topic names and brokers reject by closing
	 * the connection.
	 *
	 * @param topic the topic name
	 * @throws IllegalArgumentException if it is not a valid topic name
	 */
	static void checkTopicName(String topic) {
		checkLength(topic, "The topic");
		for (int i = 0; i < topic.length(); i++) {
			char c = topic.charAt(i);
			if (c == '+' || c == '#') {
				throw new IllegalArgumentException("The topic name must not contain wildcards (+, #): " + topic);
			}
			if (c == 0) {
				throw new IllegalArgumentException("The topic name must not contain U+0000");
			}
		}
	}

	/**
	 * Checks a topic filter for SUBSCRIBE or UNSUBSCRIBE: 1 to 65535 bytes, no U+0000, {@code #}
	 * only as the last level, and {@code +} only as a whole level (MQTT 3.1.1 section 4.7). A
	 * broker closes the connection on an invalid filter.
	 *
	 * @param filter the topic filter
	 * @throws IllegalArgumentException if it is not a valid topic filter
	 */
	static void checkTopicFilter(String filter) {
		checkLength(filter, "The topic filter");
		if (filter.indexOf(0) >= 0) {
			throw new IllegalArgumentException("The topic filter must not contain U+0000");
		}
		MqttTopic.validate(filter, true);
	}

	private static void checkLength(int length, String what) {
		if (length > MAX_LENGTH) {
			throw new IllegalArgumentException(what + " is " + length + " bytes; MQTT allows at most " + MAX_LENGTH);
		}
	}
}
