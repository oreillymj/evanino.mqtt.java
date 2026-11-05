/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/MqttTopicTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c)  2014 IBM Corp.
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
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.Assert;
import org.junit.Test;

/**
 * Port of Paho's {@code MqttTopicTest}: topic filter validation and topic matching.
 */
public class MqttTopicTest {

	/**
	 * Valid topic filters containing wildcards are accepted.
	 */
	@Test
	public void testValidTopicFilterWildcards() {
		String[] topics = new String[]{"+", "+/+", "+/foo", "+/tennis/#", "foo/+", "foo/+/bar", "/+",
				"/+/sport/+/player1", "#", "/#", "sport/#", "sport/tennis/#"};

		for (String topic : topics) {
			MqttTopic.validate(topic, true);
		}
	}

	/**
	 * Topic filters with wildcards match the topic names they should.
	 */
	@Test
	public void testMatchedTopicFilterWildcards() {
		String[][] matchingTopics = new String[][]{
				{"+/+", "sport/hockey"},
				{"/+", "/sport"},
				{"sport/tennis/player1/#", "sport/tennis/player1"},
				{"sport/tennis/player1/#", "sport/tennis/player1/ranking"},
				{"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon"},
				{"sport/#", "sport"},
				{"#", "sport/tennis/player1"},
				{"sport/tennis/player1/#", "sport/tennis/player1//wimbledon"},
				{"sport/+/player1/#", "sport/tennis/player1/wimbledon"},
				{"sport/+/player1/#", "sport/soccer/player1/UEFA"}
		};

		for (String[] pair : matchingTopics) {
			Assert.assertTrue(pair[0] + " should match " + pair[1], MqttTopic.isMatched(pair[0], pair[1]));
		}
	}

	/**
	 * Topic filters with wildcards do not match topic names they should not.
	 */
	@Test
	public void testNonMatchedTopicFilterWildcards() {
		String[][] matchingTopics = new String[][]{
				{"+/+", "/sport"},
				{"+/+", "a/b/c"},
				{"/sport/+", "/sport/"},
				{"sport/tennis/player1/#", "sport/tennis/player2"},
				{"sport1/#", "sport2"},
				{"sport/tennis1/player/#", "sport/tennis2/player"},
				{"sport//tennis/player1/#", "sport/tennis/player1//wimbledon"}
		};

		for (String[] pair : matchingTopics) {
			Assert.assertFalse(pair[0] + " should NOT match " + pair[1], MqttTopic.isMatched(pair[0], pair[1]));
		}
	}

	/**
	 * A multi-level wildcard must occupy a whole topic level.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidTopicFilterWildcards1() {
		MqttTopic.validate("sport/tennis#", true);
	}

	/**
	 * A multi-level wildcard must be the last character of the filter.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidTopicFilterWildcards2() {
		MqttTopic.validate("sport/tennis/#/ranking", true);
	}

	/**
	 * A single-level wildcard must occupy a whole topic level.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidTopicFilterWildcards3() {
		MqttTopic.validate("sport+", true);
	}

	/**
	 * A single-level wildcard cannot share a level with other characters.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidTopicFilterWildcards4() {
		MqttTopic.validate("sport/+aa", true);
	}

	/**
	 * A multi-level wildcard cannot be followed by further levels.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidTopicFilterWildcards5() {
		MqttTopic.validate("sport/#/ball/+/aa", true);
	}
}
