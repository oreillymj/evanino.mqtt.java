/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/MqttDataTypesTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original file has no notice of its own; it is part of Eclipse Paho, copyright
 * IBM Corp. and the other Paho contributors, under the Eclipse Public License v2.0
 * and the Eclipse Distribution License v1.0.
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Port of Paho's {@code MqttDataTypesTest}: UTF-8 strings survive encoding and decoding.
 * <p>
 * Paho tests its own wire encoder ({@code MqttWireMessage.encodeUTF8} / {@code decodeUTF8}).
 * This client uses Netty's MQTT codec, so each string is sent as the topic name of a PUBLISH
 * through {@link MqttEncoder} and {@link MqttDecoder} and must come back unchanged. Paho's
 * variable byte integer tests and its rejection of invalid UTF-8 cover Paho's encoder
 * specifically; Netty computes the remaining length itself and has no public API for it, and
 * does not reject those strings, so those cases are ignored with the reason.
 */
public class MqttDataTypesTest {

	/**
	 * Encodes a string as a PUBLISH topic name with Netty's encoder and decodes it again,
	 * standing in for Paho's {@code encodeUTF8} / {@code decodeUTF8} round trip.
	 *
	 * @param testString the string to encode and decode
	 * @return the decoded string
	 */
	private static String encodeAndDecodeString(String testString) {
		MqttPublishMessage publish = MqttMessageBuilders.publish()
				.topicName(testString)
				.qos(MqttQoS.AT_MOST_ONCE)
				.retained(false)
				.payload(Unpooled.EMPTY_BUFFER)
				.build();

		EmbeddedChannel encoder = new EmbeddedChannel(MqttEncoder.INSTANCE);
		Assert.assertTrue(encoder.writeOutbound(publish));
		ByteBuf encoded = encoder.readOutbound();
		encoder.finishAndReleaseAll();

		EmbeddedChannel decoder = new EmbeddedChannel(new MqttDecoder());
		Assert.assertTrue(decoder.writeInbound(encoded));
		MqttPublishMessage decoded = decoder.readInbound();
		decoder.finishAndReleaseAll();
		try {
			if (decoded.decoderResult().isFailure()) {
				throw new IllegalArgumentException(decoded.decoderResult().cause());
			}
			return decoded.variableHeader().topicName();
		} finally {
			decoded.release();
		}
	}

	/**
	 * Paho rejects a negative variable byte integer.
	 */
	@Ignore("Tests Paho's encodeMBI; Netty computes the remaining length itself and has no public API for it")
	@Test
	public void TestEncodeNegativeVBI() {
	}

	/**
	 * Paho rejects a variable byte integer larger than 268,435,455.
	 */
	@Ignore("Tests Paho's encodeMBI; Netty computes the remaining length itself and has no public API for it")
	@Test
	public void TestEncodeOversizeVBI() {
	}

	/**
	 * Paho round-trips the variable byte integer 268,435,442.
	 */
	@Ignore("Tests Paho's encodeMBI/readMBI; Netty computes the remaining length itself and has no public API for it")
	@Test
	public void TestEncodeAndDecodeVBI() {
	}

	/**
	 * Paho rejects the control character U+0001 in a UTF-8 string.
	 */
	@Ignore("Paho's encoder rejects control characters; Netty's codec encodes them, so there is no equivalent failure")
	@Test
	public void TestEncodeInvalidUTF8String() {
	}

	/**
	 * Paho rejects the C1 control character U+008C in a UTF-8 string.
	 */
	@Ignore("Paho's encoder rejects control characters; Netty's codec encodes them, so there is no equivalent failure")
	@Test
	public void TestEncodeInvalidUTF8StringInDifferentRange() {
	}

	/**
	 * A surrogate pair round-trips unchanged.
	 */
	@Test
	public void TestEncodeAndDecodeUTF8StringWithSurrogates() {
		String testString = "𐐷";
		String decodedUTF8 = encodeAndDecodeString(testString);
		Assert.assertEquals(testString, decodedUTF8);
	}

	/**
	 * Paho rejects a lone high surrogate.
	 */
	@Ignore("Paho's encoder rejects unpaired surrogates; Netty's codec replaces them with '?', so there is no equivalent failure")
	@Test
	public void TestEncodeAndDecodeUTF8StringWithTrailingHighSurrogate() {
	}

	/**
	 * Paho rejects a lone low surrogate.
	 */
	@Ignore("Paho's encoder rejects unpaired surrogates; Netty's codec replaces them with '?', so there is no equivalent failure")
	@Test
	public void TestEncodeAndDecodeUTF8StringWithLowSurrogate() {
	}

	/**
	 * An ASCII string round-trips unchanged.
	 */
	@Test
	public void TestEncodeAndDecodeUTF8String() {
		String testString = "Answer to life the universe and everything";
		String decodedUTF8 = encodeAndDecodeString(testString);
		Assert.assertEquals(testString, decodedUTF8);
	}

	/**
	 * A Chinese string round-trips unchanged.
	 */
	@Test
	public void TestEncodeAndDecodeChineseUTF8String() {
		String testString = "葛渚噓";
		String decodedUTF8 = encodeAndDecodeString(testString);
		Assert.assertEquals(testString, decodedUTF8);
	}

	/**
	 * A string of emoji round-trips unchanged.
	 */
	@Test
	public void TestEncodeAndDecodeEmojiString() {
		String testString = "👁🐝Ⓜ️️";
		String decodedUTF8 = encodeAndDecodeString(testString);
		Assert.assertEquals(testString, decodedUTF8);
	}

	/**
	 * A shared-subscription style topic mixing ASCII and Chinese round-trips unchanged.
	 */
	@Test
	public void TestEncodeAndDecodeComplexUTF8String() {
		String testString = "$shared/葛渚噓/GVTDurTopic02/葛渚噓";
		String decodedUTF8 = encodeAndDecodeString(testString);
		Assert.assertEquals(testString, decodedUTF8);
	}

	/**
	 * Every sample sentence in {@code i_can_eat_glass.txt}, in many languages and scripts,
	 * round-trips unchanged. Paho reads the file with the platform charset; it is read as
	 * UTF-8 here so the test does not depend on the platform.
	 *
	 * @throws Exception if the resource cannot be read
	 */
	@Test
	public void testICanEatGlass() throws Exception {
		InputStream in = getClass().getClassLoader().getResourceAsStream("i_can_eat_glass.txt");
		Assert.assertNotNull("i_can_eat_glass.txt not on the test classpath", in);
		try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
			for (String line; (line = br.readLine()) != null; ) {
				if (!line.startsWith("#")) {
					String[] parts = line.split(":");
					Assert.assertEquals(2, parts.length);
					String decodedUTF8 = encodeAndDecodeString(parts[1]);
					Assert.assertEquals(parts[0], parts[1], decodedUTF8);
				}
			}
		}
	}
}
