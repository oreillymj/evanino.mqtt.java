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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * {@link MqttException#getMessage()} describes the reason code. It used to look up Paho's
 * message catalog by class name, which does not exist here, so the message was empty for every
 * code. Paho itself (issue 1090) gives only "MqttException" for codes missing from its catalog,
 * such as SUBSCRIBE_FAILED. Not part of Paho's suite; needs no broker.
 */
public class MqttExceptionMessageTest {

	/**
	 * The case from Paho issue 1090: SUBSCRIBE_FAILED (128).
	 */
	@Test
	public void testSubscribeFailedHasAMessage() {
		MqttException e = new MqttException(MqttException.REASON_CODE_SUBSCRIBE_FAILED);
		Assert.assertEquals("The MQTT broker rejected the subscription request.", e.getMessage());
	}

	/**
	 * Every REASON_CODE_* constant has a message of its own, not the unknown-code fallback.
	 *
	 * @throws Exception if a constant cannot be read
	 */
	@Test
	public void testEveryReasonCodeHasAMessage() throws Exception {
		StringBuilder missing = new StringBuilder();
		for (Field f : MqttException.class.getFields()) {
			if (f.getName().startsWith("REASON_CODE_") && Modifier.isStatic(f.getModifiers())) {
				int code = ((Number) f.get(null)).intValue();
				String m = new MqttException(code).getMessage();
				if (m == null || m.trim().isEmpty() || m.startsWith("Unknown MQTT error code")) {
					missing.append(' ').append(f.getName()).append('(').append(code).append(')');
				}
			}
		}
		Assert.assertEquals("reason codes without a message:" + missing, 0, missing.length());
	}

	/**
	 * A code the client does not know still names the code in its message.
	 */
	@Test
	public void testUnknownCodeNamesTheCode() {
		Assert.assertEquals("Unknown MQTT error code: 12345", new MqttException(12345).getMessage());
	}

	/**
	 * The message does not depend on the constructor, and toString() gives the message, the
	 * code and any real cause, without repeating the message.
	 */
	@Test
	public void testToString() {
		MqttException plain = new MqttException(MqttException.REASON_CODE_CLIENT_NOT_CONNECTED);
		Assert.assertEquals(plain.getMessage() + " (32104)", plain.toString());
		// The cause describing the code is kept, as before.
		Assert.assertEquals(plain.getMessage(), plain.getCause().getMessage());

		java.io.IOException io = new java.io.IOException("refused");
		MqttException withCause = new MqttException(MqttException.REASON_CODE_SERVER_CONNECT_ERROR, io);
		Assert.assertEquals(new MqttException(MqttException.REASON_CODE_SERVER_CONNECT_ERROR).getMessage(), withCause.getMessage());
		Assert.assertEquals(withCause.getMessage() + " (32103) - java.io.IOException: refused", withCause.toString());

		MqttException wrapped = new MqttException(io);
		Assert.assertEquals(MqttException.REASON_CODE_CLIENT_EXCEPTION, wrapped.getReasonCode());
		Assert.assertTrue(wrapped.toString(), wrapped.toString().endsWith(" (0) - java.io.IOException: refused"));
	}

	/**
	 * MessageCatalog, kept for Paho compatibility, gives the same messages rather than an
	 * empty string.
	 */
	@Test
	public void testMessageCatalogFallsBack() {
		Assert.assertEquals(new MqttException(MqttException.REASON_CODE_MAX_INFLIGHT).getMessage(),
				MessageCatalog.getMessage(MqttException.REASON_CODE_MAX_INFLIGHT));
	}
}
