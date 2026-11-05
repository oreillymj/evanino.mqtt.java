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
import org.junit.ClassRule;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;
import java.net.URI;
import java.util.UUID;

/**
 * {@link MqttConnectOptions#enablePahoMode(boolean)}: the settings whose defaults differ from
 * Paho's switch to Paho's values and back, and later setters still override them. The
 * disconnect timeouts it changes are tested in {@link DisconnectQuiesceTest}. The TLS test
 * requires a broker with TLS listeners; see {@link MosquittoBroker}.
 */
public class PahoModeTest {

	/** The broker used by the TLS test. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/**
	 * Paho mode is off by default, and the defaults are this client's own.
	 */
	@Test
	public void testOffByDefault() {
		MqttConnectOptions options = new MqttConnectOptions();
		Assert.assertFalse(options.isPahoMode());
		assertClientDefaults(options);
	}

	/**
	 * Turning Paho mode on sets Paho's values.
	 */
	@Test
	public void testOnSetsPahoValues() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.enablePahoMode(true);
		Assert.assertTrue(options.isPahoMode());
		assertPahoValues(options);
	}

	/**
	 * Turning Paho mode off again restores this client's defaults, even after the settings were
	 * changed by hand.
	 */
	@Test
	public void testOffRestoresClientDefaults() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.enablePahoMode(true);
		options.setMaxInflight(50);
		options.setConnectionTimeout(12);
		options.enablePahoMode(false);
		Assert.assertFalse(options.isPahoMode());
		assertClientDefaults(options);
	}

	/**
	 * Setters called after {@code enablePahoMode(true)} override Paho's values, and Paho mode
	 * stays on.
	 */
	@Test
	public void testLaterSettersOverride() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.enablePahoMode(true);
		options.setHttpsHostnameVerificationEnabled(false);
		options.setConnectionTimeout(7);
		options.setMaxInflight(100);
		options.setMaxRetries(3);
		options.setMessageArrivedErrorsTriggerDisconnect(false);

		Assert.assertTrue(options.isPahoMode());
		Assert.assertFalse(options.isHttpsHostnameVerificationEnabled());
		Assert.assertEquals(7, options.getConnectionTimeout());
		Assert.assertEquals(100, options.getMaxInflight());
		Assert.assertEquals(3, options.getMaxRetries());
		Assert.assertFalse(options.isMessageArrivedErrorsTriggerDisconnect());
	}

	/**
	 * Calling {@code enablePahoMode(true)} replaces values set before it, so it should come
	 * first.
	 */
	@Test
	public void testEarlierSettersAreReplaced() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setMaxInflight(100);
		options.setConnectionTimeout(7);
		options.enablePahoMode(true);
		assertPahoValues(options);
	}

	/**
	 * Paho mode leaves the settings that already match Paho, or that Paho mode does not cover,
	 * alone.
	 */
	@Test
	public void testOtherSettingsUnchanged() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setAutomaticReconnect(true);
		options.setKeepAliveInterval(20);
		options.setMaxReconnectDelay(5000);
		options.enablePahoMode(true);
		Assert.assertTrue(options.getAutomaticReconnect());
		Assert.assertEquals(20, options.getKeepAliveInterval());
		Assert.assertEquals(5000, options.getMaxReconnectDelay());
		Assert.assertEquals(1L, options.getRetryInterval());

		MqttConnectOptions defaults = new MqttConnectOptions();
		defaults.enablePahoMode(true);
		Assert.assertFalse("automatic reconnect stays off, as in Paho", defaults.getAutomaticReconnect());
	}

	/**
	 * In Paho mode a TLS connection to {@code 127.0.0.1}, which the test server certificate does
	 * not name, fails hostname verification, as it does in Paho.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testPahoModeVerifiesHostname() throws Exception {
		MqttConnectOptions options = TestClients.options(
				new URI("mqtts://127.0.0.1:" + broker.getSslServerURI().getPort()), "PahoModeTest-" + UUID.randomUUID());
		options.enablePahoMode(true);
		options.setSslContext(TestTls.clientContext());
		MqttClient client = new MqttClient(options);
		client.setTimeToWait(5000);
		try {
			client.connect(options);
			Assert.fail("Hostname verification should have rejected 127.0.0.1");
		} catch (MqttException ex) {
			for (Throwable t = ex; t != null && t.getCause() != t; t = t.getCause()) {
				if (t instanceof SSLHandshakeException) {
					return;
				}
			}
			throw new AssertionError("Expected an SSLHandshakeException in the cause chain of " + ex, ex);
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * In Paho mode, {@code 0} is accepted for the connection timeout and keep-alive, as in Paho,
	 * and stored as {@code -1}: no connect timeout, and keep-alive 0 in CONNECT.
	 */
	@Test
	public void testZeroMeansDisabledInPahoMode() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.enablePahoMode(true);
		options.setConnectionTimeout(0);
		options.setKeepAliveInterval(0);
		Assert.assertEquals(-1, options.getConnectionTimeout());
		Assert.assertEquals(0, options.getConnectTimeoutMillis());
		Assert.assertEquals(-1, options.getKeepAliveInterval());
		Assert.assertEquals(0, options.getConnectKeepAlive());
	}

	/**
	 * Outside Paho mode {@code 0} is still rejected, and in Paho mode other negative values
	 * than {@code -1} still are.
	 */
	@Test
	public void testZeroRejectedOutsidePahoMode() {
		MqttConnectOptions options = new MqttConnectOptions();
		assertRejected(() -> options.setConnectionTimeout(0));
		assertRejected(() -> options.setKeepAliveInterval(0));

		options.enablePahoMode(true);
		options.enablePahoMode(false);
		assertRejected(() -> options.setConnectionTimeout(0));
		assertRejected(() -> options.setKeepAliveInterval(0));

		options.enablePahoMode(true);
		assertRejected(() -> options.setConnectionTimeout(-2));
		assertRejected(() -> options.setKeepAliveInterval(-2));
	}

	/**
	 * Paho-style options with a connection timeout and keep-alive of 0 connect, and stay
	 * connected while idle.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	public void testPahoModeConnectsWithZeroTimeouts() throws Exception {
		MqttConnectOptions options = TestClients.options(broker.getServerURI(), "PahoModeTest-" + UUID.randomUUID());
		options.enablePahoMode(true);
		options.setConnectionTimeout(0);
		options.setKeepAliveInterval(0);
		MqttClient client = new MqttClient(options);
		try {
			client.connect(options);
			Thread.sleep(2500);
			Assert.assertTrue(client.isConnected());
			client.disconnect();
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * Checks that a setter call is rejected with {@link IllegalArgumentException}.
	 *
	 * @param call the setter call
	 */
	private static void assertRejected(Runnable call) {
		try {
			call.run();
			Assert.fail("expected IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			// rejected
		}
	}

	/**
	 * Checks the values Paho mode sets.
	 *
	 * @param options the options
	 */
	private static void assertPahoValues(MqttConnectOptions options) {
		Assert.assertTrue(options.isHttpsHostnameVerificationEnabled());
		Assert.assertEquals(30, options.getConnectionTimeout());
		Assert.assertEquals(10, options.getMaxInflight());
		Assert.assertEquals(-1, options.getMaxRetries());
		Assert.assertTrue(options.isMessageArrivedErrorsTriggerDisconnect());
	}

	/**
	 * Checks this client's defaults for the settings Paho mode changes.
	 *
	 * @param options the options
	 */
	private static void assertClientDefaults(MqttConnectOptions options) {
		Assert.assertFalse(options.isHttpsHostnameVerificationEnabled());
		Assert.assertEquals(5, options.getConnectionTimeout());
		Assert.assertEquals(MqttConnectOptions.DEFAULT_MAX_INFLIGHT, options.getMaxInflight());
		Assert.assertEquals(2, options.getMaxRetries());
		Assert.assertFalse(options.isMessageArrivedErrorsTriggerDisconnect());
	}
}
