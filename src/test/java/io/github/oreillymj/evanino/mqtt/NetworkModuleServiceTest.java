/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/NetworkModuleServiceTest.java,
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

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Port of Paho's {@code NetworkModuleServiceTest}: broker URI validation. Paho validates URIs
 * in {@code NetworkModuleService.validateURI}; this client does it in
 * {@link MqttConnectOptions#parseBrokerUrl()}, which is what these tests call. Error messages
 * differ from Paho's, so only the exception type is checked.
 */
public class NetworkModuleServiceTest {

	/**
	 * Parses a broker URI the way the client does before connecting.
	 *
	 * @param uri the broker URI
	 */
	private static void validateURI(String uri) {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI(uri);
		options.parseBrokerUrl();
	}

	/**
	 * TCP, TLS, WebSocket and secure WebSocket URIs are accepted, including host names with
	 * underscores.
	 */
	@Test
	public void testValidateURI() {
		validateURI("tcp://host_literal:1883");
		validateURI("ssl://host_literal:8883");
		validateURI("ws://host_literal:80/path/to/ws");
		validateURI("wss://host_literal:443/path/to/ws");
	}

	/**
	 * A string that is not a URI is rejected.
	 */
	@Test
	public void failInvalidUri() {
		try {
			validateURI("no URI at all");
			fail("Must fail: Can't parse string to URI");
		} catch (IllegalArgumentException e) {
			// expected
		}
	}

	/**
	 * A TCP URI with a path is rejected.
	 */
	@Test
	public void failWithPathOnTcpUri() {
		try {
			validateURI("tcp://localhost:1883/somePath");
			fail("Must fail: URI path must be empty");
		} catch (IllegalArgumentException e) {
			// expected
		}
	}

	/**
	 * A TLS URI with a path is rejected.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void failWithPathOnSslUri() {
		validateURI("ssl://localhost:1883/somePath");
	}

	/**
	 * A URI with an unknown scheme is rejected.
	 */
	@Test
	public void failWithUnsuppurtedSchemeUri() {
		try {
			validateURI("unknown://localhost:1883");
			fail("Must fail: Unknow scheme");
		} catch (IllegalArgumentException e) {
			// expected
		}
	}

	/**
	 * Paho works around {@code java.net.URI} not parsing user info, host and port when the
	 * host contains an underscore. The patch itself is commented out in Paho's test.
	 */
	@Ignore("Tests Paho's NetworkModuleService.applyRFC3986AuthorityPatch, which has no equivalent here")
	@Test
	public void testApplyRFC3986AuthorityPatch() {
	}

	/**
	 * Paho creates a {@code TCPNetworkModule} for a {@code tcp://} URI.
	 */
	@Ignore("Tests Paho's NetworkModule internals; this client builds a Netty pipeline instead")
	@Test
	public void testCreateInstance() {
	}
}
