/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/MqttConnectOptionsTest.java,
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

import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;

import static io.github.oreillymj.evanino.mqtt.MqttConnectOptions.MQTT_VERSION_3_1;
import static io.github.oreillymj.evanino.mqtt.MqttConnectOptions.MQTT_VERSION_3_1_1;
import static io.github.oreillymj.evanino.mqtt.MqttConnectOptions.MQTT_VERSION_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Port of Paho's {@code MqttConnectOptionsTest}.
 */
public class MqttConnectOptionsTest {

	/**
	 * The supported MQTT versions can be set.
	 */
	@Test
	public void testValidateMQTTVersions() {
		MqttConnectOptions connectOptions = new MqttConnectOptions();

		connectOptions.setMqttVersion(MQTT_VERSION_DEFAULT);
		assertEquals(MQTT_VERSION_DEFAULT, connectOptions.getMqttVersion());
		connectOptions.setMqttVersion(MQTT_VERSION_3_1);
		assertEquals(MQTT_VERSION_3_1, connectOptions.getMqttVersion());
		connectOptions.setMqttVersion(MQTT_VERSION_3_1_1);
		assertEquals(MQTT_VERSION_3_1_1, connectOptions.getMqttVersion());
	}

	/**
	 * As in Paho, an unknown numeric version is rejected with {@code IllegalArgumentException},
	 * including 5: Paho's MQTT 3 client does not accept it.
	 */
	@Test
	public void testInvalidMQTTVersions() {
		MqttConnectOptions connectOptions = new MqttConnectOptions();
		for (int version : new int[]{-1, 1, 2, 5, 6}) {
			try {
				connectOptions.setMqttVersion(version);
				fail("version " + version + " should be rejected");
			} catch (IllegalArgumentException expected) {
				// unknown version
			}
		}
	}

	/**
	 * Paho's numeric versions select the matching protocol: 0 is the default (3.1.1), 3 is
	 * MQTT 3.1 and 4 is MQTT 3.1.1.
	 */
	@Test
	public void testPahoNumericMQTTVersions() {
		MqttConnectOptions connectOptions = new MqttConnectOptions();
		connectOptions.setMqttVersion(3);
		assertEquals(MqttVersion.MQTT_3_1, connectOptions.getMqttVersion());
		connectOptions.setMqttVersion(4);
		assertEquals(MqttVersion.MQTT_3_1_1, connectOptions.getMqttVersion());
		connectOptions.setMqttVersion(3);
		connectOptions.setMqttVersion(0);
		assertEquals(MQTT_VERSION_DEFAULT, connectOptions.getMqttVersion());
		assertEquals(MqttVersion.MQTT_3_1_1, MQTT_VERSION_DEFAULT);
	}

	/**
	 * A {@code null} version selects the default.
	 */
	@Test
	public void testNullMQTTVersionSelectsDefault() {
		MqttConnectOptions connectOptions = new MqttConnectOptions();
		connectOptions.setMqttVersion(MQTT_VERSION_3_1);

		connectOptions.setMqttVersion(null);

		assertEquals(MQTT_VERSION_DEFAULT, connectOptions.getMqttVersion());
	}

	// -------------------------------------------------------------
	// Not in Paho: re-parsing the broker address
	// -------------------------------------------------------------

	/**
	 * Re-parsing the address, which {@code setSslContext()} triggers, keeps a WebSocket URI's
	 * scheme and path; the path is still normalized to end in {@code /mqtt}.
	 *
	 * @throws Exception if the default TLS context is unavailable
	 */
	@Test
	public void testReparseKeepsWebSocketPath() throws Exception {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI("wss://localhost:8084/broker/ws");
		assertEquals(new URI("wss://localhost:8084/broker/ws/mqtt"), options.getServerURI());

		options.setSslContext(SSLContext.getDefault());

		assertEquals(new URI("wss://localhost:8084/broker/ws/mqtt"), options.getServerURI());
		assertTrue(options.isWebSocket());
		assertTrue(options.isSecure());
	}

	/**
	 * Re-parsing keeps the URL's port, rather than appending it to the already-parsed host.
	 */
	@Test
	public void testReparseKeepsPort() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI("tcp://localhost:1884");
		assertEquals(1884, options.getServerURI().getPort());

		options.setSslContext();   // clears the TLS context and marks the address for re-parsing

		assertEquals(1884, options.getServerURI().getPort());
		assertEquals("localhost", options.getHost());
	}

	/**
	 * A port set with {@code setPort} overrides the port in the URL.
	 */
	@Test
	public void testSetPortOverridesUrlPort() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI("tcp://localhost:1883");
		options.setPort(1999);

		assertEquals(1999, options.getServerURI().getPort());
		assertEquals(1999, options.getPort());
	}

	/**
	 * Whichever of {@code setPort} and a URL with a port is set last decides the port: a later
	 * {@code setServerURI} or {@code setHost} with a port replaces an earlier {@code setPort},
	 * and a later {@code setPort} replaces the URL's port. Covers parsed and unparsed options,
	 * a missing scheme, WebSocket URLs and host names with underscores.
	 */
	@Test
	public void testLaterServerUriPortOverridesEarlierSetPort() {
		String[][] cases = {
				// URL set after setPort, expected port
				{"tcp://localhost:1884", "1884"},
				{"localhost:1884", "1884"},
				{"ws://localhost:9001/broker", "9001"},
				{"tcp://my_broker:1884", "1884"},
		};
		for (String[] c : cases) {
			for (boolean parsedFirst : new boolean[]{false, true}) {
				MqttConnectOptions options = new MqttConnectOptions();
				options.setServerURI("tcp://localhost:1883");
				options.setPort(1999);
				if (parsedFirst) {
					assertEquals(1999, parsedPort(options));
				}
				options.setServerURI(c[0]);
				assertEquals(c[0] + (parsedFirst ? " (after parse)" : ""),
						Integer.parseInt(c[1]), parsedPort(options));

				options.setPort(2001);   // and setPort after the URL wins again
				assertEquals(c[0], 2001, parsedPort(options));
			}
		}

		// setHost behaves like setServerURI
		MqttConnectOptions options = new MqttConnectOptions();
		options.setPort(1999);
		options.setHost("tcp://localhost:1884");
		assertEquals(1884, parsedPort(options));
	}

	/**
	 * A URL without a port keeps a port set earlier with {@code setPort}, and the scheme
	 * default applies only when neither gives one.
	 */
	@Test
	public void testServerUriWithoutPortKeepsEarlierSetPort() {
		for (String url : new String[]{"tcp://localhost", "localhost", "ssl://localhost", "ws://localhost/mqtt", "tcp://my_broker"}) {
			MqttConnectOptions options = new MqttConnectOptions();
			options.setPort(1999);
			options.setServerURI(url);
			assertEquals(url, 1999, parsedPort(options));
		}
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI("tcp://localhost");
		assertEquals(1883, parsedPort(options));
	}

	/**
	 * Setting a new server URI after the old one has been parsed takes effect.
	 */
	@Test
	public void testSetServerURIAfterParse() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI("tcp://localhost:1883");
		assertEquals(1883, options.getServerURI().getPort());

		options.setServerURI("ws://localhost:8083");

		assertEquals("ws", options.getServerURI().getScheme());
		assertEquals(8083, options.getServerURI().getPort());
	}

	/**
	 * {@code ssl://} is an alias of {@code mqtts://}: TLS straight to the broker on port 8883,
	 * with no proxy implied by the scheme.
	 */
	@Test
	public void testSslSchemeIsTlsWithoutProxy() {
		for (String scheme : new String[]{"ssl", "mqtts"}) {
			MqttConnectOptions options = new MqttConnectOptions();
			options.setServerURI(scheme + "://localhost");

			assertEquals(scheme, 8883, options.getServerURI().getPort());
			assertTrue(scheme, options.isSecure());
			assertFalse(scheme, options.isWebSocket());
			assertNull(scheme + ":// must not configure a proxy", options.getProxy());
		}
	}

	/**
	 * {@code http://} and {@code https://} are aliases of {@code ws://} and {@code wss://}: the
	 * same WebSocket transport and default ports, with the path ending in {@code /mqtt}, which
	 * is added if missing and not doubled if present. No proxy is implied.
	 *
	 * @throws Exception if a URI cannot be built
	 */
	@Test
	public void testHttpSchemesAreWebSocketAliases() throws Exception {
		String[][] cases = {
				{"http://localhost", "ws://localhost:8083/mqtt"},
				{"http://localhost/mqtt", "ws://localhost:8083/mqtt"},
				{"http://localhost:9001/broker", "ws://localhost:9001/broker/mqtt"},
				{"http://localhost:9001/broker/mqtt/", "ws://localhost:9001/broker/mqtt"},
				{"https://localhost", "wss://localhost:8084/mqtt"},
				{"https://localhost/mqtt", "wss://localhost:8084/mqtt"},
				{"https://localhost:443/ws", "wss://localhost:443/ws/mqtt"},
		};
		for (String[] c : cases) {
			MqttConnectOptions options = new MqttConnectOptions();
			options.setServerURI(c[0]);
			assertEquals(c[0], new URI(c[1]), options.getServerURI());
			assertTrue(c[0], options.isWebSocket());
			assertEquals(c[0], c[0].startsWith("https"), options.isSecure());
			assertNull(c[0] + " must not configure a proxy", options.getProxy());
		}
	}

	/**
	 * A proxy set with {@code setProxy} or {@code setHTTPProxy} applies to every transport and
	 * is kept when the broker address is parsed.
	 *
	 * @throws Exception if the stand-in proxy socket cannot be opened
	 */
	@Test
	public void testProxyIsIndependentOfScheme() throws Exception {
		// A listening socket stands in for the proxy; nothing is connected through it here.
		try (ServerSocket proxySocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
			int proxyPort = proxySocket.getLocalPort();
			String[] proxies = {"SOCKS4", "SOCKS5", "HTTP"};
			for (String url : new String[]{"tcp://localhost", "ssl://localhost", "mqtts://localhost",
					"ws://localhost", "wss://localhost", "http://localhost", "https://localhost"}) {
				for (String proxy : proxies) {
					MqttConnectOptions options = new MqttConnectOptions();
					if (proxy.equals("SOCKS4")) {
						options.setProxy("127.0.0.1", proxyPort, null, null, false);
					} else if (proxy.equals("SOCKS5")) {
						options.setProxy("127.0.0.1", proxyPort, null, null, true);
					} else {
						options.setHTTPProxy("127.0.0.1", proxyPort, null, null, false);
					}
					options.setServerURI(url);
					assertNotNull(options.getServerURI());
					assertNotNull(url + " lost the " + proxy + " proxy", options.getProxy());
				}
			}
		}
	}

	/**
	 * Parses the options' broker address and returns the port that will be connected to. This
	 * is read from the options rather than from {@code getServerURI()}, because
	 * {@link URI#getPort()} is always {@code -1} for host names containing {@code _}.
	 *
	 * @param options the options
	 * @return the parsed port
	 */
	private static int parsedPort(MqttConnectOptions options) {
		options.getServerURI();
		return options.getPort();
	}
}
