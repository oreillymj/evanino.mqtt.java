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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Custom WebSocket headers: Paho's {@code setCustomWebSocketHeaders(Map)} alongside the single
 * header setters, and the headers reaching the server in the upgrade request. Not part of
 * Paho's suite; needs no broker, as a stand-in server captures the upgrade request.
 */
public class WebSocketHeadersTest {

	/**
	 * The map replaces every header set before, is copied, and can be added to afterwards.
	 */
	@Test
	public void testSetHeadersReplacesAndCopies() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCustomWebSocketHeader(WebSocketHeader.API_KEY, "old");

		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", "Bearer abc");
		headers.put(" X-Tenant ", "blue");
		options.setCustomWebSocketHeaders(headers);

		Map<String, String> expected = new HashMap<>();
		expected.put("Authorization", "Bearer abc");
		expected.put("X-Tenant", "blue");               // name trimmed
		Assert.assertEquals("earlier headers are replaced", expected, options.getCustomWebSocketHeaders());

		headers.put("X-Late", "ignored");               // the caller's map was copied
		Assert.assertFalse(options.getCustomWebSocketHeaders().containsKey("X-Late"));

		options.setCustomWebSocketHeader(WebSocketHeader.COOKIE, "id=1");
		options.setCustomWebSocketHeader("X-Extra", "more");
		Assert.assertEquals(4, options.getCustomWebSocketHeaders().size());
		options.removeCustomWebSocketHeader(WebSocketHeader.COOKIE);
		Assert.assertEquals(3, options.getCustomWebSocketHeaders().size());
	}

	/**
	 * A {@code null} or empty map removes all custom headers.
	 */
	@Test
	public void testNullOrEmptyMapClearsHeaders() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCustomWebSocketHeader("X-One", "1");
		options.setCustomWebSocketHeaders(null);
		Assert.assertTrue(options.getCustomWebSocketHeaders().isEmpty());

		options.setCustomWebSocketHeader("X-One", "1");
		options.setCustomWebSocketHeaders(new HashMap<>());
		Assert.assertTrue(options.getCustomWebSocketHeaders().isEmpty());
	}

	/**
	 * A blank name or value is rejected, and the headers set before are kept.
	 */
	@Test
	public void testInvalidEntryLeavesHeadersUnchanged() {
		String[][] bad = {{null, "v"}, {" ", "v"}, {"X-Name", null}, {"X-Name", "  "}};
		for (String[] entry : bad) {
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCustomWebSocketHeader("X-Keep", "kept");
			Map<String, String> headers = new LinkedHashMap<>();
			headers.put("X-Good", "fine");
			headers.put(entry[0], entry[1]);
			try {
				options.setCustomWebSocketHeaders(headers);
				Assert.fail("accepted " + entry[0] + "=" + entry[1]);
			} catch (IllegalArgumentException expected) {
				// rejected
			}
			Assert.assertEquals(java.util.Collections.singletonMap("X-Keep", "kept"), options.getCustomWebSocketHeaders());
		}
	}

	/**
	 * Headers set with the map and with the single setters are all sent in the WebSocket
	 * upgrade request.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testHeadersAreSentInUpgradeRequest() throws Exception {
		try (ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
			CompletableFuture<String> request = CompletableFuture.supplyAsync(() -> readUpgradeRequest(server));

			MqttConnectOptions options = TestClients.options(
					new java.net.URI("ws://127.0.0.1:" + server.getLocalPort()), "WebSocketHeadersTest");
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer abc");
			headers.put("X-Tenant", "blue");
			options.setCustomWebSocketHeaders(headers);
			options.setCustomWebSocketHeader(WebSocketHeader.API_KEY, "key-123");
			MqttAsyncClient client = new MqttAsyncClient(options);
			try {
				client.connect();   // fails once the stand-in server closes; only the request matters

				String sent = request.get(10, TimeUnit.SECONDS);
				Assert.assertTrue(sent, sent.startsWith("GET /mqtt HTTP/1.1"));
				assertHeader(sent, "Authorization", "Bearer abc");
				assertHeader(sent, "X-Tenant", "blue");
				assertHeader(sent, "X-API-Key", "key-123");
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * Accepts one connection and returns the HTTP request it sends, up to the blank line.
	 *
	 * @param server the listening socket
	 * @return the request line and headers
	 */
	private static String readUpgradeRequest(ServerSocket server) {
		try (Socket socket = server.accept()) {
			InputStream in = socket.getInputStream();
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			int b;
			while ((b = in.read()) != -1) {
				bytes.write(b);
				String sofar = bytes.toString("ISO-8859-1");
				if (sofar.endsWith("\r\n\r\n")) {
					return sofar;
				}
			}
			return bytes.toString("ISO-8859-1");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Checks that a request contains a header line, comparing the name without regard to case.
	 *
	 * @param request the request
	 * @param name    the header name
	 * @param value   the header value
	 */
	private static void assertHeader(String request, String name, String value) {
		for (String line : request.split("\r\n")) {
			int colon = line.indexOf(':');
			if (colon > 0 && line.substring(0, colon).trim().equalsIgnoreCase(name)
					&& line.substring(colon + 1).trim().equals(value)) {
				return;
			}
		}
		Assert.fail("header " + name + ": " + value + " not sent; request was:\n" + request);
	}
}
