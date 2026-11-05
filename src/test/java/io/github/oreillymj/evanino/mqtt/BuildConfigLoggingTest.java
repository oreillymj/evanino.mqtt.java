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
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * The debug log calls are compiled in only when {@link BuildConfig#LOGGING} is {@code true}:
 * with debug output enabled, a client logs from {@code isConnected()} in the default build and
 * logs nothing there in a {@code -Pno-logging} build. Passes in both builds. Not part of Paho's
 * suite; needs no broker.
 */
public class BuildConfigLoggingTest {

	/**
	 * {@code isConnected()} writes its debug lines if, and only if, the build includes them.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testDebugLoggingFollowsBuildConfig() throws Exception {
		MqttAsyncClient client = new MqttAsyncClient("tcp://127.0.0.1:1", "BuildConfigLoggingTest");
		PrintStream original = System.out;
		ByteArrayOutputStream captured = new ByteArrayOutputStream();
		try {
			client.enableDebugOutput(true);
			System.setOut(new PrintStream(captured, true, "UTF-8"));
			client.isConnected();
		} finally {
			System.setOut(original);
			client.enableDebugOutput(false);
			TestClients.shutdownQuietly(client);
		}
		boolean logged = new String(captured.toByteArray(), StandardCharsets.UTF_8).contains("isConnected()->");
		Assert.assertEquals("debug output from isConnected() with BuildConfig.LOGGING=" + BuildConfig.LOGGING,
				BuildConfig.LOGGING, logged);
	}
}
