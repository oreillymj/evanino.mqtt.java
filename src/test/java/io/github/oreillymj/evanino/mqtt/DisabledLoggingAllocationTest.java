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
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;

/**
 * With logging disabled, the per-message paths do not build log messages. They once
 * concatenated their log text before checking whether logging was enabled, and
 * {@code isConnected()}, called by every publish, built four strings per call: half of the
 * client's allocation at QoS 0. Not part of Paho's suite; needs no broker.
 */
public class DisabledLoggingAllocationTest {

	/** Calls measured, after as many to warm up. */
	private static final int CALLS = 100_000;

	/**
	 * {@code isConnected()} allocates almost nothing per call with logging disabled.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testIsConnectedDoesNotBuildLogMessages() throws Exception {
		java.lang.management.ThreadMXBean mx = ManagementFactory.getThreadMXBean();
		Assume.assumeTrue("needs per-thread allocation counters", mx instanceof com.sun.management.ThreadMXBean);
		com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) mx;
		long id = Thread.currentThread().getId();

		MqttAsyncClient client = new MqttAsyncClient("tcp://127.0.0.1:1", "DisabledLoggingAllocationTest");
		try {
			boolean any = false;
			for (int i = 0; i < CALLS; i++) {
				any |= client.isConnected();
			}
			long before = threads.getThreadAllocatedBytes(id);
			for (int i = 0; i < CALLS; i++) {
				any |= client.isConnected();
			}
			long perCall = (threads.getThreadAllocatedBytes(id) - before) / CALLS;
			Assert.assertFalse(any);
			Assert.assertTrue("isConnected() allocated " + perCall + " bytes per call", perCall < 16);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}
}
