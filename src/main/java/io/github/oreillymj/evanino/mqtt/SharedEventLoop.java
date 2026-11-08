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

import io.netty.channel.EventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.SystemPropertyUtil;

import java.util.concurrent.TimeUnit;

/**
 * The event loop group shared by every client that is not given one with
 * {@link MqttAsyncClient#setEventLoop(EventLoopGroup)}. It has Netty's default size, twice the
 * number of processors, or one thread per processor after
 * {@link MqttAsyncClient#enableSharedGroupSizing(boolean)}, and daemon threads named {@code netty-mqtt-*}, so thousands of clients
 * share a few I/O threads instead of each creating a group of its own.
 * <p>
 * The group is reference-counted: each client using it holds one reference, released when the
 * client is shut down or given another group. When the last reference goes, the group is shut
 * down; the next client to need one creates a new group.
 */
final class SharedEventLoop {

	/** Prefix of the shared group's thread names. */
	static final String THREAD_PREFIX = "netty-mqtt";

	private static EventLoopGroup group;
	private static int references;
	private static boolean sizedToProcessors;

	/**
	 * Prevents instantiation; this class only holds static state.
	 */
	private SharedEventLoop() {
	}

	/**
	 * Takes a reference to the shared group, creating it if there is none or the existing one
	 * has been shut down.
	 *
	 * @return the shared group
	 */
	// Netty 4.2 deprecates NioEventLoopGroup in favour of MultiThreadIoEventLoopGroup with an
	// NioIoHandler, but in 4.2 it is a subclass that creates exactly that, and Netty 4.1 has no
	// MultiThreadIoEventLoopGroup. Using it keeps the client running on both.
	@SuppressWarnings("deprecation")
	static synchronized EventLoopGroup acquire() {
		if (group == null || group.isShuttingDown()) {
			group = new io.netty.channel.nio.NioEventLoopGroup(sizedToProcessors ? processorThreads() : 0,
					new DefaultThreadFactory(THREAD_PREFIX, true));
			references = 0;
		}
		references++;
		return group;
	}

	/**
	 * Releases a reference taken with {@link #acquire()}, shutting the group down when it was
	 * the last. A group that is no longer the shared one is ignored.
	 *
	 * @param released the group the reference was taken on
	 */
	static synchronized void release(EventLoopGroup released) {
		if (released == null || released != group) {
			return;
		}
		if (--references <= 0) {
			group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
			group = null;
			references = 0;
		}
	}

	/**
	 * Returns whether a group is the current shared one.
	 *
	 * @param candidate the group
	 * @return {@code true} if it is the shared group
	 */
	static synchronized boolean isShared(EventLoopGroup candidate) {
		return candidate != null && candidate == group;
	}

	/**
	 * Returns the number of references held on the shared group, for tests.
	 *
	 * @return the reference count, 0 if there is no group
	 */
	static synchronized int referenceCount() {
		return group == null ? 0 : references;
	}

	/**
	 * Sets whether the group, when next created, has one thread per processor rather than
	 * Netty's default of two.
	 *
	 * @param enable {@code true} for one thread per processor
	 */
	static synchronized void setSizedToProcessors(boolean enable) {
		sizedToProcessors = enable;
	}

	/**
	 * Returns whether the group, when next created, has one thread per processor.
	 *
	 * @return {@code true} for one thread per processor
	 */
	static synchronized boolean isSizedToProcessors() {
		return sizedToProcessors;
	}

	/**
	 * Returns one thread per processor, or Netty's {@code io.netty.eventLoopThreads} system
	 * property if set.
	 *
	 * @return the number of event loop threads, at least 1
	 */
	private static int processorThreads() {
		return Math.max(1, SystemPropertyUtil.getInt("io.netty.eventLoopThreads", NettyRuntime.availableProcessors()));
	}
}
