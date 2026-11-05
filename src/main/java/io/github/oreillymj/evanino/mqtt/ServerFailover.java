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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * One client's progress through an {@link MqttServerList}: which broker to try, and when to give
 * up. The rules are described on {@link MqttServerList}. The client calls {@link #failed()} after
 * each failed attempt or lost connection, and {@link #connected()} on CONNACK; it only keeps one
 * of these when the list has more than one broker. Not thread-safe: the client calls it from the
 * event loop, or before a connect starts.
 */
final class ServerFailover {

	private final List<MqttConnectOptions> servers;

	private final boolean untilRetryLimit;

	private final MqttServerList.Strategy strategy;

	/** The retry limit for next-server mode, resolved; -1 for none. */
	private final int maxRetries;

	private final Random random;

	/** Whether a first connect tries each broker once and then fails. */
	private final boolean onePassOnFirstConnect;

	/** The broker of the current or next attempt. */
	private int current;

	/** Failures of the current broker since the client moved to it or last connected. */
	private int failuresOnCurrent;

	/** Failures since the last successful connection, across all brokers. */
	private int failures;

	/** The brokers that have failed since the last successful connection. */
	private final boolean[] failed;

	/**
	 * Takes a snapshot of a list.
	 *
	 * @param list a list of two or more brokers
	 */
	ServerFailover(MqttServerList list) {
		this(list, ThreadLocalRandom.current());
	}

	/**
	 * Takes a snapshot of a list, with a given source of random choices (for tests).
	 *
	 * @param list   a list of two or more brokers
	 * @param random the source of random choices for {@link MqttServerList.Strategy#RANDOM}
	 */
	ServerFailover(MqttServerList list, Random random) {
		this.servers = new ArrayList<>(list.getServers());
		this.untilRetryLimit = list.isReconnectUntilRetryLimit();
		this.strategy = list.getStrategy();
		int retries = list.getMaxRetries();
		this.maxRetries = retries == MqttServerList.RETRIES_OF_FIRST_SERVER
				? servers.get(0).getMaxRetries()
				: retries;
		this.random = random;
		this.onePassOnFirstConnect = list.isOnePassOnFirstConnect();
		this.failed = new boolean[servers.size()];
	}

	/**
	 * Returns the options of the broker for the current or next attempt.
	 *
	 * @return the options
	 */
	MqttConnectOptions current() {
		return servers.get(current);
	}

	/**
	 * Returns the index in the list of the broker for the current or next attempt.
	 *
	 * @return the index, from 0
	 */
	int currentIndex() {
		return current;
	}

	/**
	 * Returns the number of brokers.
	 *
	 * @return the number of brokers
	 */
	int size() {
		return servers.size();
	}

	/**
	 * Returns whether a first connect tries each broker once and then fails, whatever the retry
	 * limit (lists built from {@link MqttConnectOptions#setServerURIs(String[])}, as in Paho).
	 *
	 * @return {@code true} if so
	 */
	boolean isOnePassOnFirstConnect() {
		return onePassOnFirstConnect;
	}

	/**
	 * Records a successful connection to the current broker: the failures are forgotten, and the
	 * next failure is counted from there.
	 */
	void connected() {
		failuresOnCurrent = 0;
		failures = 0;
		Arrays.fill(failed, false);
	}

	/**
	 * Records a failure of the current broker, a failed attempt or a lost connection, and chooses
	 * the broker for the next attempt.
	 *
	 * @return {@code true} if there is a next attempt, now {@link #current()}; {@code false} if
	 *         the client should give up
	 */
	boolean failed() {
		failures++;
		failuresOnCurrent++;
		if (untilRetryLimit) {
			int limit = servers.get(current).getMaxRetries();
			if (limit < 0 || failuresOnCurrent <= limit) {
				return true;   // retry the same broker
			}
			failed[current] = true;
			for (int step = 1; step < servers.size(); step++) {
				int next = (current + step) % servers.size();
				if (!failed[next]) {
					moveTo(next);
					return true;
				}
			}
			return false;   // every broker has used up its retries
		}
		if (maxRetries >= 0 && failures > maxRetries) {
			return false;
		}
		failed[current] = true;
		moveTo(strategy == MqttServerList.Strategy.RANDOM
				? randomOther()
				: (current + 1) % servers.size());
		return true;
	}

	/**
	 * Returns a random broker other than the current one, preferring those that have not failed
	 * since the last successful connection.
	 *
	 * @return the index of the broker
	 */
	private int randomOther() {
		List<Integer> fresh = new ArrayList<>();
		List<Integer> others = new ArrayList<>();
		for (int i = 0; i < servers.size(); i++) {
			if (i != current) {
				others.add(i);
				if (!failed[i]) {
					fresh.add(i);
				}
			}
		}
		List<Integer> pool = fresh.isEmpty() ? others : fresh;
		return pool.get(random.nextInt(pool.size()));
	}

	/**
	 * Makes a broker the current one.
	 *
	 * @param index its index in the list
	 */
	private void moveTo(int index) {
		current = index;
		failuresOnCurrent = 0;
	}

	/**
	 * Returns how long to wait before the next attempt: the current broker's retry interval,
	 * doubled for each earlier failure of that broker (until-the-retry-limit mode) or for each
	 * earlier pass through the list (next-server mode), and capped at its maximum reconnect delay.
	 *
	 * @return the delay in milliseconds
	 */
	long delayMillis() {
		MqttConnectOptions options = current();
		int attempt = untilRetryLimit
				? failuresOnCurrent
				: (failures - 1) / servers.size() + 1;
		return MqttAsyncClient.reconnectDelayMillis(options.getRetryInterval(), attempt,
				options.getMaxReconnectDelay());
	}
}
