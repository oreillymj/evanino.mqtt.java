/*******************************************************************************
 * Copyright (c) 2017, 2026 Michael OReilly
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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Records when one client's keep-alive pings were sent, keeping the current and the previous
 * ping time so the interval between them can be reported. Each {@link MqttAsyncClient} owns
 * its own tracker, so clients in the same JVM do not see each other's pings.
 */
public class KeepAliveTracker {

	private volatile long lastPingEpoch = System.currentTimeMillis();
	private volatile long pingEpoch = System.currentTimeMillis();

	/**
	 * Creates a tracker with both ping times set to now. Called once per client.
	 */
	KeepAliveTracker() {
	}

	/**
	 * Returns the time of the most recent ping.
	 *
	 * @return the time in milliseconds since the epoch
	 */
	public long getPingEpoch() {
		return pingEpoch;
	}

	/**
	 * Records a new ping time; the current ping time becomes the previous one.
	 *
	 * @param epoch the ping time in milliseconds since the epoch
	 */
	public void setPingEpoch(long epoch) {
		lastPingEpoch = pingEpoch;
		pingEpoch = epoch;
	}

	/**
	 * Returns the time of the ping before the most recent one.
	 *
	 * @return the time in milliseconds since the epoch
	 */
	public long getLastPingEpoch() {
		return lastPingEpoch;
	}

	/**
	 * Returns the time of the ping before the most recent one, formatted for display.
	 *
	 * @return the time in {@code yyyy-MM-dd HH:mm:ss:SSS} form
	 */
	public String getLastPingDateTime() {
		return getPingDateTime(lastPingEpoch);
	}


	/**
	 * Returns the time of the most recent ping, formatted for display.
	 *
	 * @return the time in {@code yyyy-MM-dd HH:mm:ss:SSS} form
	 */
	public String getPingDateTime() {
		return getPingDateTime(pingEpoch);
	}

	/**
	 * Returns the interval between the two most recent pings.
	 *
	 * @return the interval in whole seconds
	 */
	public long getPingDeltaSecs() {
		return (pingEpoch - lastPingEpoch) / 1000;
	}


	/**
	 * Formats a time for display.
	 *
	 * @param epoch the time in milliseconds since the epoch
	 * @return the time in {@code yyyy-MM-dd HH:mm:ss:SSS} form, or the epoch if formatting fails
	 */
	private String getPingDateTime(long epoch) {
		String strDate;
		try {
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");//dd/MM/yyyy
			Date now = new Date(epoch);
			strDate = sdfDate.format(now);
		} catch (Exception e) {
			strDate = "1970-01-01 00:00:00:000";
		} finally {
		}
		return strDate;
	}
}//class
