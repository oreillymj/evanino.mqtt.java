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

import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * {@link ProxyHandlerFactory} that creates SOCKS5 proxy handlers, optionally authenticating
 * with a username and password.
 */
public class Socks5ProxyHandlerFactory implements ProxyHandlerFactory {

	private final SocketAddress proxyAddress;

	private String username;
	private String password;


	private final String TAG = "Socks5ProxyHandlerFactory()";
	private boolean enableLogging = false;

	private SimpleLogger logger = new SimpleLogger();

	/**
	 * Creates a new proxy handler factory that will create SOCKS5 proxy handlers that use the proxy at the given
	 * address and that will not perform authentication.
	 *
	 * @param proxyAddress the address of the SOCKS5 proxy server
	 */
	public Socks5ProxyHandlerFactory(final SocketAddress proxyAddress) {
		this(proxyAddress, null, null);
	}

	/**
	 * Creates a new proxy handler factory that will create SOCKS5 proxy handlers that use the proxy at the given
	 * address and that will authenticate with the given username and password.
	 *
	 * @param proxyAddress the address of the SOCKS5 proxy server
	 * @param username     the username to use when connecting to the given proxy server
	 * @param password     the password to use when connecting to the given proxy server
	 */
	public Socks5ProxyHandlerFactory(final SocketAddress proxyAddress, final String username, final String password) {
		this.proxyAddress = proxyAddress;

		this.username = username;
		this.password = password;
	}

	/**
	 * Logs a message if logging is enabled for this factory.
	 *
	 * @param data the message to log
	 */
	private void log(String data) {
		if (enableLogging) {
			logger.log(data);
		}
	}

	/**
	 * Logs a message with the given tag if logging is enabled for this factory.
	 *
	 * @param TAG  the prefix written before the message
	 * @param info the message to log
	 */
	private void log(String TAG, String info) {
		if (!enableLogging) {
			return;
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "->" + info);
	}

	/**
	 * Creates a SOCKS5 proxy handler, authenticating only if both a username and a password
	 * are set.
	 * If the proxy cannot be reached, the connection attempt fails; it is never made without
	 * the proxy.
	 *
	 * @return a new proxy handler, or {@code null} if no proxy address is set
	 */
	@Override
	public ProxyHandler createProxyHandler() {
		final Socks5ProxyHandler handler;

		// For reasons that are not immediately clear, HttpProxyHandler doesn't allow null usernames/passwords if
		// specified. If we want them to be null, we have to use the constructor that doesn't take a username/password
		// at all.
		if (BuildConfig.LOGGING && enableLogging) log(TAG, "createProxyHandler()");

		if (this.proxyAddress == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG, "createProxyHandler()->proxyAddress is null");
			return null;
		}

		InetSocketAddress proxyAddress = (InetSocketAddress) this.proxyAddress;
		if (proxyAddress.isUnresolved()) {
			proxyAddress = new InetSocketAddress(proxyAddress.getHostString(), proxyAddress.getPort());
		}

		if (this.username != null && this.password != null) {
			handler = new Socks5ProxyHandler(proxyAddress, this.username, this.password);
		} else {
			handler = new Socks5ProxyHandler(proxyAddress);
		}
		proxyAddress = null;
		return handler;

	}

	/**
	 * Returns this factory.
	 *
	 * @return this factory
	 */
	@Override
	public ProxyHandlerFactory createProxyHandlerFactory() {
		return this;
	}

	/**
	 * Returns {@link ProxyType#SOCKS5}.
	 *
	 * @return {@link ProxyType#SOCKS5}
	 */
	@Override
	public ProxyType getProxyType() {
		return ProxyType.SOCKS5;
	}

	/**
	 * Sets the proxy credentials. A {@code null} argument leaves the corresponding current
	 * value unchanged.
	 *
	 * @param username the proxy username
	 * @param password the proxy password
	 */
	@Override
	public void setProxyAuth(String username, String password) {
		if (username != null) {
			this.username = username;
		}

		if (password != null) {
			this.password = password;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SocketAddress getSocketAddress() {
		return this.proxyAddress;
	}
}
