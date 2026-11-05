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

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks4ProxyHandler;

/**
 * {@link ProxyHandlerFactory} that creates SOCKS4 proxy handlers, optionally identifying
 * with a username. SOCKS4 has no password authentication.
 */
public class Socks4ProxyHandlerFactory implements ProxyHandlerFactory {

    private final String TAG = "Socks4ProxyHandlerFactory()";
    private final boolean enableLogging=false;

    private SimpleLogger logger = new SimpleLogger();

    private final SocketAddress proxyAddress;
    private  String username;

    /**
     * Creates a new proxy handler factory that will create SOCKS4 proxy handlers that use the proxy at the given
     * address and that will not perform authentication.
     *
     * @param proxyAddress the address of the SOCKS4 proxy server
     */
    public Socks4ProxyHandlerFactory(final SocketAddress proxyAddress) {
        this(proxyAddress, null);
    }

    /**
     * Creates a new proxy handler factory that will create SOCKS4 proxy handlers that use the proxy at the given
     * address and that will authenticate with the given username.
     *
     * @param proxyAddress the address of the SOCKS4 proxy server
     * @param username the username to use when connecting to the given proxy server
     */
    public Socks4ProxyHandlerFactory(final SocketAddress proxyAddress, final String username) {
        this.proxyAddress = proxyAddress;
        this.username = username;
    }

    /**
     * Logs a message if logging is enabled for this factory.
     *
     * @param data the message to log
     */
    private void log(String data){
        if (enableLogging){
            logger.log(data);
        }
    }

    /**
     * Logs a message with the given tag if logging is enabled for this factory.
     *
     * @param TAG  the prefix written before the message
     * @param data the message to log
     */
    private void log(String TAG, String data){
        if (enableLogging){
            logger.log(TAG, data);
        }
    }

    /**
     * Creates a SOCKS4 proxy handler, using the username if one is set.
     * If the proxy cannot be reached, the connection attempt fails; it is never made without
     * the proxy.
     *
     * @return a new proxy handler, or {@code null} if no proxy address is set
     */
    @Override
    public ProxyHandler createProxyHandler() {
        final Socks4ProxyHandler handler;

        if (BuildConfig.LOGGING && enableLogging) log(TAG,"createProxyHandler()");

        if (this.proxyAddress==null){
            if (BuildConfig.LOGGING && enableLogging) log(TAG,"createProxyHandler()->proxyAddress is null");
            return null;
        }

        InetSocketAddress proxyAddress = (InetSocketAddress) this.proxyAddress;

        if (proxyAddress.isUnresolved()) {
            proxyAddress = new InetSocketAddress(proxyAddress.getHostString(), proxyAddress.getPort());
        }


        if (BuildConfig.LOGGING && enableLogging) log(TAG,"createProxyHandler() got here");

        // For reasons that are not immediately clear, HttpProxyHandler doesn't allow null usernames/passwords if
        // specified. If we want them to be null, we have to use the constructor that doesn't take a username/password
        // at all.
        if (this.username != null) {
            handler = new Socks4ProxyHandler(proxyAddress, this.username);
        } else {
            handler = new Socks4ProxyHandler(proxyAddress);
        }

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
     * Returns {@link ProxyType#SOCKS4}.
     *
     * @return {@link ProxyType#SOCKS4}
     */
    @Override
    public ProxyType getProxyType() {
        return ProxyType.SOCKS4;
    }

    /**
     * Sets the username used to identify with the proxy. A {@code null} username leaves the
     * current one unchanged, and the password is ignored because SOCKS4 does not use one.
     *
     * @param username the proxy username
     * @param password ignored
     */
    @Override
    public void setProxyAuth(String username, String password) {
        if (username!=null){
            this.username=username;
        }
    }

    /** {@inheritDoc} */
    @Override
    public SocketAddress getSocketAddress() {
        return this.proxyAddress;
    }

    // Visible for testing
    /**
     * Returns the configured username.
     *
     * @return the username, or {@code null}
     */
    String getUsername() {
        return this.username;
    }

    // Visible for testing
    /**
     * Returns the password, which SOCKS4 does not use.
     *
     * @return always {@code null}
     */
    String getPassword() {
        return null;
    }
}
