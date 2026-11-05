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

import java.net.SocketAddress;

/**
 * Creates the Netty {@link ProxyHandler} used to tunnel the broker connection through a proxy.
 */
public interface ProxyHandlerFactory {
    /**
     * Creates a new proxy handler to install at the head of a channel pipeline. A fresh handler
     * is needed for each connection attempt, as Netty proxy handlers cannot be reused.
     *
     * @return a new proxy handler, or {@code null} if the proxy is not configured
     */
    ProxyHandler createProxyHandler();

    /**
     * Returns a factory with the same proxy address and credentials. The bundled
     * implementations return {@code this}.
     *
     * @return an equivalent factory
     */
    ProxyHandlerFactory createProxyHandlerFactory();

    /**
     * Returns the kind of proxy this factory creates handlers for.
     *
     * @return the proxy type
     */
    ProxyType getProxyType();

    /**
     * Sets the credentials used to authenticate with the proxy.
     *
     * @param username the proxy username
     * @param password the proxy password
     */
    void setProxyAuth(String username, String password);

    /**
     * Returns the address of the proxy server.
     *
     * @return the proxy address
     */
    SocketAddress getSocketAddress();
}
