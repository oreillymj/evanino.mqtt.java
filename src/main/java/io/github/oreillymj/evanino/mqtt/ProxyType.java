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

/**
 * The kind of proxy, if any, that the client tunnels its broker connection through.
 */
public enum ProxyType {
    /** Connect directly to the broker without a proxy. */
    NONE,
    /** Tunnel through an HTTP proxy using the {@code CONNECT} method. */
    HTTP,
    /** Tunnel through a SOCKS4 proxy. */
    SOCKS4,
    /** Tunnel through a SOCKS5 proxy. */
    SOCKS5
}
