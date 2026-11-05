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
 * Compile-time debugging switches for the Netty pipeline.
 */
public final class NettyDebug {
	// Flip this manually when you need to see raw Netty stack traces
	/** When {@code true}, raw Netty exception stack traces are printed. */
	public static final boolean PRINT_STACK_TRACES = false;

	/**
	 * Prevents instantiation; this class only holds constants.
	 */
	private NettyDebug() {
	} // no instances
}