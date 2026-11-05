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

package bench;

/**
 * Names of the threads of this client's shared event loop group, which start lazily as
 * connections are spread over its loops; they are bounded by the group's size, so the soak test
 * reports them apart from thread growth.
 */
final class SharedLoopThreads {

	/** Thread name prefix of the shared group (see SharedEventLoop in the client). */
	static final String PREFIX = "netty-mqtt-";

	private SharedLoopThreads() {
	}
}
