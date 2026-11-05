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

/**
 * Settings fixed when the library is built. Maven expands this template (see
 * {@code templating-maven-plugin} in the POM) into {@code target/generated-sources}.
 */
final class BuildConfig {

	/**
	 * Whether the debug log calls are compiled in: {@code ${mqtt.logging}} for this build. The
	 * default build sets it to {@code true}, so {@code enableLogging(true)} turns them on at run
	 * time; while logging is off they cost a field check and HotSpot does not compile them.
	 * Building with {@code -Pno-logging} (or {@code -Dmqtt.logging=false}) sets it to
	 * {@code false}, and javac leaves the debug log calls out of the classes altogether. Log
	 * calls in {@code catch} blocks are kept in both builds.
	 */
	static final boolean LOGGING = ${mqtt.logging};

	private BuildConfig() {
	}
}
