/*
 * Copyright 2015-2021 the original author or authors.
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v2.0 which
 * accompanies this distribution and is available at
 *
 * https://www.eclipse.org/legal/epl-v20.html
 */

package org.junit.platform.suite.engine;

import static java.util.Collections.emptyList;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.platform.commons.util.Preconditions;
import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.EngineDiscoveryOrchestrator;
import org.junit.platform.launcher.core.EngineDiscoveryOrchestrator.Phase;
import org.junit.platform.launcher.core.EngineExecutionOrchestrator;
import org.junit.platform.launcher.core.LauncherDiscoveryResult;
import org.junit.platform.launcher.core.ServiceLoaderTestEngineRegistry;

class SuiteLauncher {

	private final EngineExecutionOrchestrator executionOrchestrator = new EngineExecutionOrchestrator();
	private final EngineDiscoveryOrchestrator discoveryOrchestrator;

	static SuiteLauncher create() {
		Set<TestEngine> engines = new LinkedHashSet<>();
		new ServiceLoaderTestEngineRegistry().loadTestEngines().forEach(engines::add);
		return new SuiteLauncher(engines);
	}

	private SuiteLauncher(Set<TestEngine> testEngines) {
		Preconditions.condition(hasTestEngineOtherThanSuiteEngine(testEngines),
			() -> "Cannot create SuiteLauncher without at least one other TestEngine; "
					+ "consider adding an engine implementation JAR to the classpath");
		this.discoveryOrchestrator = new EngineDiscoveryOrchestrator(testEngines, emptyList());
	}

	private boolean hasTestEngineOtherThanSuiteEngine(Set<TestEngine> testEngines) {
		return testEngines.stream().anyMatch(testEngine -> !SuiteEngineDescriptor.ENGINE_ID.equals(testEngine.getId()));
	}

	LauncherDiscoveryResult discover(LauncherDiscoveryRequest discoveryRequest, UniqueId parentId) {
		return discoveryOrchestrator.discover(discoveryRequest, Phase.DISCOVERY, parentId);
	}

	void execute(LauncherDiscoveryResult discoveryResult, EngineExecutionListener listener) {
		executionOrchestrator.execute(discoveryResult, listener);
	}

}
