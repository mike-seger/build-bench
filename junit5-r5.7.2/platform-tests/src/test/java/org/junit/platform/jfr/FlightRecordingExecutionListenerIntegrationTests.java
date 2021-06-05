/*
 * Copyright 2015-2021 the original author or authors.
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v2.0 which
 * accompanies this distribution and is available at
 *
 * https://www.eclipse.org/legal/epl-v20.html
 */

package org.junit.platform.jfr;

import static dev.morling.jfrunit.ExpectedEvent.event;
import static dev.morling.jfrunit.JfrEventsAssert.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request;

import dev.morling.jfrunit.EnableEvent;
import dev.morling.jfrunit.JfrEventTest;
import dev.morling.jfrunit.JfrEvents;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.platform.launcher.core.LauncherFactoryForTestingPurposesOnly;

@JfrEventTest
public class FlightRecordingExecutionListenerIntegrationTests {

	public JfrEvents jfrEvents = new JfrEvents();

	@Test
	@EnableEvent("org.junit.*")
	void reportsEvents() {
		var launcher = LauncherFactoryForTestingPurposesOnly.createLauncher(new JupiterTestEngine());
		var request = request() //
				.selectors(selectClass(TestCase.class)) //
				.build();

		launcher.execute(request, new FlightRecordingExecutionListener());
		jfrEvents.awaitEvents();

		assertThat(jfrEvents) //
				.contains(event("org.junit.TestPlanExecution") //
						.with("engineNames", "JUnit Jupiter")) //
				.contains(event("org.junit.TestExecution") //
						.with("displayName", "JUnit Jupiter") //
						.with("type", "CONTAINER")) //
				.contains(event("org.junit.TestExecution") //
						.with("displayName", FlightRecordingExecutionListenerIntegrationTests.class.getSimpleName()
								+ "$" + TestCase.class.getSimpleName()) //
						.with("type", "CONTAINER")) //
				.contains(event("org.junit.TestExecution") //
						.with("displayName", "test(TestReporter)") //
						.with("type", "TEST") //
						.with("result", "SUCCESSFUL")) //
				.contains(event("org.junit.ReportEntry") //
						.with("key", "message") //
						.with("value", "Hello JFR!")) //
				.contains(event("org.junit.SkippedTest") //
						.with("displayName", "skipped()") //
						.with("type", "TEST") //
						.with("reason", "for demonstration purposes"));
	}

	static class TestCase {
		@Test
		void test(TestReporter reporter) {
			reporter.publishEntry("message", "Hello JFR!");
		}

		@Test
		@Disabled("for demonstration purposes")
		void skipped() {
		}
	}
}
