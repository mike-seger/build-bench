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

import static org.apiguardian.api.API.Status.EXPERIMENTAL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

import org.apiguardian.api.API;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.reporting.ReportEntry;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/**
 * A {@link TestExecutionListener} that generates Java Flight Recorder
 * events.
 *
 * @see <a href="https://openjdk.java.net/jeps/328">JEP 328: Flight Recorder</a>
 * @since 1.8
 */
@API(status = EXPERIMENTAL, since = "1.8")
public class FlightRecordingExecutionListener implements TestExecutionListener {

	private final AtomicReference<TestPlanExecutionEvent> testPlanExecutionEvent = new AtomicReference<>();
	private final Map<String, TestExecutionEvent> testExecutionEvents = new ConcurrentHashMap<>();

	@Override
	public void testPlanExecutionStarted(TestPlan plan) {
		var event = new TestPlanExecutionEvent();
		event.containsTests = plan.containsTests();
		event.engineNames = plan.getRoots().stream().map(TestIdentifier::getDisplayName).collect(
			Collectors.joining(", "));
		testPlanExecutionEvent.set(event);
		event.begin();
	}

	@Override
	public void testPlanExecutionFinished(TestPlan plan) {
		testPlanExecutionEvent.getAndSet(null).commit();
	}

	@Override
	public void executionSkipped(TestIdentifier test, String reason) {
		var event = new SkippedTestEvent();
		event.initialize(test);
		event.reason = reason;
		event.commit();
	}

	@Override
	public void executionStarted(TestIdentifier test) {
		var event = new TestExecutionEvent();
		testExecutionEvents.put(test.getUniqueId(), event);
		event.initialize(test);
		event.begin();
	}

	@Override
	public void executionFinished(TestIdentifier test, TestExecutionResult result) {
		var throwable = result.getThrowable();
		var event = testExecutionEvents.remove(test.getUniqueId());
		event.end();
		event.result = result.getStatus().toString();
		event.exceptionClass = throwable.map(Throwable::getClass).orElse(null);
		event.exceptionMessage = throwable.map(Throwable::getMessage).orElse(null);
		event.commit();
	}

	@Override
	public void reportingEntryPublished(TestIdentifier test, ReportEntry reportEntry) {
		for (var entry : reportEntry.getKeyValuePairs().entrySet()) {
			var event = new ReportEntryEvent();
			event.uniqueId = test.getUniqueId();
			event.key = entry.getKey();
			event.value = entry.getValue();
			event.commit();
		}
	}

	@Category({ "JUnit", "Execution" })
	@StackTrace(false)
	abstract static class ExecutionEvent extends Event {
	}

	@Label("Test Execution")
	@Name("org.junit.TestPlanExecution")
	static class TestPlanExecutionEvent extends ExecutionEvent {
		@Label("Contains Tests")
		boolean containsTests;
		@Label("Engine Names")
		String engineNames;
	}

	abstract static class TestEvent extends ExecutionEvent {
		@UniqueId
		@Label("Unique Id")
		String uniqueId;
		@Label("Display Name")
		String displayName;
		@Label("Tags")
		String tags;
		@Label("Type")
		String type;

		void initialize(TestIdentifier test) {
			this.uniqueId = test.getUniqueId();
			this.displayName = test.getDisplayName();
			this.tags = test.getTags().isEmpty() ? null : test.getTags().toString();
			this.type = test.getType().name();
		}
	}

	@Label("Skipped Test")
	@Name("org.junit.SkippedTest")
	static class SkippedTestEvent extends TestEvent {
		@Label("Reason")
		String reason;
	}

	@Label("Test")
	@Name("org.junit.TestExecution")
	static class TestExecutionEvent extends TestEvent {
		@Label("Result")
		String result;
		@Label("Exception Class")
		Class<?> exceptionClass;
		@Label("Exception Message")
		String exceptionMessage;
	}

	@Label("Report Entry")
	@Name("org.junit.ReportEntry")
	static class ReportEntryEvent extends ExecutionEvent {
		@UniqueId
		@Label("Unique Id")
		String uniqueId;
		@Label("Key")
		String key;
		@Label("Value")
		String value;
	}
}
