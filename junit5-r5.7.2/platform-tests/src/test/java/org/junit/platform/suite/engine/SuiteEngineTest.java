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

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectUniqueId;
import static org.junit.platform.suite.engine.SuiteEngineDescriptor.ENGINE_ID;
import static org.junit.platform.testkit.engine.EventConditions.container;
import static org.junit.platform.testkit.engine.EventConditions.displayName;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedSuccessfully;
import static org.junit.platform.testkit.engine.EventConditions.test;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.descriptor.TestMethodTestDescriptor;
import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.PostDiscoveryFilter;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.engine.testcases.DynamicTest;
import org.junit.platform.suite.engine.testcases.MultipleTest;
import org.junit.platform.suite.engine.testcases.SimpleTest;
import org.junit.platform.suite.engine.testsuites.AbstractSuite;
import org.junit.platform.suite.engine.testsuites.DynamicSuite;
import org.junit.platform.suite.engine.testsuites.MultipleSuite;
import org.junit.platform.suite.engine.testsuites.NestedSuite;
import org.junit.platform.suite.engine.testsuites.SelectClassesSuite;
import org.junit.platform.suite.engine.testsuites.SuiteDisplayNameSuite;
import org.junit.platform.suite.engine.testsuites.SuiteSuite;
import org.junit.platform.testkit.engine.EngineTestKit;

class SuiteEngineTest {

	@Test
	void selectClasses() {
		// @formatter:off
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(SelectClassesSuite.class))
				.execute()
				.testEvents()
				.assertThatEvents()
				.haveExactly(1, event(test(SelectClassesSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(1, event(test(SimpleTest.class.getName()), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void suiteDisplayName() {
		// @formatter:off
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(SuiteDisplayNameSuite.class))
				.execute()
				.allEvents()
				.assertThatEvents()
				.haveExactly(1, event(container(displayName("Suite Display Name")), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void abstractSuiteIsNotExecuted() {
		// @formatter:off
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(AbstractSuite.class))
				.execute()
				.testEvents()
				.assertThatEvents()
				.isEmpty();
		// @formatter:on
	}

	@Test
	void privateSuiteIsNotExecuted() {
		// @formatter:off
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(PrivateSuite.class))
				.execute()
				.testEvents()
				.assertThatEvents()
				.isEmpty();
		// @formatter:on
	}

	@Suite
	@SelectClasses(SimpleTest.class)
	private static class PrivateSuite {

	}

	@Test
	void innerSuiteIsNotExecuted() {
		// @formatter:off
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(InnerSuite.class))
				.execute()
				.testEvents()
				.assertThatEvents()
				.isEmpty();
		// @formatter:on
	}

	@SuppressWarnings("InnerClassMayBeStatic")
	@Suite
	@SelectClasses(SimpleTest.class)
	private class InnerSuite {

	}

	@Test
	void nestedSuiteIsNotExecuted() {
		// @formatter:off
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(NestedSuite.class))
				.execute()
				.testEvents()
				.assertThatEvents()
				.isEmpty();
		// @formatter:on
	}

	@Test
	void dynamicSuite() {
		// @formatter:off
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(DynamicSuite.class))
				.execute()
				.testEvents()
				.assertThatEvents()
				.haveExactly(2, event(test(DynamicTest.class.getName()), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void suiteSuite() {
		// @formatter:off
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(SuiteSuite.class))
				.execute()
				.testEvents()
				.assertThatEvents()
				.haveExactly(1, event(test(SuiteSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(1, event(test(SelectClassesSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(1, event(test(SimpleTest.class.getName()), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void selectClassesByUniqueId() {
		// @formatter:off
		UniqueId uniqId = UniqueId.forEngine(ENGINE_ID)
				.append(SuiteTestDescriptor.SEGMENT_TYPE, SelectClassesSuite.class.getName());
		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectUniqueId(uniqId))
				.execute()
				.testEvents()
				.assertThatEvents()
				.haveExactly(1, event(test(SelectClassesSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(1, event(test(SimpleTest.class.getName()), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void selectMethodInTestPlanByUniqueId() {
		// @formatter:off
		UniqueId uniqueId = UniqueId.forEngine(ENGINE_ID)
				.append(SuiteTestDescriptor.SEGMENT_TYPE, MultipleSuite.class.getName())
				.append("engine", JupiterEngineDescriptor.ENGINE_ID)
				.append(ClassTestDescriptor.SEGMENT_TYPE, MultipleTest.class.getName())
				.append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()");

		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectUniqueId(uniqueId))
				.execute()
				.testEvents()
				.assertThatEvents()
				.haveExactly(1, event(test(MultipleSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(1, event(test(MultipleTest.class.getName()), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void selectSuiteByUniqueId() {
		// @formatter:off
		UniqueId uniqueId = UniqueId.forEngine(ENGINE_ID)
				.append(SuiteTestDescriptor.SEGMENT_TYPE, MultipleSuite.class.getName());

		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectUniqueId(uniqueId))
				.execute()
				.testEvents()
				.assertThatEvents()
				.haveExactly(2, event(test(MultipleSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(2, event(test(MultipleTest.class.getName()), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void selectMethodAndSuiteInTestPlanByUniqueId() {
		// @formatter:off
		UniqueId uniqueId = UniqueId.forEngine(ENGINE_ID)
				.append(SuiteTestDescriptor.SEGMENT_TYPE, MultipleSuite.class.getName())
				.append("engine", JupiterEngineDescriptor.ENGINE_ID)
				.append(ClassTestDescriptor.SEGMENT_TYPE, MultipleTest.class.getName())
				.append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()");

		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectUniqueId(uniqueId))
				.selectors(selectClass(SelectClassesSuite.class))
				.execute()
				.testEvents()
				.assertThatEvents()
				.haveExactly(1, event(test(SelectClassesSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(1, event(test(SimpleTest.class.getName()), finishedSuccessfully()))
				.haveExactly(1, event(test(MultipleSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(1, event(test(MultipleTest.class.getName()), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void selectMethodsInTestPlanByUniqueId() {
		// @formatter:off
		UniqueId uniqueId = UniqueId.forEngine(ENGINE_ID)
				.append(SuiteTestDescriptor.SEGMENT_TYPE, MultipleSuite.class.getName())
				.append("engine", JupiterEngineDescriptor.ENGINE_ID)
				.append(ClassTestDescriptor.SEGMENT_TYPE, MultipleTest.class.getName())
				.append(TestMethodTestDescriptor.SEGMENT_TYPE, "test()");

		UniqueId uniqueId2 = UniqueId.forEngine(ENGINE_ID)
				.append(SuiteTestDescriptor.SEGMENT_TYPE, MultipleSuite.class.getName())
				.append("engine", JupiterEngineDescriptor.ENGINE_ID)
				.append(ClassTestDescriptor.SEGMENT_TYPE, MultipleTest.class.getName())
				.append(TestMethodTestDescriptor.SEGMENT_TYPE, "test2()");

		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectUniqueId(uniqueId))
				.selectors(selectUniqueId(uniqueId2))
				.execute()
				.testEvents()
				.assertThatEvents()
				.haveExactly(2, event(test(MultipleSuite.class.getName()), finishedSuccessfully()))
				.haveExactly(2, event(test(MultipleTest.class.getName()), finishedSuccessfully()));
		// @formatter:on
	}

	@Test
	void postDiscoveryCanRemoveTestDescriptorsInSuite() {
		// @formatter:off
		PostDiscoveryFilter postDiscoveryFilter = testDescriptor -> testDescriptor.getSource()
				.filter(MethodSource.class::isInstance)
				.map(MethodSource.class::cast)
				.filter(classSource -> SimpleTest.class.equals(classSource.getJavaClass()))
				.map(classSource -> FilterResult.excluded("Was a test in SimpleTest"))
				.orElseGet(() -> FilterResult.included("Was not a test in SimpleTest"));

		EngineTestKit.engine(ENGINE_ID)
				.selectors(selectClass(SelectClassesSuite.class))
				.filters(postDiscoveryFilter)
				.execute()
				.testEvents()
				.assertThatEvents()
				.isEmpty();
		// @formatter:on
	}

}
