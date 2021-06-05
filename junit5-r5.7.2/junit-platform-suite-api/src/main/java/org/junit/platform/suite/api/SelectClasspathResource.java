/*
 * Copyright 2015-2021 the original author or authors.
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v2.0 which
 * accompanies this distribution and is available at
 *
 * https://www.eclipse.org/legal/epl-v20.html
 */

package org.junit.platform.suite.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apiguardian.api.API;
import org.apiguardian.api.API.Status;

/**
 * {@code @SelectClasspathResource} specifies the classpath resource to
 * <em>select</em> when running a test suite on the JUnit Platform.
 *
 * @since 1.8
 * @see Suite
 * @see org.junit.platform.runner.JUnitPlatform
 * @see org.junit.platform.engine.discovery.DiscoverySelectors#selectClasspathResource(String, org.junit.platform.engine.discovery.FilePosition)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@Documented
@API(status = Status.EXPERIMENTAL, since = "1.8")
@Repeatable(SelectClasspathResources.class)
public @interface SelectClasspathResource {

	/**
	 * A classpath resource name to select.
	 */
	String value();

	/**
	 * The line number; ignored if not greater than zero.
	 */
	int line() default 0;

	/**
	 * The column number; ignored if the line number is ignored; ignored if not
	 * greater than zero.
	 */
	int column() default 0;

}
