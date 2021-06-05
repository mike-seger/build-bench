/*
 * Copyright 2015-2021 the original author or authors.
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v2.0 which
 * accompanies this distribution and is available at
 *
 * https://www.eclipse.org/legal/epl-v20.html
 */

package org.junit.platform.launcher.core;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

import java.util.ServiceLoader;

import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.junit.platform.commons.util.ClassLoaderUtils;

/**
 * @since 1.8
 */
class ServiceLoaderRegistry {

	private static final Logger logger = LoggerFactory.getLogger(ServiceLoaderRegistry.class);

	<T> Iterable<T> load(Class<T> serviceProviderClass) {
		Iterable<T> listeners = ServiceLoader.load(serviceProviderClass, ClassLoaderUtils.getDefaultClassLoader());
		logger.config(() -> "Loaded " + serviceProviderClass.getSimpleName() + " instances: "
				+ stream(listeners.spliterator(), false).map(Object::toString).collect(toList()));
		return listeners;
	}

}
