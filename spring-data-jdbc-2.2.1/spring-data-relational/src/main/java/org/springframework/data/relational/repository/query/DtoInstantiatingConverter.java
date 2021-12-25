/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.repository.query;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.SimplePropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.Assert;

/**
 * {@link Converter} to instantiate DTOs from fully equipped domain objects.
 *
 * @author Mark Paluch
 */
public class DtoInstantiatingConverter implements Converter<Object, Object> {

	private final Class<?> targetType;
	private final MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> context;
	private final EntityInstantiator instantiator;

	/**
	 * Creates a new {@link Converter} to instantiate DTOs.
	 *
	 * @param dtoType must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param instantiator must not be {@literal null}.
	 */
	public DtoInstantiatingConverter(Class<?> dtoType,
									 MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
									 EntityInstantiators instantiator) {

		Assert.notNull(dtoType, "DTO type must not be null!");
		Assert.notNull(context, "MappingContext must not be null!");
		Assert.notNull(instantiator, "EntityInstantiators must not be null!");

		this.targetType = dtoType;
		this.context = context;
		this.instantiator = instantiator.getInstantiatorFor(context.getRequiredPersistentEntity(dtoType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
	 */
	@Override
	public Object convert(Object source) {

		if (targetType.isInterface()) {
			return source;
		}

		final PersistentEntity<?, ?> sourceEntity = context.getRequiredPersistentEntity(source.getClass());
		final PersistentPropertyAccessor sourceAccessor = sourceEntity.getPropertyAccessor(source);
		final PersistentEntity<?, ?> targetEntity = context.getRequiredPersistentEntity(targetType);
		final PreferredConstructor<?, ? extends PersistentProperty<?>> constructor = targetEntity
				.getPersistenceConstructor();

		@SuppressWarnings({"rawtypes", "unchecked"})
		Object dto = instantiator.createInstance(targetEntity, new ParameterValueProvider() {

			@Override
			public Object getParameterValue(Parameter parameter) {
				return sourceAccessor.getProperty(sourceEntity.getPersistentProperty(parameter.getName()));
			}
		});

		final PersistentPropertyAccessor dtoAccessor = targetEntity.getPropertyAccessor(dto);

		targetEntity.doWithProperties((SimplePropertyHandler) property -> {

			if (constructor.isConstructorParameter(property)) {
				return;
			}

			dtoAccessor.setProperty(property,
					sourceAccessor.getProperty(sourceEntity.getPersistentProperty(property.getName())));
		});

		return dto;
	}
}
