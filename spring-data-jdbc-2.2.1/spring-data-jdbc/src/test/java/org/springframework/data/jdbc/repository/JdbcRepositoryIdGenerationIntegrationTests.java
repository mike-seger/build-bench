/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;
import lombok.Value;
import lombok.With;
import lombok.experimental.FieldDefaults;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Testing special cases for id generation with {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
@ContextConfiguration
@ExtendWith(SpringExtension.class)
public class JdbcRepositoryIdGenerationIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryIdGenerationIntegrationTests.class;
		}
	}

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired ReadOnlyIdEntityRepository readOnlyIdrepository;
	@Autowired PrimitiveIdEntityRepository primitiveIdRepository;
	@Autowired ImmutableWithManualIdEntityRepository immutableWithManualIdEntityRepository;

	@Test // DATAJDBC-98
	public void idWithoutSetterGetsSet() {

		ReadOnlyIdEntity entity = readOnlyIdrepository.save(new ReadOnlyIdEntity(null, "Entity Name"));

		assertThat(entity.getId()).isNotNull();

		assertThat(readOnlyIdrepository.findById(entity.getId())).hasValueSatisfying(it -> {

			assertThat(it.getId()).isEqualTo(entity.getId());
			assertThat(it.getName()).isEqualTo(entity.getName());
		});
	}

	@Test // DATAJDBC-98
	public void primitiveIdGetsSet() {

		PrimitiveIdEntity entity = new PrimitiveIdEntity();
		entity.setName("Entity Name");

		PrimitiveIdEntity saved = primitiveIdRepository.save(entity);

		assertThat(saved.getId()).isNotEqualTo(0L);

		assertThat(primitiveIdRepository.findById(saved.getId())).hasValueSatisfying(it -> {

			assertThat(it.getId()).isEqualTo(saved.getId());
			assertThat(it.getName()).isEqualTo(saved.getName());
		});
	}

	@Test // DATAJDBC-393
	public void manuallyGeneratedId() {

		ImmutableWithManualIdEntity entity = new ImmutableWithManualIdEntity(null, "immutable");
		ImmutableWithManualIdEntity saved = immutableWithManualIdEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();

		assertThat(immutableWithManualIdEntityRepository.findAll()).hasSize(1);
	}

	private interface PrimitiveIdEntityRepository extends CrudRepository<PrimitiveIdEntity, Long> {}

	public interface ReadOnlyIdEntityRepository extends CrudRepository<ReadOnlyIdEntity, Long> {}

	private interface ImmutableWithManualIdEntityRepository extends CrudRepository<ImmutableWithManualIdEntity, Long> {}

	@Value
	@FieldDefaults(makeFinal = false)
	static class ReadOnlyIdEntity {

		@Id Long id;
		String name;
	}

	@Data
	static class PrimitiveIdEntity {

		@Id private long id;
		String name;
	}

	@Value
	@With
	static class ImmutableWithManualIdEntity {
		@Id Long id;
		String name;
	}

	@Configuration
	@ComponentScan("org.springframework.data.jdbc.testing")
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = CrudRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	static class TestConfiguration {

		AtomicLong lastId = new AtomicLong(0);

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryIdGenerationIntegrationTests.class;
		}

		/**
		 * {@link NamingStrategy} that harmlessly uppercases the table name, demonstrating how to inject one while not
		 * breaking existing SQL operations.
		 */
		@Bean
		NamingStrategy namingStrategy() {

			return new NamingStrategy() {

				@Override
				public String getTableName(Class<?> type) {
					return type.getSimpleName().toUpperCase();
				}
			};
		}

		@Bean
		BeforeConvertCallback<ImmutableWithManualIdEntity> idGenerator() {
			return e -> e.withId(lastId.incrementAndGet());
		}
	}
}
