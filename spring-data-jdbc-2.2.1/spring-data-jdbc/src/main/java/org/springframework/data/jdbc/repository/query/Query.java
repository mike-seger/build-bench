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
package org.springframework.data.jdbc.repository.query;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.QueryAnnotation;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 * Annotation to provide SQL statements that will get used for executing the method. The SQL statement may contain named
 * parameters as supported by {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate}. Those
 * parameters will get bound to the arguments of the annotated method.
 *
 * @author Jens Schauder
 * @author Moises Cisneros
 * @author Hebert Coelho
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@QueryAnnotation
@Documented
public @interface Query {

	/**
	 * The SQL statement to execute when the annotated method gets invoked.
	 */
	String value() default "";

	/**
	 * The named query to be used. If not defined, the name of
	 * {@code ${domainClass}.${queryMethodName}} will be used.
	 */
	String name() default "";

	/**
	 * Optional {@link RowMapper} to use to convert the result of the query to domain class instances. Cannot be used
	 * along with {@link #resultSetExtractorClass()} only one of the two can be set.
	 */
	Class<? extends RowMapper> rowMapperClass() default RowMapper.class;

	/**
	 * Optional name of a bean of type {@link RowMapper} to use to convert the result of the query to domain class instances. Cannot be used
	 * along with {@link #resultSetExtractorClass()} only one of the two can be set.
	 *
	 * @since 2.1
	 */
	String rowMapperRef() default "";

	/**
	 * Optional {@link ResultSetExtractor} to use to convert the result of the query to domain class instances. Cannot be
	 * used along with {@link #rowMapperClass()} only one of the two can be set.
	 */
	Class<? extends ResultSetExtractor> resultSetExtractorClass() default ResultSetExtractor.class;

	/**
	 * Optional name of a bean of type {@link ResultSetExtractor} to use to convert the result of the query to domain class instances. Cannot be
	 * used along with {@link #rowMapperClass()} only one of the two can be set.
	 *
	 * @since 2.1
	 */
	String resultSetExtractorRef() default "";
}
