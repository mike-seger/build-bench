/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import java.util.List;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * An SQL dialect for Oracle.
 *
 * @author Jens Schauder
 * @since 2.1
 */
public class OracleDialect extends AnsiDialect {

	/**
	 * Singleton instance.
	 */
	public static final OracleDialect INSTANCE = new OracleDialect();

	private static final IdGeneration ID_GENERATION = new IdGeneration() {
		@Override
		public boolean driverRequiresKeyColumnNames() {
			return true;
		}
	};

	protected OracleDialect() {}

	@Override
	public IdGeneration getIdGeneration() {
		return ID_GENERATION;
	}
}
