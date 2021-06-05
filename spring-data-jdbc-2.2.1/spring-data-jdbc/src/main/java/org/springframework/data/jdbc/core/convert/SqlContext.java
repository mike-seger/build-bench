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
package org.springframework.data.jdbc.core.convert;

import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;

/**
 * Utility to get from path to SQL DSL elements.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Tyler Van Gorder
 * @since 1.1
 */
class SqlContext {

	private final RelationalPersistentEntity<?> entity;
	private final Table table;

	SqlContext(RelationalPersistentEntity<?> entity) {

		this.entity = entity;
		this.table = Table.create(entity.getTableName());
	}

	Column getIdColumn() {
		return table.column(entity.getIdColumn());
	}

	Column getVersionColumn() {
		return table.column(entity.getRequiredVersionProperty().getColumnName());
	}

	Table getTable() {
		return table;
	}

	Table getTable(PersistentPropertyPathExtension path) {

		SqlIdentifier tableAlias = path.getTableAlias();
		Table table = Table.create(path.getTableName());
		return tableAlias == null ? table : table.as(tableAlias);
	}

	Column getColumn(PersistentPropertyPathExtension path) {
		return getTable(path).column(path.getColumnName()).as(path.getColumnAlias());
	}

	Column getReverseColumn(PersistentPropertyPathExtension path) {
		return getTable(path).column(path.getReverseColumnName()).as(path.getReverseColumnNameAlias());
	}
}
