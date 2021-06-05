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
package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Join;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * Renderer for {@link Join} segments. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class JoinVisitor extends TypedSubtreeVisitor<Join> {

	private final RenderContext context;
	private final RenderTarget parent;
	private final StringBuilder joinClause = new StringBuilder();
	private final ConditionVisitor conditionVisitor;
	private boolean inCondition = false;
	private boolean hasSeenCondition = false;

	JoinVisitor(RenderContext context, RenderTarget parent) {

		this.context = context;
		this.parent = parent;
		this.conditionVisitor = new ConditionVisitor(context);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#enterMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterMatched(Join segment) {

		joinClause.append(segment.getType().getSql()).append(' ');

		return super.enterMatched(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#enterNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Table && !inCondition) {
			joinClause.append(NameRenderer.render(context, (Table) segment));
			if (segment instanceof Aliased) {
				joinClause.append(" ").append(NameRenderer.render(context, (Aliased) segment));
			}
		} else if (segment instanceof Condition) {

			inCondition = true;
			if (!hasSeenCondition) {
				hasSeenCondition = true;
				return Delegation.delegateTo(conditionVisitor);
			}
		}

		return super.enterNested(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#leaveNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveNested(Visitable segment) {

		if (segment instanceof Condition) {

			inCondition = false;

			if (hasSeenCondition) {

				joinClause.append(" ON ");
				joinClause.append(conditionVisitor.getRenderedPart());

				hasSeenCondition = false;
			}
		}
		return super.leaveNested(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#leaveMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveMatched(Join segment) {
		parent.onRendered(joinClause);
		return super.leaveMatched(segment);
	}
}
