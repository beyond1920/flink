/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.expressions.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.operations.ExpressionTypeInfer;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * It checks if a {@link UnresolvedCallExpression} can work with given arguments.
 * If the call expects different types of arguments, but the given arguments
 * have types that can be casted, a {@link BuiltInFunctionDefinitions#CAST}
 * expression is inserted.
 */
@Internal
final class ResolveCallByArgumentsRule implements ResolverRule {

	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		return expression.stream()
				.map(expr -> expr.accept(new CallArgumentsCastingVisitor(context)))
				.collect(Collectors.toList());
	}

	private class CallArgumentsCastingVisitor extends RuleExpressionVisitor<Expression> {

		CallArgumentsCastingVisitor(ResolutionContext context) {
			super(context);
		}

		@Override
		public ResolvedExpression visit(UnresolvedCallExpression unresolvedCall) {

			final List<ResolvedExpression> resolvedArgs = unresolvedCall.getChildren().stream()
					.map(c -> c.accept(this))
					.map(e -> {
						if (e instanceof ResolvedExpression) {
							return (ResolvedExpression) e;
						}
						throw new TableException("Unexpected unresolved expression: " + e);
					})
					.collect(Collectors.toList());

			DataType type = ExpressionTypeInfer.infer(unresolvedCall);
			return unresolvedCall.resolve(resolvedArgs, type);
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}
}
