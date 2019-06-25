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

package org.apache.flink.table.functions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.SCALAR;

/**
 * Dictionary of function definitions for all internal used functions.
 */
public class InternalFunctionDefinitions {

	public static final BuiltInFunctionDefinition THROW_EXCEPTION =
			new BuiltInFunctionDefinition.Builder()
					.name("throwException")
					.kind(SCALAR)
					.build();

	public static final BuiltInFunctionDefinition DENSE_RANK =
			new BuiltInFunctionDefinition.Builder()
					.name("DENSE_RANK")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition FIRST_VALUE =
			new BuiltInFunctionDefinition.Builder()
					.name("FIRST_VALUE")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition LAST_VALUE =
			new BuiltInFunctionDefinition.Builder()
					.name("LAST_VALUE")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition STDDEV =
			new BuiltInFunctionDefinition.Builder()
					.name("STDDEV")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition LEAD =
			new BuiltInFunctionDefinition.Builder()
					.name("LEAD")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition LAG =
			new BuiltInFunctionDefinition.Builder()
					.name("LAG")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition RANK =
			new BuiltInFunctionDefinition.Builder()
					.name("RANK")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition ROW_NUMBER =
			new BuiltInFunctionDefinition.Builder()
					.name("ROW_NUMBER")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition SINGLE_VALUE =
			new BuiltInFunctionDefinition.Builder()
					.name("SINGLE_VALUE")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition CONCAT_AGG =
			new BuiltInFunctionDefinition.Builder()
					.name("CONCAT_AGG")
					.kind(AGGREGATE)
					.build();

	public static final BuiltInFunctionDefinition VARIANCE =
			new BuiltInFunctionDefinition.Builder()
					.name("VARIANCE")
					.kind(AGGREGATE)
					.build();

	public static List<BuiltInFunctionDefinition> getDefinitions() {
		final Field[] fields = InternalFunctionDefinitions.class.getFields();
		final List<BuiltInFunctionDefinition> list = new ArrayList<>(fields.length);
		for (Field field : fields) {
			if (FunctionDefinition.class.isAssignableFrom(field.getType())) {
				try {
					final BuiltInFunctionDefinition funcDef = (BuiltInFunctionDefinition) field.get(BuiltInFunctionDefinitions.class);
					list.add(Preconditions.checkNotNull(funcDef));
				} catch (IllegalAccessException e) {
					throw new TableException(
							"The function definition for field " + field.getName() + " is not accessible.", e);
				}
			}
		}
		return list;
	}
}
