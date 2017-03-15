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

package com.alibaba.blink.odps.type;

import com.aliyun.odps.data.Record;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Date;

/**
 * supported odps column type
 */
public enum ODPSType {

	BIGINT {
		public TypeInformation<?> toFlinkType() {
			return BasicTypeInfo.LONG_TYPE_INFO;
		}

		public void setRowField(Row row, int fieldIdx, String valueStr) {
			long value = valueStr == null ? null : Long.parseLong(valueStr);
			row.setField(fieldIdx, value);
		}

		public void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName) {
			row.setField(fieldIdx, record.getBigint(odpsColumnName));
		}
	},

	DOUBLE {
		public TypeInformation<?> toFlinkType() {
			return BasicTypeInfo.DOUBLE_TYPE_INFO;
		}

		public void setRowField(Row row, int fieldIdx, String valueStr) {
			double value = valueStr == null ? null : Double.parseDouble(valueStr);
			row.setField(fieldIdx, value);
		}

		public void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName) {
			row.setField(fieldIdx, record.getDouble(odpsColumnName));
		}
	},

	BOOLEAN {
		public TypeInformation<?> toFlinkType() {
			return BasicTypeInfo.BOOLEAN_TYPE_INFO;
		}

		public void setRowField(Row row, int fieldIdx, String valueStr) {
			boolean value = valueStr == null ? null : Boolean.parseBoolean(valueStr);
			row.setField(fieldIdx, value);
		}

		public void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName) {
			row.setField(fieldIdx, record.getBoolean(odpsColumnName));
		}
	},

	DATETIME {
		public TypeInformation<?> toFlinkType() {
			return SqlTimeTypeInfo.DATE;
		}

		public void setRowField(Row row, int fieldIdx, String valueStr) {
			// TODO
		}

		public void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName) {
			Date value = new Date(record.getDatetime(odpsColumnName).getTime());
			row.setField(fieldIdx, value);
		}
	},

	STRING {
		public TypeInformation<?> toFlinkType() {
			return BasicTypeInfo.STRING_TYPE_INFO;
		}

		public void setRowField(Row row, int fieldIdx, String valueStr) {
			row.setField(fieldIdx, valueStr);
		}

		public void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName) {
			row.setField(fieldIdx, record.getString(odpsColumnName));
		}
	},

	DECIMAL {
		public TypeInformation<?> toFlinkType() {
			return BasicTypeInfo.BIG_DEC_TYPE_INFO;
		}

		public void setRowField(Row row, int fieldIdx, String valueStr) {
			BigDecimal value = valueStr == null ? null : new BigDecimal(valueStr);
			row.setField(fieldIdx, value);
		}

		public void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName) {
			row.setField(fieldIdx, record.getDecimal(odpsColumnName));
		}
	},

	MAP {
		public TypeInformation<?> toFlinkType() {
			throw new UnsupportedOperationException("unknown column type " + toString());
		}

		public void setRowField(Row row, int fieldIdx, String valueStr) {
			throw new UnsupportedOperationException("unknown column type " + toString());
		}

		public void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName) {
			throw new UnsupportedOperationException("unknown column type " + toString());
		}
	},

	ARRAY {
		public TypeInformation<?> toFlinkType() {
			throw new UnsupportedOperationException("unknown column type " + toString());
		}

		public void setRowField(Row row, int fieldIdx, String valueStr) {
			throw new UnsupportedOperationException("unknown column type " + toString());
		}

		public void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName) {
			throw new UnsupportedOperationException("unknown column type " + toString());
		}
	};

	/**
	 * mapping from odps type to flink type information
	 *
	 * @return related flink type information
	 */
	public abstract TypeInformation<?> toFlinkType();

	/**
	 * translate the value str to the correct type value, set the translated value to specified
	 * location of row
	 *
	 * @param row      row to hold translated value
	 * @param fieldIdx index of field in row to hold translated value
	 * @param valueStr the string value to translate
	 */
	public abstract void setRowField(Row row, int fieldIdx, String valueStr);

	/**
	 * get the odps column value from Record, set the value to specified location of row
	 *
	 * @param row
	 * @param fieldIdx
	 * @param record
	 * @param odpsColumnName
	 */
	public abstract void setRowField(Row row, int fieldIdx, Record record, String odpsColumnName);
}
