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

package com.alibaba.blink.odps.util;

import com.alibaba.blink.odps.conf.ODPSConf;
import com.alibaba.blink.odps.schema.ODPSTableSchema;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * utility for ODPS
 */
public class ODPSUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(ODPSUtil.class);

	/**
	 * create odps instance using odps configuration
	 *
	 * @param odpsConf odps configuration
	 * @return odps instance
	 */
	public static Odps initOdps(ODPSConf odpsConf) {
		return initOdps(
				odpsConf.getAccessId(),
				odpsConf.getAccessKey(),
				odpsConf.getEndpoint(),
				odpsConf.getProject());
	}

	/**
	 * create odps instance
	 *
	 * @param accessId       access id
	 * @param accessKey      access key
	 * @param endpoint       endopint
	 * @param defaultProject default project
	 * @return new created odps instance
	 */
	public static Odps initOdps(String accessId, String accessKey, String endpoint,
			String defaultProject) {
		Account account = new AliyunAccount(accessId, accessKey);
		Odps odps = new Odps(account);
		odps.setEndpoint(endpoint);
		if (defaultProject != null) {
			odps.setDefaultProject(defaultProject);
		}
		return odps;
	}

	/**
	 * fetch table schema of a specific odps table
	 *
	 * @param odpsConf
	 * @param table
	 * @return
	 */
	public static ODPSTableSchema getODPSTableSchema(ODPSConf odpsConf, String table) {
		Odps odps = initOdps(odpsConf);
		Table odpsTable = odps.tables().get(odpsConf.getProject(), table);
		TableSchema schema = odpsTable.getSchema();
		boolean isView = odpsTable.isVirtualView();
		return new ODPSTableSchema(schema.getColumns(), schema.getPartitionColumns(), isView);
	}

	/**
	 * fetch size of an odps table
	 *
	 * @param odpsConf odps configuration
	 * @param table    odps table name
	 * @return total size of an odps table
	 */
	public static long getTableSize(ODPSConf odpsConf, String table) {
		Odps odps = initOdps(odpsConf);
		Table t = odps.tables().get(odpsConf.getProject(), table);
		return t.getSize();
	}

	/**
	 * fetch size of an odps table partition
	 *
	 * @param odpsConf  odps configuration
	 * @param table     odps table name
	 * @param partition a specify odps table partition, e.g pt='1',ds='2'
	 * @return total size of an odps table partition
	 */
	public static long getPartitionSize(ODPSConf odpsConf, String table, String partition) {
		Odps odps = initOdps(odpsConf);
		Table t = odps.tables().get(odpsConf.getProject(), table);
		return t.getPartition(new PartitionSpec(partition)).getSize();
	}

	/**
	 * fetch last modify time of an odps table
	 *
	 * @param odpsConf odps configuration
	 * @param table    odps table name
	 * @return last modify time of an odps table
	 */
	public static long getTableLastModifyTime(ODPSConf odpsConf, String table) {
		Odps odps = initOdps(odpsConf);
		Table t = odps.tables().get(odpsConf.getProject(), table);
		return t.getLastDataModifiedTime().getTime();
	}

	/**
	 * fetch last modify time of an odps table partition
	 *
	 * @param odpsConf  odps configuration
	 * @param table     odps table name
	 * @param partition a specify odps table partition, e.g pt='1',ds='2'
	 * @return last modify time of an odps table partition
	 */
	public static long getPartitionLastModifyTime(ODPSConf odpsConf, String table,
			String partition)
	{
		Odps odps = initOdps(odpsConf);
		Table t = odps.tables().get(odpsConf.getProject(), table);
		return t.getPartition(new PartitionSpec(partition)).getLastDataModifiedTime().getTime();
	}

	/**
	 * fetch all partitions of an odps table
	 *
	 * @param odpsConf odps configuration
	 * @param table    odps table name
	 * @return all partitions of an odps table
	 */
	public static String[] getAllPartitions(ODPSConf odpsConf, String table) {
		Odps odps = initOdps(odpsConf);
		Table t = odps.tables().get(odpsConf.getProject(), table);
		List<Partition> partitions = t.getPartitions();
		return FluentIterable.from(partitions).transform(new Function<Partition, String>() {

			@Override
			public String apply(Partition partition) {
				return partition.getPartitionSpec().toString();
			}

		}).toArray(String.class);
	}

	/**
	 * deprecate default constructor
	 */
	private ODPSUtil() {

	}
}

