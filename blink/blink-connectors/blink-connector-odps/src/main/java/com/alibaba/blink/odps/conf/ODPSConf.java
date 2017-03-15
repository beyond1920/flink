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

package com.alibaba.blink.odps.conf;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * ODPS client configuration
 */
public class ODPSConf implements Serializable {

	private static final long serialVersionUID = -2481720994496434494L;

	private final String accessId;
	private final String accessKey;
	private final String endpoint;
	private final String project;

	public ODPSConf(String accessId, String accessKey, String endpoint, String project) {
		checkArgument(StringUtils.isNotBlank(accessId), "accessId is whitespace or null!");
		checkArgument(StringUtils.isNotBlank(accessKey), "accessKey is whitespace or null!");
		checkArgument(StringUtils.isNotBlank(endpoint), "endpoint is whitespace or null!");
		checkArgument(StringUtils.isNotBlank(project), "project is whitespace or null! ");
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.endpoint = endpoint;
		this.project = project;
	}

	public String getAccessId() {
		return accessId;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getProject() {
		return project;
	}

	@Override
	public String toString() {
		return "ODPSConf{" +
				"accessId='" + accessId + '\'' +
				", accessKey='" + accessKey + '\'' +
				", endpoint='" + endpoint + '\'' +
				", project='" + project + '\'' +
				'}';
	}
}
