/**
 * Copyright 2004-2012 The Kuali Foundation
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/ecl2.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kuali.common.aws.s3.pojo;

import software.amazon.awssdk.services.s3.S3Client;

public class S3PrefixContext {

	S3Client client;
	String bucketName;
	BucketPrefixSummary summary;

	public S3Client getClient() {
		return client;
	}

	public void setClient(S3Client client) {
		this.client = client;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public BucketPrefixSummary getSummary() {
		return summary;
	}

	public void setSummary(BucketPrefixSummary summary) {
		this.summary = summary;
	}

}