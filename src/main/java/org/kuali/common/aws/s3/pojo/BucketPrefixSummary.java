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

import com.amazonaws.services.s3.model.Bucket;

public class BucketPrefixSummary implements Comparable<BucketPrefixSummary> {

	Bucket bucket;
	String prefix;
	long count;
	long size;

	public BucketPrefixSummary() {
		this(null);
	}

	public BucketPrefixSummary(String prefix) {
		super();
		this.prefix = prefix;
	}

	public int compareTo(BucketPrefixSummary other) {
		String prefix1 = getPrefix();
		String prefix2 = other.getPrefix();
		if (prefix1 == null) {
			return -1;
		} else {
			return prefix1.compareTo(prefix2);
		}
	}

	public long getCount() {
		return count;
	}

	public void setCount(long objectCount) {
		this.count = objectCount;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long objectSize) {
		this.size = objectSize;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public Bucket getBucket() {
		return bucket;
	}

	public void setBucket(Bucket bucket) {
		this.bucket = bucket;
	}

}