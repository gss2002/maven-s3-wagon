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

import java.util.Comparator;

import com.amazonaws.services.s3.model.Bucket;

public class BucketComparator implements Comparator<Bucket> {

	@Override
	public int compare(Bucket bucket1, Bucket bucket2) {
		return bucket1.getName().compareTo(bucket2.getName());
	}

}