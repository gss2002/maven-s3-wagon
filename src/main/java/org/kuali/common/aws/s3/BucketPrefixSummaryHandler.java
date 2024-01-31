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
package org.kuali.common.aws.s3;

import org.kuali.common.aws.s3.pojo.BucketPrefixSummary;
import org.kuali.common.aws.s3.pojo.S3PrefixContext;
import org.kuali.common.threads.ElementHandler;
import org.kuali.common.threads.ListIteratorContext;

import software.amazon.awssdk.services.s3.S3Client;

public class BucketPrefixSummaryHandler implements ElementHandler<S3PrefixContext> {

	public void handleElement(ListIteratorContext<S3PrefixContext> context, int index, S3PrefixContext element) {
		S3Client client = element.getClient();
		String bucketName = element.getBucketName();
		BucketPrefixSummary summary = element.getSummary();
		S3Utils.getInstance().summarize(client, bucketName, summary);
	}

}
