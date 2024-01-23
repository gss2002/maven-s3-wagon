/**
 * Copyright 2010-2015 The Kuali Foundation
 * Copyright 2018 Sean Hennessey
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
package org.kuali.maven.wagon.auth;


import org.apache.maven.wagon.authentication.AuthenticationInfo;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.common.base.Optional;

/**
 * This chain searches for AWS credentials in system properties -&gt; environment variables -&gt; ~/.m2/settings.xml
 * -&gt; AWS Configuration Profile -&gt; Amazon's EC2 Container Service/EC2 Instance Metadata Service
 */
public final class MavenAwsCredentialsProviderChain extends AWSCredentialsProviderChain {


	
    private static Optional<AuthenticationInfo> authInfo;
	private static final MavenAwsCredentialsProviderChain INSTANCE
    = new MavenAwsCredentialsProviderChain(authInfo);


    public MavenAwsCredentialsProviderChain(Optional<AuthenticationInfo> auth) {
        super(new SystemPropertiesCredentialsProvider(),
              new EnvironmentVariableCredentialsProvider(),
              WebIdentityTokenCredentialsProvider.create(),
              new AuthenticationInfoCredentialsProvider(auth),
              new ProfileCredentialsProvider(),
              new EC2ContainerCredentialsProviderWrapper());
    }
	
    public static MavenAwsCredentialsProviderChain getInstance(Optional<AuthenticationInfo> auth) {
    	authInfo = auth;
        return INSTANCE;
    }

}
