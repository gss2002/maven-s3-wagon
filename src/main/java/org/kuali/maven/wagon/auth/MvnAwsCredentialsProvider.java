package org.kuali.maven.wagon.auth;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.internal.LazyAwsCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import org.apache.maven.wagon.authentication.AuthenticationInfo;

import com.google.common.base.Optional;

import software.amazon.awssdk.utils.ToString;

public class MvnAwsCredentialsProvider implements AwsCredentialsProvider, SdkAutoCloseable {

	private static final MvnAwsCredentialsProvider MVN_CREDENTIALS_PROVIDER = new MvnAwsCredentialsProvider(builder());

	private final LazyAwsCredentialsProvider providerChain;

	/**
	 * @see #builder()
	 */
	private MvnAwsCredentialsProvider(Builder builder) {
		this.providerChain = createChain(builder);
	}

	/**
	 * Create an create of the {@link DefaultCredentialsProvider} using the default
	 * configuration. Configuration can be specified by creating an create using the
	 * {@link #builder()}.
	 */
	public static MvnAwsCredentialsProvider create() {
		return MVN_CREDENTIALS_PROVIDER;
	}

	/**
	 * Create the default credential chain using the configuration in the provided
	 * builder.
	 */
	private static LazyAwsCredentialsProvider createChain(Builder builder) {
		boolean asyncCredentialUpdateEnabled = builder.asyncCredentialUpdateEnabled;
		boolean reuseLastProviderEnabled = builder.reuseLastProviderEnabled;
		Optional<AuthenticationInfo> auth = builder.auth;

		return LazyAwsCredentialsProvider.create(() -> {
			AwsCredentialsProvider[] credentialsProviders = new AwsCredentialsProvider[] {
					SystemPropertyCredentialsProvider.create(), EnvironmentVariableCredentialsProvider.create(),
					WebIdentityTokenFileCredentialsProvider.create(),
					new AuthenticationInfoWagonCredentialsProvider(auth), ProfileCredentialsProvider.create(),
					ContainerCredentialsProvider.builder().asyncCredentialUpdateEnabled(asyncCredentialUpdateEnabled)
							.build(),
					InstanceProfileCredentialsProvider.builder()
							.asyncCredentialUpdateEnabled(asyncCredentialUpdateEnabled).build() };

			return AwsCredentialsProviderChain.builder().reuseLastProviderEnabled(reuseLastProviderEnabled)
					.credentialsProviders(credentialsProviders).build();
		});
	}

	/**
	 * Get a builder for defining a {@link DefaultCredentialsProvider} with custom
	 * configuration.
	 */
	public static Builder builder() {
		return new Builder();
	}

	@Override
	public AwsCredentials resolveCredentials() {
		return providerChain.resolveCredentials();
	}

	@Override
	public void close() {
		providerChain.close();
	}

	@Override
	public String toString() {
		return ToString.builder("MvnAwsCredentialsProvider").add("providerChain", providerChain).build();
	}

	/**
	 * Configuration that defines the {@link DefaultCredentialsProvider}'s behavior.
	 */
	public static final class Builder {
		private Boolean reuseLastProviderEnabled = true;
		private Boolean asyncCredentialUpdateEnabled = false;
		Optional<AuthenticationInfo> auth;

		/**
		 * Created with {@link #builder()}.
		 */
		private Builder() {
		}

		/**
		 * Controls whether the provider should reuse the last successful credentials
		 * provider in the chain. Reusing the last successful credentials provider will
		 * typically return credentials faster than searching through the chain.
		 *
		 * <p>
		 * By default, this is enabled.
		 * </p>
		 */
		public Builder reuseLastProviderEnabled(Boolean reuseLastProviderEnabled) {
			this.reuseLastProviderEnabled = reuseLastProviderEnabled;
			return this;
		}

		/**
		 * Configure whether this provider should fetch credentials asynchronously in
		 * the background. If this is true, threads are less likely to block when
		 * {@link #resolveCredentials()} is called, but additional resources are used to
		 * maintain the provider.
		 *
		 * <p>
		 * By default, this is disabled.
		 * </p>
		 */
		public Builder asyncCredentialUpdateEnabled(Boolean asyncCredentialUpdateEnabled) {
			this.asyncCredentialUpdateEnabled = asyncCredentialUpdateEnabled;
			return this;
		}

		public Builder auth(Optional<AuthenticationInfo> auth) {
			this.auth = auth;
			return this;
		}

		/**
		 * Create a {@link DefaultCredentialsProvider} using the configuration defined
		 * in this builder.
		 */
		public MvnAwsCredentialsProvider build() {
			return new MvnAwsCredentialsProvider(this);
		}
	}
}
