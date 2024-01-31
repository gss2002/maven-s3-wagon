/**
 * Copyright 2010-2015 The Kuali Foundation
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
package org.kuali.maven.wagon;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.proxy.ProxyInfo;
import org.apache.maven.wagon.repository.Repository;
import org.apache.maven.wagon.repository.RepositoryPermissions;
import org.kuali.common.aws.s3.S3Utils;
import org.kuali.common.aws.s3.SimpleFormatter;
import org.kuali.common.threads.ExecutionStatistics;
import org.kuali.common.threads.ThreadHandlerContext;
import org.kuali.common.threads.ThreadInvoker;
import org.kuali.common.threads.listener.PercentCompleteListener;
import org.kuali.maven.wagon.auth.MvnAwsCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.base.Optional;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.util.AwsHostNameUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.io.ResettableInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.utils.Validate;

/**
 * <p>
 * An implementation of the Maven Wagon interface that is integrated with the
 * Amazon S3 service.
 * </p>
 * 
 * <p>
 * URLs that reference the S3 service should be in the form of
 * <code>s3://bucket.name</code>. As an example
 * <code>s3://maven.kuali.org</code> puts files into the
 * <code>maven.kuali.org</code> bucket on the S3 service.
 * </p>
 * 
 * <p>
 * This implementation uses the <code>username</code> and <code>password</code>
 * portions of the server authentication metadata for credentials.
 * </p>
 * 
 * @author Ben Hale
 * @author Jeff Caddel
 */
public class S3Wagon extends AbstractWagon implements RequestFactory {

	/**
	 * Set the system property <code>maven.wagon.protocol</code> to
	 * <code>http</code> to force the wagon to communicate over <code>http</code>.
	 * Default is <code>https</code>.
	 */
	public static final String PROTOCOL_KEY = "maven.wagon.protocol";
	public static final String HTTP = "http";
	public static final String HTTP_ENDPOINT_VALUE = "http://s3.amazonaws.com";
	public static final String CENTRAL_ENDPOINT = "s3.amazonaws.com";
	public static final String VPC_ENDPOINT_KEY = "maven.wagon.s3.vpce";
	public static final String REGION = "maven.wagon.s3.region";
	public static final String AWS_S3_DEFAULT_REGION = "us-east-2";
	
	public static final String HTTPS = "https";
	public static final String MIN_THREADS_KEY = "maven.wagon.threads.min";
	public static final String MAX_THREADS_KEY = "maven.wagon.threads.max";
	public static final String DIVISOR_KEY = "maven.wagon.threads.divisor";
	public static final int DEFAULT_MIN_THREAD_COUNT = 10;
	public static final int DEFAULT_MAX_THREAD_COUNT = 50;
	public static final int DEFAULT_DIVISOR = 50;
	public static final int DEFAULT_READ_TIMEOUT = 60 * 1000;
	public static final String READ_TIMEOUT_KEY = "maven.wagon.rto";

	private static final File TEMP_DIR = getCanonicalFile(System.getProperty("java.io.tmpdir"));
	private static final String TEMP_DIR_PATH = TEMP_DIR.getAbsolutePath();
	private static final String SDK_REGION_CHAIN_IN_USE = "S3 client is using the SDK region chaining resolution.";
	private static final String S3_SERVICE_NAME = "s3";
	
    public static final int NO_SUCH_BUCKET_STATUS_CODE = 404;

    public static final int BUCKET_ACCESS_FORBIDDEN_STATUS_CODE = 403;

    public static final int BUCKET_REDIRECT_STATUS_CODE = 301;

	ThreadInvoker invoker = new ThreadInvoker();
	SimpleFormatter formatter = new SimpleFormatter();
	int minThreads = getMinThreads();
	int maxThreads = getMaxThreads();
	int divisor = getDivisor();
	String protocol = getValue(PROTOCOL_KEY, HTTPS);
	String vpce = getVpce();
	boolean http = HTTP.equals(protocol);
	int readTimeout = getValue(READ_TIMEOUT_KEY, DEFAULT_READ_TIMEOUT);
	ObjectCannedACL acl = null;
	S3TransferManager transferManager;

	private static final Logger log = LoggerFactory.getLogger(S3Wagon.class);

	S3Client client;
	S3AsyncClient aClient;

	String bucketName;
	String basedir;

	private final Mimetype mimeTypes = Mimetype.getInstance();

	public S3Wagon() {
		super(true);
		S3Listener listener = new S3Listener();
		super.addSessionListener(listener);
		super.addTransferListener(listener);
	}

	protected void validateBucket(S3Client client, String bucketName) {
		log.debug("Looking for bucket: " + bucketName);
		if(doesBucketExistV2(client, bucketName)) {
			log.debug("Found bucket '" + bucketName + "' Validating permissions");
			validatePermissions(client, bucketName);
		} else {
			log.info("Creating bucket " + bucketName);
			// If we create the bucket, we "own" it and by default have the "fullcontrol"
			// permission
			client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
		}
	}
	
    public boolean doesBucketExistV2(S3Client client, String bucketName) throws SdkClientException {
        try {
            Validate.notEmpty(bucketName, "bucketName");
            client.getBucketAcl(GetBucketAclRequest.builder().bucket(bucketName).build());
            return true;
        } catch (AwsServiceException ase) {
            // A redirect error or an AccessDenied exception means the bucket exists but it's not in this region
            // or we don't have permissions to it.
            if ((ase.statusCode() == BUCKET_REDIRECT_STATUS_CODE) || "AccessDenied".equals(ase.awsErrorDetails().errorCode())) {
                return true;
            }
            if (ase.statusCode() == NO_SUCH_BUCKET_STATUS_CODE) {
                return false;
            }
            throw ase;
        }
    }

	/**
	 * Establish that we have enough permissions on this bucket to do what we need
	 * to do
	 * 
	 * @param client    S3 Client
	 * @param bucketName AWS S3 Bucket Name.
	 */
	protected void validatePermissions(S3Client client, String bucketName) {
		// This establishes our ability to list objects in this bucket
		ListObjectsRequest zeroObjectsRequest = ListObjectsRequest.builder().bucket(bucketName).delimiter(null).prefix(null).maxKeys(0).marker(null).build();
		client.listObjects(zeroObjectsRequest);

		/**
		 * The current AWS Java SDK does not appear to have a simple method for
		 * discovering what set of permissions the currently authenticated user has on a
		 * bucket. The AWS dev's suggest that you attempt to perform an operation that
		 * would fail if you don't have the permission in question. You would then use
		 * the success/failure of that attempt to establish what your permissions are.
		 * This is definitely not ideal and they are working on it, but it is not ready
		 * yet.
		 */

		// Do something simple and quick to verify that we have write permissions on
		// this bucket
		// One way to do this would be to create an object in this bucket, and then
		// immediately delete it
		// That seems messy, inconvenient, and lame.

	}

	protected ObjectCannedACL getAclFromRepository(Repository repository) {
		RepositoryPermissions permissions = repository.getPermissions();
		if (permissions == null) {
			return null;
		}
		String filePermissions = permissions.getFileMode();
		if (StringUtils.isBlank(filePermissions)) {
			return null;
		}
		return ObjectCannedACL.valueOf(filePermissions.trim());
	}


	protected S3Client getAmazonS3Client(AwsCredentialsProvider credentials) {
		S3ClientBuilder builder = S3Client.builder();
		builder.credentialsProvider(credentials);
		ClientOverrideConfiguration clientOverride = ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofMillis(readTimeout)).apiCallAttemptTimeout(Duration.ofMillis(readTimeout)).build();
		builder.overrideConfiguration(clientOverride);
		builder.forcePathStyle(false);
	    if (!(getVpce().isEmpty())) {
	    	configureEndpoint(builder);
	    }
		S3Client client = builder.build();
		return client;
	}
	
	protected S3AsyncClient getAmazonS3AsyncClient(AwsCredentialsProvider credentials) {
		S3AsyncClientBuilder builder = S3AsyncClient.builder();
		builder.credentialsProvider(credentials);
		builder.forcePathStyle(false);
		ClientOverrideConfiguration clientOverride = ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofMillis(readTimeout)).apiCallAttemptTimeout(Duration.ofMillis(readTimeout)).build();
		builder.overrideConfiguration(clientOverride);
	    if (!(getVpce().isEmpty())) {
	    	configureAsyncEndpoint(builder);
	    }
		S3AsyncClient aClient = builder.build();
		return aClient;
	}

	public void configureEndpoint(S3ClientBuilder builder) {
		URI endpoint = getS3Endpoint(getVpce());
	    String configuredRegion = getRegion();
	    Region region = null;
	    
	    

	    // If the region was configured, set it.
	    if (configuredRegion != null && !configuredRegion.isEmpty()) {
	      region = Region.of(configuredRegion);
	    }
	    
	    if (endpoint != null) {
	        builder.endpointOverride(endpoint);
	        // No region was configured, try to determine it from the endpoint.
	        if (region == null) {
	          region = getS3RegionFromEndpoint(getVpce());
	        }
	        log.debug("Setting endpoint to {}", endpoint);
	      }	    
	    
	    if (region != null) {
	        builder.region(region);
	      } else if (configuredRegion == null) {
	        // no region is configured, and none could be determined from the endpoint.
	        // Use US_EAST_2 as default.
	        region = Region.of(AWS_S3_DEFAULT_REGION);
	        builder.crossRegionAccessEnabled(true);
	        builder.region(region);
	      } else if (configuredRegion.isEmpty()) {
	        // region configuration was set to empty string.
	        // allow this if people really want it; it is OK to rely on this
	        // when deployed in EC2.
	        log.warn(SDK_REGION_CHAIN_IN_USE);
	        log.debug(SDK_REGION_CHAIN_IN_USE);
	      }

	      log.debug("Setting region to {}", region);
	 
	}
	
	public void configureAsyncEndpoint(S3AsyncClientBuilder builder) {
		URI endpoint = getS3Endpoint(getVpce());
	    String configuredRegion = getRegion();
	    Region region = null;

	    // If the region was configured, set it.
	    if (configuredRegion != null && !configuredRegion.isEmpty()) {
	      region = Region.of(configuredRegion);
	    }
	    
	    if (endpoint != null) {
	        builder.endpointOverride(endpoint);
	        // No region was configured, try to determine it from the endpoint.
	        if (region == null) {
	          region = getS3RegionFromEndpoint(getVpce());
	        }
	        log.debug("Setting endpoint to {}", endpoint);
	      }	    
	    
	    if (region != null) {
	        builder.region(region);
	      } else if (configuredRegion == null) {
	        // no region is configured, and none could be determined from the endpoint.
	        // Use US_EAST_2 as default.
	        region = Region.of(AWS_S3_DEFAULT_REGION);
	        builder.crossRegionAccessEnabled(true);
	        builder.region(region);
	      } else if (configuredRegion.isEmpty()) {
	        // region configuration was set to empty string.
	        // allow this if people really want it; it is OK to rely on this
	        // when deployed in EC2.
	        log.warn(SDK_REGION_CHAIN_IN_USE);
	        log.debug(SDK_REGION_CHAIN_IN_USE);
	      }

	      log.debug("Setting region to {}", region);
	 
	}

	
	  /**
	   * Given a endpoint string, create the endpoint URI.
	   *
	   * @param endpoint possibly null endpoint.
	   * @param conf config to build the URI from.
	   * @return an endpoint uri
	   */
	  private static URI getS3Endpoint(String endpoint) {

	    boolean secureConnections = true;
	    if (PROTOCOL_KEY.equalsIgnoreCase("http")) {
	    	secureConnections=false;
	    }

	    String protocol = secureConnections ? "https" : "http";

	    if (endpoint == null || endpoint.isEmpty()) {
	      // don't set an endpoint if none is configured, instead let the SDK figure it out.
	      return null;
	    }

	    if (!endpoint.contains("://")) {
	      endpoint = String.format("%s://%s", protocol, endpoint);
	    }

	    try {
	      return new URI(endpoint);
	    } catch (URISyntaxException e) {
	      throw new IllegalArgumentException(e);
	    }
	  }
	  
	  /**
	   * Parses the endpoint to get the region.
	   * If endpoint is the central one, use US_EAST_1.
	   *
	   * @param endpoint the configure endpoint.
	   * @return the S3 region, null if unable to resolve from endpoint.
	   */
	  private Region getS3RegionFromEndpoint(String endpoint) {

	    if(!endpoint.endsWith(CENTRAL_ENDPOINT)) {
	      log.debug("Endpoint {} is not the default; parsing", endpoint);
	      return AwsHostNameUtils.parseSigningRegion(endpoint, S3_SERVICE_NAME).orElse(null);
	    }

	    // endpoint is for US_EAST_1;
	    return Region.US_EAST_1;
	  }

	@Override
	protected void connectToRepository(Repository source, AuthenticationInfo auth, ProxyInfo proxy) {

		AwsCredentialsProvider credentials = getCredentials(auth);
		this.client = getAmazonS3Client(credentials);
		this.aClient = getAmazonS3AsyncClient(credentials);
		this.transferManager = S3TransferManager.builder().s3Client(this.aClient).build();
		this.bucketName = source.getHost();
		validateBucket(client, bucketName);
		this.basedir = getBaseDir(source);

		// If they've specified <filePermissions> in settings.xml, that always wins
		ObjectCannedACL repoAcl = getAclFromRepository(source);
		if (repoAcl != null) {
			log.info("File permissions: " + repoAcl.name());
			acl = repoAcl;
		}
	}

	@Override
	protected boolean doesRemoteResourceExist(final String resourceName) {
		try {
			client.getObject(GetObjectRequest.builder().bucket(bucketName).key(basedir + resourceName).build());
		} catch (AwsServiceException e) {
			return false;
		}
		return true;
	}

	@Override
	protected void disconnectFromRepository() {
		// Nothing to do for S3
	}

	/**
	 * Pull an object out of an S3 bucket and write it to a file
	 */
	@Override
	protected void getResource(final String resourceName, final File destination, final TransferProgress progress)
			throws ResourceDoesNotExistException, IOException {
		// Obtain the object from S3
		ResponseInputStream<GetObjectResponse> object = null;
		try {
			String key = basedir + resourceName;
			object = client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build());
		} catch (Exception e) {
			throw new ResourceDoesNotExistException("Resource " + resourceName + " does not exist in the repository",
					e);
		}

		// first write the file to a temporary location
		File temporaryDestination = File.createTempFile(destination.getName(), ".tmp", destination.getParentFile());
		InputStream in = null;
		OutputStream out = null;
		try {
			in = object;
			out = new TransferProgressFileOutputStream(temporaryDestination, progress);
			byte[] buffer = new byte[1024];
			int length;
			while ((length = in.read(buffer)) != -1) {
				out.write(buffer, 0, length);
			}
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
		}
		// then move, to have an atomic operation to guarantee we don't have a partially
		// downloaded file on disk
		Files.move(temporaryDestination.toPath(), destination.toPath(),
				java.nio.file.StandardCopyOption.REPLACE_EXISTING);
	}

	/**
	 * Is the S3 object newer than the timestamp passed in?
	 */
	@Override
	protected boolean isRemoteResourceNewer(final String resourceName, final long timestamp) {
		HeadObjectResponse metadata = client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(basedir + resourceName).build());
		return metadata.lastModified().compareTo(Instant.ofEpochMilli(timestamp)) < 0;
	}

	/**
	 * List all of the objects in a given directory
	 */
	@Override
	protected List<String> listDirectory(String directory) throws Exception {
		// info("directory=" + directory);
		if (StringUtils.isBlank(directory)) {
			directory = "";
		}
		String delimiter = "/";
		String prefix = basedir + directory;
		if (!prefix.endsWith(delimiter)) {
			prefix += delimiter;
		}
		// info("prefix=" + prefix);
		ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucketName).prefix(prefix).delimiter(delimiter).build();
		ListObjectsResponse objectListing = client.listObjects(request);
		// info("truncated=" + objectListing.isTruncated());
		// info("prefix=" + prefix);
		// info("basedir=" + basedir);
		List<String> fileNames = new ArrayList<String>();
		for (S3Object summary : objectListing.contents()) {
			// info("summary.getKey()=" + summary.getKey());
			String key = summary.key();
			String relativeKey = key.startsWith(basedir) ? key.substring(basedir.length()) : key;
			boolean add = !StringUtils.isBlank(relativeKey) && !relativeKey.equals(directory);
			if (add) {
				// info("Adding key - " + relativeKey);
				fileNames.add(relativeKey);
			}
		}
		for (CommonPrefix commonPrefix : objectListing.commonPrefixes()) {
			String value = commonPrefix.prefix().startsWith(basedir) ? commonPrefix.prefix().substring(basedir.length()) : commonPrefix.prefix();
			// info("commonPrefix=" + commonPrefix);
			// info("relativeValue=" + relativeValue);
			// info("Adding common prefix - " + value);
			fileNames.add(value);
		}
		// StringBuilder sb = new StringBuilder();
		// sb.append("\n");
		// for (String fileName : fileNames) {
		// sb.append(fileName + "\n");
		// }
		// info(sb.toString());
		return fileNames;
	}

	protected void info(String msg) {
		System.out.println("[INFO] " + msg);
	}

	/**
	 * Normalize the key to our S3 object:<br>
	 * Convert <code>./css/style.css</code> into <code>/css/style.css</code><br>
	 * Convert <code>/foo/bar/../../css/style.css</code> into
	 * <code>/css/style.css</code><br>
	 * 
	 * @param key S3 Key string.
	 * @return Normalized version of {@code key}.
	 */
	protected String getCanonicalKey(String key) {
		// release/./css/style.css
		String path = basedir + key;

		// /temp/release/css/style.css
		File file = getCanonicalFile(new File(TEMP_DIR, path));
		String canonical = file.getAbsolutePath();

		// release/css/style.css
		int pos = TEMP_DIR_PATH.length() + 1;
		String suffix = canonical.substring(pos);

		// Always replace backslash with forward slash just in case we are running on
		// Windows
		String canonicalKey = suffix.replace("\\", "/");

		// Return the canonical key
		return canonicalKey;
	}

	protected static File getCanonicalFile(String path) {
		return getCanonicalFile(new File(path));
	}

	protected static File getCanonicalFile(File file) {
		try {
			return new File(file.getCanonicalPath());
		} catch (IOException e) {
			throw new IllegalArgumentException("Unexpected IO error", e);
		}
	}

	/**
	 * Create a PutObjectRequest based on the PutContext
	 */
	public PutObjectRequest getPutObjectRequest(PutFileContext context) {
		File source = context.getSource();
		String destination = context.getDestination();
		TransferProgress progress = context.getProgress();
		return getPutObjectRequest(source, destination, progress);
	}

	protected InputStream getInputStream(File source, TransferProgress progress) throws IOException {
		if (progress == null) {
			return new ResettableInputStream(source);
		} else {
			return new TransferProgressFileInputStream(source, progress);
		}
	}

	/**
	 * Create a PutObjectRequest based on the source file and destination passed in.
	 * 
	 * @param source      Local file to upload.
	 * @param destination Destination S3 key.
	 * @param progress    Transfer listener.
	 * @return {@link PutObjectRequest} instance.
	 */
	protected PutObjectRequest getPutObjectRequest(File source, String destination, TransferProgress progress) {
		try {
			String key = getCanonicalKey(destination);
			InputStream input = getInputStream(source, progress);
			String contentType = mimeTypes.getMimetype(source);
			long contentLength = source.length();
			PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).contentLength(contentLength).contentType(contentType).build();
			if (acl != null) {
				request = request.toBuilder().acl(acl).build();
			}
			return request;
		} catch (FileNotFoundException e) {
			throw AwsServiceException.create("File not found", e);
		} catch (IOException e) {
			throw AwsServiceException.create("Error reading file", e);
		}
	}

	/**
	 * On S3 there are no true "directories". An S3 bucket is essentially a
	 * Hashtable of files stored by key. The integration between a traditional file
	 * system and an S3 bucket is to use the path of the file on the local file
	 * system as the key to the file in the bucket. The S3 bucket does not contain a
	 * separate key for the directory itself.
	 */
	public final void putDirectory(File sourceDir, String destinationDir) throws TransferFailedException {

		// Examine the contents of the directory
		List<PutFileContext> contexts = getPutFileContexts(sourceDir, destinationDir);
		for (PutFileContext context : contexts) {
			// Progress is tracked by the thread handler when uploading files this way
			context.setProgress(null);
		}

		// Sum the total bytes in the directory
		long bytes = sum(contexts);

		// Show what we are up to
		log.info(getUploadStartMsg(contexts.size(), bytes));

		// Store some context for the thread handler
		ThreadHandlerContext<PutFileContext> thc = new ThreadHandlerContext<PutFileContext>();
		thc.setList(contexts);
		thc.setHandler(new FileHandler());
		thc.setMax(maxThreads);
		thc.setMin(minThreads);
		thc.setDivisor(divisor);
		thc.setListener(new PercentCompleteListener<PutFileContext>());

		// Invoke the threads
		ExecutionStatistics stats = invoker.invokeThreads(thc);

		// Show some stats
		long millis = stats.getExecutionTime();
		long count = stats.getIterationCount();
		log.info(getUploadCompleteMsg(millis, bytes, count));
	}

	protected String getUploadCompleteMsg(long millis, long bytes, long count) {
		String rate = formatter.getRate(millis, bytes);
		String time = formatter.getTime(millis);
		StringBuilder sb = new StringBuilder();
		sb.append("Files: " + count);
		sb.append("  Time: " + time);
		sb.append("  Rate: " + rate);
		return sb.toString();
	}

	protected String getUploadStartMsg(int fileCount, long bytes) {
		StringBuilder sb = new StringBuilder();
		sb.append("Files: " + fileCount);
		sb.append("  Bytes: " + formatter.getSize(bytes));
		return sb.toString();
	}

	protected int getRequestsPerThread(int threads, int requests) {
		int requestsPerThread = requests / threads;
		while (requestsPerThread * threads < requests) {
			requestsPerThread++;
		}
		return requestsPerThread;
	}

	protected long sum(List<PutFileContext> contexts) {
		long sum = 0;
		for (PutFileContext context : contexts) {
			File file = context.getSource();
			long length = file.length();
			sum += length;
		}
		return sum;
	}

	/**
	 * Store a resource into S3
	 */
	@Override
	protected void putResource(final File source, final String destination, final TransferProgress progress)
			throws IOException {

		// Create a new PutObjectRequest
		PutObjectRequest request = getPutObjectRequest(source, destination, progress);

		// Upload the file to S3, using multi-part upload for large files
		S3Utils.getInstance().upload(source, request, client, transferManager);
	}

	protected String getDestinationPath(final String destination) {
		return destination.substring(0, destination.lastIndexOf('/'));
	}

	/**
	 * Convert "/" -&gt; ""<br>
	 * Convert "/snapshot/" &gt; "snapshot/"<br>
	 * Convert "/snapshot" -&gt; "snapshot/"<br>
	 * 
	 * @param source Repository info.
	 * @return Normalized repository base dir.
	 */
	protected String getBaseDir(final Repository source) {
		StringBuilder sb = new StringBuilder(source.getBasedir());
		sb.deleteCharAt(0);
		if (sb.length() == 0) {
			return "";
		}
		if (sb.charAt(sb.length() - 1) != '/') {
			sb.append('/');
		}
		return sb.toString();
	}

	/**
	 * Create AWSCredentionals from the information in system properties,
	 * environment variables, settings.xml, or EC2 instance metadata (only
	 * applicable when running the wagon on an Amazon EC2 instance)
	 * 
	 * @param authenticationInfo Authentication credentials from Maven settings.
	 * @return Resolved AWS Credentials.
	 */
	protected AwsCredentialsProvider getCredentials(final AuthenticationInfo authenticationInfo) {
		Optional<AuthenticationInfo> auth = Optional.fromNullable(authenticationInfo);
		MvnAwsCredentialsProvider chain = MvnAwsCredentialsProvider.builder().auth(auth).build();
		return (AwsCredentialsProvider) chain;
	}

	@Override
	protected PutFileContext getPutFileContext(File source, String destination) {
		PutFileContext context = super.getPutFileContext(source, destination);
		context.setFactory(this);
		context.setTransferManager(this.transferManager);
		context.setClient(this.client);
		return context;
	}

	protected int getMinThreads() {
		return getValue(MIN_THREADS_KEY, DEFAULT_MIN_THREAD_COUNT);
	}

	protected int getMaxThreads() {
		return getValue(MAX_THREADS_KEY, DEFAULT_MAX_THREAD_COUNT);
	}

	protected String getVpce() {
		return getValue(VPC_ENDPOINT_KEY, "");
	}

	protected String getRegion() {
		return getValue(REGION, "");
	}

	protected int getDivisor() {
		return getValue(DIVISOR_KEY, DEFAULT_DIVISOR);
	}

	protected int getValue(String key, int defaultValue) {
		String value = System.getProperty(key);
		if (StringUtils.isEmpty(value)) {
			return defaultValue;
		} else {
			return new Integer(value);
		}
	}

	protected String getValue(String key, String defaultValue) {
		String value = System.getProperty(key);
		if (StringUtils.isEmpty(value)) {
			return defaultValue;
		} else {
			return value;
		}
	}

	public int getReadTimeout() {
		return readTimeout;
	}

	public void setReadTimeout(int readTimeout) {
		this.readTimeout = readTimeout;
	}

}
