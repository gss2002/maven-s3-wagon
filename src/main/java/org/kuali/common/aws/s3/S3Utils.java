
package org.kuali.common.aws.s3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.tree.DefaultMutableTreeNode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.kuali.common.aws.s3.pojo.AccountSummary;
import org.kuali.common.aws.s3.pojo.AccountSummaryContext;
import org.kuali.common.aws.s3.pojo.BucketComparator;
import org.kuali.common.aws.s3.pojo.BucketDisplay;
import org.kuali.common.aws.s3.pojo.BucketPrefixSummary;
import org.kuali.common.aws.s3.pojo.BucketSummary;
import org.kuali.common.aws.s3.pojo.S3PrefixContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * Utility methods related to Amazon S3
 */
public class S3Utils {
	private static final Logger log = LoggerFactory.getLogger(S3Utils.class);
	// Use multi part upload for files larger than 100 megabytes
	public static final long MULTI_PART_UPLOAD_THRESHOLD = Size.MB.getValue() * 100;
	private static final String PREFIX = "prefix";
	private static final String COUNT = "count";
	private static final String SIZE = "size";
	SimpleFormatter formatter = new SimpleFormatter();

	private static S3Utils instance;

	public static synchronized S3Utils getInstance() {
		if (instance == null) {
			instance = new S3Utils();
		}
		return instance;
	}

	protected S3Utils() {
		super();
	}

	public AWSCredentials getCredentials(String accessKey, String secretKey) {
		return new BasicAWSCredentials(accessKey, secretKey);
	}

	public AmazonS3Client getClient(String accessKey, String secretKey) {
		AWSCredentials credentials = getCredentials(accessKey, secretKey);
		return new AmazonS3Client(credentials);
	}

	/**
	 * Upload a single file to Amazon S3. If the file is larger than <code>MULTI_PART_UPLOAD_THRESHOLD</code> a multi-part upload is used.
	 * Multi-part uploads split the file into several smaller chunks with each chunk being uploaded in a different thread. Once all the
	 * threads have completed the file is automatically reassembled on S3 as a single file.
	 */
	public void upload(File file, PutObjectRequest request, AmazonS3 client, TransferManager manager) {
		// Store the file on S3
		if (file.length() < MULTI_PART_UPLOAD_THRESHOLD) {
			// Use normal upload for small files
			client.putObject(request);
		} else {
			log.debug("Blocking multi-part upload: " + file.getAbsolutePath());
			// Use multi-part upload for large files
			blockingMultiPartUpload(request, manager);
		}
	}

	/**
	 * Use this method to reliably upload large files and wait until they are fully uploaded before continuing. Behind the scenes this is
	 * accomplished by splitting the file up into manageable chunks and using separate threads to upload each chunk. Consider using
	 * multi-part uploads on files larger than <code>MULTI_PART_UPLOAD_THRESHOLD</code>. When this method returns, all threads have finished
	 * and the file has been reassembled on S3. The benefit to this method is that if any one thread fails, only the portion of the file
	 * that particular thread was handling will have to be re-uploaded (instead of the entire file). A reasonable number of automatic
	 * retries occurs if an individual upload thread fails. If the file upload fails this method throws <code>AmazonS3Exception</code>
	 */
	public void blockingMultiPartUpload(PutObjectRequest request, TransferManager manager) {
		// Use multi-part upload for large files
		Upload upload = manager.upload(request);
		try {
			// Block and wait for the upload to finish
			upload.waitForCompletion();
		} catch (Exception e) {
			throw new AmazonS3Exception("Unexpected error uploading file", e);
		}
	}

	public ListObjectsRequest getListObjectsRequest(String bucketName, String prefix, String delimiter, Integer maxKeys) {
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setDelimiter(delimiter);
		request.setPrefix(prefix);
		request.setMaxKeys(maxKeys);
		return request;
	}

	public ListObjectsRequest getListObjectsRequest(String bucketName, String prefix, String delimiter) {
		return getListObjectsRequest(bucketName, prefix, delimiter, null);
	}

	public ListObjectsRequest getListObjectsRequest(String bucketName, String prefix) {
		return getListObjectsRequest(bucketName, prefix, null);
	}

	public ListObjectsRequest getListObjectsRequest(String bucketName) {
		return getListObjectsRequest(bucketName, null);
	}

	public List<DefaultMutableTreeNode> getLeaves(DefaultMutableTreeNode node) {
		Enumeration<?> e = node.breadthFirstEnumeration();
		List<DefaultMutableTreeNode> leaves = new ArrayList<DefaultMutableTreeNode>();
		while (e.hasMoreElements()) {
			DefaultMutableTreeNode element = (DefaultMutableTreeNode) e.nextElement();
			if (element.isLeaf()) {
				leaves.add(element);
			}
		}
		return leaves;
	}

	public DefaultMutableTreeNode buildTree(List<String> prefixes, String delimiter) {
		Map<String, DefaultMutableTreeNode> map = new HashMap<String, DefaultMutableTreeNode>();
		for (String prefix : prefixes) {
			BucketPrefixSummary summary = new BucketPrefixSummary(prefix);
			DefaultMutableTreeNode node = new DefaultMutableTreeNode(summary);
			if (prefix != null) {
				String parentKey = getParentPrefix(prefix, delimiter);
				DefaultMutableTreeNode parent = map.get(parentKey);
				parent.add(node);
			}
			map.put(prefix, node);
		}
		return map.get(null);
	}

	public String getParentPrefix(String prefix, String delimiter) {
		String[] tokens = StringUtils.split(prefix, delimiter);
		if (tokens.length == 1) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < tokens.length - 1; i++) {
			sb.append(tokens[i] + delimiter);
		}
		return sb.toString();
	}

	public void buildPrefixList(AmazonS3Client client, String bucketName, List<String> prefixes, String prefix, String delimiter, BaseCase baseCase) {
		log.info(prefix);
		prefixes.add(prefix);
		ListObjectsRequest request = getListObjectsRequest(bucketName, prefix, delimiter);
		ObjectListing listing = client.listObjects(request);
		List<String> commonPrefixes = listing.getCommonPrefixes();
		for (String commonPrefix : commonPrefixes) {
			if (!baseCase.isBaseCase(commonPrefix)) {
				buildPrefixList(client, bucketName, prefixes, commonPrefix, delimiter, baseCase);
			}
		}
	}

	public void summarize(AmazonS3Client client, String bucketName, DefaultMutableTreeNode node) {
		List<DefaultMutableTreeNode> leaves = getLeaves(node);
		for (DefaultMutableTreeNode leaf : leaves) {
			BucketPrefixSummary summary = (BucketPrefixSummary) leaf.getUserObject();
			summarize(client, bucketName, summary);
		}
		fillInSummaries(node);
	}

	public List<BucketPrefixSummary> getBucketSummaryLeafs(DefaultMutableTreeNode node) {
		List<DefaultMutableTreeNode> leaves = getLeaves(node);
		List<BucketPrefixSummary> summaries = new ArrayList<BucketPrefixSummary>();
		for (DefaultMutableTreeNode leaf : leaves) {
			BucketPrefixSummary summary = (BucketPrefixSummary) leaf.getUserObject();
			summaries.add(summary);
		}
		Collections.sort(summaries);
		return summaries;
	}

	public void fillInSummaries(DefaultMutableTreeNode node) {
		BucketPrefixSummary summary = (BucketPrefixSummary) node.getUserObject();
		List<DefaultMutableTreeNode> children = getChildren(node);
		for (DefaultMutableTreeNode child : children) {
			fillInSummaries(child);
			BucketPrefixSummary childSummary = (BucketPrefixSummary) child.getUserObject();
			long count = childSummary.getCount();
			long size = childSummary.getSize();
			summary.setCount(summary.getCount() + count);
			summary.setSize(summary.getSize() + size);
		}
	}

	public List<DefaultMutableTreeNode> getChildren(DefaultMutableTreeNode node) {
		Enumeration<?> e = node.children();
		List<DefaultMutableTreeNode> children = new ArrayList<DefaultMutableTreeNode>();
		while (e.hasMoreElements()) {
			DefaultMutableTreeNode child = (DefaultMutableTreeNode) e.nextElement();
			children.add(child);
		}
		return children;
	}

	public AccountSummary getAccountSummary(AccountSummaryContext context) {
		AmazonS3Client client = getClient(context.getAccessKey(), context.getSecretKey());
		List<Bucket> buckets = getBuckets(client, context.getIncludes(), context.getExcludes());
		List<BucketSummary> summaries = getBucketSummaries(client, buckets);
		AccountSummary summary = new AccountSummary();
		summary.setAccessKey(context.getAccessKey());
		summary.setBucketSummaries(summaries);
		updateAccountSummary(summary);
		return summary;
	}

	public AccountSummary getAccountSummary(String accessKey, String secretKey, List<String> includes, List<String> excludes) {
		AccountSummaryContext context = new AccountSummaryContext();
		context.setAccessKey(accessKey);
		context.setSecretKey(secretKey);
		context.setIncludes(includes);
		context.setExcludes(excludes);
		return getAccountSummary(context);
	}

	public List<BucketSummary> getBucketSummaries(AmazonS3Client client, List<Bucket> buckets) {
		List<BucketSummary> summaries = new ArrayList<BucketSummary>();
		int count = 1;
		for (Bucket bucket : buckets) {
			long start = System.currentTimeMillis();
			System.out.print("[INFO] " + count + " - " + bucket.getName() + " - ");
			BucketSummary summary = getBucketSummary(client, bucket);
			System.out.println(formatter.getTime(System.currentTimeMillis() - start));
			summaries.add(summary);
			count++;
		}
		return summaries;
	}

	public BucketSummary getBucketSummary(AmazonS3Client client, Bucket bucket) {
		BucketSummary summary = new BucketSummary();
		summary.setBucket(bucket);
		ListObjectsRequest request = getListObjectsRequest(bucket.getName());
		ObjectListing current = client.listObjects(request);
		updateBucketSummary(summary, current.getObjectSummaries());
		while (current.isTruncated()) {
			current = client.listNextBatchOfObjects(current);
			updateBucketSummary(summary, current.getObjectSummaries());
		}
		log.debug("Completed summary for '{}'", bucket.getName());
		return summary;
	}

	public BucketPrefixSummary summarize(AmazonS3Client client, String bucketName) {
		BucketPrefixSummary summary = new BucketPrefixSummary();
		ListObjectsRequest request = getListObjectsRequest(bucketName);
		ObjectListing current = client.listObjects(request);
		summarize(summary, current.getObjectSummaries());
		while (current.isTruncated()) {
			current = client.listNextBatchOfObjects(current);
			summarize(summary, current.getObjectSummaries());
		}
		log.debug("Completed summary for '{}'", bucketName);
		return summary;
	}

	public BucketPrefixSummary summarize(AmazonS3Client client, String bucketName, BucketPrefixSummary summary) {
		ListObjectsRequest request = getListObjectsRequest(bucketName, summary.getPrefix());
		ObjectListing current = client.listObjects(request);
		summarize(summary, current.getObjectSummaries());
		while (current.isTruncated()) {
			current = client.listNextBatchOfObjects(current);
			summarize(summary, current.getObjectSummaries());
		}
		log.debug("Completed summary for prefix '{}'", summary.getPrefix());
		return summary;
	}

	public void updateBucketSummary(BucketSummary summary, List<S3ObjectSummary> summaries) {
		for (S3ObjectSummary element : summaries) {
			summary.setSize(summary.getSize() + element.getSize());
			summary.setCount(summary.getCount() + 1);
		}
	}

	public void summarize(BucketPrefixSummary summary, List<S3ObjectSummary> summaries) {
		for (S3ObjectSummary element : summaries) {
			summary.setSize(summary.getSize() + element.getSize());
			summary.setCount(summary.getCount() + 1);
			if (log.isDebugEnabled()) {
				log.debug(summary.getCount() + " - " + element.getKey() + " - " + formatter.getSize(element.getSize()));
			}
		}
		if (log.isDebugEnabled()) {
			String prefix = summary.getPrefix();
			long count = summary.getCount();
			long bytes = summary.getSize();
			log.debug(rpad(prefix, 40) + " Total Count: " + lpad(count + "", 3) + " Total Size: " + lpad(formatter.getSize(bytes), 9));
		}
	}

	public String toString(DefaultMutableTreeNode node) {
		return toString(node, null, null);
	}

	public String toString(DefaultMutableTreeNode node, Size size) {
		return toString(node, size, null);
	}

	public String toString(DefaultMutableTreeNode node, Comparator<BucketPrefixSummary> comparator) {
		return toString(node, null, comparator);
	}

	public List<BucketPrefixSummary> getBucketSummaryList(DefaultMutableTreeNode node, Comparator<BucketPrefixSummary> comparator) {
		List<BucketPrefixSummary> list = new ArrayList<BucketPrefixSummary>();
		Enumeration<?> e = node.breadthFirstEnumeration();
		while (e.hasMoreElements()) {
			DefaultMutableTreeNode element = (DefaultMutableTreeNode) e.nextElement();
			BucketPrefixSummary summary = (BucketPrefixSummary) element.getUserObject();
			list.add(summary);
		}
		if (comparator == null) {
			Collections.sort(list);
		} else {
			Collections.sort(list, comparator);
		}
		return list;
	}

	public List<BucketDisplay> getBucketDisplayList(List<BucketPrefixSummary> summaries, Size size) {
		List<BucketDisplay> list = new ArrayList<BucketDisplay>();
		for (BucketPrefixSummary summary : summaries) {
			BucketDisplay display = new BucketDisplay();
			display.setPrefix(summary.getPrefix() == null ? "/" : summary.getPrefix());
			display.setCount(summary.getCount());
			display.setSize(formatter.getSize(summary.getSize(), size));
			list.add(display);
		}
		return list;
	}

	public List<S3PrefixContext> getS3PrefixContexts(AmazonS3Client client, String bucketName, List<BucketPrefixSummary> summaries) {
		List<S3PrefixContext> contexts = new ArrayList<S3PrefixContext>();
		for (BucketPrefixSummary summary : summaries) {
			S3PrefixContext context = new S3PrefixContext();
			context.setClient(client);
			context.setBucketName(bucketName);
			context.setSummary(summary);
			contexts.add(context);
		}
		return contexts;
	}

	public String toString(DefaultMutableTreeNode node, Size size, Comparator<BucketPrefixSummary> comparator) {
		List<BucketPrefixSummary> bucketSummaryList = getBucketSummaryList(node, comparator);
		List<BucketDisplay> list = getBucketDisplayList(bucketSummaryList, size);
		int maxPrefixLength = PREFIX.length();
		int maxCountLength = COUNT.length();
		int maxSizeLength = SIZE.length();
		for (BucketDisplay display : list) {
			maxPrefixLength = Math.max(maxPrefixLength, display.getPrefix().length());
			maxCountLength = Math.max(maxCountLength, (display.getCount() + "").length());
			maxSizeLength = Math.max(maxSizeLength, display.getSize().length());
		}
		StringBuilder sb = new StringBuilder();
		sb.append(rpad(PREFIX, maxPrefixLength) + " " + lpad(COUNT, maxCountLength) + " " + lpad(SIZE, maxSizeLength) + "\n");
		for (BucketDisplay display : list) {
			sb.append(rpad(display.getPrefix(), maxPrefixLength));
			sb.append(" ");
			sb.append(lpad(display.getCount() + "", maxCountLength));
			sb.append(" ");
			sb.append(lpad(display.getSize(), maxSizeLength));
			sb.append("\n");
		}
		return sb.toString();
	}

	public int[] getColumnLengths(List<String> columns, List<String[]> rows) {
		int[] columnLengths = new int[columns.size()];
		for (int i = 0; i < columnLengths.length; i++) {
			columnLengths[i] = columns.get(i).length();
		}
		for (String[] row : rows) {
			for (int i = 0; i < columns.size(); i++) {
				columnLengths[i] = Math.max(columnLengths[i], row[i].length());
			}
		}
		return columnLengths;
	}

	public List<String> toList(String csv) {
		if (StringUtils.isBlank(csv)) {
			return new ArrayList<String>();
		} else {
			String[] tokens = StringUtils.split(csv, ",");
			return toList(tokens);
		}
	}

	public List<String> toList(String[] tokens) {
		List<String> list = new ArrayList<String>();
		for (String token : tokens) {
			list.add(token);
		}
		return list;
	}

	public void updateAccountSummary(AccountSummary summary) {
		long size = 0;
		long files = 0;
		for (BucketSummary bucketSummary : summary.getBucketSummaries()) {
			size = size + bucketSummary.getSize();
			files = files + bucketSummary.getCount();
		}
		summary.setSize(size);
		summary.setCount(files);
	}

	public String toString(AccountSummary summary) {
		List<String> columns = getBucketSummaryColumns();
		List<String[]> rows = getRows(summary.getBucketSummaries());

		// Add a blank row for spacing
		rows.add(new String[] { "", "", "" });

		// Add a row showing total count and size
		String count = formatter.getCount(summary.getCount());
		String size = formatter.getSize(summary.getSize());
		rows.add(new String[] { "Totals", count, size });

		// Convert the rows to a string and return
		return toString(columns, rows);
	}

	public String toCSV(AccountSummary summary) {
		return toCSV(summary, true);
	}

	public String toCSV(AccountSummary summary, boolean printColumnHeaders) {
		List<String> columns = getAccountSummaryCSVColumns();
		List<String[]> rows = getAccountSummaryCSVRows(summary.getBucketSummaries(), new Date());
		return toCSV(columns, rows, printColumnHeaders);
	}

	public String toCSV(List<String> columns, List<String[]> rows, boolean printColumnHeaders) {
		StringBuilder sb = new StringBuilder();
		if (printColumnHeaders) {
			for (int i = 0; i < columns.size(); i++) {
				if (i != 0) {
					sb.append(",");
				}
				String column = columns.get(i);
				sb.append(column);
			}
			sb.append("\n");
		}
		for (int i = 0; i < rows.size(); i++) {
			String[] row = rows.get(i);
			for (int j = 0; j < row.length; j++) {
				if (j != 0) {
					sb.append(",");
				}
				sb.append(row[j]);
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	public String toString(List<String> columns, List<String[]> rows) {
		int[] columnLengths = getColumnLengths(columns, rows);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < columnLengths.length; i++) {
			if (i != 0) {
				sb.append("  ");
			}
			String name = columns.get(i);
			sb.append(lpad(name, columnLengths[i]));
		}
		sb.append("\n");
		for (int i = 0; i < rows.size(); i++) {
			String[] row = rows.get(i);
			for (int j = 0; j < row.length; j++) {
				if (j != 0) {
					sb.append("  ");
				}
				int columnLength = columnLengths[j];
				sb.append(lpad(row[j], columnLength));
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	public List<Bucket> getBuckets(AmazonS3Client client, List<String> includes, List<String> excludes) {
		List<Bucket> buckets = client.listBuckets();
		int originalSize = buckets.size();
		log.info("Located " + buckets.size() + " total buckets");
		Iterator<Bucket> itr = buckets.iterator();
		while (itr.hasNext()) {
			Bucket bucket = itr.next();
			String bucketName = bucket.getName();
			if (!include(bucketName, includes, excludes)) {
				log.info("Excluding '" + bucket.getName() + "'");
				itr.remove();
			}
		}
		if (originalSize != buckets.size()) {
			log.info("Trimmed bucket list contains " + buckets.size() + " buckets");
		}
		Collections.sort(buckets, new BucketComparator());
		return buckets;
	}

	public boolean include(String bucketName, List<String> includes, List<String> excludes) {
		if (excludes.contains(bucketName)) {
			return false;
		} else {
			return includes.size() == 0 || includes.contains(bucketName);
		}
	}

	public String lpad(String s, int size) {
		return StringUtils.leftPad(s, size, " ");
	}

	public String rpad(String s, int size) {
		return StringUtils.rightPad(s, size, " ");
	}

	public List<String> getBucketSummaryColumns() {
		List<String> columns = new ArrayList<String>();
		columns.add("Bucket");
		columns.add("Files");
		columns.add("Size");
		return columns;
	}

	public List<String[]> getRows(List<BucketSummary> summaries) {
		List<String[]> rows = new ArrayList<String[]>();
		for (BucketSummary summary : summaries) {
			rows.add(getRow(summary));
		}
		return rows;
	}

	protected String[] getRow(BucketSummary summary) {
		String[] row = new String[3];
		row[0] = summary.getBucket().getName();
		row[1] = formatter.getCount(summary.getCount());
		row[2] = formatter.getSize(summary.getSize());
		return row;
	}

	public List<String> getAccountSummaryCSVColumns() {
		List<String> columns = new ArrayList<String>();
		columns.add("bucket");
		columns.add("files");
		columns.add("bytes");
		columns.add("date");
		return columns;
	}

	protected List<String[]> getAccountSummaryCSVRows(List<BucketSummary> summaries, Date date) {
		List<String[]> rows = new ArrayList<String[]>();
		for (BucketSummary summary : summaries) {
			String[] row = getAccountSummaryCSVRow(summary, date);
			rows.add(row);
		}
		return rows;
	}

	protected String[] getAccountSummaryCSVRow(BucketSummary summary, Date date) {
		String[] row = new String[4];
		row[0] = summary.getBucket().getName();
		row[1] = summary.getCount() + "";
		row[2] = summary.getSize() + "";
		row[3] = formatter.getDate(date);
		return row;
	}

	public void write(File file, String data, boolean append) {
		try {
			FileUtils.write(file, data, append);
		} catch (IOException e) {
			throw new AmazonS3Exception("Error writing to file", e);
		}
	}

}
