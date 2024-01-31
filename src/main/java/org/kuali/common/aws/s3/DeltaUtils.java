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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.kuali.common.aws.s3.pojo.AccountDeltaSummary;
import org.kuali.common.aws.s3.pojo.BucketDeltaLine;
import org.kuali.common.aws.s3.pojo.BucketDeltaSummary;
import org.kuali.common.aws.s3.pojo.BucketSummaryLine;
import org.kuali.common.aws.s3.pojo.BucketSummaryLineComparator;

public class DeltaUtils {
	SimpleFormatter formatter = new SimpleFormatter();
	SimpleDateFormat shortDateFormatter = new SimpleDateFormat("yyyy-MM-dd");

	private static DeltaUtils instance;

	public static synchronized DeltaUtils getInstance() {
		if (instance == null) {
			instance = new DeltaUtils();
		}
		return instance;
	}

	protected DeltaUtils() {
		super();
	}

	public AccountDeltaSummary getAccountDeltaSummary(File csv) {

		List<String> lines = getLines(csv);
		List<BucketSummaryLine> summaryLines = getBucketSummaryLines(lines);
		List<String> buckets = getBucketNames(summaryLines);
		List<BucketDeltaSummary> bucketDeltaSummaries = new ArrayList<BucketDeltaSummary>();

		for (String bucket : buckets) {
			List<BucketSummaryLine> matchingLines = getMatchingSummaryLines(bucket, summaryLines);
			BucketDeltaSummary bds = getBucketDeltaSummary(matchingLines);
			List<BucketDeltaLine> deltaLines = getBucketDeltaLines(matchingLines);
			bds.setBucket(bucket);
			bds.setDeltaLines(deltaLines);
			bucketDeltaSummaries.add(bds);
		}

		AccountDeltaSummary ads = new AccountDeltaSummary();
		ads.setBucketDeltaSummaries(bucketDeltaSummaries);
		return ads;
	}

	public BucketDeltaSummary getBucketDeltaSummary(List<BucketSummaryLine> summaryLines) {
		// If we don't have at least 2 lines we can't calculate any delta's
		if (summaryLines.size() < 2) {
			return new BucketDeltaSummary();
		}

		BucketSummaryLine first = summaryLines.get(0);
		BucketSummaryLine last = summaryLines.get(summaryLines.size() - 1);

		Date startDate = first.getDate();
		Date endDate = last.getDate();
		long interval = endDate.getTime() - startDate.getTime();
		long fileDelta = last.getFiles() - first.getFiles();
		long byteDelta = last.getBytes() - first.getBytes();

		BucketDeltaSummary deltaSummary = new BucketDeltaSummary();
		deltaSummary.setByteDelta(byteDelta);
		deltaSummary.setInterval(interval);
		deltaSummary.setStartDate(startDate);
		deltaSummary.setEndDate(endDate);
		deltaSummary.setFileDelta(fileDelta);
		return deltaSummary;
	}

	public String toString(AccountDeltaSummary summary) {
		List<String[]> rows = new ArrayList<String[]>();
		for (BucketDeltaSummary bds : summary.getBucketDeltaSummaries()) {
			rows.addAll(getRows(bds.getBucket(), bds.getDeltaLines()));
			String files = formatter.getCount(bds.getFileDelta());
			String bytes = formatter.getSize(bds.getByteDelta());
			String start = shortDateFormatter.format(bds.getStartDate());
			String end = shortDateFormatter.format(bds.getEndDate());
			String interval = formatter.getTime(bds.getInterval());
			rows.add(new String[] { "", "", "", "", "", "" });
			rows.add(new String[] { "totals", files, bytes, start, end, interval });
			rows.add(getPerDayRow(bds));
			rows.add(new String[] { "", "", "", "", "", "" });
			rows.add(new String[] { "", "", "", "", "", "" });
		}
		List<String> columns = new ArrayList<String>();
		columns.add("bucket");
		columns.add("files");
		columns.add("size");
		columns.add("start");
		columns.add("end");
		columns.add("interval");
		return S3Utils.getInstance().toString(columns, rows);
	}

	protected String[] getPerDayRow(BucketDeltaSummary bds) {
		String perDay = "per day";
		double days = bds.getInterval() / SimpleFormatter.DAY;
		String files = formatter.getCount((long) (bds.getFileDelta() / days));
		long byteDelta = bds.getByteDelta();
		long bytesPerDay = (long) (byteDelta / days);
		String bytes = formatter.getSize(bytesPerDay);
		return new String[] { perDay, files, bytes, "", "", "" };
	}

	public List<String[]> getRows(String bucket, List<BucketDeltaLine> deltaLines) {
		List<String[]> rows = new ArrayList<String[]>();
		for (BucketDeltaLine deltaLine : deltaLines) {
			String files = formatter.getCount(deltaLine.getFileDelta());
			String size = formatter.getSize(deltaLine.getByteDelta());
			String start = shortDateFormatter.format(deltaLine.getStartDate());
			String end = shortDateFormatter.format(deltaLine.getEndDate());
			String interval = formatter.getTime(deltaLine.getInterval());
			String[] row = new String[] { bucket, files, size, start, end, interval };
			rows.add(row);
		}
		return rows;
	}

	public List<String> getLines(File file) {
		try {
			return FileUtils.readLines(file);
		} catch (IOException e) {
			throw new IllegalStateException("Can't read lines from " + getCanonicalPath(file), e);
		}
	}

	public String getCanonicalPath(File file) {
		try {
			return file.getCanonicalPath();
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to determine canonical path for " + file, e);
		}
	}

	public List<BucketSummaryLine> getBucketSummaryLines(List<String> lines) {
		List<BucketSummaryLine> summaryLines = new ArrayList<BucketSummaryLine>();
		// Start at 1 instead of zero in order to skip the first line since it is just
		// the column headers
		for (int i = 1; i < lines.size(); i++) {
			String line = lines.get(i);
			String[] tokens = line.split(",");
			String bucket = tokens[0];
			Long files = new Long(tokens[1]);
			Long bytes = new Long(tokens[2]);
			Date date = formatter.parseDate(tokens[3]);
			BucketSummaryLine bsl = new BucketSummaryLine();
			bsl.setBucket(bucket);
			bsl.setFiles(files);
			bsl.setBytes(bytes);
			bsl.setDate(date);
			summaryLines.add(bsl);
		}
		return summaryLines;
	}

	public List<String> getBucketNames(List<BucketSummaryLine> summaryLines) {
		Map<String, String> map = new TreeMap<String, String>();
		for (BucketSummaryLine bsl : summaryLines) {
			map.put(bsl.getBucket(), bsl.getBucket());
		}
		return new ArrayList<String>(map.keySet());

	}

	public List<BucketDeltaLine> getBucketDeltaLines(List<BucketSummaryLine> summaryLines) {
		List<BucketDeltaLine> bdls = new ArrayList<BucketDeltaLine>();
		for (int i = 0; i < summaryLines.size() - 1; i++) {
			BucketSummaryLine current = summaryLines.get(i);
			BucketSummaryLine next = summaryLines.get(i + 1);
			long fileDelta = next.getFiles() - current.getFiles();
			long byteDelta = next.getBytes() - current.getBytes();
			Date startDate = current.getDate();
			Date endDate = next.getDate();
			long interval = endDate.getTime() - startDate.getTime();
			BucketDeltaLine bdl = new BucketDeltaLine();
			bdl.setFileDelta(fileDelta);
			bdl.setByteDelta(byteDelta);
			bdl.setStartDate(startDate);
			bdl.setEndDate(endDate);
			bdl.setInterval(interval);
			bdls.add(bdl);
		}
		return bdls;
	}

	public List<BucketSummaryLine> getMatchingSummaryLines(String bucket, List<BucketSummaryLine> summaryLines) {
		Collections.sort(summaryLines, new BucketSummaryLineComparator());
		List<BucketSummaryLine> matchingLines = new ArrayList<BucketSummaryLine>();
		for (BucketSummaryLine bsl : summaryLines) {
			if (bsl.getBucket().equals(bucket)) {
				matchingLines.add(bsl);
			}
		}
		return matchingLines;
	}

}