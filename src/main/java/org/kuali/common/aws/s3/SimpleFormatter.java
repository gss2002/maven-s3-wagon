package org.kuali.common.aws.s3;

import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Format time, bytes, counts, dates, and transfer rates into human friendly form
 *
 * @author Jeff Caddel
 * @since May 27, 2010 6:46:17 PM
 */
public class SimpleFormatter {
	public static final double SECOND = 1000;
	public static final double MINUTE = 60 * SECOND;
	public static final double HOUR = 60 * MINUTE;
	public static final double DAY = 24 * HOUR;
	public static final double YEAR = 365 * DAY;
	public static final double DECADE = 10 * YEAR;
	public static final double CENTURY = 10 * DECADE;
	private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ";

	NumberFormat largeSizeFormatter = NumberFormat.getInstance();
	NumberFormat sizeFormatter = NumberFormat.getInstance();
	NumberFormat timeFormatter = NumberFormat.getInstance();
	NumberFormat rateFormatter = NumberFormat.getInstance();
	NumberFormat countFormatter = NumberFormat.getInstance();
	SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT);

	public SimpleFormatter() {
		super();
		sizeFormatter.setGroupingUsed(false);
		sizeFormatter.setMaximumFractionDigits(1);
		sizeFormatter.setMinimumFractionDigits(1);
		largeSizeFormatter.setGroupingUsed(false);
		largeSizeFormatter.setMaximumFractionDigits(3);
		largeSizeFormatter.setMinimumFractionDigits(3);
		timeFormatter.setGroupingUsed(false);
		timeFormatter.setMaximumFractionDigits(3);
		timeFormatter.setMinimumFractionDigits(3);
		rateFormatter.setGroupingUsed(false);
		rateFormatter.setMaximumFractionDigits(3);
		rateFormatter.setMinimumFractionDigits(3);
		countFormatter.setGroupingUsed(true);
		countFormatter.setMaximumFractionDigits(0);
		countFormatter.setMinimumFractionDigits(0);
		dateFormatter.setLenient(false);
	}

	/**
	 * Parse a date from the string. The string must be in the same format as what is returned by the getDate() methods
	 */
	public Date parseDate(String date) {
		try {
			return dateFormatter.parse(date);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Can't parse [" + date + "]", e);
		}
	}

	/**
	 * Return a formatted date
	 */
	public String getDate(long millis) {
		return getDate(new Date(millis));
	}

	/**
	 * Return a formatted date
	 */
	public String getDate(Date date) {
		return dateFormatter.format(date);
	}

	/**
	 * Given a number of bytes and the number of milliseconds it took to transfer that number of bytes, return bytes/s, KB/s, MB/s, GB/s,
	 * TB/s, PB/s, or EB/s as appropriate
	 */
	public String getRate(long millis, long bytes) {
		double seconds = millis / SECOND;
		double bytesPerSecond = bytes / seconds;
		Size bandwidthLevel = getSizeEnum(bytesPerSecond);
		double transferRate = bytesPerSecond / bandwidthLevel.getValue();
		return rateFormatter.format(transferRate) + " " + bandwidthLevel.getRateLabel();
	}

	/**
	 * Return a formatted <code>count</code>
	 */
	public String getCount(long count) {
		return countFormatter.format(count);
	}

	/**
	 * Given milliseconds, return milliseconds, seconds, minutes, hours, days, years, decades, or centuries as appropriate. Note that years,
	 * decades, and centuries are approximations since the logic always assumes there are exactly 365 days per year.
	 */
	public String getTime(long millis) {
		long abs = Math.abs(millis);
		if (abs < SECOND) {
			return millis + "ms";
		} else if (abs < MINUTE) {
			return timeFormatter.format(millis / SECOND) + "s";
		} else if (abs < HOUR) {
			return timeFormatter.format(millis / MINUTE) + "m";
		} else if (abs < DAY) {
			return timeFormatter.format(millis / HOUR) + "h";
		} else if (abs < YEAR) {
			return timeFormatter.format(millis / DAY) + "d";
		} else if (abs < DECADE) {
			return timeFormatter.format(millis / YEAR) + "y";
		} else if (abs < CENTURY) {
			return timeFormatter.format(millis / DECADE) + " decades";
		} else {
			return timeFormatter.format(millis / CENTURY) + " centuries";
		}
	}

	/**
	 * Given a number of bytes return bytes, kilobytes, megabytes, gigabytes, terabytes, petabytes, or exabytes as appropriate.
	 */
	public String getSize(long bytes) {
		return getSize(bytes, null);
	}

	/**
	 * Given a number of bytes return a string formatted into the unit of measure indicated
	 */
	public String getSize(long bytes, Size unitOfMeasure) {
		unitOfMeasure = (unitOfMeasure == null) ? getSizeEnum(bytes) : unitOfMeasure;
		StringBuilder sb = new StringBuilder();
		sb.append(getFormattedSize(bytes, unitOfMeasure));
		if (unitOfMeasure.equals(Size.BYTE)) {
			sb.append(" ");
		}
		sb.append(unitOfMeasure.getSizeLabel());
		return sb.toString();
	}

	public String getFormattedSize(long bytes, Size size) {
		switch (size) {
		case BYTE:
			return bytes + "";
		case KB:
		case MB:
		case GB:
			return sizeFormatter.format(bytes / (double) size.getValue());
		default:
			return largeSizeFormatter.format(bytes / (double) size.getValue());
		}
	}

	public Size getSizeEnum(double bytes) {
		bytes = Math.abs(bytes);
		if (bytes < Size.KB.getValue()) {
			return Size.BYTE;
		} else if (bytes < Size.MB.getValue()) {
			return Size.KB;
		} else if (bytes < Size.GB.getValue()) {
			return Size.MB;
		} else if (bytes < Size.TB.getValue()) {
			return Size.GB;
		} else if (bytes < Size.PB.getValue()) {
			return Size.TB;
		} else if (bytes < Size.EB.getValue()) {
			return Size.PB;
		} else {
			return Size.EB;
		}
	}

	public NumberFormat getLargeSizeFormatter() {
		return largeSizeFormatter;
	}

	public void setLargeSizeFormatter(NumberFormat sizeFormatter) {
		this.largeSizeFormatter = sizeFormatter;
	}

	public NumberFormat getTimeFormatter() {
		return timeFormatter;
	}

	public void setTimeFormatter(NumberFormat timeFormatter) {
		this.timeFormatter = timeFormatter;
	}

	public NumberFormat getRateFormatter() {
		return rateFormatter;
	}

	public void setRateFormatter(NumberFormat rateFormatter) {
		this.rateFormatter = rateFormatter;
	}

	public NumberFormat getSizeFormatter() {
		return sizeFormatter;
	}

	public void setSizeFormatter(NumberFormat smallSizeFormatter) {
		this.sizeFormatter = smallSizeFormatter;
	}
}
