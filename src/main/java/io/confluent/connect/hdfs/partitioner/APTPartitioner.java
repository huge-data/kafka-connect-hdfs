package io.confluent.connect.hdfs.partitioner;

import io.confluent.common.config.ConfigException;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTimeZone;

public class APTPartitioner extends TimeBasedPartitioner {

	private static long partitionDurationMs = TimeUnit.MINUTES.toMillis(10);
	private static String pathFormat = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'minute'=mm/";

	@Override
	public void configure(Map<String, Object> config) {
		String localeString = (String) config.get(HdfsSinkConnectorConfig.LOCALE_CONFIG);
		if (localeString.equals("")) {
			throw new ConfigException(HdfsSinkConnectorConfig.LOCALE_CONFIG, localeString, "Locale cannot be empty.");
		}
		String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
		if (timeZoneString.equals("")) {
			throw new ConfigException(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, timeZoneString,
					"Timezone cannot be empty.");
		}
		String hiveIntString = (String) config.get(HdfsSinkConnectorConfig.HIVE_INTEGRATION_CONFIG);
		boolean hiveIntegration = hiveIntString != null && hiveIntString.toLowerCase().equals("true");
		Object interval = config.get(HdfsSinkConnectorConfig.APT_PARTITIONER_INTERVAL_MINUTE);
		if (interval != null) {
			int intervalMinute = Integer.parseInt((String) interval);
			partitionDurationMs = TimeUnit.MINUTES.toMillis(intervalMinute);
		}
		Locale locale = new Locale(localeString);
		DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
		init(partitionDurationMs, pathFormat, locale, timeZone, hiveIntegration);
	}

	public String getPathFormat() {
		return pathFormat;
	}
}
