package io.confluent.connect.hdfs.partitioner;

import static org.junit.Assert.assertEquals;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

public class APTPartitionerTest {

	private static final long partitionDurationMs = TimeUnit.MINUTES.toMillis(10);

	@Test
	public void testAPTPartitioner() throws Exception {
		Map<String, Object> config = createConfig();

		APTPartitioner partitioner = new APTPartitioner();
		partitioner.configure(config);

		String pathFormat = partitioner.getPathFormat();
		String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
		long timestamp = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
		String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);
		String path = partitioner.generatePartitionedPath("topic", encodedPartition);
		assertEquals("topic/year=2014/month=02/day=01/hour=03/minute=00/", path);
	}

	@Test
	public void testAPTPartitioner2() throws Exception {
		Map<String, Object> config = createConfig();

		APTPartitioner partitioner = new APTPartitioner();
		partitioner.configure(config);

		String pathFormat = partitioner.getPathFormat();
		String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
		long timestamp = new DateTime(2015, 8, 12, 15, 23, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
		String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);
		String path = partitioner.generatePartitionedPath("topic", encodedPartition);
		assertEquals("topic/year=2015/month=08/day=12/hour=15/minute=20/", path);
	}


	private Map<String, Object> createConfig() {
		Map<String, Object> config = new HashMap<>();
		config.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, "en");
		config.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
		return config;
	}
}
