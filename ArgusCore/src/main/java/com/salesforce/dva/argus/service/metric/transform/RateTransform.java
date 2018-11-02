package com.salesforce.dva.argus.service.metric.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.salesforce.dva.argus.entity.Metric;
import com.salesforce.dva.argus.service.metric.MetricReader;
import com.salesforce.dva.argus.system.SystemAssert;
import com.salesforce.dva.argus.system.SystemException;
import com.salesforce.dva.argus.util.QueryContext;
import com.salesforce.dva.argus.util.QueryUtils;
/**
 * It provides methods to implement Rate transform
 * @author Raj Sarkapally (rsarkapally@salesforce.com)
 *
 */
public class RateTransform implements Transform{
	private static long DEFAULT_INTERVAL=60*1000;
	private static boolean DEFAULT_SKIP_NAGATIVE_VALUES=true;
	private static boolean DEFAULT_INTERPOLATE_MISSING_DATAPOINTS=true;

	@Override
	public List<Metric> transform(QueryContext queryContext, List<Metric> metrics) {
		Long[] startAndEndTimestamps = QueryUtils.getStartAndEndTimesWithMaxInterval(queryContext);
		return performRate(metrics, startAndEndTimestamps[0], startAndEndTimestamps[1], DEFAULT_INTERVAL, 
				DEFAULT_SKIP_NAGATIVE_VALUES, DEFAULT_INTERPOLATE_MISSING_DATAPOINTS);
	}

	@Override
	public List<Metric> transform(QueryContext queryContext,
			List<Metric> metrics, List<String> constants) {
		SystemAssert.requireArgument(constants != null && constants.size() == 3,
				"Rate Transform needs 3 constants (interval, skipNegativeValues, interpolateMissingValues)!");

		Long[] startAndEndTimestamps = QueryUtils.getStartAndEndTimesWithMaxInterval(queryContext);
		long intervalInMilli = _getTimeIntervalInMilliSeconds(constants.get(0));
		return performRate(metrics, startAndEndTimestamps[0], startAndEndTimestamps[1], intervalInMilli, 
				Boolean.valueOf(constants.get(1)), Boolean.valueOf(constants.get(2)));
	}

	@Override
	public List<Metric> transform(QueryContext queryContext,
			List<Metric>... metrics) {
		throw new UnsupportedOperationException("Rate transform doesn't need list of list");
	}

	@Override
	public String getResultScopeName() {
		return TransformFactory.Function.RATE.name();
	}

	private List<Metric> performRate(List<Metric> metrics,long startTimestampInMilli, long endTimestampInMilli, long intervalInMilli,
			boolean skipNegativeValues, boolean interpolateMissingDP){
		List<Metric> result= new ArrayList<>();
		for(Metric metric:metrics) {
			if(metric.getDatapoints().size()>=2) {
				TreeMap<Long, Double> sortedDatapoints = new TreeMap<>(metric.getDatapoints());
				startTimestampInMilli = startTimestampInMilli > 0 ? startTimestampInMilli:sortedDatapoints.firstKey();
				endTimestampInMilli = endTimestampInMilli > 0 ?endTimestampInMilli:sortedDatapoints.lastKey();
				if(interpolateMissingDP) {
					addFirstNLastDatapointsIfMissing(sortedDatapoints, startTimestampInMilli, endTimestampInMilli, intervalInMilli);
					sortedDatapoints=performInterpolation(sortedDatapoints, intervalInMilli);
				}
				sortedDatapoints = performDifferentiation(sortedDatapoints, interpolateMissingDP, intervalInMilli);
				if(skipNegativeValues) {
					sortedDatapoints = removeNegativeValues(sortedDatapoints);
					if(interpolateMissingDP) {
						sortedDatapoints=performInterpolation(sortedDatapoints, intervalInMilli);
					}
				}
				metric.setDatapoints(sortedDatapoints);
				result.add(metric);
			}else {
				result.add(metric);
			}
		}
		return result;
	}

	public TreeMap<Long, Double> performInterpolation(TreeMap<Long, Double> sortedDatapoints, long intervalInMilli) {
		if(sortedDatapoints.size()<2) {
			return sortedDatapoints;
		}
		TreeMap<Long, Double> result = new TreeMap<>();
		Long prevTimestamp = sortedDatapoints.firstKey();
		Entry<Long, Double> prevDP = sortedDatapoints.firstEntry();
		for(Entry<Long, Double> currDP:sortedDatapoints.entrySet()) {
			while(currDP.getKey() > (prevTimestamp+intervalInMilli)) {
				Long missingTimestamp = prevTimestamp+intervalInMilli;
				Double missingValue= getInterpolatedvalue(prevDP, currDP, missingTimestamp);
				result.put(missingTimestamp, missingValue);
				prevTimestamp = missingTimestamp;
			}
			result.put(currDP.getKey(), currDP.getValue());
			prevDP=currDP;
			prevTimestamp=currDP.getKey();
		}
		return result;
	}

	private TreeMap<Long, Double> removeNegativeValues(TreeMap<Long, Double> datapoints){
		TreeMap<Long, Double> result = new TreeMap<>();
		for(Entry<Long, Double> entry:datapoints.entrySet()) {
			if(entry.getValue()>=0) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

	private TreeMap<Long, Double> performDifferentiation(TreeMap<Long, Double> sortedDatapoints, boolean interpolateFirstDP, long intervalInMilli) {
		TreeMap<Long, Double> result = new TreeMap<>();
		Double prev = null, curr=null;
		for (Entry<Long, Double> entry : sortedDatapoints.entrySet()) {
			curr = entry.getValue();
			if (prev !=null){
				result.put(entry.getKey(), curr - prev);
			}
			prev = curr;
		}
		//Add first DP using interpolation
		if(interpolateFirstDP) {
			//If there is no counter reset during first two DPs, Use first two DPs for interpolation 
			if(sortedDatapoints.firstEntry().getValue()>=0 && sortedDatapoints.higherEntry(sortedDatapoints.firstKey()).getValue()>=sortedDatapoints.firstEntry().getValue()) {
				double previousDPValue = getInterpolatedvalue(sortedDatapoints.firstEntry(), sortedDatapoints.higherEntry(sortedDatapoints.firstKey()), sortedDatapoints.firstKey()-intervalInMilli);
				result.put(sortedDatapoints.firstKey(), sortedDatapoints.firstEntry().getValue()-previousDPValue);
			}
		}
		return result;
	}

	private void addFirstNLastDatapointsIfMissing(TreeMap<Long,Double> sortedDatapoints, long startTimestampInMilli, long endTimestampInMilli, long intervalInMilli) {
		if(sortedDatapoints.size()>=2) {
			if(sortedDatapoints.firstKey() >= (startTimestampInMilli + intervalInMilli)) {
				double firstDPValue= getInterpolatedvalue(sortedDatapoints.firstEntry(), sortedDatapoints.higherEntry(sortedDatapoints.firstKey()), startTimestampInMilli);
				sortedDatapoints.put(startTimestampInMilli, firstDPValue);
			}
			if(endTimestampInMilli >= (sortedDatapoints.lastKey()+intervalInMilli)) {
				double lastDPValue= getInterpolatedvalue(sortedDatapoints.lowerEntry(sortedDatapoints.lastKey()), sortedDatapoints.lastEntry(), endTimestampInMilli);
				sortedDatapoints.put(endTimestampInMilli, lastDPValue);
			}
		}
	}

	private double getInterpolatedvalue(Entry<Long, Double> prevDP, Entry<Long, Double> nextDP, long timestamp){
		double slope = (nextDP.getValue()-prevDP.getValue())/(nextDP.getKey()-prevDP.getKey());
		double result = prevDP.getValue() + slope*(timestamp-prevDP.getKey()); 
		return result;
	}

	private static long _getTimeIntervalInMilliSeconds(String interval) {
		MetricReader.TimeUnit timeunit = null;
		try {
			timeunit = MetricReader.TimeUnit.fromString(interval.substring(interval.length() - 1));
			long timeDigits = Long.parseLong(interval.substring(0, interval.length() - 1));
			return timeDigits * timeunit.getValue();
		} catch (Exception t) {
			throw new SystemException("Please input a valid time interval!");
		}
	}

}
