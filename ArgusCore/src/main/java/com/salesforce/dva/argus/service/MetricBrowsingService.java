package com.salesforce.dva.argus.service;

import java.util.List;

/*
 * Provides methods to browse metrics from in memory/cached data structure
 */
public interface MetricBrowsingService {
	/**
	 * Builds a new trie based data structure by reading data from schema service
	 */
	public void buildSchemaTrie();
	
	/**
	 * Returns list of next level nodes
	 * @param prefix String whose descendants to be returned
	 * @return List of descendants
	 */
	public List<String> getNextLevelNodes(String prefix);
}
