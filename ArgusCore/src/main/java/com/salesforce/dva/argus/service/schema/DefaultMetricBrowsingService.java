/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * 3. Neither the name of Salesforce.com nor the names of its contributors may
 * be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.dva.argus.service.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.salesforce.dva.argus.entity.MetricSchemaRecord;
import com.salesforce.dva.argus.entity.MetricSchemaRecordQuery;
import com.salesforce.dva.argus.service.DefaultService;
import com.salesforce.dva.argus.service.MetricBrowsingService;
import com.salesforce.dva.argus.service.SchemaService;
import com.salesforce.dva.argus.service.SchemaService.RecordType;
import com.salesforce.dva.argus.system.SystemConfiguration;
/**
 * Implementation of metric browsing service
 * @author Raj Sarkapally (rsarkapally@salesforce.com)
 *
 */
@Singleton
public class DefaultMetricBrowsingService extends DefaultService implements MetricBrowsingService{
	private final Logger _logger = LoggerFactory.getLogger(DefaultMetricBrowsingService.class);
	private static Set<Character> DEFAULT_DELIMITERS;
	private static Character DEFAULT_DELIMITER='.';
	private final SchemaService schemaService;
	SchemaTrie trieRoot;

	class SchemaTrie {
		Character value;
		private HashMap<Character, SchemaTrie> children;

		public SchemaTrie(Character value){
			this.value=value;
			children=new HashMap<>();
		}

		public Character getValue() {
			return value;
		}

		public void setValue(Character value) {
			this.value = value;
		}

		public HashMap<Character, SchemaTrie> getChildren() {
			return children;
		}

		public void setChildren(HashMap<Character, SchemaTrie> children) {
			this.children = children;
		}
	}


	@Inject
	public DefaultMetricBrowsingService(SystemConfiguration systemConfiguration, SchemaService schemaService) {
		super(systemConfiguration);
		this.schemaService=schemaService;
		trieRoot=new SchemaTrie(DEFAULT_DELIMITER);
		DEFAULT_DELIMITERS=new HashSet<>(Arrays.asList(DEFAULT_DELIMITER));
	}

	public void buildSchemaTrie(){

		Thread schemaThread = new Thread(new Runnable() {
			public void run() {
				long startTime=System.nanoTime();
				_logger.info("Schema trie buiding started");
				List<Character> allowedChars=_getAllowedCharacters();

				for(Character ch:allowedChars){
					if(Thread.currentThread().isInterrupted()){
						break;
					}
					try{
						List<MetricSchemaRecord> schemaRecords=schemaService.getUnique(new MetricSchemaRecordQuery("*", ch+"*", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("scope"), null);
						List<String> scopes=_getUniqueScopes(schemaRecords);
						_logger.info("Scopes starting with '" + ch + "' are " + scopes.size());
						//TODO delete the following line
						int i=0;
						for(String scope:scopes){
							_logger.info("processing scope " + scope + " -- " + i++);
							_insertWord(scope);
							_insertAllMetricsGivenScope(scope);
						}
					}catch(Throwable th){
						_logger.error("Error occured while populating scope starting with " + ch + "Reason:" + th.getMessage()); 
					}

				}
				_logger.info("Schema trie buiding completed in " + ((System.nanoTime()-startTime)/1000000000) + " seconds");

			}
		});

		schemaThread.start();

	}

	//TODO delete this method as getUnique() should return unique values
	private List<String> _getUniqueScopes(List<MetricSchemaRecord> list){
		Set<String> result = new HashSet<>();
		for(MetricSchemaRecord record:list){
			result.add(record.getScope());
		}

		List<String> retValue = new ArrayList<>(result);
		Collections.sort(retValue);
		return retValue;
	}

	public void _insertWord(String s){
		_insertWord(s, DEFAULT_DELIMITERS);  
	}

	public void _insertWord(SchemaTrie currTrieNode, String s, Set<Character> delimiters){
		if(s!=null && s.length()>=0){
			SchemaTrie nextTrieNode;
			int currIndex=0;
			Character currCharacter; 
			while(currIndex < s.length()){
				currCharacter=s.charAt(currIndex++);
				nextTrieNode=currTrieNode.getChildren().get(currCharacter);
				if(nextTrieNode ==null){
					nextTrieNode=new SchemaTrie(currCharacter);
					currTrieNode.getChildren().put(currCharacter, nextTrieNode);
				}else{
					nextTrieNode=currTrieNode.getChildren().get(currCharacter);
				}
				currTrieNode=nextTrieNode;
			}
			if(!delimiters.contains(s.charAt(s.length()-1))){
				nextTrieNode=new SchemaTrie(DEFAULT_DELIMITER);
				currTrieNode.getChildren().put(DEFAULT_DELIMITER, nextTrieNode);
			}
		}
	}

	public void _insertWord(String s, Set<Character> delimiters){
		_insertWord(trieRoot, s, delimiters); 
	}

	public List<String> getNextLevelNodes(String prefix){
		Set<String> result;
		List<String> returnData=new ArrayList<>();

		if(prefix==null || prefix.length()==0 || (prefix.length()==1 && DEFAULT_DELIMITER.equals(prefix.charAt(0)))){
			return _getRootNodesFirstCharacters();
		}else{
			result= _getDescendants(prefix,DEFAULT_DELIMITERS);
			returnData.addAll(result);
			Collections.sort(returnData);
		}

		return returnData;
	}

	private List<String> _getRootNodesFirstCharacters(){
		List<String> result=new ArrayList<>();
		List<Character> allPossibleNodes=_getAllowedCharacters();
		allPossibleNodes.forEach(item->{
			if(trieRoot.getChildren().keySet().contains(item)){
				result.add(String.valueOf(item));
			}
		});
		return result;
	}

	public Set<String> _getDescendants(String prefix, Set<Character> delimiters){
		SchemaTrie trieNode = _findTrieNode(prefix);
		if(trieNode==null){
			return new HashSet<>();
		}
		String lastWord=_getLastWord(prefix, delimiters);
		return _getFirstLevelDescendants(trieNode, lastWord, delimiters);
	}

	private Set<String> _getFirstLevelDescendants(SchemaTrie trieNode, String partialResult, Set<Character> delimiters){
		Set<String> result=new HashSet<>();
		for(Character child:trieNode.getChildren().keySet()){
			if(delimiters.contains(child)){
				result.add(partialResult);
			}else{
				result.addAll(_getFirstLevelDescendants(trieNode.getChildren().get(child), partialResult+child,delimiters));
			}
		}
		return result;
	}
	/**
	 * Returns the SchemaNode for a last character of given string	
	 * @param s Given input string
	 * @return The schemaNode object for a last character
	 */
	private SchemaTrie _findTrieNode(String s){
		if(s==null || s.length()==0 || (s.length()==1 && s.charAt(0)==DEFAULT_DELIMITER)){
			return trieRoot;
		}
		SchemaTrie result=trieRoot;
		int index=0;
		while(result!=null && index<s.length()){
			result=result.getChildren().get(s.charAt(index++));
		}
		return result;
	}


	/**
	 * 
	 * @param s Given input string
	 * @param delimiter Delimiter is a character used to separate the words in a given string
	 * @return The last word of a given string wrt delimiter
	 */
	private String _getLastWord(String s, Set<Character> delimiters){

		StringBuilder result = new StringBuilder();
		int index=s.length()-1;
		while(index>=0 && !delimiters.contains(s.charAt(index))){
			result.append(s.charAt(index--));
		}
		return result.reverse().toString();
	}

	private List<Character> _getAllowedCharacters(){
		List<Character> result= new ArrayList<>();

		for(Character ch='a';ch<='z';ch++){
			result.add(ch);
		}

		for(Character ch='A';ch<='Z';ch++){
			result.add(ch);
		}

		for(Character ch='0';ch<='9';ch++){
			result.add(ch);
		}

		result.addAll(Arrays.asList('-','_','.', '/'));
		return result;
	}

	private void _insertAllMetricsGivenScope(String scope){
		SchemaTrie rootForMetrics=_findTrieNode(scope+DEFAULT_DELIMITER);
		Set<String> metrics=_getAllMetricsByScope(scope);
		for(String metric:metrics){
			_insertWord(rootForMetrics, metric, DEFAULT_DELIMITERS); 
		}

	}

	private Set<String> _getAllMetricsByScope(String scope){
		Set<String> result=new HashSet<>();

		List<MetricSchemaRecord> list= schemaService.getUnique(new MetricSchemaRecordQuery("*", scope, "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null);

		for(MetricSchemaRecord record:list){
			result.add(record.getMetric());
		}

		return result;
	}

}
