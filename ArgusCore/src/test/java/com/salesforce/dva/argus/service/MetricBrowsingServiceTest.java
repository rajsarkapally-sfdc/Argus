package com.salesforce.dva.argus.service;

import static org.junit.Assert.*;

import org.junit.Test;

import com.salesforce.dva.argus.AbstractTest;
import com.salesforce.dva.argus.entity.MetricSchemaRecord;
import com.salesforce.dva.argus.entity.MetricSchemaRecordQuery;
import com.salesforce.dva.argus.service.SchemaService.RecordType;
import com.salesforce.dva.argus.service.schema.DefaultMetricBrowsingService;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MetricBrowsingServiceTest extends AbstractTest{

	@Test
	public void testRootNodesShouldReturnAllFirstLevelNodes(){
		SchemaService schemaServiceMock=mock(SchemaService.class);
		MetricBrowsingService metricBrowsingService=new DefaultMetricBrowsingService(system.getConfiguration(), schemaServiceMock);
		
		List<MetricSchemaRecord> comScope=new ArrayList<>();
		comScope.add(new MetricSchemaRecord("com","dummymetric"));
		
		List<MetricSchemaRecord> funnelScope=new ArrayList<>();
		funnelScope.add(new MetricSchemaRecord("funnel","dummymetric"));
		
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "c*", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("scope"), null)).
		thenReturn(comScope);
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "f*", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("scope"), null)).
		thenReturn(funnelScope);
		
		List<MetricSchemaRecord> metrics=new ArrayList<>();
		
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "com", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null)).
		thenReturn(metrics);
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "funnel", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null)).
		thenReturn(metrics);
		
		List<String> expectedResult=Arrays.asList("com","funnel");
		metricBrowsingService.buildSchemaTrie();
		assertEquals(expectedResult,metricBrowsingService.getNextLevelNodes(null));
	}
	
	@Test
	public void testSecondLevelNodesShouldReturnAllMetricNamesUnderGivenScope(){
		SchemaService schemaServiceMock=mock(SchemaService.class);
		MetricBrowsingService metricBrowsingService=new DefaultMetricBrowsingService(system.getConfiguration(), schemaServiceMock);
		
		List<MetricSchemaRecord> comScope=new ArrayList<>();
		comScope.add(new MetricSchemaRecord("com","dummymetric"));
		
		List<MetricSchemaRecord> funnelScope=new ArrayList<>();
		funnelScope.add(new MetricSchemaRecord("funnel","dummymetric"));
		
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "c*", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("scope"), null)).
		thenReturn(comScope);
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "f*", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("scope"), null)).
		thenReturn(funnelScope);
		
		List<MetricSchemaRecord> comMetrics=new ArrayList<>();
		comMetrics.add(new MetricSchemaRecord("com","m1"));
		comMetrics.add(new MetricSchemaRecord("com","m2"));
		comMetrics.add(new MetricSchemaRecord("com","m3"));
		comMetrics.add(new MetricSchemaRecord("com","m4"));
		
		List<MetricSchemaRecord> funnelMetrics=new ArrayList<>();
		
		funnelMetrics.add(new MetricSchemaRecord("funnel","m5"));
		funnelMetrics.add(new MetricSchemaRecord("funnel","m6"));
		funnelMetrics.add(new MetricSchemaRecord("funnel","m7"));
		funnelMetrics.add(new MetricSchemaRecord("funnel","m8"));
		
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "com", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null)).
		thenReturn(comMetrics);
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "funnel", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null)).
		thenReturn(funnelMetrics);
		
		metricBrowsingService.buildSchemaTrie();
		
		List<String> expectedResultForCom=Arrays.asList("m1","m2","m3","m4");
		assertEquals(expectedResultForCom,metricBrowsingService.getNextLevelNodes("com."));

		List<String> expectedResultForFunnel=Arrays.asList("m5","m6","m7","m8");
		
		assertEquals(expectedResultForFunnel,metricBrowsingService.getNextLevelNodes("funnel."));
	}
	
	@Test
	public void testSameLevelNodesShouldReturnSameLevelNodesStartingWithGivenValue(){
		SchemaService schemaServiceMock=mock(SchemaService.class);
		MetricBrowsingService metricBrowsingService=new DefaultMetricBrowsingService(system.getConfiguration(), schemaServiceMock);
		
		List<MetricSchemaRecord> comScope=new ArrayList<>();
		comScope.add(new MetricSchemaRecord("com1","dummymetric"));
		comScope.add(new MetricSchemaRecord("com2","dummymetric"));
		comScope.add(new MetricSchemaRecord("com3","dummymetric"));
		
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "c*", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("scope"), null)).
		thenReturn(comScope);
			
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "com1", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null)).
		thenReturn(Arrays.asList(new MetricSchemaRecord("com1","m1")));
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "com2", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null)).
		thenReturn(Arrays.asList(new MetricSchemaRecord("com2","m2")));
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "com3", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null)).
		thenReturn(Arrays.asList(new MetricSchemaRecord("com3","m3")));
		
		List<String> expectedResult=Arrays.asList("com1","com2","com3");
		
		metricBrowsingService.buildSchemaTrie();
		assertEquals(expectedResult,metricBrowsingService.getNextLevelNodes("com"));
	}
	
	@Test
	public void testCompleteNameShouldReturnSameNode(){
		SchemaService schemaServiceMock=mock(SchemaService.class);
		MetricBrowsingService metricBrowsingService=new DefaultMetricBrowsingService(system.getConfiguration(), schemaServiceMock);
		
		List<MetricSchemaRecord> comScope=new ArrayList<>();
		comScope.add(new MetricSchemaRecord("com","dummymetric"));
		
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "c*", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("scope"), null)).
		thenReturn(comScope);
			
		when(schemaServiceMock.getUnique(new MetricSchemaRecordQuery("*", "com", "*", "*", "*"), Integer.MAX_VALUE, RecordType.fromName("metric"), null)).
		thenReturn(Arrays.asList(new MetricSchemaRecord("com","m1")));
		
		List<String> expectedResult=Arrays.asList("com");
		
		metricBrowsingService.buildSchemaTrie();
		assertEquals(expectedResult,metricBrowsingService.getNextLevelNodes("com"));
	}
}
