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
	 
package com.salesforce.dva.argus.ws.resources;

import com.salesforce.dva.argus.entity.MetricSchemaRecord;
import com.salesforce.dva.argus.service.DiscoveryService;
import com.salesforce.dva.argus.service.MetricBrowsingService;
import com.salesforce.dva.argus.service.SchemaService.RecordType;
import com.salesforce.dva.argus.ws.annotation.Description;
import com.salesforce.dva.argus.ws.dto.MetricDiscoveryQueryDto;
import com.salesforce.dva.argus.ws.dto.MetricDiscoveryResultDto;

import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

/**
 * Provides methods to discover resources.
 *
 * @author  Tom Valine (tvaline@salesforce.com)
 */
@Path("/discover")
@Description("Provides methods to discover resources.")
public class DiscoveryResources extends AbstractResource {

    //~ Instance fields ******************************************************************************************************************************

    private DiscoveryService _discoveryService = system.getServiceFactory().getDiscoveryService();
    private MetricBrowsingService _metricBrowsingService = system.getServiceFactory().getMetricBrowsingService();

    //~ Methods **************************************************************************************************************************************

    /**
     * Discover metric schema records. If type is specified, then records of that particular type are returned.
     *
     * @param   req             The HTTP request.
     * @param   namespaceRegex  The namespace filter.
     * @param   scopeRegex      The scope filter.
     * @param   metricRegex     The metric name filter.
     * @param   tagkRegex       The tag key filter.
     * @param   tagvRegex       The tag value filter.
     * @param   limit           The maximum number of records to return.
     * @param   page            The page of results to return
     * @param   type            The field for which to retrieve unique values.  If null, the entire schema record including all the fields is returned.
     *
     * @return  The filtered set of schema records or unique values if a specific field is requested.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/metrics/schemarecords")
    @Description("Discover metric schema records. If type is specified, then records of that particular type are returned.")
    public List<? extends Object> getRecordsByPage(@Context HttpServletRequest req,
    	@DefaultValue("*") @QueryParam("namespace") final String namespaceRegex,
        @QueryParam("scope") final String scopeRegex,
        @QueryParam("metric") final String metricRegex,
        @DefaultValue("*") @QueryParam("tagk") final String tagkRegex,
        @DefaultValue("*") @QueryParam("tagv") final String tagvRegex,
        @DefaultValue("10") @QueryParam("limit") final int limit,
        @DefaultValue("1") @QueryParam("page") final int page,
        @QueryParam("type") String type) {
        
        if (type == null) {
            List<MetricSchemaRecord> schemaRecords = _discoveryService.filterRecords(namespaceRegex, scopeRegex, metricRegex, tagkRegex, tagvRegex, page*limit,
                null);
            
            boolean format = req.getParameterMap().containsKey("format") && 
            		(req.getParameter("format") == null || Boolean.parseBoolean(req.getParameter("format")));
            if(format) {
            	List<String> records = new ArrayList<>(schemaRecords.size()); 
                _formatToString(schemaRecords, records);
                return _getSubList(records,limit*(page-1), records.size());
            }
            
            return _getSubList(schemaRecords,limit*(page-1),schemaRecords.size());
        } else {
            List<MetricSchemaRecord> records = _discoveryService.getUniqueRecords(namespaceRegex, scopeRegex, metricRegex, tagkRegex, tagvRegex,
                RecordType.fromName(type), page*limit, null);

            return _getValueForType(_getSubList(records, limit*(page-1), records.size()), RecordType.fromName(type)); 
        }
    }
    
    /**
     * Used for metrics browsing
     *
     * @param   req             The HTTP request.
     * @param   query            The field for which to retrieve the descendants
     *
     * @return  The filtered set of schema records or unique values if a specific field is requested.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/metrics/browsing")
    @Description("Returns the next level descendants")
    public List<String> getNextLevelDescendants(@Context HttpServletRequest req,@QueryParam("query") String query) {
        return _metricBrowsingService.getNextLevelNodes(query);
    }
    
    /**
     * Discover metric schema records. If type is specified, then records of that particular type are returned.
     *
     * @param   req             The HTTP request.
     * @param   MetricDiscoveryQueryDto This contains metric query parameters along with scanner starting schema record
     *
     * @return  The filtered set of schema records or unique values if a specific field is requested.
     */
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/metrics/schemarecords")
    @Description("Discover metric schema records. If type is specified, then records of that particular type are returned.")
    public MetricDiscoveryResultDto getRecordsByStartRow(@Context HttpServletRequest req,
    	MetricDiscoveryQueryDto metricDiscoveryQueryDto) {
        
        if (metricDiscoveryQueryDto.getType() == null) {
            List<MetricSchemaRecord> schemaRecords = _discoveryService.filterRecords(metricDiscoveryQueryDto.getNamespace(), metricDiscoveryQueryDto.getScope(), 
            		metricDiscoveryQueryDto.getMetric(), metricDiscoveryQueryDto.getTagKey(), metricDiscoveryQueryDto.getTagValue(), metricDiscoveryQueryDto.getLimit(),
            		metricDiscoveryQueryDto.getScanStartSchemaRecord());
            
            boolean format = req.getParameterMap().containsKey("format") && 
            		(req.getParameter("format") == null || Boolean.parseBoolean(req.getParameter("format")));
            if(format) {
            	List<String> records = new ArrayList<>(schemaRecords.size()); 
                _formatToString(schemaRecords, records);
                return  new MetricDiscoveryResultDto(records, schemaRecords.get(schemaRecords.size()-1));
            }
            
            return  new MetricDiscoveryResultDto(schemaRecords, schemaRecords.get(schemaRecords.size()-1));
        } else {
            List<MetricSchemaRecord> records = _discoveryService.getUniqueRecords(metricDiscoveryQueryDto.getNamespace(), metricDiscoveryQueryDto.getScope(), 
            		metricDiscoveryQueryDto.getMetric(), metricDiscoveryQueryDto.getTagKey(), metricDiscoveryQueryDto.getTagValue(),
                RecordType.fromName(metricDiscoveryQueryDto.getType()), metricDiscoveryQueryDto.getLimit(), metricDiscoveryQueryDto.getScanStartSchemaRecord());

            return  new MetricDiscoveryResultDto(_getValueForType(records, RecordType.fromName(metricDiscoveryQueryDto.getType())), records.get(records.size()-1));
            
        }
    }
    
    private static List<String> _getValueForType(List<MetricSchemaRecord> records, RecordType type) {
    	
    	List<String> result=new ArrayList<>();
    	
    	for(MetricSchemaRecord record:records){
        	result.add(_getValueForType(record, type));
        }
    	
    	return result;
    }
    

    private static String _getValueForType(MetricSchemaRecord record, RecordType type) {
        switch (type) {
            case NAMESPACE:
                return record.getNamespace();
            case SCOPE:
                return record.getScope();
            case METRIC:
                return record.getMetric();
            case TAGK:
                return record.getTagKey();
            case TAGV:
                return record.getTagValue();
            default:
                return null;
        }
    }
    
	private static void _formatToString(List<MetricSchemaRecord> schemaRecords, List<String> records) {
		
		for(MetricSchemaRecord msr : schemaRecords) {
			records.add(MetricSchemaRecord.print(msr));
		}
	}
	
	private static <T> List<T> _getSubList(List<T> list, int from, int to){
		
		if(list.size()<from){
			return new ArrayList<T>();
		}else{
			return list.subList(from, to);
		}
		
	}
}
/* Copyright (c) 2016, Salesforce.com, Inc.  All rights reserved. */
