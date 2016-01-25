package ru.atc.camel.zabbix.metrics;

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;

public class ZabbixAPIComponent extends UriEndpointComponent {

	public ZabbixAPIComponent() {
		super(ZabbixAPIEndpoint.class);
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		
		ZabbixAPIEndpoint endpoint = new ZabbixAPIEndpoint(uri, remaining, this);		
		ZabbixAPIConfiguration configuration = new ZabbixAPIConfiguration();
		
		// use the built-in setProperties method to clean the camel parameters map
		setProperties(configuration, parameters);
		
		endpoint.setConfiguration(configuration);		
		return endpoint;
	}
}