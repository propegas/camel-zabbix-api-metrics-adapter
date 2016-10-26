package ru.atc.camel.zabbix.metrics;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultPollingEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;

@UriEndpoint(scheme = "zabbixapi", title = "ZabbixAPI", syntax = "zabbixapi://operationPath", consumerOnly = true, consumerClass = ZabbixAPIConsumer.class, label = "zabbixapi")
public class ZabbixAPIEndpoint extends DefaultPollingEndpoint {

    private String operationPath;
    @UriParam
    private ZabbixAPIConfiguration configuration;

    public ZabbixAPIEndpoint(String uri, String operationPath, ZabbixAPIComponent component) {
        super(uri, component);
        this.operationPath = operationPath;
    }

    public Producer createProducer() throws Exception {
        throw new UnsupportedOperationException("ZabbixAPIProducer is not implemented");
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        ZabbixAPIConsumer consumer = new ZabbixAPIConsumer(this, processor);
        return consumer;
    }

    public boolean isSingleton() {
        return true;
    }

    public String getOperationPath() {
        return operationPath;
    }

    public void setOperationPath(String operationPath) {
        this.operationPath = operationPath;
    }

    public ZabbixAPIConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(ZabbixAPIConfiguration configuration) {
        this.configuration = configuration;
    }

}