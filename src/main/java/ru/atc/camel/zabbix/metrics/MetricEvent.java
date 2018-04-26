package ru.atc.camel.zabbix.metrics;

import java.io.Serializable;

/**
 * Created by vgoryachev on 27.09.2017.
 * Package: ru.atc.monitoring.correlation.service.osystem.model.
 */
public class MetricEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String action;

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
