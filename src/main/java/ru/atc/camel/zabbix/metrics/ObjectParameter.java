package ru.atc.camel.zabbix.metrics;

/**
 * Created by vgoryachev on 01.03.2017.
 * Package: ru.atc.camel.zabbix.metrics.
 */
public class ObjectParameter {
    private String parameterName;
    private String parameterValue;

    public ObjectParameter() {
        //default
    }

    public ObjectParameter(String parameterName, String parameterValue) {
        this.parameterName = parameterName;
        this.parameterValue = parameterValue;
    }

    public String getParameterName() {
        return parameterName;
    }

    public void setParameterName(String parameterName) {
        this.parameterName = parameterName;
    }

    public String getParameterValue() {
        return parameterValue;
    }

    public void setParameterValue(String parameterValue) {
        this.parameterValue = parameterValue;
    }
}
