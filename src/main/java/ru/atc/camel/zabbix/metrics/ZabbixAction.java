package ru.atc.camel.zabbix.metrics;

import java.util.List;

/**
 * Created by vgoryachev on 01.03.2017.
 * Package: ru.atc.camel.zabbix.metrics.
 */
public class ZabbixAction {
    private String action;
    private String objectType;
    private String objectId;
    private String objectName;
    private List<ObjectParameter> parameters;
    private String source;

    public ZabbixAction() {
        //default
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getObjectType() {
        return objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public List<ObjectParameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<ObjectParameter> parameters) {
        this.parameters = parameters;
    }

    public String getParameter(String parameterName) {
        for (ObjectParameter parameter : parameters) {
            if (parameter.getParameterName().equals(parameterName))
                return parameter.getParameterValue();
        }
        return null;
    }
}
