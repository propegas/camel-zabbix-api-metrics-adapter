package ru.atc.camel.zabbix.metrics.api;

public class RESTNetworkAdvisorEvents {

    private String key;
    private String severity;
    private String description;
    private String sourceName;
    private String sourceAddr;
    private Long firstOccurrenceHostTime;
    private String origin;
    private String eventCategory;
    private String nodeWwn;
    private String module;
	
	
	@Override
	public String toString() {
		return key + " " + severity + " " + description;
	}


	public String getKey() {
		return key;
	}


	public void setKey(String key) {
		this.key = key;
	}


	public String getSeverity() {
		return severity;
	}


	public void setSeverity(String severity) {
		this.severity = severity;
	}


	public String getDescription() {
		return description;
	}


	public void setDescription(String description) {
		this.description = description;
	}


	public String getSourceAddr() {
		return sourceAddr;
	}


	public void setSourceAddr(String sourceAddr) {
		this.sourceAddr = sourceAddr;
	}


	public String getSourceName() {
		return sourceName;
	}


	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}


	public Long getFirstOccurrenceHostTime() {
		return firstOccurrenceHostTime;
	}


	public void setFirstOccurrenceHostTime(Long firstOccurrenceHostTime) {
		this.firstOccurrenceHostTime = firstOccurrenceHostTime;
	}


	public String getOrigin() {
		return origin;
	}


	public void setOrigin(String origin) {
		this.origin = origin;
	}


	public String getNodeWwn() {
		return nodeWwn;
	}


	public void setNodeWwn(String nodeWwn) {
		this.nodeWwn = nodeWwn;
	}


	public String getEventCategory() {
		return eventCategory;
	}


	public void setEventCategory(String eventCategory) {
		this.eventCategory = eventCategory;
	}


	public String getModule() {
		return module;
	}


	public void setModule(String module) {
		this.module = module;
	}
}