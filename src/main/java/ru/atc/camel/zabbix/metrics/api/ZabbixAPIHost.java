package ru.atc.camel.zabbix.metrics.api;

import java.util.ArrayList;

public class ZabbixAPIHost {

    private String hostid;
    private String host;
    private String name;
    private String status;

    private ArrayList<ZabbixAPIGroups> groups[];
    /*
    private String[] parentTemplates;
    private String[] macros;
    private String[] interfaces;
    */
    
	
	/*
	@Override
	public String toString() {
		return hostid + " " + name + " [" + host + "]";
	}
	*/


	public String getHostid() {
		return hostid;
	}

	public void setHostid(String hostid) {
		this.hostid = hostid;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public ArrayList<ZabbixAPIGroups>[] getGroups() {
		return groups;
	}

	public void setGroups(ArrayList<ZabbixAPIGroups> groups[]) {
		this.groups = groups;
	}




}