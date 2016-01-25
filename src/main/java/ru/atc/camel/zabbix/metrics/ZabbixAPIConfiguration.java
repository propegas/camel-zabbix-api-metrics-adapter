package ru.atc.camel.zabbix.metrics;

//import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

@UriParams
public class ZabbixAPIConfiguration {	
    
	private String zabbixip;
	
	private String username;
	
	private String adaptername;
	
	private String password;
	
	private String source;
	
	private String zabbixapiurl;
	
	private String zabbix_item_description_pattern; 
	
	private String zabbix_item_ke_pattern;
	
	private String test;
	
	private int lastid = 0;
	
    private int delay = 720;
  
	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public int getLastid() {
		return lastid;
	}

	public void setLastid(int lastid) {
		this.lastid = lastid;
	}

	public String getZabbixip() {
		return zabbixip;
	}

	public void setZabbixip(String zabbixip) {
		this.zabbixip = zabbixip;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getAdaptername() {
		return adaptername;
	}

	public void setAdaptername(String adaptername) {
		this.adaptername = adaptername;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getZabbixapiurl() {
		return zabbixapiurl;
	}

	public void setZabbixapiurl(String zabbixapiurl) {
		this.zabbixapiurl = zabbixapiurl;
	}

	public String getZabbix_item_description_pattern() {
		return zabbix_item_description_pattern;
	}

	public void setZabbix_item_description_pattern(String zabbix_item_description_pattern) {
		this.zabbix_item_description_pattern = zabbix_item_description_pattern;
	}

	public String getTest() {
		return test;
	}

	public void setTest(String test) {
		this.test = test;
	}

	public String getZabbix_item_ke_pattern() {
		return zabbix_item_ke_pattern;
	}

	public void setZabbix_item_ke_pattern(String zabbix_item_ke_pattern) {
		this.zabbix_item_ke_pattern = zabbix_item_ke_pattern;
	}


}