package ru.atc.camel.zabbix.metrics;

import org.apache.camel.spi.UriParams;

@UriParams
public class ZabbixAPIConfiguration {

    private String zabbixip;

    private String username;

    private String adaptername;

    private String password;

    private String source;

    private String zabbixapiurl;

    private String zabbixItemDescriptionPattern;

    private String zabbixItemDescriptionAttributePattern;

    private String zabbixItemDescriptionCheckItemPattern;

    private String zabbixItemDescriptionDefaultPattern;

    private String itemCiPattern;

    private String itemCiSearchPattern;

    private String itemCiParentPattern;

    private String itemCiTypePattern;

    private String test;

    private boolean usejms;

    private int delay;

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
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

    public String getZabbixItemDescriptionPattern() {
        return zabbixItemDescriptionPattern;
    }

    public void setZabbixItemDescriptionPattern(String zabbixItemDescriptionPattern) {
        this.zabbixItemDescriptionPattern = zabbixItemDescriptionPattern;
    }

    public String getTest() {
        return test;
    }

    public void setTest(String test) {
        this.test = test;
    }

    public String getItemCiPattern() {
        return itemCiPattern;
    }

    public void setItemCiPattern(String itemCiPattern) {
        this.itemCiPattern = itemCiPattern;
    }

    public String getItemCiSearchPattern() {
        return itemCiSearchPattern;
    }

    public void setItemCiSearchPattern(String itemCiSearchPattern) {
        this.itemCiSearchPattern = itemCiSearchPattern;
    }

    public String getItemCiParentPattern() {
        return itemCiParentPattern;
    }

    public void setItemCiParentPattern(String itemCiParentPattern) {
        this.itemCiParentPattern = itemCiParentPattern;
    }

    public String getItemCiTypePattern() {
        return itemCiTypePattern;
    }

    public void setItemCiTypePattern(String itemCiTypePattern) {
        this.itemCiTypePattern = itemCiTypePattern;
    }

    public boolean isUsejms() {
        return usejms;
    }

    public void setUsejms(boolean usejms) {
        this.usejms = usejms;
    }

    public String getZabbixItemDescriptionAttributePattern() {
        return zabbixItemDescriptionAttributePattern;
    }

    public void setZabbixItemDescriptionAttributePattern(String zabbixItemDescriptionAttributePattern) {
        this.zabbixItemDescriptionAttributePattern = zabbixItemDescriptionAttributePattern;
    }

    public String getZabbixItemDescriptionDefaultPattern() {
        return zabbixItemDescriptionDefaultPattern;
    }

    public void setZabbixItemDescriptionDefaultPattern(String zabbixItemDescriptionDefaultPattern) {
        this.zabbixItemDescriptionDefaultPattern = zabbixItemDescriptionDefaultPattern;
    }

    public String getZabbixItemDescriptionCheckItemPattern() {
        return zabbixItemDescriptionCheckItemPattern;
    }

    public void setZabbixItemDescriptionCheckItemPattern(String zabbixItemDescriptionCheckItemPattern) {
        this.zabbixItemDescriptionCheckItemPattern = zabbixItemDescriptionCheckItemPattern;
    }
}