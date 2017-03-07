package ru.atc.camel.zabbix.metrics;

/**
 * Created by vgoryachev on 03.03.2017.
 * Package: ru.atc.camel.zabbix.metrics.
 */
public class ZabbixActionException extends Exception {
    public ZabbixActionException() {
        super();
    }

    public ZabbixActionException(String message) {
        super(message);
    }

    public ZabbixActionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZabbixActionException(Throwable cause) {
        super(cause);
    }
}
