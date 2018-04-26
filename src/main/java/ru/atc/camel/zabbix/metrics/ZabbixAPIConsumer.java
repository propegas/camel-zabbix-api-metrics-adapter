package ru.atc.camel.zabbix.metrics;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.atc.monitoring.zabbix.api.DefaultZabbixApi;
import ru.atc.monitoring.zabbix.api.Request;
import ru.atc.monitoring.zabbix.api.RequestBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.atc.adapters.message.CamelMessageManager.genAndSendErrorMessage;
import static ru.atc.adapters.message.CamelMessageManager.genHeartbeatMessage;
import static ru.atc.zabbix.general.CiItems.checkItemForCi;
import static ru.atc.zabbix.general.CiItems.getTransformedItemName;

public class ZabbixAPIConsumer extends ScheduledPollConsumer {

    private static final int CONNECT_TIMEOUT = 10000;
    private static final int SOCKET_TIMEOUT = 120000;
    private static final int MAX_CONN_PER_ROUTE = 40;
    private static final int MAX_CONN_TOTAL = 40;
    private static final int CONNECTION_TIME_TO_LIVE = 120;

    private static final Logger logger = LoggerFactory.getLogger("mainLogger");
    private static final Logger loggerErrors = LoggerFactory.getLogger("errorsLogger");

    private static ZabbixAPIEndpoint endpoint;
    private DefaultZabbixApi zabbixApi;

    public ZabbixAPIConsumer(ZabbixAPIEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        ZabbixAPIConsumer.endpoint = endpoint;

        this.setTimeUnit(TimeUnit.MINUTES);
        this.setInitialDelay(0);
        this.setDelay(endpoint.getConfiguration().getDelay());
    }

    @Override
    protected int poll() throws Exception {

        String operationPath = endpoint.getOperationPath();

        if ("metrics".equals(operationPath))
            return processSearchDevices();

        // only one operation implemented for now !
        throw new IllegalArgumentException("Incorrect operation: " + operationPath);
    }

    @Override
    public long beforePoll(long timeout) throws Exception {

        logger.info("*** Before Poll!!!");
        // send HEARTBEAT
        genHeartbeatMessage(getEndpoint().createExchange(), endpoint.getConfiguration().getAdaptername());

        return timeout;
    }

    private int processSearchDevices() {

        List<Map<String, Object>> itemsList;
        List<Map<String, Object>> webitemsList;

        List<Map<String, Object>> listFinal = new ArrayList<>();

        String eventsuri = endpoint.getConfiguration().getZabbixapiurl();
        logger.info("Try to get Zabbix Metrics from {}", eventsuri);

        zabbixApi = null;
        try {
            String zabbixapiurl = endpoint.getConfiguration().getZabbixapiurl();
            String username = endpoint.getConfiguration().getUsername();
            String password = endpoint.getConfiguration().getPassword();
            // String adapterName = endpoint.getConfiguration().getAdaptername();

            //zabbixApi = getZabbixApi(this, zabbixapiurl, username, password, adapterName);

            HttpClient httpClient2 = HttpClients.custom()
                    .setConnectionTimeToLive(CONNECTION_TIME_TO_LIVE, TimeUnit.SECONDS)
                    .setMaxConnTotal(MAX_CONN_TOTAL).setMaxConnPerRoute(MAX_CONN_PER_ROUTE)
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setSocketTimeout(SOCKET_TIMEOUT).setConnectTimeout(CONNECT_TIMEOUT).build())
                    .setRetryHandler(new DefaultHttpRequestRetryHandler(5, true))
                    .setConnectionManagerShared(true)
                    .build();

            zabbixApi = new DefaultZabbixApi(zabbixapiurl, (CloseableHttpClient) httpClient2);
            zabbixApi.init();

            boolean login = zabbixApi.login(username, password);
            if (!login) {
                genErrorMessage("Failed to login to Zabbix API");
                throw new RuntimeException("Failed to login to Zabbix API.");
            }

            // Get all Items marked as [FOR_INTEGRATION] from Zabbix
            logger.info("Try to get Items marked as [FOR_INTEGRATION]...");
            itemsList = getAllItems(zabbixApi);
            if (itemsList != null)
                listFinal.addAll(itemsList);

            // Get all Web-Items from Zabbix
            logger.info("Try to get Web-Items...");
            webitemsList = getAllWebItems(zabbixApi);
            if (itemsList != null)
                listFinal.addAll(webitemsList);

            processExchangeItems(listFinal);

            logger.info("Sended Metrics: {}", listFinal.size());

            logger.info("Try get Metrics Value Mappings...");
            String fullSql = getValueMappings(zabbixApi);
            if (fullSql != null) {
                processExchangeValueMappings(fullSql);
            }

        } catch (Exception e) {
            genErrorMessage("Error while get Metrics from API", e);
            return 0;
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug(" **** Close zabbixApi Client: {}",
                        zabbixApi != null ? zabbixApi.toString() : null);
            }

            if (zabbixApi != null) {
                zabbixApi.destroy();
            }

            // send HTTP-request to correlation API for refresh context
            processExchangeRefreshApi();

        }

        return 1;
    }

    private void processExchangeRefreshApi() {
        //logger.info("Create Exchange container for Refresh metrics in correlation API...");
        Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setHeader("queueName", "Refresh");
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setAction("updateMetrics");
        exchange.getIn().setBody(metricEvent, MetricEvent.class);
        exchange.getIn().setHeader("action", "updateMetrics");

        try {
            logger.info("Wait 1 minute before processing refresh...");
            TimeUnit.SECONDS.sleep(60);
            logger.info("Create Exchange container for Refresh metrics in correlation API...");
            getProcessor().process(exchange);
            if (exchange.getException() != null) {
                genErrorMessage("Ошибка при отправке JMS-запроса", exchange.getException());
            }
        } catch (Exception e) {
            genErrorMessage("Ошибка при отправке JMS-запроса", e);
        }
    }

    private void processExchangeValueMappings(String fullSql) {
        logger.info("Create Exchange container for Metrics Value Mappings...");
        logger.debug("Mappings SQL: {}", fullSql);
        Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setHeader("queueName", "Mappings");
        exchange.getIn().setBody(fullSql);

        logger.info("Sended All Metrics Value Mappings.");

        try {
            getProcessor().process(exchange);
            if (exchange.getException() != null) {
                genErrorMessage("Ошибка при передаче сообщения в базу данных ", exchange.getException());
            }
        } catch (Exception e) {
            genErrorMessage("Ошибка при передаче сообщения в базу данных ", e);
        }
    }

    private void processExchangeItems(List<Map<String, Object>> listFinal) {
        logger.info("Create Exchange containers for metrics...");
        for (Map<String, Object> aListFinal : listFinal) {
            if (logger.isDebugEnabled()) {
                logger.debug("Create Exchange container for entry: {}", aListFinal.toString());
            }
            Exchange exchange = getEndpoint().createExchange();

            if (aListFinal.get("enabled") == null)
                aListFinal.put("enabled", true);
            exchange.getIn().setBody(aListFinal);
            exchange.getIn().setHeader("queueName", "Metrics");

            try {
                getProcessor().process(exchange);
                if (exchange.getException() != null) {
                    genErrorMessage("Ошибка при передаче сообщения в базу данных", exchange.getException());
                }
            } catch (Exception e) {
                genErrorMessage("Ошибка при передаче сообщения в базу данных", e);
            }

        }
    }

    private String getValueMappings(DefaultZabbixApi zabbixApi) {
        Request getRequest;
        JSONObject getResponse;

        try {

            getRequest = RequestBuilder.newBuilder().method("valuemap.get")
                    .paramEntry("output", "extend")
                    .paramEntry("selectMappings", "extend")
                    .build();

        } catch (Exception ex) {
            genErrorMessage("Failed create JSON request for get all Mappings.", ex);
            throw new RuntimeException("Failed create JSON request for get all Mappings.", ex);
        }

        JSONArray mappings;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            mappings = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            genErrorMessage("Failed create JSON response for get all Mappings.", e);
            logger.error("Failed get JSON response result for all Mappings.", e);
            return null;
        }

        String fullSql = "";
        String sql = "";
        if (mappings != null) {
            logger.info("Finded Zabbix Value Mappings count: {}", mappings.size());

            for (int i = 0; i < mappings.size(); i++) {
                JSONObject mapping = mappings.getJSONObject(i);
                JSONArray valueMappings = mapping.getJSONArray("mappings");
                String name = mapping.getString("name");
                int valuemapid = mapping.getIntValue("valuemapid");
                for (int j = 0; j < valueMappings.size(); j++) {
                    JSONObject valueMapping = valueMappings.getJSONObject(j);
                    int mappingid = valueMapping.getIntValue("value");
                    int value = valueMapping.getIntValue("value");
                    String newvalue1 = valueMapping.getString("newvalue");
                    String newvalue = StringEscapeUtils.escapeJson(newvalue1);
                    sql = String.format("%s ('%d', '%s', '%d', '%d', '%s' ),",
                            sql,
                            valuemapid,
                            name,
                            mappingid,
                            value,
                            newvalue
                    );
                }
            }

            String sqlPrefixPart = "delete from metrics_valuemap; " +
                    "insert into metrics_valuemap (valuemapid, valuemapname,mappingid , value, newvalue ) values ";
            fullSql = String.format("%s %s",
                    sqlPrefixPart,
                    sql.substring(0, sql.length() - 1));

            logger.debug("**** Value Mapping Insert fullSql: {}", fullSql);
        }

        return fullSql;
    }

    private List<Map<String, Object>> getAllWebItems(DefaultZabbixApi zabbixApi) {

        Request getRequest;
        JSONObject getResponse;

        try {
            JSONObject filter = new JSONObject();
            filter.put("type", new String[]{"9"});

            getRequest = RequestBuilder.newBuilder().method("item.get")
                    .paramEntry("filter", filter)
                    .paramEntry("output", new String[]{"hostid", "name", "itemid", "description", "key_", "value_type", "type", "lastclock", "units"})
                    .paramEntry("monitored", true)
                    .paramEntry("webitems", true)
                    .paramEntry("selectHosts", new String[]{"host"})
                    .build();

        } catch (Exception ex) {
            genErrorMessage("Failed create JSON request for get all Web Items.", ex);
            throw new RuntimeException("Failed create JSON request for get all Web Items.", ex);
        }
        JSONArray items;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            items = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            genErrorMessage("Failed create JSON response for get all Web Items.", e);
            throw new RuntimeException("Failed get JSON response result for all Web Items.", e);
        }
        List<Map<String, Object>> deviceList = new ArrayList<>();

        List<Map<String, Object>> listFinal = new ArrayList<>();

        logger.info("Finded Zabbix Web Items count: {}", items.size());

        for (int i = 0; i < items.size(); i++) {

            JSONObject item = items.getJSONObject(i);
            String hostname = item.getJSONArray("hosts").getJSONObject(0).getString("host");
            String hostid = item.getString("hostid");
            Integer itemid = Integer.parseInt(item.getString("itemid"));
            Integer valueType = Integer.parseInt(item.getString("value_type"));
            String name = item.getString("name");
            String key = item.getString("key_");
            String units = item.getString("units");
            Long timestamp = (long) Integer.parseInt(item.getString("lastclock"));
            String[] webelements = getElementsFromWebItem(key);

            // if item has $1-$9 macros (get it from key params)
            if (name.matches(".*\\$\\d+.*")) {
                name = getTransformedItemName(name, key);
            }

            /*
            hostreturn[0] = ciid;
            hostreturn[1] = newitemname;
            hostreturn[2] = devicetype;
            hostreturn[3] = parentid;
             */
            String[] externalid;
            try {
                externalid = checkItemForCi(name, hostid, hostname,
                        ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiPattern(),
                        ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiParentPattern(),
                        ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiTypePattern());
            } catch (Exception e) {
                genErrorMessage("Failed while checking Zabbix Item CI", e);
                throw new RuntimeException("Failed while checking Zabbix Item CI");
            }

            Map<String, Object> answer = new HashMap<>();
            answer.put("itemid", itemid);
            answer.put("itemname", name);
            answer.put("type", valueType);
            answer.put("key", key);
            answer.put("prototype_key", key);
            answer.put("webscenario", webelements[0]);
            answer.put("webstep", webelements[1]);
            answer.put("externalid", String.format("%s:%s",
                    ZabbixAPIConsumer.endpoint.getConfiguration().getSource(),
                    externalid[0]));
            answer.put("units", units);
            answer.put("source", ZabbixAPIConsumer.endpoint.getConfiguration().getSource());
            answer.put("lastpoll", timestamp);
            answer.put("valuemapid", null);

            deviceList.add(answer);

        }

        listFinal.addAll(deviceList);

        return listFinal;

    }

    private List<Map<String, Object>> getAllItems(DefaultZabbixApi zabbixApi) {
        Request getRequest;
        JSONObject getResponse;
        try {
            JSONObject filter = new JSONObject();
            filter.put("description", new String[]{endpoint.getConfiguration().getZabbixItemDescriptionPattern()});

            getRequest = RequestBuilder.newBuilder().method("item.get")
                    .paramEntry("search", filter)
                    .paramEntry("output", new String[]{"hostid", "name", "itemid", "description", "key_", "value_type", "type", "lastclock", "units", "valuemapid", "status"})
                    //.paramEntry("monitored", true)
                    .paramEntry("selectHosts", new String[]{"host", "name"})
                    .paramEntry("selectItemDiscovery", new String[]{"key_"})

                    .build();

        } catch (Exception ex) {
            genErrorMessage("Failed create JSON request for get all CI Items.", ex);
            throw new RuntimeException("Failed create JSON request for get all CI Items.", ex);
        }
        JSONArray items;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            items = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            genErrorMessage("Failed create JSON response for get all CI Items.", e);
            throw new RuntimeException("Failed get JSON response result for all CI Items.", e);
        }
        List<Map<String, Object>> deviceList = new ArrayList<>();
        List<Map<String, Object>> listFinal = new ArrayList<>();

        logger.info("Finded Zabbix General Items count: {}", items.size());

        Pattern itemDescriptionSearchItemPattern = Pattern.compile(".*" + ZabbixAPIConsumer.endpoint.getConfiguration()
                        .getZabbixItemDescriptionCheckItemPattern() + ".*",
                Pattern.MULTILINE | Pattern.DOTALL);
        Pattern itemDescriptionDefaultPattern = Pattern.compile(".*" + ZabbixAPIConsumer.endpoint.getConfiguration()
                        .getZabbixItemDescriptionDefaultPattern() + ".*",
                Pattern.MULTILINE | Pattern.DOTALL);

        for (int i = 0; i < items.size(); i++) {

            JSONObject item = items.getJSONObject(i);
            String hostname = item.getJSONArray("hosts").getJSONObject(0).getString("host");
            Integer itemid = Integer.parseInt(item.getString("itemid"));
            Integer valueType = Integer.parseInt(item.getString("value_type"));
            String hostid = item.getString("hostid");
            String name = item.getString("name");
            String key = item.getString("key_");
            String description = item.getString("description");
            String status = item.getString("status");
            String prototypeKey = key;

            try {
                if (!item.getJSONObject("itemDiscovery").isEmpty())
                    prototypeKey = item.getJSONObject("itemDiscovery").getString("key_");
            } catch (Exception e) {
                // заглушка
            }

            String units = item.getString("units");
            Integer valuemapid = Integer.parseInt(item.getString("valuemapid"));
            Long timestamp = (long) Integer.parseInt(item.getString("lastclock"));

            if (name.matches(".*\\$\\d+.*")) {
                name = getTransformedItemName(name, key);
            }

            /*
            hostreturn[0] = ciid;
            hostreturn[1] = newitemname;
            hostreturn[2] = devicetype;
            hostreturn[3] = parentid;
             */
            String[] externalid;
            try {
                externalid = checkItemForCi(name, hostid, hostname,
                        ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiPattern(),
                        ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiParentPattern(),
                        ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiTypePattern());
            } catch (Exception e) {
                genErrorMessage("Failed while checking Zabbix Item CI", e);
                throw new RuntimeException("Failed while checking Zabbix Item CI");
            }

            boolean enabled;
            enabled = "0".equals(status);

            Matcher matcher = itemDescriptionSearchItemPattern.matcher(description);
            //if(description.matches("(?m)(.*)\\[DEFAULT::([\\w]*)\\](.*)")) {
            if (matcher.matches()) {
                logger.debug("*** Found Zabbix Item with Search Item Pattern in Description: {}", description);
                String checkItemKey = matcher.group(1).toUpperCase();
                logger.debug("*** Check Item Key {} on host id {}", checkItemKey, hostid);
                boolean found;
                if (isItemKeyExistsOnHost(checkItemKey, hostid)) {
                    logger.debug("*** Custom Item Key {} found on host id {}.", checkItemKey, hostid);
                    found = true;
                } else
                    found = false;

                Matcher matcher2 = itemDescriptionDefaultPattern.matcher(description);
                if (matcher2.matches()) {
                    logger.debug("*** Found Zabbix Item with Default State Item Pattern in Description: {}", description);
                    String defaultValue = matcher2.group(1).toUpperCase();
                    boolean enabledItem = Boolean.parseBoolean(defaultValue);
                    logger.debug("*** Item Default metric state: {}", checkItemKey, defaultValue);
                    if (found)
                        enabled = enabledItem;
                    else enabled = !enabledItem;
                } else
                    enabled = false;
            }

            logger.info("*** Set Item metric id {} state: {}", itemid, enabled);

            Map<String, Object> answer = new HashMap<>();
            answer.put("itemid", itemid);
            answer.put("itemname", name);
            answer.put("type", valueType);
            answer.put("key", key);
            answer.put("prototype_key", prototypeKey);
            answer.put("webscenario", null);
            answer.put("webstep", null);
            answer.put("externalid", String.format("%s:%s",
                    ZabbixAPIConsumer.endpoint.getConfiguration().getSource(),
                    externalid[0]));
            answer.put("units", units);
            answer.put("valuemapid", valuemapid);
            answer.put("source", ZabbixAPIConsumer.endpoint.getConfiguration().getSource());
            answer.put("lastpoll", timestamp);
            answer.put("enabled", enabled);

            deviceList.add(answer);

        }

        listFinal.addAll(deviceList);

        return listFinal;

    }

    private boolean isItemKeyExistsOnHost(String checkItemKey, String hostid) {

        JSONArray items = getItemsByKeyAndHostId(checkItemKey, hostid);
        if (items.isEmpty())
            return false;

        for (int i = 0; i < items.size(); i++) {
            try {
                // if custom (not discovered) item exists and enabled
                Object item = items.getJSONObject(i).get("itemDiscovery");
                if (item instanceof JSONArray && ((JSONArray) item).isEmpty())
                    return true;
            } catch (Exception e) {
                // заглушка
            }
        }

        return false;
    }

    public JSONArray getItemsByKeyAndHostId(String checkItemKey, String hostid) {
        Request getRequest;
        JSONObject getResponse;
        try {
            JSONObject filter = new JSONObject();
            filter.put("key_", new String[]{checkItemKey});

            getRequest = RequestBuilder.newBuilder().method("item.get")
                    .paramEntry("search", filter)
                    .paramEntry("output", new String[]{"name", "itemid", "key_"})
                    .paramEntry("monitored", true)
                    .paramEntry("hostids", new String[]{hostid})
                    .paramEntry("selectItemDiscovery", new String[]{"key_"})
                    .build();

        } catch (Exception ex) {
            genErrorMessage("Failed create JSON request for get Items by Key on Host.", ex);
            throw new RuntimeException("Failed create JSON request for get Items by Key on Host.", ex);
        }
        JSONArray items;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            items = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            genErrorMessage("Failed create JSON response for get Items by Key on Host.", e);
            throw new RuntimeException("Failed get JSON response result for get Items by Key on Host.", e);
        }

        logger.info("Found Zabbix Items by Key count: {}", items.size());

        return items;
    }

    private String[] getElementsFromWebItem(String key) {

        Pattern p = Pattern.compile("(.*)\\[(.*)\\]");
        Matcher matcher = p.matcher(key);

        String keyparams;
        String webscenario = "";
        String webstep = "";

        // if Web Item has webscenario pattern
        // Example:
        // web.test.in[WebScenario,,bps]
        if (matcher.matches()) {

            logger.debug("*** Finded Zabbix Web Item key with Pattern: {}", key);
            // save as ne CI name
            keyparams = matcher.group(2);

            // get scenario and step from key params
            String[] params;
            params = keyparams.split(",");
            logger.debug("*** Finded Zabbix Web Item key params (size): {} ", params.length);

            if (params.length > 1) {
                webscenario = params[0];
                webstep = params[1];
            } else if (params.length == 1) {
                webscenario = params[0];
            }

            logger.debug("*** Finded Zabbix Web Item key params: {}:{} ", webscenario, webstep);

        }

        String[] webelements = new String[]{"", ""};
        webelements[0] = webscenario;
        webelements[1] = webstep;

        logger.debug("New Zabbix Web Item Scenario: {}", webelements[0]);
        logger.debug("New Zabbix Web Item Step: {}", webelements[1]);

        return webelements;
    }

    private void genErrorMessage(String message) {
        genAndSendErrorMessage(this, message, new RuntimeException("No additional exception's text."),
                endpoint.getConfiguration().getAdaptername());
    }

    private void genErrorMessage(String message, Exception exception) {
        genAndSendErrorMessage(this, message, exception,
                endpoint.getConfiguration().getAdaptername());
    }

}