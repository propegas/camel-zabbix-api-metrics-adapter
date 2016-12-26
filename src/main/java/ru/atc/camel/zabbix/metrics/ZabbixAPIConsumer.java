package ru.atc.camel.zabbix.metrics;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.github.hengyunabc.zabbix.api.DefaultZabbixApi;
import io.github.hengyunabc.zabbix.api.Request;
import io.github.hengyunabc.zabbix.api.RequestBuilder;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        logger.info("Try to get Zabbix Metrics from " + eventsuri);

        HttpClient httpClient2 = HttpClients.custom()
                .setConnectionTimeToLive(CONNECTION_TIME_TO_LIVE, TimeUnit.SECONDS)
                .setMaxConnTotal(MAX_CONN_TOTAL).setMaxConnPerRoute(MAX_CONN_PER_ROUTE)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setSocketTimeout(SOCKET_TIMEOUT).setConnectTimeout(CONNECT_TIMEOUT).build())
                .setRetryHandler(new DefaultHttpRequestRetryHandler(5, true))
                .build();

        DefaultZabbixApi zabbixApi = null;
        try {
            String zabbixapiurl = endpoint.getConfiguration().getZabbixapiurl();
            String username = endpoint.getConfiguration().getUsername();
            String password = endpoint.getConfiguration().getPassword();

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

            logger.info("Sended Metrics: " + listFinal.size());

            logger.info("Try get Metrics Value Mappings...");
            String fullSql = getValueMappings(zabbixApi);
            if (fullSql != null) {
                processExchangeValueMappings(fullSql);
            }

        } catch (Exception e) {
            genErrorMessage("Error while get Metrics from API", e);
            return 0;
        } finally {
            logger.debug(String.format(" **** Close zabbixApi Client: %s",
                    zabbixApi != null ? zabbixApi.toString() : null));

            if (zabbixApi != null) {
                zabbixApi.destory();
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

        try {
            logger.info("Wait 1 minute before processing refresh...");
            TimeUnit.MINUTES.sleep(1);
            logger.info("Create Exchange container for Refresh metrics in correlation API...");
            getProcessor().process(exchange);
            if (exchange.getException() != null) {
                genErrorMessage("Ошибка при выполнении HTTP-запроса", exchange.getException());
            }
        } catch (Exception e) {
            genErrorMessage("Ошибка при выполнении HTTP-запроса", e);
        }
    }

    private void processExchangeValueMappings(String fullSql) {
        logger.info("Create Exchange container for Metrics Value Mappings...");
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
            logger.debug("Create Exchange container for entry: " + aListFinal.toString());
            Exchange exchange = getEndpoint().createExchange();

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

            getRequest = RequestBuilder.newBuilder().method("valuemapping.get")
                    .paramEntry("output", "extend")
                    .paramEntry("with_mappings", true)
                    .build();

        } catch (Exception ex) {
            genErrorMessage("Failed create JSON request for get all Mappings.", ex);
            throw new RuntimeException("Failed create JSON request for get all Mappings.", ex);
        }

        JSONArray mappings;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: " + getRequest);

            mappings = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: " + getResponse);

        } catch (Exception e) {
            genErrorMessage("Failed create JSON response for get all Mappings.", e);
            logger.error("Failed get JSON response result for all Mappings.", e);
            return null;
        }

        String fullSql = "";
        String sql = "";
        if (mappings != null) {
            logger.info("Finded Zabbix Value Mappings count: " + mappings.size());

            for (int i = 0; i < mappings.size(); i++) {
                JSONObject mapping = mappings.getJSONObject(i);
                JSONArray valueMappings = mapping.getJSONArray("mappings");
                String name = mapping.getString("name");
                int valuemapid = mapping.getIntValue("valuemapid");
                for (int j = 0; j < valueMappings.size(); j++) {
                    JSONObject valueMapping = valueMappings.getJSONObject(j);
                    int mappingid = valueMapping.getIntValue("mappingid");
                    int value = valueMapping.getIntValue("value");
                    String newvalue = valueMapping.getString("newvalue");
                    sql = sql + String.format(" ('%d', '%s', '%d', '%d', '%s' ),",
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

            logger.debug("**** Value Mapping Insert fullSql: " + fullSql);
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
            logger.debug("****** Finded Zabbix getRequest: " + getRequest);

            items = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: " + getResponse);

        } catch (Exception e) {
            genErrorMessage("Failed create JSON response for get all Web Items.", e);
            throw new RuntimeException("Failed get JSON response result for all Web Items.", e);
        }
        List<Map<String, Object>> deviceList = new ArrayList<>();

        List<Map<String, Object>> listFinal = new ArrayList<>();

        logger.info("Finded Zabbix Web Items count: " + items.size());

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
                    .paramEntry("output", new String[]{"hostid", "name", "itemid", "description", "key_", "value_type", "type", "lastclock", "units", "valuemapid"})
                    .paramEntry("monitored", true)
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
            logger.debug("****** Finded Zabbix getRequest: " + getRequest);

            items = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: " + getResponse);

        } catch (Exception e) {
            genErrorMessage("Failed create JSON response for get all CI Items.", e);
            throw new RuntimeException("Failed get JSON response result for all CI Items.", e);
        }
        List<Map<String, Object>> deviceList = new ArrayList<>();
        List<Map<String, Object>> listFinal = new ArrayList<>();

        logger.info("Finded Zabbix General Items count: " + items.size());

        for (int i = 0; i < items.size(); i++) {

            JSONObject item = items.getJSONObject(i);
            String hostname = item.getJSONArray("hosts").getJSONObject(0).getString("host");
            Integer itemid = Integer.parseInt(item.getString("itemid"));
            Integer valueType = Integer.parseInt(item.getString("value_type"));
            String hostid = item.getString("hostid");
            String name = item.getString("name");
            String key = item.getString("key_");
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

            deviceList.add(answer);

        }

        listFinal.addAll(deviceList);

        return listFinal;

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

            logger.debug("*** Finded Zabbix Web Item key with Pattern: " + key);
            // save as ne CI name
            keyparams = matcher.group(2);

            // get scenario and step from key params
            String[] params;
            params = keyparams.split(",");
            logger.debug(String.format("*** Finded Zabbix Web Item key params (size): %d ", params.length));

            if (params.length > 1) {
                webscenario = params[0];
                webstep = params[1];
            } else if (params.length == 1) {
                webscenario = params[0];
            }

            logger.debug(String.format("*** Finded Zabbix Web Item key params: %s:%s ", webscenario, webstep));

        }

        String[] webelements = new String[]{"", ""};
        webelements[0] = webscenario;
        webelements[1] = webstep;

        logger.debug("New Zabbix Web Item Scenario: " + webelements[0]);
        logger.debug("New Zabbix Web Item Step: " + webelements[1]);

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