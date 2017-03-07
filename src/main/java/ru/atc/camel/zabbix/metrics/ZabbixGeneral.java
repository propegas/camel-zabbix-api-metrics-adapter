package ru.atc.camel.zabbix.metrics;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.jms.JmsConsumer;
import org.apache.camel.component.jms.JmsMessage;
import org.apache.camel.impl.DefaultConsumer;
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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static ru.atc.adapters.message.CamelMessageManager.genAndSendErrorMessage;

/**
 * Created by vgoryachev on 02.03.2017.
 * Package: ru.atc.camel.zabbix.metrics.
 */
public final class ZabbixGeneral {

    public static final String VMWARE_IPADDRESS_CUSTOM_ITEM_KEY_FORMAT = "vmwareIpaddressItemSet.pl[\"{HOST.HOST}\", \"vmware.ipaddress\", \"%s\"]";
    public static final String VMWARE_IPADDRESS_CUSTOM_ITEM_NAME = "Collect custom vmware.ipaddress (by adapter)";
    private static final int CONNECT_TIMEOUT = 10000;
    private static final int SOCKET_TIMEOUT = 120000;
    private static final int MAX_CONN_PER_ROUTE = 40;
    private static final int MAX_CONN_TOTAL = 40;
    private static final int CONNECTION_TIME_TO_LIVE = 120;
    private static final Logger logger = LoggerFactory.getLogger("mainLogger");
    private static final Logger loggerError = LoggerFactory.getLogger("errorLogger");
    private static DefaultZabbixApi zabbixApi;

    private ZabbixGeneral() {
        throw new IllegalAccessError("Utility class");
    }

    public static DefaultZabbixApi getZabbixApi(DefaultConsumer defaultConsumer, String zabbixapiurl,
                                                String username, String password, String adapterName) {
        HttpClient httpClient2 = HttpClients.custom()
                .setConnectionTimeToLive(CONNECTION_TIME_TO_LIVE, TimeUnit.SECONDS)
                .setMaxConnTotal(MAX_CONN_TOTAL).setMaxConnPerRoute(MAX_CONN_PER_ROUTE)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setSocketTimeout(SOCKET_TIMEOUT).setConnectTimeout(CONNECT_TIMEOUT).build())
                .setRetryHandler(new DefaultHttpRequestRetryHandler(2, true))
                //.setConnectionManager(manager)
                .setConnectionManagerShared(true)
                .build();

        zabbixApi = null;
        try {

            zabbixApi = new DefaultZabbixApi(zabbixapiurl, (CloseableHttpClient) httpClient2);
            zabbixApi.init();

            boolean login = zabbixApi.login(username, password);
            if (!login) {
                /*if (defaultConsumer != null)
                    genErrorMessage(defaultConsumer, "Failed to login to Zabbix API", adapterName);*/
                //throw new RuntimeException("Failed to login to Zabbix API.");
                //if (zabbixApi != null) {
                zabbixApi.destroy();
                //}
                return null;
            }
        } catch (Exception e) {
            if (defaultConsumer != null)
                genErrorMessage(defaultConsumer, "Error while get Metrics from API", e, adapterName);
            return null;
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug(" **** Close zabbixApi Client: {}",
                        zabbixApi != null ? zabbixApi.toString() : null);
            }

            if (zabbixApi != null) {
                zabbixApi.destroy();
            }

            // send HTTP-request to correlation API for refresh context
            //processExchangeRefreshApi();

        }
        return zabbixApi;
    }

    private static void genErrorMessage(DefaultConsumer defaultConsumer, String message, Exception exception, String adapterName) {
        genAndSendErrorMessage(defaultConsumer, message, exception, adapterName);
    }

    private static void genErrorMessage(DefaultConsumer defaultConsumer, String message, String adapterName) {
        genAndSendErrorMessage(defaultConsumer, message, new RuntimeException("No additional exception's text."), adapterName);
    }

    public static boolean zabbixActionProceeded(DefaultZabbixApi zabbixApi, ZabbixAction zabbixAction, Properties prop) throws ZabbixActionException {

        if ("item".equalsIgnoreCase(zabbixAction.getObjectType())) {
            if ("update".equalsIgnoreCase(zabbixAction.getAction())) {
                try {
                    return updateZabbixItem(zabbixAction);
                } catch (ZabbixActionException e) {
                    throw new ZabbixActionException("Processing zabbix action failed.", e);
                }
            }
        }
        return false;
    }

    private static boolean updateZabbixItem(ZabbixAction zabbixAction) throws ZabbixActionException {
        try {
            if (zabbixAction.getParameter("type").equals(String.valueOf(100))) {
                return updateIpAddress(zabbixAction);
            }

            return updateZabbixItemStatusById(zabbixAction.getObjectId(),
                    zabbixAction.getParameter("enabled"));
        } catch (Exception e) {
            throw new ZabbixActionException("Updating zabbix item failed.", e);
        }

    }

    private static boolean updateZabbixItemStatusById(String itemId, String enabled) throws ZabbixActionException {
        Request getRequest;
        JSONObject getResponse;
        int enabledValue = 1;
        if ("true".equals(enabled))
            enabledValue = 0;
        try {
            getRequest = RequestBuilder.newBuilder().method("item.update")
                    .paramEntry("status", enabledValue)
                    .paramEntry("itemid", itemId)
                    .build();

        } catch (Exception ex) {
            //genErrorMessage("Failed create JSON request for update Item's status .", ex);
            throw new ZabbixActionException("Failed create JSON request for update Item's status.", ex);
        }

        JSONObject items;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            items = getResponse.getJSONObject("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            //genErrorMessage("Failed create JSON response for get Items by Key on Host.", e);
            throw new ZabbixActionException("Failed get JSON response result for update Item's status.", e);
        }

        JSONArray itemids = items.getJSONArray("itemids");
        if (itemids.isEmpty())
            return false;

        if (!itemids.get(0).equals(itemId))
            return false;

        logger.info("Updated Item with ID = {}", itemId);

        return true;

    }

    private static boolean updateIpAddress(ZabbixAction zabbixAction) throws ZabbixActionException {
        String ipAddress = zabbixAction.getParameter("newValue");
        String enabledString = zabbixAction.getParameter("enabled");
        int enabledValue = 1;
        if ("true".equals(enabledString))
            enabledValue = 0;

        String itemId = zabbixAction.getObjectId();
        String hostId = getHostIdByItemId(itemId);
        String checkKey = "vmwareIpaddressItemSet.pl";
        String createKey = String.format(VMWARE_IPADDRESS_CUSTOM_ITEM_KEY_FORMAT, ipAddress);
        String createName = VMWARE_IPADDRESS_CUSTOM_ITEM_NAME;

        JSONArray items = getItemsByKeyAndHostId(checkKey, hostId);
        /*if (items.isEmpty())
            return false;*/

        List<String> customItemIds = new ArrayList<>();
        String discoveredDefaultItem = "";
        JSONObject basedItem = null;
        for (int i = 0; i < items.size(); i++) {
            try {
                // if custom (not discovered) item exists and enabled
                Object item = items.getJSONObject(i).get("itemDiscovery");
                String itemidFromResponse = items.getJSONObject(i).get("itemid").toString();
                if (item instanceof JSONArray && ((JSONArray) item).isEmpty()) {
                    customItemIds.add(itemidFromResponse);
                } else {
                    basedItem = items.getJSONObject(i);
                    discoveredDefaultItem = itemidFromResponse;
                }
            } catch (Exception e) {
                // заглушка
            }
        }

        String[] customItems = customItemIds.toArray(new String[0]);

        if (enabledValue == 0) {
            updateZabbixItemStatusById(discoveredDefaultItem, "false");
            for (String customItem : customItems) {
                deleteZabbixItemById(customItem);
                //updateZabbixItemStatusById(customItem, "false");
            }
            //todo реализовать метод удаления Item
            //deleteItemByIds(customItems);
            addCustomItem(basedItem, hostId, createKey, createName);
            return true;
        }

        if (enabledValue == 1) {
            updateZabbixItemStatusById(discoveredDefaultItem, "true");
            for (String customItem : customItems) {
                //updateZabbixItemStatusById(customItem, "false");
                deleteZabbixItemById(customItem);
            }
            //todo реализовать метод удаления Item
            //deleteItemByIds(customItems);
            //addCustomItem(hostId, createKey);
            return true;
        }

        //getItemsByKeyAndHostId("", "");
        return false;
    }

    private static void deleteZabbixItemById(String customItemId) throws ZabbixActionException {

        Request getRequest;
        JSONObject getResponse;
        try {
            getRequest = RequestBuilder.newBuilder()
                    .method("item.delete")
                    .paramEntry(new String[]{customItemId})
                    .build();
        } catch (Exception ex) {
            //genErrorMessage("Failed create JSON request for get Items by Key on Host.", ex);
            throw new ZabbixActionException("Failed create JSON request for Deleting Items by Key on Host.", ex);
        }

        JSONObject items;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            items = getResponse.getJSONObject("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            //genErrorMessage("Failed create JSON response for get Items by Key on Host.", e);
            throw new ZabbixActionException("Failed get JSON response result for Deleting Items by Key on Host.", e);
        }

        try {
            JSONArray itemids = items.getJSONArray("itemids");
            if (itemids.isEmpty())
                throw new ZabbixActionException("Failed deleting Items by Key on Host. Empty Response.");

            logger.info("Deleted Item with ID = {}", itemids.get(0));
            //hostIdsFromResponse.get(0).get
        } catch (Exception e) {
            throw new ZabbixActionException("Failed deleting Items by Key on Host. Empty Response.");
        }
    }

    private static void addCustomItem(JSONObject basedItem, String hostId, String createKey, String createName) throws ZabbixActionException {
        Request getRequest;
        JSONObject getResponse;
        try {

            getRequest = RequestBuilder.newBuilder().method("item.create")
                    //.paramEntry("output", new String[]{"name", "itemid", "key_"})
                    .paramEntry("name", createName)
                    .paramEntry("key_", createKey)
                    .paramEntry("hostid", hostId)
                    .paramEntry("type", basedItem.get("type"))
                    .paramEntry("value_type", basedItem.get("value_type"))
                    .paramEntry("interfaceid", basedItem.get("interfaceid"))
                    .paramEntry("history", basedItem.get("history"))
                    .paramEntry("delay", basedItem.get("delay"))
                    .build();

        } catch (Exception ex) {
            //genErrorMessage("Failed create JSON request for get Items by Key on Host.", ex);
            throw new ZabbixActionException("Failed create JSON request for creating Items by Key on Host.", ex);
        }
        JSONObject items;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            items = getResponse.getJSONObject("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            //genErrorMessage("Failed create JSON response for get Items by Key on Host.", e);
            throw new ZabbixActionException("Failed get JSON response result for creating Items by Key on Host.", e);
        }

        try {
            JSONArray itemids = items.getJSONArray("itemids");
            if (itemids.isEmpty())
                throw new ZabbixActionException("Failed creating Items by Key on Host. Empty Response.");

            logger.info("Created Item with ID = {}", itemids.get(0));
            //hostIdsFromResponse.get(0).get
        } catch (Exception e) {
            throw new ZabbixActionException("Failed creating Items by Key on Host. Empty Response.");
        }

        //return items;

    }

    private static JSONArray getItemsByKeyAndHostId(String checkKey, String hostId) throws ZabbixActionException {

        Request getRequest;
        JSONObject getResponse;
        try {
            JSONObject filter = new JSONObject();
            filter.put("key_", new String[]{checkKey});

            getRequest = RequestBuilder.newBuilder().method("item.get")
                    .paramEntry("search", filter)
                    .paramEntry("output", new String[]{"name", "itemid", "key_", "interfaceid", "value_type", "type", "delay", "history"})
                    .paramEntry("hostids", new String[]{hostId})
                    .paramEntry("selectItemDiscovery", new String[]{"key_"})
                    .build();

        } catch (Exception ex) {
            //genErrorMessage("Failed create JSON request for get Items by Key on Host.", ex);
            throw new ZabbixActionException("Failed create JSON request for get Items by Key on Host.", ex);
        }
        JSONArray items;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            items = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            //genErrorMessage("Failed create JSON response for get Items by Key on Host.", e);
            throw new ZabbixActionException("Failed get JSON response result for get Items by Key on Host.", e);
        }

        logger.info("Found Zabbix Items by Key count: {}", items.size());

        return items;
        //return null;
    }

    private static String getHostIdByItemId(String itemId) throws ZabbixActionException {
        Request getRequest;
        JSONObject getResponse;
        try {
            JSONObject filter = new JSONObject();

            getRequest = RequestBuilder.newBuilder().method("item.get")
                    .paramEntry("search", filter)
                    .paramEntry("output", new String[]{"name", "itemid", "key_", "hostid"})
                    .paramEntry("itemids", new String[]{itemId})
                    .paramEntry("selectItemDiscovery", new String[]{"key_"})
                    .build();

        } catch (Exception ex) {
            //genErrorMessage("Failed create JSON request for get Items by Key on Host.", ex);
            throw new ZabbixActionException("Failed create JSON request for get Items by ID.", ex);
        }
        JSONArray items;
        try {
            getResponse = zabbixApi.call(getRequest);
            logger.debug("****** Finded Zabbix getRequest: {}", getRequest);

            items = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: {}", getResponse);

        } catch (Exception e) {
            //genErrorMessage("Failed create JSON response for get Items by Key on Host.", e);
            throw new ZabbixActionException("Failed get JSON response result for get Items by ID.", e);
        }

        logger.info("Found Zabbix Items by Key count: {}", items.size());

        if (items.isEmpty())
            return null;

        String hostid = items.getJSONObject(0).get("hostid").toString();
        return hostid;
        //return items;
        //return null;
    }

    public static void processZabbixAction(Exchange exchange) throws ZabbixActionException, JMSException, IOException {
        logger.info("Begin exchange from ActiveMQ...");
        //logger.info("Test 2");

        CamelContext context = exchange.getContext();
        Route route = context.getRoute("route2");
        Session session = null;
        JmsConsumer consumer;
        consumer = (JmsConsumer) route.getConsumer();
        //try {
        if (!(exchange.getIn() instanceof JmsMessage)) {
            return;
        }
        JmsMessage in = (JmsMessage) exchange.getIn();

        session = in.getJmsSession();

        //CamelContext context = exchange.getContext();
        //Route route = context.getRoute("route2");
        //consumer = (JmsConsumer) route.getConsumer();

        //JmsMessage in = (JmsMessage) exchange.getIn();
        //session = in.getJmsSession();
        Message jmsMessage1 = in.getJmsMessage();
        TextMessage jmsMessage = (TextMessage) jmsMessage1;
        String text = jmsMessage.getText();

        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        ZabbixAction zabbixAction = mapper.readValue(text, ZabbixAction.class);

        boolean b = false;
        try {
            b = !isZabbixActionProceeded(zabbixAction);
            if (b) {
                logger.info("Repeat");
                //genErrorMessage(consumer, "Failed to proceeded Zabbix API.", Main.getProp().getProperty("adaptername"));
                session.recover();
                throw new ZabbixActionException("Failed to proceeded Zabbix API.");
            } else {
                // ackknowledge
                logger.info("Done");
                jmsMessage.acknowledge();
            }
        } catch (ZabbixActionException e) {
            loggerError.error("Error: ", e);
            logger.info("Repeat");
            //genErrorMessage(consumer, "Failed to proceeded Zabbix API.", e, Main.getProp().getProperty("adaptername"));
            //try {
            if (session != null) {
                session.recover();
                throw new ZabbixActionException("Failed to proceeded Zabbix API.");
            }
            /*} catch (JMSException ex) {
                loggerError.error("Error: ", ex);
            }*/
        }

        /*} catch (Exception ex) {
            loggerError.error("Error: ", ex);
            logger.info("Repeat");
            genErrorMessage(consumer, "Failed to proceeded Zabbix API.", ex, Main.getProp().getProperty("adaptername"));
            try {
                if (session != null)
                    session.recover();
            } catch (JMSException e) {
                loggerError.error("Error: ", e);
            }
        }*/
    }

    public static boolean isZabbixActionProceeded(ZabbixAction zabbixAction) throws ZabbixActionException {
        String zabbixapiurl = Main.getProp().getProperty("zabbixapiurl");
        String username = Main.getProp().getProperty("username");
        String password = Main.getProp().getProperty("password");
        String adapterName = Main.getProp().getProperty("adaptername");
        zabbixApi = getZabbixApi(null, zabbixapiurl, username, password, adapterName);
        if (zabbixApi != null) {
            return zabbixActionProceeded(zabbixApi, zabbixAction, Main.getProp());
        } else {
            return false;
        }
    }
}
