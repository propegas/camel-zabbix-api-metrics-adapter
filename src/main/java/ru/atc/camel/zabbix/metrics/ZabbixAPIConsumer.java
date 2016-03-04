package ru.atc.camel.zabbix.metrics;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.github.hengyunabc.zabbix.api.DefaultZabbixApi;
import io.github.hengyunabc.zabbix.api.Request;
import io.github.hengyunabc.zabbix.api.RequestBuilder;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.at_consulting.itsm.device.Device;
import ru.at_consulting.itsm.event.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.atc.zabbix.general.CiItems.checkItemForCi;
import static ru.atc.zabbix.general.CiItems.getTransformedItemName;

//import ru.atc.zabbix.general.Ci;

//import java.security.KeyManagementException;
//import java.security.KeyStore;
//import java.security.KeyStoreException;
//import javax.net.ssl.SSLContext;
//import org.apache.http.HttpVersion;
//import org.apache.http.client.CookieStore;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.HttpPut;
//import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
//import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
//import org.apache.http.impl.client.DefaultHttpClient;
//import org.apache.http.impl.client.HttpClientBuilder;
//import org.apache.http.impl.client.HttpClients;
//import org.apache.http.params.CoreProtocolPNames;
//import org.apache.http.ssl.SSLContextBuilder;
//import com.google.gson.JsonObject;
//import net.sf.ehcache.search.expression.And;
//import scala.xml.dtd.ParameterEntityDecl;

public class ZabbixAPIConsumer extends ScheduledPollConsumer {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	private static ZabbixAPIEndpoint endpoint;

	//private static String SavedWStoken;

    //private static CloseableHttpClient httpClient;

	public ZabbixAPIConsumer(ZabbixAPIEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		ZabbixAPIConsumer.endpoint = endpoint;
		// this.afterPoll();
		this.setTimeUnit(TimeUnit.MINUTES);
		this.setInitialDelay(0);
		this.setDelay(endpoint.getConfiguration().getDelay());
	}

    public static void genHeartbeatMessage(Exchange exchange) {
        // TODO Auto-generated method stub
        long timestamp = System.currentTimeMillis();
        timestamp = timestamp / 1000;
        // String textError = "Возникла ошибка при работе адаптера: ";
        Event genevent = new Event();
        genevent.setMessage("Сигнал HEARTBEAT от адаптера");
        genevent.setEventCategory("ADAPTER");
        genevent.setObject("HEARTBEAT");
        genevent.setSeverity(PersistentEventSeverity.OK.name());
        genevent.setTimestamp(timestamp);
        genevent.setEventsource(String.format("%s", endpoint.getConfiguration().getAdaptername()));

        logger.info(" **** Create Exchange for Heartbeat Message container");
        // Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);

        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Heartbeats");
        exchange.getIn().setHeader("Type", "Heartbeats");
        exchange.getIn().setHeader("Source", endpoint.getConfiguration().getAdaptername());

        try {
            // Processor processor = getProcessor();
            // .process(exchange);
            // processor.process(exchange);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
        }
    }


	@Override
	protected int poll() throws Exception {

		String operationPath = endpoint.getOperationPath();

		if (operationPath.equals("metrics"))
			return processSearchDevices();

		// only one operation implemented for now !
		throw new IllegalArgumentException("Incorrect operation: " + operationPath);
	}

	@Override
	public long beforePoll(long timeout) throws Exception {

		logger.info("*** Before Poll!!!");
		// only one operation implemented for now !
		// throw new IllegalArgumentException("Incorrect operation: ");

		// send HEARTBEAT
		genHeartbeatMessage(getEndpoint().createExchange());

		return timeout;
	}

    private int processSearchDevices() throws Exception {

        // Long timestamp;

        List<Map<String, Object>> itemsList;
        List<Map<String, Object>> webitemsList;

		List<Map<String, Object>> listFinal = new ArrayList<>();

		//List<Device> listFinal = new ArrayList<Device>();

        String eventsuri = endpoint.getConfiguration().getZabbixapiurl();
        String uri = String.format("%s", eventsuri);

        System.out.println("***************** URL: " + uri);

        logger.info("Try to get Metrics...");
        // logger.info("Get events URL: " + uri);

        //JsonObject json = null;

        DefaultZabbixApi zabbixApi = null;
        try {
            String zabbixapiurl = endpoint.getConfiguration().getZabbixapiurl();
            String username = endpoint.getConfiguration().getUsername();
            String password = endpoint.getConfiguration().getPassword();
            // String url = "http://192.168.90.102/zabbix/api_jsonrpc.php";
            zabbixApi = new DefaultZabbixApi(zabbixapiurl);
            zabbixApi.init();

            boolean login = zabbixApi.login(username, password);
            //System.err.println("login:" + login);
            if (!login) {

                throw new RuntimeException("Failed to login to Zabbix API.");
            }

            // Get all Items marked as [FOR_INTEGRATION] from Zabbix
            itemsList = getAllItems(zabbixApi);
            if (itemsList != null)
                listFinal.addAll(itemsList);

            // Get all Web-Items from Zabbix
            webitemsList = getAllWebItems(zabbixApi);
            if (itemsList != null)
                listFinal.addAll(webitemsList);

            logger.info("Create Exchange containers for metrics...");
            for (Map<String, Object> aListFinal : listFinal) {
                logger.debug("Create Exchange container for entry: " + aListFinal.toString());
                Exchange exchange = getEndpoint().createExchange();

                exchange.getIn().setBody(aListFinal);
                //exchange.getIn().setHeader("DeviceId", listFinal.get(i).getId());
                //exchange.getIn().setHeader("DeviceType", listFinal.get(i).getDeviceType());
                exchange.getIn().setHeader("queueName", "Metrics");


                try {
                    getProcessor().process(exchange);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }

            logger.info("Sended Metrics: " + listFinal.size());

            logger.info("Try get Metrics Value Mappings...");
            String fullSql = getValueMappings(zabbixApi);
            if (fullSql != null) {
                logger.info("Create Exchange container for Metrics Value Mappings...");
                Exchange exchange = getEndpoint().createExchange();
                exchange.getIn().setHeader("queueName", "Mappings");
                exchange.getIn().setBody(fullSql);

                logger.info("Sended All Metrics Value Mappings.");

                try {
                    getProcessor().process(exchange);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        } catch (NullPointerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error(String.format("NullPointerException while get Metrics from API: %s ", e));

            // close all DB connections
            //logger.info("Close all DB connections");
            //Main.ds.close();
            genErrorMessage(e.getMessage() + " " + e.toString());
            //httpClient.close();
            return 0;
        } catch (Error e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error(String.format("Error while get Metrics from API: %s ", e));

            // close all DB connections
            //logger.info("Close all DB connections");
            //Main.ds.close();
            genErrorMessage(e.getMessage() + " " + e.toString());
            //httpClient.close();
            if (zabbixApi != null) {
                zabbixApi.destory();
            }
            return 0;
        } catch (Throwable e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error(String.format("Throwable while get Metrics from API: %s ", e));

            // close all DB connections
            //logger.info("Close all DB connections");
            //Main.ds.close();
            genErrorMessage(e.getMessage() + " " + e.toString());
            //httpClient.close();
            if (zabbixApi != null) {
                zabbixApi.destory();
            }
            return 0;
        } finally {
            if (zabbixApi != null) {
                logger.debug(String.format(" **** Close zabbixApi Client: %s", zabbixApi.toString()));
            }
            // httpClient.close();
            if (zabbixApi != null) {
                zabbixApi.destory();
            }

            // close all DB connections
            //logger.info("Close all DB connections");
            //Main.ds.close();
            // dataSource.close();
            // return 0;
        }

        return 1;
    }

    private String getValueMappings(DefaultZabbixApi zabbixApi) {
        Request getRequest;
        JSONObject getResponse;
        // JsonObject params = new JsonObject();
        try {


            getRequest = RequestBuilder.newBuilder().method("valuemapping.get")
                    .paramEntry("output", "extend")
                    .paramEntry("with_mappings", true)
                    .build();

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException("Failed create JSON request for get all Mappings.");
        }

        JSONArray mappings;
        try {
            getResponse = zabbixApi.call(getRequest);
            //System.err.println(getRequest);
            logger.debug("****** Finded Zabbix getRequest: " + getRequest);

            mappings = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: " + getResponse);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error("Failed get JSON response result for all Mappings.");
            //throw new RuntimeException("Failed get JSON response result for all Mappings.");
            return null;
        }

        String fullSql;
        String sql = "";
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


        return fullSql;
    }

    private List<Map<String, Object>> getAllWebItems(DefaultZabbixApi zabbixApi) {
        // TODO Auto-generated method stub
		Request getRequest;
		JSONObject getResponse;
		// JsonObject params = new JsonObject();
		try {
			JSONObject filter = new JSONObject();
            filter.put("type", new String[]{"9"});

			getRequest = RequestBuilder.newBuilder().method("item.get")
                    .paramEntry("filter", filter)
                    .paramEntry("output", new String[]{"hostid", "name", "itemid", "description",
                            "key_", "value_type", "type", "lastclock", "units"})
                    .paramEntry("monitored", true)
                    .paramEntry("webitems", true)
                    .paramEntry("selectHosts", new String[] { "host" })
					.build();

		} catch (Exception ex) {
			ex.printStackTrace();
            throw new RuntimeException("Failed create JSON request for get all Web Items.");
        }
		JSONArray items;
		try {
			getResponse = zabbixApi.call(getRequest);
			//System.err.println(getRequest);
			logger.debug("****** Finded Zabbix getRequest: " + getRequest);

			items = getResponse.getJSONArray("result");
			logger.debug("****** Finded Zabbix getResponse: " + getResponse);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
            throw new RuntimeException("Failed get JSON response result for all Web Items.");
        }
		List<Map<String, Object>> deviceList = new ArrayList<>();

		List<Map<String, Object>> listFinal = new ArrayList<>();

		//List<Device> listFinal = new ArrayList<Device>();

        logger.info("Finded Zabbix Web Items count: " + items.size());

		for (int i = 0; i < items.size(); i++) {


			JSONObject item = items.getJSONObject(i);
			String hostname = item.getJSONArray("hosts").getJSONObject(0).getString("host");
            String hostid = item.getString("hostid");
            Integer itemid = Integer.parseInt(item.getString("itemid"));
			Integer value_type = Integer.parseInt(item.getString("value_type"));
			String name = item.getString("name");
			String key = item.getString("key_");
			String units = item.getString("units");
			Long timestamp = (long) Integer.parseInt(item.getString("lastclock"));
            String[] webelements = getElementsFromWebItem(key);

            // Matcher m = Pattern.compile("\\$\\d+").matcher(name);
            // if item has $1-$9 macros (get it from key params)
            if (name.matches(".*\\$\\d+.*")){
				name = getTransformedItemName(name, key);
			}

            /*
            hostreturn[0] = ciid;
            hostreturn[1] = newitemname;
            hostreturn[2] = devicetype;
            hostreturn[3] = parentid;
             */
            String externalid[] = checkItemForCi(name, hostid, hostname,
                    ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiPattern(),
                    ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiParentPattern(),
                    ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiTypePattern());

			Map<String, Object> answer = new HashMap<>();
			answer.put("itemid", itemid);
			answer.put("itemname", name);
			answer.put("type", value_type);
			answer.put("key", key);
            answer.put("webscenario", webelements[0]);
            answer.put("webstep", webelements[1]);
            answer.put("externalid", String.format("%s:%s",
                    ZabbixAPIConsumer.endpoint.getConfiguration().getSource(),
                    externalid[0]));
            answer.put("units", units);
            answer.put("source", ZabbixAPIConsumer.endpoint.getConfiguration().getSource());
            answer.put("lastpoll", timestamp);


			deviceList.add(answer);


        }

		listFinal.addAll(deviceList);

		return listFinal;

	}

    private List<Map<String, Object>> getAllItems(DefaultZabbixApi zabbixApi) {
        // TODO Auto-generated method stub
        Request getRequest;
        JSONObject getResponse;
        // JsonObject params = new JsonObject();
        try {
            JSONObject filter = new JSONObject();
            filter.put("description", new String[]{endpoint.getConfiguration().getZabbix_item_description_pattern()});

            getRequest = RequestBuilder.newBuilder().method("item.get")
                    .paramEntry("search", filter)
                    .paramEntry("output", new String[]{"hostid", "name", "itemid", "description",
                            "key_", "value_type", "type", "lastclock", "units", "valuemapid"})
                    .paramEntry("monitored", true)
                    .paramEntry("selectHosts", new String[]{"host", "name"})

                    .build();

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException("Failed create JSON request for get all Hosts.");
        }
        JSONArray items;
        try {
            getResponse = zabbixApi.call(getRequest);
            //System.err.println(getRequest);
            logger.debug("****** Finded Zabbix getRequest: " + getRequest);

            items = getResponse.getJSONArray("result");
            logger.debug("****** Finded Zabbix getResponse: " + getResponse);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new RuntimeException("Failed get JSON response result for all CI Items.");
        }
        List<Map<String, Object>> deviceList = new ArrayList<>();

        List<Map<String, Object>> listFinal = new ArrayList<>();

        //List<Device> listFinal = new ArrayList<Device>();

        logger.info("Finded Zabbix General Items count: " + items.size());

        for (int i = 0; i < items.size(); i++) {

            JSONObject item = items.getJSONObject(i);
            String hostname = item.getJSONArray("hosts").getJSONObject(0).getString("host");
            Integer itemid = Integer.parseInt(item.getString("itemid"));
            Integer value_type = Integer.parseInt(item.getString("value_type"));
            String hostid = item.getString("hostid");
            String name = item.getString("name");
            String key = item.getString("key_");
            String units = item.getString("units");
            Integer valuemapid = Integer.parseInt(item.getString("valuemapid"));
            Long timestamp = (long) Integer.parseInt(item.getString("lastclock"));


            //Matcher m = Pattern.compile("\\$\\d+").matcher(name);
            if (name.matches(".*\\$\\d+.*")) {
                name = getTransformedItemName(name, key);
            }

            /*
            hostreturn[0] = ciid;
            hostreturn[1] = newitemname;
            hostreturn[2] = devicetype;
            hostreturn[3] = parentid;
             */
            String externalid[] = checkItemForCi(name, hostid, hostname,
                    ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiPattern(),
                    ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiParentPattern(),
                    ZabbixAPIConsumer.endpoint.getConfiguration().getItemCiTypePattern());

            Map<String, Object> answer = new HashMap<>();
            answer.put("itemid", itemid);
            answer.put("itemname", name);
            answer.put("type", value_type);
            answer.put("key", key);
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

    /*
	private String getParentElements(String itemname, String hostid, String hostname) {
		
		String pattern = endpoint.getConfiguration().getZabbix_item_ke_pattern();
		
		// Example item as CI : 
		// [test CI item] bla-bla
		Pattern p = Pattern.compile(pattern);
		Matcher matcher = p.matcher(itemname);
		
		String ciid;
		//String hostnameend = "";
		
		// if Item has CI pattern
		if (matcher.matches()) {
			logger.debug("*** Finded Zabbix Item with Pattern as CI: " + itemname);
			// save as ne CI name
			itemname = matcher.group(1).toUpperCase();

		    // get SHA-1 hash for hostname-item block for saving as ciid
		    // Example:
		    // KRL-PHOBOSAU--PHOBOS:KRL-PHOBOSAU:TEST CI ITEM
			
			//String hostname = getHostnameByHostid(zabbixApi, hostid );
			
		    logger.debug(String.format("*** Trying to generate hash for Item with Pattern: %s:%s", hostname, itemname));
		    String hash = "";
			try {
				hash = hashString(String.format("%s:%s", hostname, itemname), "SHA-1");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    logger.debug("*** Generated Hash: " + hash );
		    ciid = hash;
			//ciid = itemid;
			
		}
		
		// use parent host as CI for item
		else {
			logger.debug("*** Use parent host as CI for item: " + itemname);
			ciid = hostid;
			
		}
		
		logger.debug("*** CIID: " + ciid );
		return ciid;
	}
*/

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
			}
			else if (params.length == 1) {
				webscenario = params[0];
			}

            logger.debug(String.format("*** Finded Zabbix Web Item key params: %s:%s ", webscenario, webstep));


		}
		// if Item has no CI pattern
		else {


        }

        String[] webelements = new String[] { "", ""} ;
		webelements[0] = webscenario;
		webelements[1] = webstep;
		//hostreturn[1] = hostnameend;

        logger.debug("New Zabbix Web Item Scenario: " + webelements[0]);
		logger.debug("New Zabbix Web Item Step: " + webelements[1]);

        return webelements;
	}

	private void genErrorMessage(String message) {
		// TODO Auto-generated method stub
        if (endpoint.getConfiguration().isUsejms()) {
            long timestamp = System.currentTimeMillis();
            timestamp = timestamp / 1000;
            String textError = "Возникла ошибка при работе адаптера: ";
            Event genevent = new Event();
            genevent.setMessage(textError + message);
            genevent.setEventCategory("ADAPTER");
            genevent.setSeverity(PersistentEventSeverity.CRITICAL.name());
            genevent.setTimestamp(timestamp);
            genevent.setEventsource(String.format("%s", endpoint.getConfiguration().getSource()));
            genevent.setStatus("OPEN");
            genevent.setHost("adapter");

            logger.info(" **** Create Exchange for Error Message container");
            Exchange exchange = getEndpoint().createExchange();
            exchange.getIn().setBody(genevent, Device.class);

            exchange.getIn().setHeader("EventIdAndStatus", "Error_" + timestamp);
            exchange.getIn().setHeader("Timestamp", timestamp);
            exchange.getIn().setHeader("queueName", "Events");
            exchange.getIn().setHeader("Type", "Error");

            try {
                getProcessor().process(exchange);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            logger.info(" **** No usejms=true property found. Error has not been sended.");
        }

	}

    public enum PersistentEventSeverity {
        OK, INFO, WARNING, MINOR, MAJOR, CRITICAL;

        public static PersistentEventSeverity fromValue(String v) {
            return valueOf(v);
        }

        public String value() {
            return name();
        }
    }

}