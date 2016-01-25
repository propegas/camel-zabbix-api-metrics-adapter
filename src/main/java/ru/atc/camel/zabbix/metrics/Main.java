package ru.atc.camel.zabbix.metrics;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

//import java.io.File;
import javax.jms.ConnectionFactory;

//import javax.sql.DataSource;

import org.apache.activemq.ActiveMQConnectionFactory;
//import org.apache.camel.CamelContext;
//import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.component.sql.SqlComponent;
import org.apache.camel.model.dataformat.JsonDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;
//import org.apache.camel.spring.SpringCamelContext;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;

//import ru.at_consulting.itsm.device.Device;
//import org.apache.camel.processor.idempotent.FileIdempotentRepository;
import ru.at_consulting.itsm.event.Event;

public class Main {
	
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	public static String activemq_port = null;
	public static String activemq_ip = null;
	public static String sql_ip = null;
	public static String sql_database = null;
	public static String sql_user = null;
	public static String sql_password = null;
	public static String usejms = null;
	public static void main(String[] args) throws Exception {
		
		logger.info("Starting Custom Apache Camel component example");
		logger.info("Press CTRL+C to terminate the JVM");
			
		/*
		if ( args.length == 6  ) {
			activemq_port = (String)args[1];
			activemq_ip = (String)args[0];
			sql_ip = (String)args[2];
			sql_database = (String)args[3];
			sql_user = (String)args[4];
			sql_password = (String)args[5];
		}
		*/
		
		// get Properties from file
		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("zabbixapi.properties");

			// load a properties file
			prop.load(input);

			// get the property value and print it out
			System.out.println(prop.getProperty("sql_ip"));
			System.out.println(prop.getProperty("sql_database"));
			System.out.println(prop.getProperty("sql_user"));
			System.out.println(prop.getProperty("sql_password"));
			
			sql_ip = prop.getProperty("sql_ip");
			sql_database = prop.getProperty("sql_database");
			sql_user = prop.getProperty("sql_user");
			sql_password = prop.getProperty("sql_password");
			usejms = prop.getProperty("usejms");
			activemq_ip = prop.getProperty("activemq_ip");
			activemq_port = prop.getProperty("activemq_port");

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		if (activemq_port == null || activemq_port == "" )
			activemq_port = "61616";
		if (activemq_ip == null || activemq_ip == "" )
			activemq_ip = "172.20.19.195";
		if (sql_ip == null || sql_ip == "" )
			sql_ip = "192.168.157.73";
		if (sql_database == null || sql_database == "" )
			sql_database = "monitoring";
		if (sql_user == null || sql_user == "" )
			sql_user = "postgres";
		if (sql_password == null || sql_password == "" )
			sql_password = "";
		
		logger.info("activemq_ip: " + activemq_ip);
		logger.info("activemq_port: " + activemq_port);
		
		org.apache.camel.main.Main main = new org.apache.camel.main.Main();
		main.enableHangupSupport();
		//main.addOption(option);
		
		main.addRouteBuilder(new RouteBuilder() {
			
			@Override
			public void configure() throws Exception {
			
				JsonDataFormat myJson = new JsonDataFormat();
				myJson.setPrettyPrint(true);
				myJson.setLibrary(JsonLibrary.Jackson);
				myJson.setJsonView(Event.class);
				//myJson.setPrettyPrint(true);
				
				PropertiesComponent properties = new PropertiesComponent();
				properties.setLocation("classpath:zabbixapi.properties");
				getContext().addComponent("properties", properties);

				ConnectionFactory connectionFactory = new ActiveMQConnectionFactory
						("tcp://" + activemq_ip + ":" + activemq_port);		
				getContext().addComponent("activemq", JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));
				
				
				//ApplicationContext appContext = new ClassPathXmlApplicationContext(
				//		"applicationContext.xml");
			//	CamelContext camelContext = SpringCamelContext.springCamelContext(
			//			appContext, false);
				//getContext().reg
				
				SqlComponent sql = new SqlComponent();
				BasicDataSource ds = setupDataSource();
				sql.setDataSource(ds);
				getContext().addComponent("sql", sql);
				
							
				// Heartbeats
				if (usejms.equals("true")){
					from("timer://foo?period={{heartbeatsdelay}}")
					//.choice()
			        .process(new Processor() {
						public void process(Exchange exchange) throws Exception {
							ZabbixAPIConsumer.genHeartbeatMessage(exchange);
						}
					})
					//.bean(WsdlNNMConsumer.class, "genHeartbeatMessage", exchange)
			        .marshal(myJson)
			        .to("activemq:{{heartbeatsqueue}}")
					.log("*** Heartbeat: ${id}");
				}
		        
				// get metrics and ci
				from("zabbixapi://metrics?"
		    			+ "delay={{delay}}&"
		    			+ "zabbixapiurl={{zabbixapiurl}}&"
		    			+ "username={{username}}&"
		    			+ "password={{password}}&"
		    			+ "adaptername={{adaptername}}&"
		    			+ "zabbix_item_ke_pattern={{zabbix_item_ke_pattern}}&"
		    			+ "source={{source}}&"
		    			+ "zabbix_item_description_pattern={{zabbix_item_description_pattern}}&"
		    			+ "zabbixip={{zabbixip}}")

		    		.choice()
					.when(header("queueName").isEqualTo("Metrics"))
						//.to("sql:{{sql.insertMetric}}?dataSource=dataSource")
						.to("sql:{{sql.insertMetric}}")
						.log(LoggingLevel.DEBUG,"**** Inserted new metric ${body[itemid]}")
						//.log("*** Metric: ${id} ${header.DeviceId}")
					.otherwise()
						.choice()
							.when(constant(usejms).isEqualTo("true"))
							.marshal(myJson)
							.to("activemq:{{eventsqueue}}")
							.log(LoggingLevel.ERROR, "*** Error: ${id} ${header.DeviceId}")
							.endChoice()
						.endChoice()
					.end()
					
		    		.log(LoggingLevel.DEBUG,"${id} ${header.DeviceId} ${header.DeviceType} ");
		    		//.to("activemq:{{devicesqueue}}");
				
				
				// get history for metrics
				/*
				from("zabbixapi://metricshistory?"
		    			+ "delay={{historydelay}}&"
		    			+ "zabbixapiurl={{zabbixapiurl}}&"
		    			+ "username={{username}}&"
		    			+ "password={{password}}&"
		    			+ "adaptername={{adaptername}}&"
		    			+ "source={{source}}&"
		    			+ "zabbix_item_description_pattern={{zabbix_item_description_pattern}}&"
		    			+ "zabbixip={{zabbixip}}")

		    		.choice()
					.when(header("queueName").isEqualTo("Metrics"))
						//.to("sql:{{sql.insertMetric}}?dataSource=dataSource")
						.to("sql:{{sql.insertMetricHistory}}")
						.log("**** Inserted new metric ${body[itemid]}")
						//.log("*** Metric: ${id} ${header.DeviceId}")
					.otherwise()
						.marshal(myJson)
						.to("activemq:{{eventsqueue}}")
						.log("*** Error: ${id} ${header.DeviceId}")
					.end()
					
		    		.log("${id} ${header.DeviceId} ${header.DeviceType} ");
		    		//.to("activemq:{{devicesqueue}}");
				
				*/
			}
		});
		
		main.run();
	}
	
	
	private static BasicDataSource setupDataSource() {
		
		String url = String.format("jdbc:postgresql://%s:%s/%s",
				sql_ip, "5432",	sql_database);
		
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername( sql_user );
        ds.setPassword( sql_password );
        ds.setUrl(url);
              
        return ds;
    }
    
}