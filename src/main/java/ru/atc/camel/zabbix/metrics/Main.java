package ru.atc.camel.zabbix.metrics;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jdbc.JdbcComponent;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.component.sql.SqlComponent;
import org.apache.camel.model.dataformat.JsonDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.at_consulting.itsm.event.Event;

import javax.jms.ConnectionFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

//import java.io.File;
//import javax.sql.DataSource;
//import org.apache.camel.CamelContext;
//import org.apache.camel.Endpoint;
//import org.apache.camel.spring.SpringCamelContext;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
//import ru.at_consulting.itsm.device.Device;
//import org.apache.camel.processor.idempotent.FileIdempotentRepository;

public class Main {
	
	public static String activemq_port = null;
	public static String activemq_ip = null;
	public static String sql_ip = null;
	public static String sql_port = null;
	public static String sql_database = null;
	public static String sql_user = null;
	public static String sql_password = null;
	public static String usejms = null;
    public static BasicDataSource ds;
    private static Logger logger = LoggerFactory.getLogger(Main.class);

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
			System.out.println(prop.getProperty("sql_port"));
			System.out.println(prop.getProperty("sql_database"));
			System.out.println(prop.getProperty("sql_user"));
			System.out.println(prop.getProperty("sql_password"));
			
			sql_ip = prop.getProperty("sql_ip");
			sql_port = prop.getProperty("sql_port");
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
		
		if (activemq_port == null || Objects.equals(activemq_port, ""))
			activemq_port = "61616";
		if (activemq_ip == null || Objects.equals(activemq_ip, ""))
			activemq_ip = "172.20.19.195";
		if (sql_ip == null || Objects.equals(sql_ip, ""))
			sql_ip = "192.168.157.73";
		if (sql_port == null || Objects.equals(sql_port, ""))
			sql_port = "5432";
		if (sql_database == null || Objects.equals(sql_database, ""))
			sql_database = "monitoring";
		if (sql_user == null || Objects.equals(sql_user, ""))
			sql_user = "postgres";
		if (sql_password == null || Objects.equals(sql_password, ""))
			sql_password = "";
		
		logger.info("activemq_ip: " + activemq_ip);
		logger.info("activemq_port: " + activemq_port);
		logger.info("sql_ip: " + sql_ip);
		logger.info("sql_port: " + sql_port);
		
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
                ds = setupDataSource();
                sql.setDataSource(ds);
				getContext().addComponent("sql", sql);

				JdbcComponent jdbc = new JdbcComponent();
				jdbc.setDataSource(ds);
				getContext().addComponent("jdbc", jdbc);

                // If access to the original message is not needed,
                // then its recommended to turn this option off as it may improve performance.
                getContext().setAllowUseOriginalMessage(false);

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
						+ "itemCiPattern={{zabbix_item_ke_pattern}}&"
						+ "itemCiSearchPattern={{zabbix_item_ke_search_pattern}}&"
						+ "itemCiParentPattern={{zabbix_item_ci_parent_pattern}}&"
						+ "itemCiTypePattern={{zabbix_item_ci_type_pattern}}&"
		    			+ "source={{source}}&"
                        + "usejms={{usejms}}&"
                        + "zabbix_item_description_pattern={{zabbix_item_description_pattern}}&"
		    			+ "zabbixip={{zabbixip}}")

		    		.choice()
					.when(header("queueName").isEqualTo("Metrics"))
						//.to("sql:{{sql.insertMetric}}?dataSource=dataSource")
						.to("sql:{{sql.insertMetric}}")
						.log(LoggingLevel.DEBUG,"**** Inserted new metric ${body[itemid]}")
						//.log("*** Metric: ${id} ${header.DeviceId}")
						.when(header("queueName").isEqualTo("Mappings"))
						//.to("sql:{{sql.insertMetric}}?dataSource=dataSource")
						.to("sql:{{sql.deleteAllMetricMapping}}")
						.to("jdbc:BasicDataSource")
						.log(LoggingLevel.DEBUG, "**** Inserted Metrics mapping ${body[itemid]}")
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
				sql_ip, sql_port, sql_database);
		
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername( sql_user );
        ds.setPassword( sql_password );
        ds.setUrl(url);
              
        return ds;
    }
    
}