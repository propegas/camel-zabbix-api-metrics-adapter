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
import ru.atc.adapters.type.Event;

import javax.jms.ConnectionFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static ru.atc.adapters.message.CamelMessageManager.genHeartbeatMessage;

public final class Main {

    private static final Logger logger = LoggerFactory.getLogger("mainLogger");
    private static final Logger loggerError = LoggerFactory.getLogger("errorLogger");
    private static String activemqPort;
    private static String activemqIp;
    private static String sqlIp;
    private static String sqlPort;
    private static String sqlDatabase;
    private static String sqlUser;
    private static String sqlPassword;
    private static String usejms;
    private static String adaptername;

    private Main() {

    }

    public static void main(String[] args) throws Exception {

        logger.info("Starting Custom Apache Camel component example");
        logger.info("Press CTRL+C to terminate the JVM");

        // get Properties from file
        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream("zabbixapi.properties");

            // load a properties file
            prop.load(input);

            // get the property value and print it out

            sqlIp = prop.getProperty("sql_ip");
            sqlPort = prop.getProperty("sql_port");
            sqlDatabase = prop.getProperty("sql_database");
            sqlUser = prop.getProperty("sql_user");
            sqlPassword = prop.getProperty("sql_password");
            usejms = prop.getProperty("usejms");
            activemqIp = prop.getProperty("activemq.ip");
            activemqPort = prop.getProperty("activemq.port");
            adaptername = prop.getProperty("adaptername");

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

        logger.info("activemqIp: " + activemqIp);
        logger.info("activemqPort: " + activemqPort);
        logger.info("sqlIp: " + sqlIp);
        logger.info("sqlPort: " + sqlPort);

        org.apache.camel.main.Main main = new org.apache.camel.main.Main();
        main.addRouteBuilder(new IntegrationRoute());
        main.run();
    }

    private static BasicDataSource setupDataSource() {

        String url = String.format("jdbc:postgresql://%s:%s/%s",
                sqlIp, sqlPort, sqlDatabase);

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername(sqlUser);
        ds.setPassword(sqlPassword);
        ds.setUrl(url);

        return ds;
    }

    private static class IntegrationRoute extends RouteBuilder {

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

            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://" + activemqIp + ":" + activemqPort);
            getContext().addComponent("activemq", JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));

            SqlComponent sql = new SqlComponent();
            BasicDataSource ds = setupDataSource();
            sql.setDataSource(ds);
            getContext().addComponent("sql", sql);

            JdbcComponent jdbc = new JdbcComponent();
            jdbc.setDataSource(ds);
            getContext().addComponent("jdbc", jdbc);

            // If access to the original message is not needed,
            // then its recommended to turn this option off as it may improve performance.
            getContext().setAllowUseOriginalMessage(false);

            // Heartbeats
            if ("true".equals(usejms)) {
                from("timer://foo?period={{heartbeatsdelay}}")
                        .process(new Processor() {
                            public void process(Exchange exchange) throws Exception {
                                genHeartbeatMessage(exchange, adaptername);
                            }
                        })
                        .marshal(myJson)
                        .to("activemq:{{heartbeatsqueue}}")
                        .log("*** Heartbeat: ${id}");
            }

            // get metrics and ci
            from(new StringBuilder()
                    .append("zabbixapi://metrics?")
                    .append("delay={{delay}}&")
                    .append("zabbixapiurl={{zabbixapiurl}}&")
                    .append("username={{username}}&")
                    .append("password={{password}}&")
                    .append("adaptername={{adaptername}}&")
                    .append("itemCiPattern={{zabbix_item_ke_pattern}}&")
                    .append("itemCiSearchPattern={{zabbix_item_ke_search_pattern}}&")
                    .append("itemCiParentPattern={{zabbix_item_ci_parent_pattern}}&")
                    .append("itemCiTypePattern={{zabbix_item_ci_type_pattern}}&")
                    .append("source={{source}}&")
                    .append("usejms={{usejms}}&")
                    .append("zabbixItemDescriptionPattern={{zabbixItemDescriptionPattern}}")
                    .toString())

                    .choice()

                    .when(header("queueName").isEqualTo("Metrics"))
                    .to("sql:{{sql.insertMetric}}")
                    .log(LoggingLevel.DEBUG, logger, "**** Inserted new metric ${body[itemid]}")

                    .when(header("queueName").isEqualTo("Mappings"))
                    .to("sql:{{sql.deleteAllMetricMapping}}")
                    .to("jdbc:BasicDataSource")
                    .log(LoggingLevel.DEBUG, logger, "**** Inserted Metrics mapping ${body[itemid]}")

                    .when(header("queueName").isEqualTo("Refresh"))
                    .to("{{api.metrics.refresh}}")
                    .log(LoggingLevel.INFO, logger, "**** Send HTTP request to API for correlation context refresh ")

                    .otherwise()
                    .choice()

                    .when(constant(usejms).isEqualTo("true"))
                    .marshal(myJson)
                    .to("activemq:{{errorsqueue}}")
                    .log(LoggingLevel.INFO, logger, "Error: ${id} ${header.DeviceId}")
                    .log(LoggingLevel.ERROR, logger, "*** NEW ERROR BODY: ${in.body}")

                    .endChoice()
                    .endChoice()
                    .end()

                    .log(LoggingLevel.DEBUG, logger, "${id} ${header.DeviceId} ${header.DeviceType} ");

        }
    }

}