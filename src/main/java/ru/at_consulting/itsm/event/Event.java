package ru.at_consulting.itsm.event;


import java.io.Serializable;
//import java.util.Collection;
import java.util.List;

//import org.apache.camel.Consume;

//import javax.persistence.Column;
//import javax.persistence.Entity;
//import javax.persistence.GeneratedValue;
//import javax.persistence.Id;
//import javax.persistence.Table;
//import javax.persistence.ElementCollection;

public class Event implements  Serializable {
	
	/**
     * 
      */
     private static final long serialVersionUID = 1L;

//   @Consume(uri="activemq:Events.pojo")
     
//     @Id
//     @GeneratedValue
     //@Column(name = "id")
     private Integer uuid;
     
     //@Column(name = "Date_reception")
     private Long timestamp;
     
     //@Column(name = "Severity", nullable = true)
     private String severity;
     
     //@Column(name = "Msg", nullable = true)
     private String message;
     
     //@Column(name = "Status", nullable = true)
     private String status;
     
     //@Column(name = "Mc_host", nullable = true)
     public String host;
     
     //@Column(name = "Mc_object", nullable = true)
     public String object;
     public String parametr; 
     
     //@Column(name = "Mc_service", nullable = true)
     public String service;
     
     //@Column(name = "Mc_event_subcategory", nullable = true)
     public String category;
     
//   @Id
//   @GeneratedValue
     //@Column(name = "Event_handle", nullable = true)
     private String externalid;
     
     //@Column
     private String ci;
     
    // @Column
     public String cialias;
     
     private String origin;
     
     private String eventCategory;
     
     private String module;
     
     public String eventurl;
     private String eventsource;
     
     //@Column
     private Integer repeatCounter = 0;
     
    // @Column(name = "relatedEvents",  nullable = true)
    // @ElementCollection(targetClass=Integer.class)
     private List<Integer> relatedEvents;
     
     public void onEvent(String body) {
           
     }
     
        @Override
        public String toString() {
             return ("Message: "+this.getMessage() +
                                " on host: "+ this.getHost() +
                         " with severity: "+ this.getSeverity() +
                         " object: "+ this.getObject() +
                         " parameter: "+ this.getParametr() +
                         " and status: "+ this.getStatus());
        }
     
     public String getHost() {
           return this.host;
     }
     
     public void setService(String service) {
         this.service = service;
     }
     
     public String getService() {
         return this.service;
    }
     
     public void setHost(String host) {
           this.host = host;
     }

     public Integer getUuid() {
           return uuid;
     }

     public void setUuid(Integer uuid) {
           this.uuid = uuid;
     }

     public Long getTimestamp() {
           return timestamp;
     }

     public void setTimestamp(Long timestamp) {
           this.timestamp = timestamp;
     }

     public String getSeverity() {
           return severity;
     }

     public void setSeverity(String severity) {
           this.severity = severity;
     }

     public String getMessage() {
           return message;
     }
     public void setMessage(String message) {
           this.message = message;
     }

     public String getStatus() {
           return status;
     }

     public void setStatus(String status) {
           this.status = status;
     }

     public String getParametr() {
           return parametr;
     }

     public void setParametr(String parametr) {
           this.parametr = parametr;
     }

     public String getExternalid() {
           return externalid;
     }

     public void setExternalid(String externalid) {
           this.externalid = externalid;
     }

     public String getCategory() {
           return category;
     }

     public void setCategory(String category) {
           this.category = category;
     }

     public String getObject() {
           return object;
     }

     public void setObject(String object) {
           this.object = object;
     }

     public String getCi() {
           return ci;
     }

     public void setCi(String ci) {
           this.ci = ci;
     }
     
     public String getEventsource() {
           return eventsource;
     }

     public void setEventsource(String eventsource) {
           this.eventsource = eventsource;
     }

     public int getRepeatCounter() {
           return repeatCounter;
     }

     public void setRepeatCounter(Integer repeatCounter) {
           this.repeatCounter = repeatCounter;
     }
     
//   public int[] getRelatedEvents() {
//         return relatedEvents;
//   }
//
//   public void setRelatedEvents(int relatedEvents[]) {
//         this.relatedEvents = relatedEvents;
//   }
     
     public void setRelatedEvent(int related) {
           this.relatedEvents.add(related);
     }

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getEventCategory() {
		return eventCategory;
	}

	public void setEventCategory(String eventCategory) {
		this.eventCategory = eventCategory;
	}

	public String getModule() {
		return module;
	}

	public void setModule(String module) {
		this.module = module;
	}
     
     

	       
	       
	  


}
