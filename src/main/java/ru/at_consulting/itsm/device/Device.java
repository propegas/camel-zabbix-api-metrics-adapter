package ru.at_consulting.itsm.device;


import java.io.Serializable;
//import java.util.Collection;
//import java.util.List;

//import org.apache.camel.Consume;

//import javax.persistence.Column;
//import javax.persistence.Entity;
//import javax.persistence.GeneratedValue;
//import javax.persistence.Id;
//import javax.persistence.Table;
//import javax.persistence.ElementCollection;

public class Device implements  Serializable {
	
	/**
     * 
      */
     private static final long serialVersionUID = 1L;

//   @Consume(uri="activemq:Events.pojo")
     
//     @Id
//     @GeneratedValue
     //@Column(name = "id")
     private String id;
     
     //@Column(name = "Date_reception")
     private String DeviceType;
     
     private String Source;
     
     private String DeviceState;
     
     //@Column(name = "Severity", nullable = true)
     private String serialNumber;
     
     //@Column(name = "Msg", nullable = true)
     private String CommState;
     
     //@Column(name = "Status", nullable = true)
     private String modelName;
     
     //@Column(name = "Mc_host", nullable = true)
     public String modelNumber;
     
     //@Column(name = "Mc_object", nullable = true)
     private String location;
     
     private String hostName; 
     
     private String name; 
     
     private String ipAddress; 
     
     private String parentID;
     
     
    
     public void onDevice(String body) {
           
     }
     
        @Override
        public String toString() {
             return ("Device: "+ this.getId() +
            		     " ParentId: "+ this.getParentID() +
                         " on host: "+ this.getHostName() +
                         " name: "+ this.getName() +
                         " serial number: "+ this.getSerialNumber() +
                         " location: "+ this.getLocation() +
                         " device state: "+ this.getDeviceState() +
             			 " comm state: "+ this.getCommState());
        }

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getDeviceType() {
			return DeviceType;
		}

		public void setDeviceType(String deviceType) {
			DeviceType = deviceType;
		}

		public String getSerialNumber() {
			return serialNumber;
		}

		public void setSerialNumber(String serialNumber) {
			this.serialNumber = serialNumber;
		}

		public String getCommState() {
			return CommState;
		}

		public void setCommState(String commState) {
			CommState = commState;
		}

		public String getModelName() {
			return modelName;
		}

		public void setModelName(String modelName) {
			this.modelName = modelName;
		}

		public String getLocation() {
			return location;
		}

		public void setLocation(String location) {
			this.location = location;
		}

		public String getHostName() {
			return hostName;
		}

		public void setHostName(String hostName) {
			this.hostName = hostName;
		}

		public String getIpAddress() {
			return ipAddress;
		}

		public void setIpAddress(String ipAddress) {
			this.ipAddress = ipAddress;
		}

		public String getDeviceState() {
			return DeviceState;
		}

		public void setDeviceState(String deviceState) {
			DeviceState = deviceState;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getParentID() {
			return parentID;
		}

		public void setParentID(String parentID) {
			this.parentID = parentID;
		}

		public String getSource() {
			return Source;
		}

		public void setSource(String source) {
			Source = source;
		}

	     
     

	       
	       
	  


}
