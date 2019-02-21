package com.boa.trainings.avroschema;

public class CustomerSchema {
	
	public static final String SCHEMA_DEFINITION="{"+
		    "\"namespace\":\"com.boa.schemanamespaces\","+
		    "\"type\":\"record\","+
		    "\"name\":\"Customer\","+
		    "\"fields\":["+
		    "{\"name\":\"id\",\"type\":\"int\"},"+
		    "{\"name\":\"name\",\"type\":\"string\"}]}";

}
