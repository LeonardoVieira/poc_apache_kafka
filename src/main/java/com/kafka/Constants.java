package com.kafka;

public interface Constants {

	public static final Long MAX_READS = 5l;
	public static final String TOPIC_NAME = "testeKafka";
	public static final Integer PARTITION = 0;
	public static final String BROKER = "localhost";
	public static final Integer PORT = 9092;
	
	public interface ErrorMessages {
		public static final String METADATA_NOT_FIND = "Can't find metadata for Topic and Partition. Exiting";
		public static final String LEADER_NOT_FIND = "Can't find Leader for Topic and Partition. Exiting";
		public static final String NEW_LEADER_NOT_FIND = "Unable to find new leader after Broker failure. Exiting";
	}
}