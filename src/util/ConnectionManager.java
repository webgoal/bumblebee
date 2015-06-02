package util;

import java.sql.Connection;

public interface ConnectionManager {
	Connection getConsumerConnection();
	Connection getProducerConnection();
}
