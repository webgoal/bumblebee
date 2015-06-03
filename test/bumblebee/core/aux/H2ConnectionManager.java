package bumblebee.core.aux;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import util.ConnectionManager;

public class H2ConnectionManager implements ConnectionManager {

	@Override
	public Connection getConsumerConnection() {
		try {
			return DriverManager.getConnection("jdbc:h2:mem:;MODE=MySQL");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Connection getProducerConnection() {
		try {
			return DriverManager.getConnection("jdbc:h2:mem:;MODE=MySQL");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
