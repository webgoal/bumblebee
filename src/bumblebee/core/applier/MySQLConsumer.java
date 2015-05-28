package bumblebee.core.applier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;

import com.mysql.jdbc.StringUtils;

import bumblebee.core.Event;
import bumblebee.core.interfaces.Consumer;

public class MySQLConsumer implements Consumer {
	private static final String host     = "192.168.59.103";
	private static final String user     = "root";
	private static final String pass     = "mypass";
	private static final String database = "db1";
	private static final Integer port    = 3306;
	

	@Override
	public void insert(Event event) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url  = "jdbc:mysql://" + host + ":" + port + "/" + database;
			Connection connection  = DriverManager.getConnection(url, user, pass);
			Statement stmt = connection.createStatement();
			stmt.executeUpdate(prepareInsertSQL(event));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String prepareInsertSQL(Event event) {
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO ");
		sb.append(event.getCollection());
		sb.append(" SET ");
		event.getData().forEach((k,v) -> sb.append(k + " = '" + v + "', "));
		sb.replace(sb.length() - 2, sb.length(), ";");
		System.out.println(sb.toString());
		return sb.toString();
	}
}
