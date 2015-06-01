package bumblebee.core.applier;

public class MySQLPositionManager {

	private String db;
	private String table;

	public MySQLPositionManager(String db, String table) {
		this.db = db;
		this.table = table;
	}

	public String getCurrentLogName() {
		return "mysql-bin.000001";
	}

	public Long getCurrentLogPosition() {
		return 4L;
	}

	public String prepareUpdateSQL(String logName, Long logPosition) {
		return "UPDATE " + db + "." + table + " SET log_name = '" + logName + "' SET log_pos = '" + logPosition + "'";
	}

}
