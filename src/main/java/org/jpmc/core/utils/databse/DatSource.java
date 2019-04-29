package org.jpmc.core.utils.databse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.jpmc.core.utils.FileProcessor;

public class DatSource {
	
	public static Connection getConnection() {
		Connection connection = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			connection = DriverManager.getConnection(FileProcessor.prop.getProperty("JDBC_CONNECTION_URL"),FileProcessor.prop.getProperty("JDBC_USERNAME"),FileProcessor.prop.getProperty("JDBC_PASSWORD"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}
}
