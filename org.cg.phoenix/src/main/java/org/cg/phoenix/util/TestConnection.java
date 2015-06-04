package org.cg.phoenix.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

public class TestConnection {
	
	
	public static void select(Connection conn, String taleName) throws SQLException,
	ClassNotFoundException {
		ResultSet rs = conn.createStatement().executeQuery(
				"select * from "+taleName+" limit 1");

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();

		while (rs.next()) {
			for (int i = 1; i <= columnsNumber; i++) {
				System.out.print(rsmd.getColumnName(i) + ":" + rs.getObject(i)
						+ ",");
			}
			System.out.println();

		}

		conn.commit();
		conn.close();
	}
	
	
	public static void main(String args[]) throws ClassNotFoundException, SQLException{
		
		String address = args[0];
		String tableName = args[1];
		
		Connection conn;
		Properties prop = new Properties();
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
	
		conn = DriverManager
				.getConnection(address,prop);
		
		select(conn, tableName);
		conn.close();
		
	}
	

}
