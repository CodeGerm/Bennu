package org.cg.phoenix.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;



public abstract class PhoenixWriter<V> {

	private static Logger logger = Logger.getLogger(PhoenixWriter.class);
	private Connection conn;
	private Statement st ;
	private String tableName;
	private String address;

	public PhoenixWriter(String address, String tableName) {
		logger.info("initialize phoenix connection");
		try {
			this.conn = DriverManager.getConnection(address);
			this.st = conn.createStatement();
		} catch (SQLException e) {
			logger.error("error in connecting: ", e);
		}
		this.tableName = tableName;
		this.address = address;
	}

	//Assemble all the Objects to a SQL Query
	public String connector(Object... o){
		String k = "";

		for (int i = 0; i < o.length - 1; i++) {
			Object o1 = o[i];
			if(o1==null)
				k +=" null, ";
			else if (o1.getClass().getName().equals("java.lang.String"))
				k += "'" + ((String) o1).replace("'", "''").replace("\\", "")+ "'" + ",";
			//To get rid off Phoenix's bug before Phoenix 4.4
			//else if (o1.getClass().getName().equals("java.lang.Double"))
			//	k += "0.0,";
			else
				k += o1.toString() + ",";
		}

		if(o[o.length - 1]==null)
			k +=" null ";
		else if (o[o.length - 1].getClass().getName().equals("java.lang.String"))
			k += "'" + ((String) o[o.length - 1]).replace("'", "''").replace("\\", "") + "'";
		//To get rid off Phoenix's bug before Phoenix 4.4
		//else if (o[o.length - 1].getClass().getName().equals("java.lang.Double"))
		//	k += "0.0,";
		else
			k += o[o.length - 1].toString();

		return k;
	}

	//Transfer an event object to a SQL query
	public abstract String eventToString(V event, String tableName);


	public void upsertEvents(V event) {

		try {
			st.addBatch(eventToString(event, tableName));
		} catch (Exception e) {
			logger.error("error in upsert: ", e);
		}

	}

	public void commit(){
		logger.info("commiting to "+tableName+" table");
		try {
			st.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			logger.error("error in commiting: ", e);
		}

	}

	public void close() {
		//logger.info("closing connection");
		try {
			st.close();
			conn.close();
		} catch (SQLException e) {
			logger.error("error in closing: ", e);
		}
	}

	public void reconnect() {
		//logger.info("recreating connection");
		try {
			this.conn = DriverManager.getConnection(address);
			this.st = conn.createStatement();
		} catch (SQLException e) {
			logger.error("error in reconnecting: ", e);
		}
	}


}
