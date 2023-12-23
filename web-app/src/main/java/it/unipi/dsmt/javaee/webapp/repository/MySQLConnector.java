package it.unipi.dsmt.javaee.webapp.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;

public class MySQLConnector {
    private static final String URL = "jdbc:mysql://db:3306/fraud_detection_db";
    private static final String USER = "fraud_detection_user";
    private static final String PASSWORD = "fraud_detection_password";

    private final Connection connection;

    public MySQLConnector() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }
 
    public void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}