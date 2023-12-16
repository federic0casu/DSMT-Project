package it.unipi.dsmt.javaee.webapp.DAO;

import it.unipi.dsmt.javaee.webapp.DTO.UserDTO;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;

public class UserDAO {
    private final Connection connection;
    public UserDAO(Connection connection) {
        this.connection = connection;
    }
    public UserDTO getUserByUsernameAndPassword(String username, String password) throws SQLException {
        String query = "SELECT * FROM users WHERE username = ? AND password = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, username);
            preparedStatement.setString(2, password);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return new UserDTO(resultSet.getInt("id"),
                            resultSet.getString("username"),
                            resultSet.getString("password"));
                }
            }
        }
        return null; // Return null if no user found
    }
}

