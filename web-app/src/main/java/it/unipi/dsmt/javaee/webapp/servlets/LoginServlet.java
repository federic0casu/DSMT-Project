package it.unipi.dsmt.javaee.webapp.servlets;

import it.unipi.dsmt.javaee.webapp.DAO.UserDAO;
import it.unipi.dsmt.javaee.webapp.DTO.UserDTO;
import it.unipi.dsmt.javaee.webapp.repository.MySQLConnector;

import jakarta.servlet.annotation.WebServlet;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

@WebServlet(name = "LoginServlet", value = "/LoginServlet")
public class LoginServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            String username = request.getParameter("username");
            String password = request.getParameter("password");
            MySQLConnector connector = new MySQLConnector();

            try (Connection connection = connector.getConnection()) {
                if (connection != null) {
                    UserDAO userDAO = new UserDAO(connection);
                    UserDTO user = userDAO.getUserByUsernameAndPassword(username, password);

                    if (user != null) {
                        // Successful login
                        HttpSession session = request.getSession();
                        session.setAttribute("username", username);
                        response.sendRedirect("pages/map.html"); // Redirect to the dashboard
                    } else {
                        // Failed login
                        response.sendRedirect("index.jsp?error=true"); // Redirect back to the login page with an error parameter
                    }
                } else {
                    response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                }
            } catch (SQLException e) {
                // Handle database access errors
                System.err.println(e.getMessage());
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            } finally {
                connector.closeConnection();
            }

        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}