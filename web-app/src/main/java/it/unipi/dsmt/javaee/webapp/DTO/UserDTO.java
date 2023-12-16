package it.unipi.dsmt.javaee.webapp.DTO;

public class UserDTO {
    private final int id;
    private final String username;
    private final String password;

    public UserDTO(int id, String username, String password) {
        this.id = id;
        this.username = username;
        this.password = password;
    }
}
