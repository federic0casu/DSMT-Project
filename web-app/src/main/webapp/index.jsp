<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Login</title>
        <link rel="stylesheet" href="css/login.style.css">
    </head>
    <body>
        <div class="container">
            <h2>Login</h2>
            <form action="${pageContext.request.contextPath}/LoginServlet" method="post">
                <label for="username">Username:</label><br>
                <input type="text" id="username" name="username" required><br><br>

                <label for="password">Password:</label><br>
                <input type="password" id="password" name="password" required><br><br>

                <input type="submit" value="Login">
            </form>
            <% if (request.getParameter("error") != null) { %>
            <div class="error-message" id="error-message">
                We have a little problem. Please make sure we've got your credentials right.
            </div>
            <% } %>
        </div>
    </body>
</html>
