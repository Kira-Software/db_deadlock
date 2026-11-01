
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    public static final int POSTGRESQL = 1;

    public static Connection getDefaultParameterConnection(int type) {

        if (type != POSTGRESQL) {
            throw new IllegalArgumentException("Unsupported database");
        }

        String url = "jdbc:postgresql://localhost:5432/adb_deadlock";
        String username = "kirubelbayru";
        String password = "";
       // String password = System.getenv("DB_PASSWORD");

        try {
            return DriverManager.getConnection(
                    url, username, password
            );
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Database connection failed", e
            );
        }
    }
}
