
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestConnection {

    public static void main(String[] args) {

        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL);
             Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT current_database(), version()")) {

            if (rs.next()) {
                System.out.println("Connected successfully!");
                System.out.println("Database: " + rs.getString(1));
                System.out.println("Server: " + rs.getString(2));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
