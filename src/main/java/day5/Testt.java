package day5;

import java.sql.*;

public class Testt {
    public static void main(String[] args) throws SQLException {
      Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456");

      PreparedStatement ps = conn.prepareStatement("select *from t_md_areas");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            String id = resultSet.getString("id");
            System.out.println(id);
        }
    }
}
