package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class HiveJdbcTest {

	public void start() throws Exception {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		
		Connection conn = DriverManager.getConnection("jdbc:hive2://cm.local:10000/default", "xxxxx", "xxxxx");
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("desc formatted test.titanic");
			ResultSetMetaData rsmd = rs.getMetaData();
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				if (i > 1) System.out.print('\t');
				System.out.print(rsmd.getColumnName(i));
			}
			System.out.println();
			while (rs.next()) {
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					if (i > 1) System.out.print('\t');
					System.out.print(rs.getString(i));
				}
				System.out.println();
			}
			rs.close();
			stmt.close();
		} finally {
			conn.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		HiveJdbcTest hjt = new HiveJdbcTest();
		hjt.start();
	}

}
