package hive;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaTest {
	
	private Logger log = LoggerFactory.getLogger(HiveMetaTest.class);

	public void start() throws Exception {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		
		Connection conn = DriverManager.getConnection("jdbc:hive2://cm.local:10000/default", "xxxxx", "xxxxxx");
		try {
			ResultSet rs;
			
			DatabaseMetaData dbmd = conn.getMetaData();

			log.info("Tables");
			rs = dbmd.getTables(null, "test", null, null);
			while (rs.next()) {
				String table_schem = rs.getString("TABLE_SCHEM");
				String table_name = rs.getString("TABLE_NAME");
				String remarks = rs.getString("REMARKS");
			
				log.info("{}\t{}\t{}", table_schem, table_name, remarks);
			}
			rs.close();
			
			log.info("\nColumns");
			rs = dbmd.getColumns(null, "test", "titanic", null);
			while (rs.next()) {
				String table_schem = rs.getString("TABLE_SCHEM");
				String table_name = rs.getString("TABLE_NAME");
				String column_name = rs.getString("COLUMN_NAME");
				String type_name = rs.getString("TYPE_NAME").toLowerCase();
				int column_size = rs.getInt("COLUMN_SIZE");
				int decimal_digits = rs.getInt("DECIMAL_DIGITS");
				int nullable = rs.getInt("NULLABLE");
				int ordinal_position = rs.getInt("ORDINAL_POSITION");
				String remarks = rs.getString("REMARKS");
			
				log.info("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}", table_schem, table_name, column_name, remarks, type_name, column_size, decimal_digits, nullable, ordinal_position);
			}
			rs.close();

			
			log.info("\nPrimary Keys");
			rs = dbmd.getPrimaryKeys(null, "test", "titanic");
			while (rs.next()) {
				String table_schem = rs.getString("TABLE_SCHEM");
				String table_name = rs.getString("TABLE_NAME");
				String column_name = rs.getString("COLUMN_NAME");
				int keq_seq = rs.getInt("KEQ_SEQ");
				String pk_name = rs.getString("PK_NAME");
			
				log.info("{}\t{}\t{}\t{}\t{}", table_schem, table_name, pk_name, column_name, keq_seq);
			}
			rs.close();
		} finally {
			conn.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		HiveMetaTest hmt = new HiveMetaTest();
		hmt.start();
	}

}