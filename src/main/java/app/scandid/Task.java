package app.scandid;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Task {

	public static void main(String[] args) {
		String jdbcURL = "jdbc:mysql://localhost:3306/scandid";
		String username = "root";
		String password = null;

		String[][] data = { { "Washing Machine", "Electronics", "4", "50000", "2025-01-19" },
				{ "CPU", "Electronics", "3", "20000", "2025-01-19" } };

		Connection mysqlConnection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			mysqlConnection = DriverManager.getConnection(jdbcURL, username, password);
			for (String[] row : data) {
				int categoryId = getOrInsert(mysqlConnection, "Category", "CategoryName", row[1]);
				int productId = getOrInsert(mysqlConnection, "Product", "ProductName", row[0], categoryId);

				System.out.println("Inserting data into transaction table");
				String insertTransaction = "INSERT INTO Transaction (ProductID, Quantity, Price, TransactionDate) VALUES (?, ?, ?, ?)";
				try (PreparedStatement stmt = mysqlConnection.prepareStatement(insertTransaction)) {
					stmt.setInt(1, productId);
					stmt.setInt(2, Integer.parseInt(row[2]));
					stmt.setDouble(3, Double.parseDouble(row[3]));
					stmt.setDate(4, Date.valueOf(row[4]));
					stmt.executeUpdate();
				}
			}
			System.out.println("Data Inseted Succesfully");
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static int getOrInsert(Connection connection, String table, String column, String value)
			throws SQLException {
		return getOrInsert(connection, table, column, value, -1);
	}

	private static int getOrInsert(Connection connection, String table, String column, String value, int categoryId) {
		System.out.println("Inserting data into " + table);
		String query = "SELECT " + table + "ID FROM " + table + " WHERE " + column + " = ?";
		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.setString(1, value);
			ResultSet rs = stmt.executeQuery();
			if (rs.next())
				return rs.getInt(1);
		} catch (Exception e) {
			System.out.println("Error while creating the preparedStmt");
			return 0;
		}

		String insertQuery = "INSERT INTO " + table + " (" + column + (categoryId != -1 ? ", CategoryID" : "")
				+ ") VALUES (?" + (categoryId != -1 ? ", ?" : "") + ")";
		try (PreparedStatement stmt = connection.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS)) {
			stmt.setString(1, value);
			if (categoryId != -1)
				stmt.setInt(2, categoryId);
			stmt.executeUpdate();
			ResultSet rs = stmt.getGeneratedKeys();
			if (rs.next())
				return rs.getInt(1);
		} catch (Exception e) {
			System.out.println("Error while inserting data into table " + table);
			return 0;
		}
		return 0;
	}

}
