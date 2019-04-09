import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Connect {
    public static Connection Connection = null;
    public static String TransactionTableName = "TRANSVLAD916"; // table of transactions
    public static String AccountTableName = "ACCVLAD916"; // table of accounts

    public Connection connectToDB2() {
	if (Connection != null)
	    return Connection;
	try {
	    Class.forName("com.ibm.db2.jcc.DB2Driver");
	    Connection = DriverManager.getConnection("jdbc:db2://localhost:5035/DALLASB", "USER01", "USER01");
	    System.out.println("connection done");
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (ClassNotFoundException cnfex) {
	    System.out.println("Problem in" + " loading or registering IBM DB2 JDBC driver");
	    cnfex.printStackTrace();
	}
	return Connection;
    };

    public void CloseConnection() {
	if (Connection != null) {
	    try {
		Connection.close();
		Connection = null;
		System.out.println("connection close");
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
    }

    public void createTables() {
	try {
	    Statement statement = connectToDB2().createStatement();

	    deleteTables();

	    String createAccountTable = "CREATE TABLE " + AccountTableName + "(ACCOUNT_ID INT NOT NULL,"
		    + "NAME VARCHAR(30) NOT NULL," + "STATUS INT NOT NULL," + "PASSWORD VARCHAR(50) NOT NULL,"
		    + "PRIMARY KEY(ACCOUNT_ID));";

	    String createTransactTable = "CREATE TABLE " + TransactionTableName
		    + "(TRAN_ID INT GENERATED ALWAYS AS IDENTITY NOT NULL," + "ACCOUNT_ID INT NOT NULL,"
		    + "AMOUNT DECIMAL NOT NULL," + "PRIMARY KEY(TRAN_ID)," + "FOREIGN KEY (ACCOUNT_ID) " + "REFERENCES "
		    + AccountTableName + " (ACCOUNT_ID)" + " ON DELETE CASCADE);";

	    statement.execute(createAccountTable);
	    statement.execute(createTransactTable);
	    System.out.println("create tables done");
	    statement.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }

    public void deleteTables() {
	try {
	    Statement statement = connectToDB2().createStatement();

	    statement.executeUpdate("DROP TABLE " + TransactionTableName + ";");
	    statement.executeUpdate("DROP TABLE " + AccountTableName + ";");
	    System.out.println("drop table done");
	    statement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public boolean isAccInTable(int id) {
	boolean accInTable = false;
	try {
	    Statement statement = connectToDB2().createStatement();
	    String checkAccount = "SELECT COUNT(NAME) FROM " + AccountTableName + " where ACCOUNT_ID='" + id + "';";
	    ResultSet res = statement.executeQuery(checkAccount);
	    if (res.next()) {
		if (res.getInt(1) != 0) {
		    accInTable = true;
		    System.out.println(id + " acc in table");
		} else {
		    System.out.println(id + " acc not in table");
		}
	    }
	    res.close();
	    statement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return accInTable;
    }

    public boolean checkStatusAccount(int id) {
	boolean accInOpenMode = false;
	try {
	    // if (isAccInTable(id)) {
	    Statement statement = connectToDB2().createStatement();
	    String checkAccount = "SELECT STATUS FROM " + AccountTableName + " where ACCOUNT_ID='" + id + "';";
	    ResultSet res = statement.executeQuery(checkAccount);
	    if (res.next()) {
		if (res.getInt(1) == 1) { // 1 = true = open, 0 = false = close
		    accInOpenMode = true;
		    System.out.println(id + " acc in open mode");
		} else {
		    System.out.println(id + " acc not in open mode");
		}
	    }
	    res.close();
	    statement.close();
	    // }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return accInOpenMode;
    }

    public void insertAccount(int id, String name, int status, String pass) {
	try {
	    if (!isAccInTable(id)) {
		Statement statement = connectToDB2().createStatement();
		String InsertInfo = "INSERT INTO " + AccountTableName + " (ACCOUNT_ID,NAME,STATUS,PASSWORD) "
			+ "VALUES (" + id + ",'" + name + "'," + status + ",'" + pass + "'" + ");";
		statement.executeUpdate(InsertInfo);
		System.out.println(id + " acc insert done");
		statement.close();
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void deleteAccount(int id) {
	try {
	    if (isAccInTable(id)) {
		Statement statement = connectToDB2().createStatement();
		String deleteInfo = "DELETE FROM " + AccountTableName + " WHERE ACCOUNT_ID = " + id + ";";
		statement.executeUpdate(deleteInfo);
		statement.close();
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public String viewAccName(int id) {
	String name = null;
	try {
	    if (isAccInTable(id)) {
		ResultSet result = null;
		Statement statement = connectToDB2().createStatement();
		String viewName = "SELECT NAME FROM " + AccountTableName + " WHERE ACCOUNT_ID = '" + id + "';";
		result = statement.executeQuery(viewName);
		if (result.next()) {
		    name = result.getString(1);
		}
		result.close();
		statement.close();
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return name;
    }

    public void closeStatusAccount(int id) {
	try {
	    if (isAccInTable(id)) {
		Statement statement = connectToDB2().createStatement();
		if (checkStatusAccount(id)) {
		    String ChangeStatus = "UPDATE " + AccountTableName + " SET STATUS=0 WHERE ACCOUNT_ID=" + id + ";";
		    statement.executeUpdate(ChangeStatus);
		}
		System.out.println(id + " now acc status closed");
		statement.close();
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void openStatusAccount(int id) {
	try {
	    if (isAccInTable(id)) {
		Statement statement = connectToDB2().createStatement();
		if (!checkStatusAccount(id)) {
		    String ChangeStatus = "UPDATE " + AccountTableName + " SET STATUS=1 WHERE ACCOUNT_ID=" + id + ";";
		    statement.executeUpdate(ChangeStatus);
		    System.out.println(id + " now status acc in open mode");
		}
		statement.close();
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public int printMoney(int id) {
	int sum = 0;
	try {
	    if (isAccInTable(id)) {
		// if (checkStatusAccount(id)) {
		ResultSet result = null;
		Statement statement = connectToDB2().createStatement();
		String youreCash = "SELECT ACCOUNT_ID,SUM(AMOUNT) AS BALANCE FROM " + TransactionTableName
			+ " where ACCOUNT_ID=" + id + " GROUP BY ACCOUNT_ID;";
		result = statement.executeQuery(youreCash);
		if (result.next()) {
		    sum = result.getInt(2);
		}
		result.close();
		statement.close();
	    }
	    // }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return sum;
    }

    public int putMoney(int id, int money) {
	try {
	    if (isAccInTable(id)) {
		if (checkStatusAccount(id)) {
		    Statement statement = connectToDB2().createStatement();
		    String makeTrans = "INSERT INTO " + TransactionTableName + " (ACCOUNT_ID,AMOUNT) " + "VALUES(" + id
			    + "," + money + ");";
		    statement.executeUpdate(makeTrans);
		    statement.close();
		}
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return printMoney(id);
    }

    public int takeMoney(int id, int money) {
	int sub = 0;
	try {
	    if (isAccInTable(id)) {
		if (checkStatusAccount(id)) {
		    Statement statement = connectToDB2().createStatement();
		    sub = printMoney(id) - money;
		    if (sub >= 0) {
			String makeTrans = "INSERT INTO " + TransactionTableName + " (ACCOUNT_ID,AMOUNT) " + "VALUES("
				+ id + ",'-" + (money) + "');";
			statement.executeUpdate(makeTrans);
			statement.close();
		    } else {
			System.out.println("No enough money on ballance.");
			return 0;
		    }
		}
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return sub;
    }

    public void transactMoney(int fromId, int toId, int money) {
	int sub = 0;
	try {
	    if (isAccInTable(fromId) && isAccInTable(toId)) {
		if (checkStatusAccount(fromId) && checkStatusAccount(toId)) {
		    Statement statement = connectToDB2().createStatement();
		    sub = printMoney(fromId) - money;
		    if (sub >= 0) {
			String makeTransTake = "INSERT INTO " + TransactionTableName + " (ACCOUNT_ID,AMOUNT) "
				+ "VALUES(" + fromId + ",'-" + (money) + "');";
			statement.executeUpdate(makeTransTake);

			String makeTransPut = "INSERT INTO " + TransactionTableName + " (ACCOUNT_ID,AMOUNT) "
				+ "VALUES(" + toId + "," + money + ");";
			statement.executeUpdate(makeTransPut);
			statement.close();
		    } else {
			System.out.println("No enough money on ballance.");
		    }
		}
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public static void main(String[] args) {
	Connect con = new Connect();

	con.createTables();

	System.out.println("\n !!! insertAcc");
	con.insertAccount(1, "Vlad", 1, "111");
	con.insertAccount(2, "petua", 1, "222");
	con.insertAccount(3, "vasya", 0, "333");

	System.out.println("\n !!! put money on acc not found");
	con.putMoney(4, 100);

	System.out.println("\n !!! put 100 Money on 1 and 2");
	con.putMoney(1, 100);
	con.putMoney(2, 100);

	System.out.println("\n !!! put money on close acc");
	con.putMoney(3, 100);

	System.out.println("\n !!! printMoney");
	System.out.println(con.printMoney(1));
	System.out.println(con.printMoney(2));
	System.out.println(con.printMoney(3));

	System.out.println("\n !!! take money from close acc");
	con.takeMoney(3, 10);

	System.out.println("\n !!! take a lot money");
	con.takeMoney(2, 999);

	System.out.println("\n !!! transact money from close acc");
	con.transactMoney(3, 1, 10);

	System.out.println("\n !!! transact money to close acc");
	con.transactMoney(1, 3, 10);

	System.out.println("\n !!! open 3 acc");
	con.openStatusAccount(3);

	System.out.println("\n !!! put 100 Money on 3");
	con.putMoney(3, 100);

	System.out.println("\n !!! printMoney");
	System.out.println(con.printMoney(1));
	System.out.println(con.printMoney(2));
	System.out.println(con.printMoney(3));

	System.out.println("\n !!! put and take Money");
	con.putMoney(1, 50);
	con.takeMoney(3, 50);

	System.out.println("\n !!! printMoney");
	System.out.println(con.printMoney(1));
	System.out.println(con.printMoney(2));
	System.out.println(con.printMoney(3));

	System.out.println("\n !!! viewName");
	System.out.println(con.viewAccName(1));
	System.out.println(con.viewAccName(2));
	System.out.println(con.viewAccName(3));

	System.out.println("\n !!! chech status 3 acc");
	con.checkStatusAccount(3);

	System.out.println("\n !!! del 3 acc");
	con.deleteAccount(3);

	System.out.println("\n !!! transact from 1 to 2");
	con.transactMoney(1, 2, 15);

	System.out.println("\n !!! printMoney");
	System.out.println(con.printMoney(1));
	System.out.println(con.printMoney(2));
	System.out.println(con.printMoney(3));

	con.CloseConnection();
    }

}