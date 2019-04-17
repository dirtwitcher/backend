import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class Connect {
    public static Connection Connection = null;
    public static String TransactionTableName = "TRANSVLAD916"; // table of transactions
    public static String AccountTableName = "ACCVLAD916"; // table of accounts
    public static Integer idAcc = 1;

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
	    System.out.println("drop tables done");
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
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return accInOpenMode;
    }

    public void insertAccount(int id, String name, int status, String pass) {
	try {
	    Statement statement = connectToDB2().createStatement();
	    String InsertInfo = "INSERT INTO " + AccountTableName + " (ACCOUNT_ID,NAME,STATUS,PASSWORD) " + "VALUES ("
		    + id + ",'" + name + "'," + status + ",'" + pass + "'" + ");";
	    statement.executeUpdate(InsertInfo);
	    System.out.println(id + " acc insert done");
	    statement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void deleteAccount(int id) {
	try {
	    Statement statement = connectToDB2().createStatement();
	    String deleteInfo = "DELETE FROM " + AccountTableName + " WHERE ACCOUNT_ID = " + id + ";";
	    statement.executeUpdate(deleteInfo);
	    System.out.println(id + " acc drop done");
	    statement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public String viewAccName(int id) {
	String name = null;
	try {
	    ResultSet result = null;
	    Statement statement = connectToDB2().createStatement();
	    String viewName = "SELECT NAME FROM " + AccountTableName + " WHERE ACCOUNT_ID = '" + id + "';";
	    result = statement.executeQuery(viewName);
	    if (result.next()) {
		name = result.getString(1);
		System.out.println("name: " + name);
	    }
	    result.close();
	    statement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return name;
    }

    public Integer viewId(String log, String pass) {
	Integer idAccView = 0;
	try {
	    ResultSet result = null;
	    Statement statement = connectToDB2().createStatement();
	    String viewName = "SELECT ACCOUNT_ID FROM " + AccountTableName + " where NAME=" + log + " AND PASSWORD="
		    + pass + ";";
	    result = statement.executeQuery(viewName);
	    if (result.next()) {
		idAccView = result.getInt(1);
	    }
	    result.close();
	    statement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return idAccView;
    }

    public void closeStatusAccount(int id) {
	try {
	    Statement statement = connectToDB2().createStatement();
	    String ChangeStatus = "UPDATE " + AccountTableName + " SET STATUS=0 WHERE ACCOUNT_ID=" + id + ";";
	    statement.executeUpdate(ChangeStatus);
	    System.out.println(id + " now acc status closed");
	    statement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void openStatusAccount(int id) {
	try {
	    Statement statement = connectToDB2().createStatement();
	    String ChangeStatus = "UPDATE " + AccountTableName + " SET STATUS=1 WHERE ACCOUNT_ID=" + id + ";";
	    statement.executeUpdate(ChangeStatus);
	    System.out.println(id + " now status acc in open mode");
	    statement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public int printMoney(int id) {
	int sum = 0;
	try {
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
	    System.out.println("print money. acc: " + id + ", money: " + sum);
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return sum;
    }

    public void putMoney(int id, int money) {
	try {
	    Statement statement = connectToDB2().createStatement();
	    String makeTrans = "INSERT INTO " + TransactionTableName + " (ACCOUNT_ID,AMOUNT) " + "VALUES(" + id + ","
		    + money + ");";
	    statement.executeUpdate(makeTrans);
	    statement.close();
	    System.out.println("acc: " + id + " + " + money + "$$");
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void takeMoney(int id, int money) {
	int sub = 0;
	try {
	    Statement statement = connectToDB2().createStatement();
	    sub = printMoney(id) - money;
	    if (sub >= 0) {
		String makeTrans = "INSERT INTO " + TransactionTableName + " (ACCOUNT_ID,AMOUNT) " + "VALUES(" + id
			+ ",'-" + (money) + "');";
		statement.executeUpdate(makeTrans);
		statement.close();
		System.out.println("acc: " + id + " - " + money + "$$");
	    } else {
		System.out.println("No enough money on ballance.");
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void transactMoney(int fromId, int toId, int money) {
	int sub = 0;
	try {
	    Statement statement = connectToDB2().createStatement();
	    sub = printMoney(fromId) - money;
	    if (sub >= 0) {
		String makeTransTake = "INSERT INTO " + TransactionTableName + " (ACCOUNT_ID,AMOUNT) " + "VALUES("
			+ fromId + ",'-" + (money) + "');";
		statement.executeUpdate(makeTransTake);

		String makeTransPut = "INSERT INTO " + TransactionTableName + " (ACCOUNT_ID,AMOUNT) " + "VALUES(" + toId
			+ "," + money + ");";
		statement.executeUpdate(makeTransPut);
		statement.close();
	    } else {
		System.out.println("No enough money on ballance.");
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public static void main(String[] args) {
	Connect con = new Connect();
	con.createTables();

	int count = 200;

	try {
	    ServerSocket serverSock = new ServerSocket(48916);
	    System.out.println("Wait for new connections");
	    while (count > 0) {

		Socket sock = serverSock.accept(); // wait. listening

		InputStream sis = sock.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(sis));
		String request = br.readLine(); // Now you get SOME index.html HTTP/1.1
		System.out.println("request = " + request);

		String[] requestParam = request.split(" ");
		String requestURL = requestParam[1];
		System.out.println("requestURL = " + requestURL);

		if (requestURL.equals("/favicon.ico")) {
		    System.out.println("Ignoring /favicon.ico : '" + requestURL + "'");
		    continue;
		}

		String[] requestedParm = requestURL.split("/");
		String action = requestedParm[1];

//	!!!	!!!	AUTH	!!!	////////////////
		if (action.equals("auth")) {
		    String log = "";
		    String pass = "";
		    try {
			log = requestedParm[2];
		    } catch (Exception e) {
		    }
		    try {
			pass = requestedParm[3];
		    } catch (Exception e) {
		    }

		    System.out.println("action: " + action + " log: " + log + " pass: " + pass);

		    int isAcc = 0;
		    if (con.viewId(log, pass) != 0) {
			isAcc = 1;
		    } else {
			isAcc = 0;
		    }

		    Date today = new Date();
		    String header = "HTTP/1.1 201 \r\n" + today + "\r\n" + "Content-Type: text/json\r\n"
			    + "Access-Control-Allow-Origin: *\r\n" + "Connection: close\r\n" + "\r\n";

		    String output = "{ \"idHeader\": " + con.viewId(log, pass) + ", \"iAmInAcc\": " + isAcc + " }";

		    System.out.println(output);

		    String httpResponse = header + output;

		    sock.getOutputStream().write(httpResponse.getBytes("UTF-8"));

		    br.close();
		    count--;
		} else

//	!!!	!!!	REGISTR	!!!	////////////////
		if (action.equals("registr")) {
		    String log = "";
		    String pass = "";
		    try {
			log = requestedParm[2];
		    } catch (Exception e) {
		    }
		    try {
			pass = requestedParm[3];
		    } catch (Exception e) {
		    }

		    System.out.println("action: " + action + " log: " + log + " pass: " + pass);

		    if (con.viewId(log, pass) == 0) {
			con.insertAccount(idAcc, log, 1, pass);
			idAcc++;
		    }

		    br.close();
		    count--;
		} else

//	!!!	!!!	PUT	!!!	////////////////
		if (action.equals("put")) {
		    String id = "";
		    String money = "";
		    try {
			id = requestedParm[2];
		    } catch (Exception e) {
		    }
		    try {
			money = requestedParm[3];
		    } catch (Exception e) {
		    }

		    System.out.println("action: " + action + " id: " + id + " money: " + money);

		    if ((con.isAccInTable(Integer.parseInt(id))) && con.checkStatusAccount(Integer.parseInt(id))) {

			con.putMoney(Integer.parseInt(id), Integer.parseInt(money));

			Date today = new Date();
			String header = "HTTP/1.1 201 \r\n" + today + "\r\n" + "Content-Type: text/json\r\n"
				+ "Access-Control-Allow-Origin: *\r\n" + "Connection: close\r\n" + "\r\n";

			String output = "{ \"moneyOper\": " + con.printMoney(Integer.parseInt(id)) + " }";

			System.out.println(output);

			String httpResponse = header + output;

			sock.getOutputStream().write(httpResponse.getBytes("UTF-8"));
		    }
		    br.close();
		    count--;
		} else

//	!!!	!!!	TAKE	!!!	////////////////
		if (action.equals("take")) {
		    String id = "";
		    String money = "";
		    try {
			id = requestedParm[2];
		    } catch (Exception e) {
		    }
		    try {
			money = requestedParm[3];
		    } catch (Exception e) {
		    }

		    System.out.println("action: " + action + " id: " + id + " money: " + money);

		    if ((con.isAccInTable(Integer.parseInt(id))) && con.checkStatusAccount(Integer.parseInt(id))) {

			con.takeMoney(Integer.parseInt(id), Integer.parseInt(money));

			Date today = new Date();
			String header = "HTTP/1.1 201 \r\n" + today + "\r\n" + "Content-Type: text/json\r\n"
				+ "Access-Control-Allow-Origin: *\r\n" + "Connection: close\r\n" + "\r\n";

			String output = "{ \"moneyOper\": " + con.printMoney(Integer.parseInt(id)) + " }";

			System.out.println(output);

			String httpResponse = header + output;

			sock.getOutputStream().write(httpResponse.getBytes("UTF-8"));
		    }
		    br.close();
		    count--;
		} else

//	!!!	!!!	TRANSACT	!!!	////////////////
		if (action.equals("transact")) {
		    String id = "";
		    String otherId = "";
		    String money = "";
		    try {
			id = requestedParm[2];
		    } catch (Exception e) {
		    }
		    try {
			otherId = requestedParm[3];
		    } catch (Exception e) {
		    }
		    try {
			money = requestedParm[4];
		    } catch (Exception e) {
		    }

		    System.out
			    .println("action: " + action + " id: " + id + " otherId: " + otherId + " money: " + money);

		    if (con.isAccInTable(Integer.parseInt(id)) && con.isAccInTable(Integer.parseInt(otherId))) {
			if (con.checkStatusAccount(Integer.parseInt(id))
				&& con.checkStatusAccount(Integer.parseInt(otherId))) {

			    con.transactMoney(Integer.parseInt(id), Integer.parseInt(otherId), Integer.parseInt(money));

			    Date today = new Date();
			    String header = "HTTP/1.1 201 \r\n" + today + "\r\n" + "Content-Type: text/json\r\n"
				    + "Access-Control-Allow-Origin: *\r\n" + "Connection: close\r\n" + "\r\n";

			    String output = "{ \"moneyOper\": " + con.printMoney(Integer.parseInt(id)) + " }";

			    System.out.println(output);

			    String httpResponse = header + output;

			    sock.getOutputStream().write(httpResponse.getBytes("UTF-8"));
			}
		    }
		    br.close();
		    count--;
		} else

//	!!!	!!!	PRINT	!!!	////////////////
		if (action.equals("print")) {
		    String id = "";
		    try {
			id = requestedParm[2];
		    } catch (Exception e) {
		    }

		    System.out.println("action: " + action + " id: " + id);

		    if (con.isAccInTable(Integer.parseInt(id))) {

			Date today = new Date();
			String header = "HTTP/1.1 201 \r\n" + today + "\r\n" + "Content-Type: text/json\r\n"
				+ "Access-Control-Allow-Origin: *\r\n" + "Connection: close\r\n" + "\r\n";

			String output = "{ \"moneyOper\": " + con.printMoney(Integer.parseInt(id)) + " }";

			System.out.println(output);

			String httpResponse = header + output;

			sock.getOutputStream().write(httpResponse.getBytes("UTF-8"));
		    }
		    br.close();
		    count--;
		}

	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
