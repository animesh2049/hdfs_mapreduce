package phaseI;


public class DataNode implements DataNodeRemoteInterfaces {
	
	private static String persistanceFile = "datanode.conf";
	
	public DataNode(){}
	
	public byte[] writeBlock(byte[] message) {
		Class.forName("com.mysql.jdbc.Driver");
		try {
			Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306","root","ishi2002");
			Statement stmt=con.createStatement();
			stmt.executeUpdate("create database if not exists blockdata");
			stmt.execute("use blockdata");
			stmt.executeUpdate("create table if not exists personal(rollno int auto_increment,name varchar(20),age int,address varchar(20),primary key(rollno))");
			stmt.executeUpdate("insert into personal(name,age,address) values('ayushi',18,'jnv')");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public byte[] readBlock(byte[] message) {
		
	}
	
	public static void main(String[] args){
		
	}
	
}
