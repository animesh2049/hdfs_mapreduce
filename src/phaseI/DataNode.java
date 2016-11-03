package phaseI;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import javax.swing.plaf.SliderUI;


public class DataNode implements DataNodeRemoteInterfaces {
	private static String persistanceFile = "datanode.conf";
	private static Registry registry = null;
	private static RemoteInterfaces nameNode = null;
	private static String host = null;
	private static int myId = 1;
	
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
		
		try {
			registry = LocateRegistry.getRegistry(host);
			nameNode = (RemoteInterfaces) registry.lookup("NameNode");
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
			System.exit(1);
		}
		
		Thread heartBeatThread = new Thread(new Runnable() {
			public void run() {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.err.println("Interrupted from sleep");
				}
				try {
					sendHeartBeat();
				} catch (RemoteException e) {
					System.err.println("Unable to find NameNode");
				}
			}
		});
		Thread blockReportThread = new Thread(new Runnable() {
			public void run() {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					System.err.println("Interrupted from sleep");
				}
				sendBlockReport();
			}
		});
		
		heartBeatThread.start();
		blockReportThread.start();

	}
	
	public static void sendHeartBeat() throws RemoteException {
		nameNode.heartBeat(Hdfs.HeartBeatRequest.newBuilder().setId(myId).build().toByteArray());
	}
	
	public static void sendBlockReport() {
		
	}
}
