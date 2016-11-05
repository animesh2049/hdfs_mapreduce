package phaseI;


import phaseI.Hdfs.ReadBlockRequest;
import phaseI.Hdfs.ReadBlockResponse;
import phaseI.Hdfs.WriteBlockRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import phaseI.Hdfs;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Enumeration;

import com.google.protobuf.InvalidProtocolBufferException;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import javax.swing.plaf.SliderUI;

public class DataNode implements DataNodeRemoteInterfaces {

	private static Registry registry = null;
	private static RemoteInterfaces nameNode = null;
	private static String host = null;
	private static int myId = 1;
	private static Connection con;
	private static Statement stmt;
	private static String myIp;
	private static int myPort;
	private static String interfaceToConnect;
	private static String nameNodeIp;
	
	public DataNode(){
		myPort = 1099;
		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306","testuser","test");
			stmt = con.createStatement();
			Class.forName("com.mysql.jdbc.Driver");
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
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
	
	public byte[] writeBlock(byte[] message) {
		Hdfs.WriteBlockResponse.Builder response = Hdfs.WriteBlockResponse.newBuilder();
		try {
			stmt.executeUpdate("create database if not exists hdfs");
			stmt.execute("use hdfs");
			stmt.executeUpdate("create table if not exists datablock(blocknum int,data longtext,primary key(blocknum))");
			WriteBlockRequest writeBlockRequest;
			writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(message);
			String data=new String();
			data=writeBlockRequest.getData(0).toString();
			
			PreparedStatement pstmt = con.prepareStatement("insert into datablock(blocknum,data) values(?,?)");
			pstmt.setInt(1, writeBlockRequest.getBlockInfo().getBlockNumber());
			pstmt.setString(2, data);
			pstmt.executeUpdate();

			response.setStatus(0);
			
			
		} catch (SQLException e) {
			response.setStatus(1);
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			response.setStatus(1);
			e.printStackTrace();
		} 
		
		Hdfs.WriteBlockResponse finalRes=response.build();
		return finalRes.toByteArray();
	}
	
	public byte[] readBlock(byte[] message) {
		Hdfs.ReadBlockResponse.Builder readBlockResponse = Hdfs.ReadBlockResponse.newBuilder();
		try {
			ReadBlockRequest readBlockRequest;
			readBlockRequest = Hdfs.ReadBlockRequest.parseFrom(message);
			stmt.execute("use hdfs");
			PreparedStatement pstmt = con.prepareStatement("select data from datablock where blocknum = ?");
			pstmt.setInt(1, readBlockRequest.getBlockNumber());
			ResultSet rs = pstmt.executeQuery();
			String dt=null;
			while(rs.next())
			{
				dt=rs.getString(1);
			}
			if(dt==null)
				readBlockResponse.setStatus(1);
			else
				readBlockResponse.setStatus(0);
				
		}  catch (SQLException e) {
			readBlockResponse.setStatus(1);
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			readBlockResponse.setStatus(1);
			e.printStackTrace();
		}
		Hdfs.ReadBlockResponse finalRes = readBlockResponse.build();
		return finalRes.toByteArray();
	}
	
	public static void main(String[] args){
		
		if(args.length < 1) {
			System.out.println("Please provide namenode ip");
			return;
		}
		
		nameNodeIp = args[0];
		
		Inet4Address inetAddress = null;
		
		try {
			Enumeration<InetAddress> enumeration = NetworkInterface.getByName(interfaceToConnect).getInetAddresses();
			while (enumeration.hasMoreElements()) {
				InetAddress tempInetAddress = enumeration.nextElement();
				if (tempInetAddress instanceof Inet4Address) {
					inetAddress = (Inet4Address) tempInetAddress;
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
		}
		
		if (inetAddress == null) {
			System.err.println("Error Obtaining Network Information");
			System.exit(-1);
		}
		
		try {
			registry = LocateRegistry.getRegistry(nameNodeIp);
			nameNode = (RemoteInterfaces) registry.lookup("NameNode");
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
			System.exit(1);
		}
		

		try {
			DataNode dataNode = new DataNode();
			DataNodeRemoteInterfaces mystub = (DataNodeRemoteInterfaces) UnicastRemoteObject.exportObject(dataNode, 0);
			Registry localRegistry = LocateRegistry.getRegistry(inetAddress.getHostAddress());
		}

	}
	
	public static void sendHeartBeat() throws RemoteException {
		nameNode.heartBeat(Hdfs.HeartBeatRequest.newBuilder().setId(myId).build().toByteArray());
	}
	
	public static void sendBlockReport() {
		Hdfs.BlockReportRequest.Builder blockReport = Hdfs.BlockReportRequest.newBuilder();
		ResultSet res = null;
		ArrayList<Integer> blockNumbers = new ArrayList<Integer>();
		
		blockReport.setId(myId);
		try {
			res = stmt.executeQuery("select blocknum from datablock");
			while(res.next()) {
				blockNumbers.add(res.getInt(1));
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		blockReport.addAllBlockNumbers(blockNumbers);
	}
}
