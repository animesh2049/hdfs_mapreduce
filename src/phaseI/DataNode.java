package phaseI;


import phaseI.Hdfs;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Enumeration;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ByteString;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


public class DataNode extends UnicastRemoteObject implements DataNodeRemoteInterfaces {

	private static final long serialVersionUID = 1L;
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
	
	public DataNode() throws RemoteException {
		super();
		myId = 1;
		myIp = ""; // Set your ip here
		myPort = 1099; // Default
		try {
			registry = LocateRegistry.getRegistry("172.28.128.3");
			nameNode = (RemoteInterfaces) registry.lookup("NameNode");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306?useSSL=false","testuser","test");
			stmt = con.createStatement();
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			stmt.executeUpdate("create database if not exists hdfs");
			stmt.execute("use hdfs");
			stmt.executeUpdate("create table if not exists datablock(blocknum int,data longtext,primary key(blocknum))");	
		} catch (Exception e) {
			System.err.println("Err in Database : " + e.toString());
		}
				
		Thread heartBeatThread = new Thread(new Runnable() {
			public void run() {
				while(true) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						System.err.println("Interrupted from sleep");
					}
					try {
						sendHeartBeat();
					} catch (RemoteException e) {
						System.err.println("Unable to find NameNode : " + e.toString());
						while(true) {
							try {
								nameNode = (RemoteInterfaces) LocateRegistry.getRegistry("172.28.128.3").lookup("NameNode");
								break;
							} catch (Exception e1) {
								
							}
						}
					}	
				}
			}
		});
		Thread blockReportThread = new Thread(new Runnable() {
			public void run() {
				while(true) {
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						System.err.println("Interrupted from sleep");
					}
					try {
						sendBlockReport();	
					} catch (Exception e) {
						System.err.println("Err sending Block Report : " + e.toString());
						while(true) {
							try {
								nameNode = (RemoteInterfaces) LocateRegistry.getRegistry("172.28.128.3").lookup("NameNode");
								break;
							} catch (Exception e1) {
								
							}
						}
					}
				}
			}
		});
		
		heartBeatThread.start();
		blockReportThread.start();
		
	}
	
	public byte[] writeBlock(byte[] message) {
		Hdfs.WriteBlockResponse.Builder response = Hdfs.WriteBlockResponse.newBuilder();
		try {
			Hdfs.WriteBlockRequest writeBlockRequest;
			writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(message);
			String data = null;
			try {
				data = new String(ByteString.copyFrom(writeBlockRequest.getDataList()).toByteArray(), "UTF-8");
			} catch (Exception e) {
				System.out.println("Error in encoding");
				data = "";
			}

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
			Hdfs.ReadBlockRequest readBlockRequest;
			readBlockRequest = Hdfs.ReadBlockRequest.parseFrom(message);
			stmt.execute("use hdfs");
			PreparedStatement pstmt = con.prepareStatement("select data from datablock where blocknum = ?");
			pstmt.setInt(1, readBlockRequest.getBlockNumber());
			ResultSet rs = pstmt.executeQuery();
			String dt = null;
			while(rs.next())
			{
				dt = rs.getString(1);
			}
			if(dt == null)
				readBlockResponse.setStatus(1);
			else {
				readBlockResponse.setStatus(0);
				readBlockResponse.addData(ByteString.copyFromUtf8(dt));
			}	
				
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
		nameNodeIp = "172.28.128.3";
		/*if(args.length < 1) {
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
		}*/
		
		System.setProperty("java.rmi.server.hostname", "10.1.40.121");
		
		try {
			LocateRegistry.createRegistry(1099);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			Registry localRegistry = LocateRegistry.getRegistry();
			localRegistry.rebind("DataNode", new DataNode());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Booted DataNode...");
	}
	
	public static void sendHeartBeat() throws RemoteException {
		nameNode.heartBeat(Hdfs.HeartBeatRequest.newBuilder().setId(myId).build().toByteArray());
	}
	
	public static void sendBlockReport() throws RemoteException {
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
		Hdfs.DataNodeLocation.Builder myLocation = Hdfs.DataNodeLocation.newBuilder();
		myLocation.setIp(myIp);
		myLocation.setPort(myPort);
		Hdfs.DataNodeLocation tempLocation = myLocation.build();
		blockReport.setLocation(tempLocation);
		nameNode.blockReport(blockReport.build().toByteArray());
	}
}
