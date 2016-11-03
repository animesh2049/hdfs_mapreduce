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

import com.google.protobuf.InvalidProtocolBufferException;

public class DataNode implements DataNodeRemoteInterfaces {

	
	
	private static String persistanceFile = "datanode.conf";
	
	public DataNode(){}
	
	public byte[] writeBlock(byte[] message) {
		Hdfs.WriteBlockResponse.Builder response = Hdfs.WriteBlockResponse.newBuilder();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306","root","ishi2002");
			Statement stmt=con.createStatement();
			stmt.executeUpdate("create database if not exists blockdata");
			stmt.execute("use blockdata");
			stmt.executeUpdate("create table if not exists datablock(blocknum int,data longtext,primary key(blocknum))");
			WriteBlockRequest writeBlockRequest;
			writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(message);
			String data=new String();
			data=writeBlockRequest.getData(0).toString();
			
			PreparedStatement pstmt=con.prepareStatement("insert into datablock(blocknum,data) values(?,?)");
			pstmt.setInt(1, writeBlockRequest.getBlockInfo().getBlockNumber());
			pstmt.setString(2, data);
			pstmt.executeUpdate();

		//	Hdfs.WriteBlockResponse.Builder response = Hdfs.WriteBlockResponse.newBuilder();
			response.setStatus(0);
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			response.setStatus(1);
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			response.setStatus(1);
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			response.setStatus(1);
			// TODO Auto-generated catch block
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
			Class.forName("com.mysql.jdbc.Driver");
			Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306","root","ishi2002");
			Statement stmt=con.createStatement();
			stmt.execute("use blockdata");
			PreparedStatement pstmt=con.prepareStatement("select data from datablock where blocknum = ?");
			pstmt.setInt(1, readBlockRequest.getBlockNumber());
		//	pstmt.setString(2, data);
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
				
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			readBlockResponse.setStatus(1);
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			readBlockResponse.setStatus(1);
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			readBlockResponse.setStatus(1);
			e.printStackTrace();
		}
		Hdfs.ReadBlockResponse finalRes=readBlockResponse.build();
		return finalRes.toByteArray();
	}
	
	public static void main(String[] args){
		
	}
	
}
