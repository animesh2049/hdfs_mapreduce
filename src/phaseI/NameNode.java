package phaseI;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import phaseI.Hdfs;
import phaseI.Hdfs.DataNodeLocation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class NameNode implements RemoteInterfaces {
	private static int handle = 0;
	private static int blockNumber = 0;
	private static HashMap<String, Integer> handler;
	private static HashMap<Integer, ArrayList<Integer>> handleToBlocks;
	private static HashMap<Integer, HashSet<DataNodeLocation>> blockToReplicas;
	private static String persistanceFile = "namenodefile.conf";
	private static HashSet<Integer> aliveDataNode;
	private static HashMap<Integer, DataNodeLocation> idToDatanode;
	private static HashMap<Integer, ArrayList<Integer>> idToBlock;
	private static Lock lock;
	
	public NameNode() throws RemoteException {
		super();
	}
	
	private static int commit(int fileHandle) throws IOException {
		String fileName = "";
		for(String temp : handler.keySet()) {
			if(handler.get(temp) == fileHandle) {
				fileName = temp;
				break;
			}
		}
		
		String toWrite = fileName + "--" + fileHandle + "--";
		for(Integer block : handleToBlocks.get(fileHandle)) {
			toWrite += block + ",";
		}
		toWrite = toWrite.substring(0, toWrite.length()-1);
		
	    BufferedWriter bw = null;
	    
	    lock.lock();
	    try {					// Do it in lock
	    	bw = new BufferedWriter(new FileWriter(persistanceFile, true));
	    	bw.write(toWrite);
	    	bw.newLine();
	    	bw.flush();
	    	bw.close();
	    } catch (Exception e) {
	    	System.err.println("Err msg : " + e.toString());
	    	lock.unlock();
	    	return 1;
	    }
	    lock.unlock();
	    
	    return 0;
	}
	
	public byte[] openFile(byte[] message){
		Hdfs.OpenFileRequest request;
		int responseHandle = 0;
		String fileName = "";
		boolean forRead = false;
		Hdfs.OpenFileResponse.Builder response = Hdfs.OpenFileResponse.newBuilder();
		
		try {
			request = Hdfs.OpenFileRequest.parseFrom(message);
			fileName = request.getFileName();
			forRead = request.getForRead();
		} catch (Exception e){
			System.err.println("Error msg is : " + e.toString());
		}
		
		if(forRead) {
			if (handler.containsKey(fileName)) {
				responseHandle = handler.get(fileName);
				ArrayList<Integer> blocks = handleToBlocks.get(responseHandle);
				response.addAllBlockNums(blocks);
				response.setStatus(0);
				response.setHandle(responseHandle);
			} else {
				response.setStatus(1);
			}
		} else {  // For writing
			responseHandle = ++handle;
			handler.put(fileName, handle);
			handleToBlocks.put(handle, new ArrayList<Integer>());
			response.setHandle(responseHandle);
			response.setStatus(0);
		}
		
		Hdfs.OpenFileResponse encoded_response = response.build();
		return encoded_response.toByteArray();
	}
	
	public byte[] closeFile(byte[] message) {
		Hdfs.CloseFileRequest request = null;
		
		try {
			request = Hdfs.CloseFileRequest.parseFrom(message);
			int status = commit(request.getHandle()); // do it in another thread no need of locks here
			return Hdfs.CloseFileResponse.newBuilder().setStatus(status).build().toByteArray();
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
			return Hdfs.CloseFileResponse.newBuilder().setStatus(1).build().toByteArray();
		}
	}
	
	public byte[] assignBlock(byte[] message) {
		int tempHandle = 0;
		Random randomNumber = new Random();
		
		try {
			tempHandle = Hdfs.AssignBlockRequest.parseFrom(message).getHandle();
		} catch (Exception e) {
			System.err.println("Err msg : " + e.toString());
		}
		int tempBlockNumber = ++blockNumber;
		handleToBlocks.get(tempHandle).add(tempBlockNumber);
		int temp1 = randomNumber.nextInt(aliveDataNode.size());
		int temp2;
		while ( (temp2 = randomNumber.nextInt(aliveDataNode.size())) == temp1) {
			continue;
		}
		
		int tempNodeId1 = (int) aliveDataNode.toArray()[temp1];
		int tempNodeId2 = (int) aliveDataNode.toArray()[temp2];
		ArrayList<Hdfs.DataNodeLocation> tempDataNodeLocations =  new ArrayList<Hdfs.DataNodeLocation>();
		tempDataNodeLocations.add(idToDatanode.get(tempNodeId1));
		tempDataNodeLocations.add(idToDatanode.get(tempNodeId2));
		Hdfs.BlockLocations.Builder tempBlockLocations = Hdfs.BlockLocations.newBuilder();
		tempBlockLocations.addAllLocations(tempDataNodeLocations);
		
		Hdfs.AssignBlockResponse.Builder tempResponse = Hdfs.AssignBlockResponse.newBuilder();
		tempResponse.setNewBlock(tempBlockLocations);
		tempResponse.setStatus(0);
		return tempResponse.build().toByteArray();
	}
	
	public byte[] listFile(byte[] message) {
		Hdfs.ListFilesResponse.Builder listFileResponse = Hdfs.ListFilesResponse.newBuilder();
		listFileResponse.addAllFileNames(handler.keySet());
		return listFileResponse.build().toByteArray();
	}
	
	public byte[] blockLocations(byte[] message) {
		Hdfs.BlockLocationRequest request = null;
		
		Hdfs.BlockLocationResponse.Builder encoded_response = Hdfs.BlockLocationResponse.newBuilder();
		
		try {
			request = Hdfs.BlockLocationRequest.parseFrom(message);
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
		}
		
		ArrayList<Integer> blocks = (ArrayList<Integer>) request.getBlockNumsList(); 
		for (Integer block : blocks) {
			Hdfs.BlockLocations.Builder temp = Hdfs.BlockLocations.newBuilder();
			temp.addAllLocations(blockToReplicas.get(block));
			temp.setBlockNumber(block);
			encoded_response.addBlockLocations(temp);
		}
		
		encoded_response.setStatus(0);
		Hdfs.BlockLocationResponse finalResponse = encoded_response.build();
		return finalResponse.toByteArray();
	}
	
	public static void main(String[] args) throws IOException {
		String line;
		handler = new HashMap<String, Integer>();
		handleToBlocks = new HashMap<Integer, ArrayList<Integer>>();
		blockToReplicas = new HashMap<Integer, HashSet<DataNodeLocation>>();
		aliveDataNode = new HashSet<Integer>();
		idToDatanode = new HashMap<Integer, DataNodeLocation>();
		idToBlock = new HashMap<Integer, ArrayList<Integer>>();
		lock = new ReentrantLock();
		
		InputStream fStream = new FileInputStream(persistanceFile);
		InputStreamReader fStreamReader = new InputStreamReader(fStream, Charset.forName("UTF-8"));
		BufferedReader fBufferReader = new BufferedReader(fStreamReader);
		
		while( (line = fBufferReader.readLine()) != null) {
			String[] splittedString = line.split("--");
			handler.put(splittedString[0], Integer.parseInt(splittedString[1]));
			String[] blockString = splittedString[2].split(",");
			ArrayList<Integer> blockNums = new ArrayList<Integer>();
			for(String tempString : blockString) {
				blockNums.add(Integer.parseInt(tempString));
			}
			handleToBlocks.put(Integer.parseInt(splittedString[1]), blockNums);
		}
		
		fStream.close();
		fStreamReader.close();
		fBufferReader.close();
		
		try {
			NameNode namenode = new NameNode();
			RemoteInterfaces mystub = (RemoteInterfaces) UnicastRemoteObject.exportObject(namenode, 0);
			Registry localRegistry = LocateRegistry.getRegistry();
			localRegistry.bind("NameNode", mystub);			
		} catch (Exception e){
			System.err.println("Server Exception: " + e.toString());
		}
	}
	
	public byte[] heartBeat(byte[] message) {
		
		try {
			aliveDataNode.add(Hdfs.HeartBeatRequest.parseFrom(message).getId()); // Put this in a thread and this operation should be inside lock	
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
			return Hdfs.HeartBeatResponse.newBuilder().setStatus(1).build().toByteArray();
		}
		
		return Hdfs.HeartBeatResponse.newBuilder().setStatus(0).build().toByteArray();
	}
	
	public byte[] blockReport(byte[] message) {
		int dataNodeId = -1;
		DataNodeLocation dataNodeLocation = null;
		ArrayList<Integer> blockNums = null;
		
		try {
			Hdfs.BlockReportRequest reportRequest = Hdfs.BlockReportRequest.parseFrom(message);
			dataNodeId = reportRequest.getId();
			dataNodeLocation = reportRequest.getLocation();
			blockNums = (ArrayList<Integer>) reportRequest.getBlockNumbersList();	
		} catch (Exception e) {
			System.err.println("Err msg : " + e.toString());
		}
		
		idToDatanode.put(dataNodeId, dataNodeLocation); // Put this in a thread and these two operations should be inside lock
		idToBlock.put(dataNodeId, blockNums);
		
		return Hdfs.BlockReportResponse.newBuilder().addStatus(0).build().toByteArray();
	}
}
