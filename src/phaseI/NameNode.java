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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Collections;

import phaseI.Hdfs.DataNodeLocation;


public class NameNode extends UnicastRemoteObject implements RemoteInterfaces {
	private static final long serialVersionUID = 1L;
	private static int handle = 0;
	private static int blockNumber = 0;
	private static HashMap<String, Integer> tempHandler, finalHandler;
	private static HashMap<Integer, ArrayList<Integer>> handleToBlocks;
	private static HashMap<Integer, HashSet<DataNodeLocation>> blockToReplicas;
	private static String persistanceFile = "namenodefile.conf";
	private static HashSet<Integer> aliveDataNode;
	private static HashMap<Integer, DataNodeLocation> idToDataNode;
	private static HashMap<Integer, ArrayList<Integer>> idToBlock;
	private static Lock lock1, lock2, lock3, lock4, lock5;
	private static String myIp, interfaceToConnect;
	
	public NameNode() throws NumberFormatException, IOException, RemoteException {
		super();
		String line;
		tempHandler = new HashMap<String, Integer>();
		finalHandler = new HashMap<String, Integer>();
		handleToBlocks = new HashMap<Integer, ArrayList<Integer>>();
		blockToReplicas = new HashMap<Integer, HashSet<DataNodeLocation>>();
		aliveDataNode = new HashSet<Integer>();
		idToDataNode = new HashMap<Integer, DataNodeLocation>();
		idToBlock = new HashMap<Integer, ArrayList<Integer>>();
		lock1 = new ReentrantLock();
		lock2 = new ReentrantLock();
		lock3 = new ReentrantLock();
		lock4 = new ReentrantLock();
		lock5 = new ReentrantLock();
		
		InputStream fStream = null;
		try {
			fStream = new FileInputStream(persistanceFile);
		} catch (FileNotFoundException e1) {
			System.err.println("Config file not found :( creating new file");
			File confFile = new File(persistanceFile);
			confFile.createNewFile();
		}
		
		InputStreamReader fStreamReader = new InputStreamReader(fStream, Charset.forName("UTF-8"));
		BufferedReader fBufferReader = new BufferedReader(fStreamReader);
		
		while( (line = fBufferReader.readLine()) != null) {
			String[] splittedString = line.split("--");
			finalHandler.put(splittedString[0], Integer.parseInt(splittedString[1]));
			String[] blockString = splittedString[2].split(",");
			ArrayList<Integer> blockNums = new ArrayList<Integer>();
			for(String tempString : blockString) {
				blockNums.add(Integer.parseInt(tempString));
			}
			
			handleToBlocks.put(Integer.parseInt(splittedString[1]), blockNums);
		}
		Integer maxInteger = 0;
		for (ArrayList<Integer> temp1 : handleToBlocks.values()) {
			Integer temp2 = Collections.max(temp1);
			if(temp2 > maxInteger)
				maxInteger = temp2;
		}
		blockNumber = maxInteger;
		
		try {
			fStream.close();
			fStreamReader.close();
			fBufferReader.close();
		} catch (IOException e) {
			System.err.println("Err msg : " + e.toString());
			e.printStackTrace();
		}
	}
	
	private static int commit(int fileHandle) throws IOException {
		String fileName = "";
		for(String temp : tempHandler.keySet()) {
			if(tempHandler.get(temp) == fileHandle) {
				fileName = temp;
				break;
			}
		}
		
		lock3.lock();
		finalHandler.put(fileName, fileHandle);
		lock3.unlock();
		
		String toWrite = fileName + "--" + fileHandle + "--";
		for(Integer block : handleToBlocks.get(fileHandle)) {
			toWrite += block + ",";
		}
		toWrite = toWrite.substring(0, toWrite.length()-1);
		
	    BufferedWriter bw = null;
	    
	    lock1.lock();
	    try {					
	    	bw = new BufferedWriter(new FileWriter(persistanceFile, true));
	    	bw.write(toWrite);
	    	bw.newLine();
	    	bw.flush();
	    	bw.close();
	    } catch (Exception e) {
	    	System.err.println("Err msg : " + e.toString());
	    	lock1.unlock();
	    	return 1;
	    }
	    lock1.unlock();
	    
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
			if (finalHandler.containsKey(fileName)) {
				responseHandle = finalHandler.get(fileName);
				ArrayList<Integer> blocks = handleToBlocks.get(responseHandle);
				response.addAllBlockNums(blocks);
				response.setStatus(0);
				response.setHandle(responseHandle);
			} else {
				response.setStatus(1);
			}
		} else {  // For writing
			lock2.lock();
			responseHandle = ++handle;
			tempHandler.put(fileName, handle);
			handleToBlocks.put(handle, new ArrayList<Integer>());
			lock2.unlock();
			
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
		
		lock5.lock();
		int tempBlockNumber = ++blockNumber;
		handleToBlocks.get(tempHandle).add(tempBlockNumber);
		lock5.unlock();
		
		int temp1 = randomNumber.nextInt(aliveDataNode.size());
		int temp2;
		while ( (temp2 = randomNumber.nextInt(aliveDataNode.size())) == temp1) {
			continue;
		}
		
		int tempNodeId1 = (int) aliveDataNode.toArray()[temp1];
		int tempNodeId2 = (int) aliveDataNode.toArray()[temp2];
		ArrayList<Hdfs.DataNodeLocation> tempDataNodeLocations =  new ArrayList<Hdfs.DataNodeLocation>();
		Hdfs.DataNodeLocation dn1 = idToDataNode.get(tempNodeId1);
		Hdfs.DataNodeLocation dn2 = idToDataNode.get(tempNodeId2);
		HashSet<Hdfs.DataNodeLocation> myloc = new HashSet<Hdfs.DataNodeLocation>();

		myloc.add(dn1); myloc.add(dn2);
		tempDataNodeLocations.add(idToDataNode.get(tempNodeId1));
		tempDataNodeLocations.add(idToDataNode.get(tempNodeId2));
		
		lock5.lock();
		blockToReplicas.put(handle, myloc);
		lock5.unlock();
			
		Hdfs.BlockLocations.Builder tempBlockLocations = Hdfs.BlockLocations.newBuilder();
		tempBlockLocations.addAllLocations(tempDataNodeLocations);
		tempBlockLocations.setBlockNumber(tempBlockNumber);
		
		Hdfs.AssignBlockResponse.Builder tempResponse = Hdfs.AssignBlockResponse.newBuilder();
		tempResponse.setNewBlock(tempBlockLocations);
		tempResponse.setStatus(0);
		return tempResponse.build().toByteArray();
	}
	
	public byte[] listFile(byte[] message) {
		Hdfs.ListFilesResponse.Builder listFileResponse = Hdfs.ListFilesResponse.newBuilder();
		listFileResponse.addAllFileNames(finalHandler.keySet());
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
		
		ArrayList<Integer> blocks = new ArrayList<Integer>(request.getBlockNumsList()); 
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
		
		/*Inet4Address inetAddress = null;
		
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
			System.out.println("Error starting rmiregistry");
			System.exit(1);	
		}
		
			try {
				Registry localRegistry = LocateRegistry.getRegistry();
				localRegistry.rebind("NameNode", new NameNode());
			} catch (Exception e) {
				System.out.println("Server Err : " + e.toString());
			}
		System.out.println("Booted Server");
	}
	
	public byte[] heartBeat(byte[] message) {
		int id;
		try {
			id = Hdfs.HeartBeatRequest.parseFrom(message).getId(); // Put this in a thread and this operation should be inside lock
			aliveDataNode.add(id);
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
			blockNums = new ArrayList<Integer>(reportRequest.getBlockNumbersList());	
		} catch (Exception e) {
			System.err.println("Err msg : " + e.toString());
		}
		
		lock4.lock();
		idToDataNode.put(dataNodeId, dataNodeLocation);
		idToBlock.put(dataNodeId, blockNums);
		lock4.unlock();

		for(Integer temp : blockNums) {
                        if(blockToReplicas.containsKey(temp)) {
                                lock4.lock();
                                blockToReplicas.get(temp).add(dataNodeLocation);
                                lock4.unlock();
                        } else {
                                HashSet<Hdfs.DataNodeLocation> temp1 = new HashSet<Hdfs.DataNodeLocation>();
                                temp1.add(dataNodeLocation);
                                lock4.lock();
                                blockToReplicas.put(temp, temp1);
                                lock4.unlock();
                        }
                }
		
		return Hdfs.BlockReportResponse.newBuilder().addStatus(0).build().toByteArray();
	}
}
