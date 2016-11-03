package phaseI;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.List;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.*;
import com.google.protobuf.ByteString;

import phaseI.Hdfs;
import phaseI.Hdfs.BlockLocations;
import phaseI.Hdfs.DataNodeLocation;
import phaseI.Hdfs.WriteBlockResponse;

public class Client {
	private static Integer blockSize = 32000000;
	private static Registry registry = null;
	private static RemoteInterfaces nameNode = null;
	private static String host = null;											// It should contain the address of Namenode
	
	public static void main(String[] args) throws NotBoundException, IOException{

		try {
			registry = LocateRegistry.getRegistry(host);
			nameNode = (RemoteInterfaces) registry.lookup("NameNode");
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
			System.exit(1);
		}
		
		Scanner scan = new Scanner(System.in);
		boolean quit = false;
		while(true){
			System.out.print("~$>");
			String input = scan.nextLine();
			String[] inputArray = input.split(" ");
			if(inputArray.length < 1){
				System.out.println("Please provide command");
				continue;
			}
			
			switch (inputArray[0]){
			case "get":
				if(inputArray.length <= 1 ) {
					quit = true;
					System.err.println("No Filename given");
					break;
				}
				getFile(inputArray[1]);
				break;
			case "put":
				if(inputArray.length <= 1 ) {
					quit = true;
					System.err.println("No Filename given");
					break;
				}
				putFile(inputArray[1]);
				break;
			case "list":
				listFiles();
				break;
			case "quit":
				System.out.println("Going to quit :)");
				quit = true;
				break;
			default :
				System.out.println("Undefined command");
				break;
				
			}
			if(quit) break;
			
		}
		scan.close();
	}
	
	public static void getFile(String fileName) throws NotBoundException, IOException {
		int handle = 0;
		byte[] encoded_response = null;
		Hdfs.OpenFileResponse response = null;
		int status = -1;
		
		Hdfs.OpenFileRequest.Builder request = Hdfs.OpenFileRequest.newBuilder();
		request.setFileName(fileName);
		request.setForRead(true);
		Hdfs.OpenFileRequest encoded_req = request.build();
		
		encoded_response = nameNode.openFile(encoded_req.toByteArray());
		response = Hdfs.OpenFileResponse.parseFrom(encoded_response);
		
		status = response.getStatus();
		if(status != 0) {
			System.out.println("Some error occurred");
			return;
		}
		
		handle = response.getHandle();
		ArrayList<Integer> blocks = (ArrayList<Integer>) response.getBlockNumsList();
		
		Hdfs.BlockLocationRequest.Builder locationRequest = Hdfs.BlockLocationRequest.newBuilder();
		locationRequest.addAllBlockNums(blocks);
		Hdfs.BlockLocationRequest encoded_locationRequest = locationRequest.build();
		
		byte[] encodedLocationResponse = nameNode.blockLocations(encoded_locationRequest.toByteArray());
		
		Hdfs.BlockLocationResponse blockLocationResponse = Hdfs.BlockLocationResponse.parseFrom(encodedLocationResponse);
		if( blockLocationResponse.getStatus() != 0) {
			System.err.println("Err occured");
			return ;
		}
		
		FileOutputStream fileWriter = new FileOutputStream(fileName);
		for (BlockLocations block : blockLocationResponse.getBlockLocationsList()) {
			
			for (DataNodeLocation node : block.getLocationsList()) {
				DataNodeRemoteInterfaces dataNode = (DataNodeRemoteInterfaces) LocateRegistry.getRegistry(node.getIp(), node.getPort()).lookup("DataNode");
				Hdfs.ReadBlockRequest.Builder readBlockRequest = Hdfs.ReadBlockRequest.newBuilder();
				readBlockRequest.setBlockNumber(block.getBlockNumber());
				byte[] readBlockResponse = dataNode.readBlock(readBlockRequest.build().toByteArray());
				if (Hdfs.ReadBlockResponse.parseFrom(readBlockResponse).getStatus() != 0){
					System.err.println("Err occured Trying next ...");
				} else {
					fileWriter.write(Hdfs.ReadBlockResponse.parseFrom(readBlockResponse).getData(0).toByteArray());
					break;
				}
			}
		}
		fileWriter.close();
	}
	
	public static void putFile(String fileName) throws NotBoundException, IOException {
		int handle = -1;
		Hdfs.OpenFileRequest.Builder openFileRequest = Hdfs.OpenFileRequest.newBuilder();
		int bytesRead = 0;
		InputStream fStream = new FileInputStream(fileName);
		byte[] fileChunk = new byte[blockSize];
		
		openFileRequest.setForRead(false);
		openFileRequest.setFileName(fileName);
		
		Path path = Paths.get(fileName);
		
		if(!Files.isReadable(path)) {
			System.err.println("Err: File not readable");
			return;
		}
		if(!Files.exists(path)) {
			System.err.println("Err: File doesn't exist");
			return;
		}
		if(Files.isDirectory(path)) {
			System.err.println("Err: It is a directory");
		}
		
		byte[] encodedOpenFileResponse = nameNode.openFile(openFileRequest.build().toByteArray());
		if (Hdfs.OpenFileResponse.parseFrom(encodedOpenFileResponse).getStatus() != 0) {
			System.err.println("Err occurred");
			return;
		}
		
		handle = Hdfs.OpenFileResponse.parseFrom(encodedOpenFileResponse).getHandle();
		
		
		while( (bytesRead = fStream.read(fileChunk)) != -1) {
			Hdfs.AssignBlockRequest.Builder assignBlockRequest = Hdfs.AssignBlockRequest.newBuilder();
			assignBlockRequest.setHandle(handle);
			byte[] assignBlockResponse = nameNode.assignBlock(assignBlockRequest.build().toByteArray());
			
			if (Hdfs.AssignBlockResponse.parseFrom(assignBlockResponse).getStatus() != 0) {
				System.err.println("Err occurred");
				return;
			}
			
			Hdfs.BlockLocations blockLocations = Hdfs.AssignBlockResponse.parseFrom(assignBlockResponse).getNewBlock();
			ArrayList<Hdfs.DataNodeLocation> locationsToReplicate = (ArrayList<Hdfs.DataNodeLocation>) blockLocations.getLocationsList();
			Hdfs.WriteBlockRequest.Builder writeBlockRequest = Hdfs.WriteBlockRequest.newBuilder();
			writeBlockRequest.setBlockInfo(blockLocations);
			writeBlockRequest.addData(ByteString.copyFrom(fileChunk)); // Check the case when fileChunk is not full
			boolean gotDataNode = false;
			DataNodeRemoteInterfaces dn = null;
			
			for (Hdfs.DataNodeLocation tempLocation : locationsToReplicate) {
				
				try {
					dn = (DataNodeRemoteInterfaces) LocateRegistry.getRegistry(tempLocation.getIp(), tempLocation.getPort());
					gotDataNode = true;
				} catch (Exception e) {
					continue;
				}
				
				byte[] writeBlockResponse = dn.writeBlock(writeBlockRequest.build().toByteArray());
				if (Hdfs.WriteBlockResponse.parseFrom(writeBlockResponse).getStatus() != 0) {
					System.err.println("Err occurred");
					gotDataNode = false;
					continue;
				} else {
					break;
				}
			}
			
			if(!gotDataNode) {
				System.err.println("Some err occurred :(");
				return;
			}
		}
		
		fStream.close();
		
		byte[] closeFileResponse = nameNode.closeFile(Hdfs.CloseFileRequest.newBuilder().setHandle(handle).build().toByteArray());
		if (Hdfs.CloseFileResponse.parseFrom(closeFileResponse).getStatus() != 0) {
			System.err.println("Some Err occurred :(");
			return;
		}
	}
	
	public static void listFiles() throws NotBoundException, IOException {
		byte[] encodedListFileResponse = nameNode.listFile(Hdfs.ListFilesRequest.newBuilder().build().toByteArray());
		if (Hdfs.ListFilesResponse.parseFrom(encodedListFileResponse).getStatus() != 0) {
			System.err.println("Some err occurred ");
			return;
		}
		Set<String> fileNames = (Set<String>) Hdfs.ListFilesResponse.parseFrom(encodedListFileResponse).getFileNamesList();
		for (String fileName : fileNames) {
			System.out.println(fileName);
		}
	}
}
