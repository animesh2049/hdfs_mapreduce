package phaseII;
import java.util.*;
import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import phaseII.MapReduce;
import phaseII.MapReduce.*;
public class JobClient{
	private static Registry registry = null;
	private static RemoteInterfaces jobTracker = null;
	public static void main(String args[]){
		try {
			registry = LocateRegistry.getRegistry("172.28.128.3");
			jobTracker = (RemoteInterfaces) registry.lookup("JobTracker");
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
			System.exit(1);
		}
		
		Scanner scan = new Scanner(System.in);
		while(true){
			System.out.print("~$>");
			String input = scan.nextLine();
			String[] inputArray = input.split(" ");
			if(inputArray.length < 1){
				System.out.println("Please provide command");
				continue;
			}
			if(inputArray[0] == "quit")
				break;
			else if(inputArray.length != 5)
			{
				System.out.println("incorrect command");
				continue;
			}
			String mapName = inputArray[0];
			String reducerName = inputArray[1];
			String inputFile = inputArray[2];
			String outputFile = inputArray[3];
			int numReducer = Integer.parseInt(inputArray[4]);
			int jobId = submitJob(mapName, reducerName, inputFile, outputFile, numReducer);
			int status = 1;
			whike(status == 1)
				status = statusJob(jobId);
		}
	}
	public static int submitJob(String mapName, String reducerName, String inputFile, String outputFile, int numReducer){
		byte[] encoded_response = null;
		MapReduce.JobSubmitResponse response = null;
		MapReduce.JobSubmitRequest.Builder request = MapReduce.JobSubmitRequest.newBuilder();
		request.setMapName = mapName;
		request.setReducerName = reducerName;
		request.setInputFile = inputFile;
		request.setOutputFile = outputFile;
		request.setNumReduceTasks = numReducer;	
		MapReduce.JobSubmitRequest encoded_req = request.build();
		try {
			encoded_response = jobTracker.jobSubmit(encoded_req.toByteArray());	
		} catch (Exception e) {
			while(true) {
				try {
					jobTracker = (RemoteInterfaces) LocateRegistry.getRegistry("172.28.128.3").lookup("JobTracker");
					encoded_response = jobTracker.jobSubmit(encoded_req.toByteArray());
					break;
				} catch (Exception e1) {}
			}
		}
		response = MapReduce.JobSubmitResponse.parseFrom(encoded_response);
		int jobid = null;
		if(response.getStatus==0)
			jobid=response.getJobId();
		return jobid;
	}
	public static void statusJob(int jobId)
	{
		byte[] encoded_response = null;
		MapReduce.JobStatusResponse response = null;
		MapReduce.JobStatusRequest.Builder request = MapReduce.JobStatusRequest.newBuilder();
		request.setJobId(jobId);
		MapReduce.JobStatusRequest encoded_req = request.build();
		try {
			encoded_response = jobTracker.getJobStatus(encoded_req.toByteArray());	
		} catch (Exception e) {}
		response = MapReduce.JobStatusResponse.parseFrom(encoded_response);
		return response.getStatus();
	}
}