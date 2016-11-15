package phaseII;

import phaseI.RemoteInterfaces;
import phaseII.MapReduce;
import phaseI.Hdfs;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker extends UnicastRemoteObject implements IJobTracker{
	
	private static Integer jobId = 0;
	private static Integer taskId = 0;
	private static ConcurrentLinkedDeque<MapReduce.MapTaskInfo> mapTaskQueue;
	private static ConcurrentLinkedDeque<MapReduce.ReducerTaskInfo> reduceTaskQueue;
	private static Lock lock1, lock2;
	private static HashMap<Integer, Job> jobIdToJob;
	private static RemoteInterfaces nameNode = null;
	
	
	public JobTracker() throws RemoteException {
		super();
		mapTaskQueue = new ConcurrentLinkedDeque<MapReduce.MapTaskInfo>();
		reduceTaskQueue = new ConcurrentLinkedDeque<MapReduce.ReducerTaskInfo>();
		lock1 = new ReentrantLock();
		lock2 = new ReentrantLock();
		jobIdToJob = new HashMap<Integer, Job>();
		
		try {
			nameNode = (RemoteInterfaces) LocateRegistry.getRegistry("172.28.128.3").lookup("NameNode");
		} catch (Exception e){
			System.err.println("Err msg : " + e.toString());
			System.exit(1);
		}
		
	}
	
	public static void createMapTask(Integer tempJobId) throws InvalidProtocolBufferException, RemoteException {
		Job newJob = jobIdToJob.get(tempJobId);
		String fileName = newJob.getInputFileName();
		Hdfs.OpenFileResponse tempResponse = Hdfs.OpenFileResponse.parseFrom(nameNode.openFile(Hdfs.OpenFileRequest.newBuilder().setFileName(fileName).setForRead(true).build().toByteArray()));
		
		if(tempResponse.getStatus() != 0) {
			System.out.println("Some error occurred in reading from hdfs :(");
			return;
		}
		
		Hdfs.BlockLocationResponse blockLocationResponse = Hdfs.BlockLocationResponse.parseFrom(nameNode.blockLocations(Hdfs.BlockLocationRequest.newBuilder().addAllBlockNums(tempResponse.getBlockNumsList()).build().toByteArray()));
		
		if(blockLocationResponse.getStatus() != 0) {
			System.out.println("Some error in getting locations :(");
			return ;
		}
		Integer tempTaskId = 0;
		for(Hdfs.BlockLocations blockLocations : blockLocationResponse.getBlockLocationsList()) {
			tempTaskId = getTaskId();
			mapTaskQueue.add(MapReduce.MapTaskInfo.newBuilder().setMapName(newJob.getMapperName()).setJobId(tempJobId).setTaskId(tempTaskId).addInputBlocks(MapReduce.BlockLocations.parseFrom(blockLocations.toByteArray())).build());
		}
		
		newJob.setTotalMapper(blockLocationResponse.getBlockLocationsCount());
	}
	
	public void createReduceTask(Integer tempJobId) {
		Job tempJob = jobIdToJob.get(tempJobId);
		Integer reducerNumber = tempJob.getReducerNumber();
		Integer mapOutputNumber = tempJob.getMapOutputFiles().size();
	}
	
	public static Integer getJobId(){
		lock1.lock();
		++jobId;
		lock1.unlock();
		return new Integer(jobId);
	}
	
	public static Integer getTaskId(){
		lock2.lock();
		++taskId;
		lock2.unlock();
		return new Integer(taskId);
	}
	
	public static void main(String[] args) {
		System.setProperty("java.rmi.server.hostname", "10.1.40.121");
		
		try {
			LocateRegistry.createRegistry(1099);
		} catch (Exception e) {
			System.out.println("Error starting rmiregistry");	
		}
		
		try {
			Registry localRegistry = LocateRegistry.getRegistry();
			localRegistry.rebind("JobTracker", new JobTracker());
		} catch (Exception e) {
			System.out.println("Server Err : " + e.toString());
		}
		System.out.println("Booted JobTracker");
	}
	
	public byte[] jobSubmit(byte[] message) {
		try {
			MapReduce.JobSubmitRequest req = MapReduce.JobSubmitRequest.parseFrom(message);
			Job newJob = new Job();
			Integer myJobId = getJobId();
			newJob.setMapName(req.getMapName());
			newJob.setReducerName(req.getReducerName());
			newJob.setInputFile(req.getInputFile());
			newJob.setOutFile(req.getOutputFile());
			newJob.setReducerNumber(req.getNumReduceTasks());
			newJob.setToGrep(req.getTogrep());
			jobIdToJob.put(myJobId, newJob);
			createMapTask(myJobId);
			
			return MapReduce.JobSubmitResponse.newBuilder().setStatus(0).setJobId(myJobId).build().toByteArray();	
		} catch(Exception e) {
			System.out.println("Error while getting job");
			return MapReduce.JobSubmitResponse.newBuilder().setStatus(0).build().toByteArray();
		}
	}
	
	public byte[] getJobStatus(byte[] message) {
		
	}

	public byte[] heartBeat(byte[] message) {
		Job tempJob = null;
		MapReduce.HeartBeatResponse.Builder heartBeatResponse = MapReduce.HeartBeatResponse.newBuilder();
		try {
			MapReduce.HeartBeatRequest heartBeat = MapReduce.HeartBeatRequest.parseFrom(message);
			for(MapReduce.MapTaskStatus mapStatus : heartBeat.getMapStatusList()) {
				tempJob = jobIdToJob.get(mapStatus.getJobId());
				if(mapStatus.getTaskCompleted()) {
					tempJob.addFinishedMapTask();
					tempJob.addOutputFile(mapStatus.getMapOutputFile());
				}
				if(tempJob.getMapperNumber() == tempJob.getFinishedMapTask()) {
					createReduceTask(mapStatus.getJobId());
				}
			}
			
			for(MapReduce.ReduceTaskStatus reduceStatus : heartBeat.getReduceStatusList()) {
				tempJob = jobIdToJob.get(reduceStatus.getJobId());
				if(reduceStatus.getTaskCompleted()) {
					tempJob.addFinishedReduceTask();
				}
				if(tempJob.getReducerNumber() == tempJob.getFinishedReduceTask()) {
					tempJob.setIsComplete();
				}
			}
			
			Integer freeMapSlots = heartBeat.getNumMapSlotsFree();
			Integer freeReduceSlots = heartBeat.getNumReduceSlotsFree();
			
			while((freeMapSlots > 0) && (mapTaskQueue.size() > 0)) {
				heartBeatResponse.addMapTasks(mapTaskQueue.poll());
				freeMapSlots--;
			}
			
			while((freeReduceSlots > 0) && (reduceTaskQueue.size() > 0)) {
				heartBeatResponse.addReduceTasks(reduceTaskQueue.poll());
				freeReduceSlots--;
			}
			
			heartBeatResponse.setStatus(0);
				
		} catch (Exception e) {
			System.out.println("Some error while checking heartbeat :(");
			heartBeatResponse.setStatus(1);
		}
		
		return heartBeatResponse.build().toByteArray();
	}
}
