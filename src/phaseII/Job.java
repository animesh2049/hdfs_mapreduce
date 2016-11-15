package phaseII;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Job {
	private String mapName, reducerName, inputFile;
	private String outputFile, toGrep;
	private Integer totalReducers, totalMappers;
	private Integer mapTasksStarted, reduceTasksStarted;
	private Integer finishedMapTasks, finishedReduceTasks;
	private Lock lock1, lock2;
	private boolean isComplete ;
	private ArrayList<String> mapOutputFiles;
	
	public Job(){
		super();
		this.mapName = new String();
		this.reducerName = new String();
		this.inputFile = new String();
		this.outputFile = new String();
		this.toGrep = new String();
		this.reduceTasksStarted = 0;
		this.mapTasksStarted = 0;
		this.finishedMapTasks = this.finishedReduceTasks = 0;
		this.lock1 = new ReentrantLock();
		this.lock2 = new ReentrantLock();
		this.isComplete = false;
		this.mapOutputFiles = new ArrayList<String>();
	}
	
	public void addOutputFile(String fileName) {
		this.mapOutputFiles.add(fileName);
	}
	
	public void addFinishedMapTask(){
		lock1.lock();
		++finishedMapTasks;
		lock1.unlock();
	}
	
	public void setIsComplete(){
		this.isComplete = true;
	}
	
	public void addFinishedReduceTask(){
		lock2.lock();
		++finishedReduceTasks;
		lock2.unlock();
	}
	
	public void setMapName(String mapName) {
		this.mapName = mapName;
	}
	
	public void setReducerName(String reducerName) {
		this.reducerName = reducerName;
	}
	
	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}
	
	public void setOutFile(String outFile) {
		this.outputFile = outFile;
	}
	
	public void setToGrep(String toGrep) {
		this.toGrep = toGrep;
	}
	
	public void setReducerNumber(Integer totalReducers) {
		this.totalReducers = totalReducers;
	}
	
	public void setTotalMapper(Integer totalMappers) {
		this.totalMappers = totalMappers;
	}
	
	public Integer getReducerNumber() {
		return this.totalReducers;
	}
	
	public Integer getMapperNumber() {
		return this.totalMappers;
	}
	
	public String getMapperName() {
		return this.mapName;
	}
	
	public String getReducerName() {
		return this.reducerName;
	}
	
	public String getInputFileName() {
		return this.inputFile;
	}
	
	public String getOutputFileName() {
		return this.outputFile;
	}
	
	public Integer getFinishedMapTask() {
		return this.finishedMapTasks;
	}
	
	public Integer getFinishedReduceTask() {
		return this.finishedReduceTasks;
	}
	
	public boolean isJobComplete() {
		return this.isComplete;
	}
	
	public ArrayList<String> getMapOutputFiles() {
		return this.mapOutputFiles;
	}
}
