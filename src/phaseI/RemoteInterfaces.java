package phaseI;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteInterfaces extends Remote{
	byte[] openFile(byte[] message) throws RemoteException;
	byte[] closeFile(byte[] message) throws RemoteException;
	byte[] assignBlock(byte[] message) throws RemoteException;
	byte[] listFile(byte[] message) throws RemoteException;
	byte[] blockLocations(byte[] message) throws RemoteException;
	byte[] heartBeat(byte[] message) throws RemoteException;
	byte[] blockReport(byte[] message) throws RemoteException;
}
