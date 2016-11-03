package phaseI;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeRemoteInterfaces extends Remote {
	byte[] writeBlock(byte[] message) throws RemoteException;
	byte[] readBlock(byte[] message) throws RemoteException;
}
