killall rmiregistry 2>/dev/null
javac -d bin -cp libraries/protobuf-java-3.1.0.jar:. src/phaseI/{NameNode,Hdfs,RemoteInterfaces,DataNodeRemoteInterfaces}.java
cd bin
rmiregistry &
java -Djava.rmi.server.codebase=file:$PWD/ phaseI.NameNode
