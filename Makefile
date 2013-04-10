# Makefile for Zookeeper

# Otherwise use Sun's compiler.
JAVA_HOME=/cad2/ece419s/java/jdk1.6.0/
JAVAC=${JAVA_HOME}/bin/javac -source 1.6
JAVADOC=${JAVA_HOME}/bin/javadoc -use -source 1.6 -author -version -link http://java.sun.com/j2se/1.6.0/docs/api/ 
MKDIR=mkdir
RM=rm -rf
CAT=cat

FILES=Barrier.java ClientDriver.java FileServer.java FileServerHandler.java FileServerHandlerThread.java JobPacket.java JobTracker.java JobTrackerHandler.java JobTrackerHandlerThread.java Queue.java SyncPrimitive.java WorkPacket.java Worker.java ZkConnector.java 

build:
  javac -classpath ../lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar ${FILES}

# Create documentation using javadoc
docs:
	${JAVADOC} -d docs *.java

# Clean up
clean: 
	${RM} *.class *~ docs/*

# docs isn't a real target
.PHONY : docs
