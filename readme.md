# Build

Compile and test
```
./gradlew build
```

Compile only
```
./gradlew assemble
```

Test
```
./gradlew test
```

For those who use Windows, you should run `gradlew.bat` with the same parameters.

Both IDEA and Eclipse have plugins for Gradle.

Some existing tests need Java 8.


# Code location

`src/main/java` is for word count code.

`src/test/java` is for tests. And `src/test/resources` is for test data.

In most cases, adding or modifying files in other places is not necessary.


# Directions

Interface `Master` and class `WordCount` are already here.
`WordCount` should implement `Master` and have a constructor taking an integer (number of workers)
and array of strings (file paths) as input. There is documentation for every method of `Master`.
You should read them before implementing.

There are two basic tests here.
These tests should be passed before submission (class `TestUtil` can be modified
if there is error due to platform-dependent methods).
You may also want to add more tests, like larger files, unicode content, fault tolerance, etc.

You shouldn't use `Process.isAlive` to detect whether a worker is dead.

#Project Structure:  
The project has the following structure:  
<pre>
project-2-group-2
|__finalout - This is where the final, consolidated file created by the master is stored (if file writing is enabled)
|   
|__out - This is where intermediate files created by workers are stored
|
|__src  
   |__main  
      |__java  
      |  |_WordCount.java - This is the file that contains the code executed by the master node
      |	 |_Worker.java - This is the file that contains the worker code
      |	 |_Utilities.java - This class contains extra helper functions that are utilized in the master and workers.	
      |
      |__test  
</pre>

Description of classes (Master):-
wordCount - This class contains the main thread that starts the program. It is responsible for compiling and creating each worker, and 	     assigning it the correct input file path from the pending files queue. It is also responsible for periodically checking the list of active processes in order to determine which workers are alive and which are not, and depending on the number of files remaining to be processed, spin new workers as required. It also handles the merging of worker output files to create the final consolidated word count.

MasterIO - This class is responsible for handling the master end of the I/O communication with a particular worker. This thread defines the socket connections and monitors the status of the worker's progress, and sends new files to it when required, and collects acknowledgement of completion when the job is finished. It also handles updation of the pending queue, processing map and active workers map as and when required.

Master Heartbeat - This thread is responsible for handling the master end of the heartbeat communication with a particular worker. This thread defines the socket connections and monitors whether a worker is alive or dead by listening for signals from the worker periodically. It is also resposnsible for updating the active workers map if worker failure is detected so that the main thread can apprpriately spin a new worker and kill existing worker process.

Description of classes (Worker):-
worker - This is the main thread within the worker process. It is responsible for handling the I/O and wordcount implementation as well as communication with the masterIO thread. It defines the socket connections, and receives the input and output file paths from the master. It performs the local word count for the assigned file and writes the output to file and places it in the appropriate directory. It continues to write files until there are no files remaining to be written. It is also responsible for creating the heartbeat thread for the worker process.

Worker Heartbeat - This thread is responsible for handling the worker end of the heartbeat communication with the master's corresponding heartbeat thread for that particular worker. This thread defines and establishes the socket connections with the master heartbeat thread, and periodically informs the master that it is alive. 

Description of classes (Utilities):-
Check port availability - This class allows us to find the first available port in order to allocate it to a new worker that may be created due to worker failure. It checks if the port is currently in use, and returns the corresponding boolean value

Custom Comparator - This class allows us to sort the priority queue in descending order based on the frequency of the word appearing in the hashmap.

In addition to these classes, we have also defined Singletons :- activeWorkers, inactiveWorkers, Pending and processing
activeWorkers - A map that keeps track of active worker processes and the corresponding process object
inactiveWorkers - A  set of workerIDs that have been identified to be inactive
pending - A queue that holds all the filepaths that have not yet been processed by any worker
processing - A map that keeps track of all the files currently being processed and the corresponding workerID processing it

In order to run the project natively, please create a txt file named "fileNames.txt", that contains absolute paths for all input files, and place it at the highest directory level within the project folder. Specify the number of workers on line 177 of WordCount.java and run the program. Intermediate files created by workers are stored in the "out" folder, and final consolidated output generated by the master is stored in the "finalout" folder (if file write is enabled)

Workflow of the project :-

Initially when the WordCount class is run, the following steps are exectuted:
1. The master builds an array of filepaths by reading the fileNames.txt, and sets a number of global variables including this array
2. The run method compiles the Worker and assigns it a unique workerID before creating the worker process
3. While creating the worker process, the master finds the first free port and assigns the socket to it. It then builds the worker process, and the corresponding heartbeat and IO threads to handle communications with that particular worker and starts the worker process.
4. Once the worker has been created and started, it creates its own heartbeat thread with the appropriate sockets, and begins communicating with the master heartbeat end point. Simultanelously, it receives the file path from the Master IO thread and begins the local word count. Once the word count is completed, the worker sends acknowledgement for process completeion to its MasterIO handler, at which point, if there are any pending files, the process repeats. If there are no pending files, then the masterIO sends a signal to the worker at which point the worker terminates.
5. If a Master heartbeat thread detects that a worker has died, it adds the corresponding worker Id to the list of inactive processes, and shifts the corresponding file back into the pending queue, which is then allocated to a new worker down the line.
6. The main master thread periodically checks for inactive processes, and spins new workers if there are files remaining to be processed. 
7. When the main master thread observes that both the pending and processing queues are empty, all the workers have stopped, and the master begins merging the output files. It scans the entire "out" directory and builds a hashmap maintaing the counts for each word it encounters. Once the hashmap is built, the entries are fed into a priority queue sorted in descending order based on the frequency and all entries are written to the output stream, and the application terminates. 

