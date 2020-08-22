# MapReduce System

A distributed MapReduce system.

## Basic Ideas

The thorough explanation of a MapReduce System can be found here:
http://nil.csail.mit.edu/6.824/2020/papers/mapreduce.pdf

## Implementation

The main process consists of two stages, Map stage and Reduce stage. Each stage is essentially a loop, which keeps encapsulating Map/Reduce tasks and dispatching them into the Go Channel, until all the tasks are done.

The worker processes retrieve Map/Reduce tasks from the Go Channel by RPC, which is non-blocking. If no task currently available in the Go Channel, the worker process gets nothing. It will wait for a while and then retry. If all the Map/Reduce tasks are down, the main process will dispatch an empty task to the worker processes, as to inform them that the mission is completed and they may exit.

A Goroutine is created everytime a Map/Reduce task is retrieved by a worker process to monitor the status of the task. If a certain amount of time passed (which is determiend by the user) and the task hasn't been completed, it will be regarded as a failed one. Corresponding intermediate files will be cleared by the main process, and the task will be re-dispatched into the Go Channel.
