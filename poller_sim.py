import random
import heapq
import statistics

POLL_SIZE_FROM_NS = 16
NUM_NAMESPACES = 160
MAX_INFLIGHTS_HWM = 128
MAX_INFLIGHTS_LWM = 100
MAX_WORKING_SET_SIZE = 6
IOS_PASSED_TILL_COMPLETE = 1000000

class PriorityQueue(object):
    def __init__(self):
        self._queue = []

    def push(self, item, priority):
        heapq.heappush(self._queue, (priority, item))

    def pop(self):
        return heapq.heappop(self._queue)[-1]

    def num_elelments(self):
        return len(self._queue)

class IO(object):
    def __init__(self, ns):
        self.ns = ns


class Namespace(object):
    def __init__(self):
        self.curr_inflight = 0
        self.batches_sizes = [0]

    def close_batch(self):
        self.batches_sizes.append(0)

    def get_new_sqe(self):
        self.batches_sizes[-1] += 1
        self.curr_inflight += 1
        return (IO(self), self.curr_inflight)

    def return_sqe(self):
        self.curr_inflight -= 1
        return self.curr_inflight

class System(object):
    def __init__(self):
        self.namespaces = [Namespace() for _ in range(NUM_NAMESPACES)]
        self.active_namespace_indx = 0
        self.curr_working_set_size = 0
        self.q = PriorityQueue()
        self.T = 0
        self.io_returned(None)

    def get_curr_ns_advnace_if_needed(self):
        if (self.namespaces[self.active_namespace_indx].batches_sizes[-1] >= POLL_SIZE_FROM_NS) and  (self.curr_working_set_size < MAX_WORKING_SET_SIZE):
            self.namespaces[self.active_namespace_indx].close_batch()
            self.active_namespace_indx = (self.active_namespace_indx + 1) % NUM_NAMESPACES 
        return self.namespaces[self.active_namespace_indx] 

    def get_io_serve_time(self):
        while True:
            t = random.gauss(200, 10)
            if t > 0:
                return t
        
    def get_completion_time(self):
        return self.T + self.get_io_serve_time()

    def io_returned(self, io):
        if io:
            if (io.ns.return_sqe() == 0):
                self.curr_working_set_size -= 1
        if self.q.num_elelments() <= MAX_INFLIGHTS_LWM:
            while self.q.num_elelments() < MAX_INFLIGHTS_HWM:
                active_ns = self.get_curr_ns_advnace_if_needed()
                (io, curr_ns_inflight) = active_ns.get_new_sqe()
                if curr_ns_inflight == 1:
                    self.curr_working_set_size += 1
                io_completion_time = self.get_completion_time()
                self.q.push((io, io_completion_time), io_completion_time)

    def run(self):
        completed_ios = 0
        while completed_ios < IOS_PASSED_TILL_COMPLETE:
            (io, time) = self.q.pop()
            self.T = time
            self.io_returned(io)
            completed_ios += 1
        print([(statistics.mean(ns.batches_sizes[0:-1]), statistics.stdev(ns.batches_sizes[0:-1])) for ns in self.namespaces])

System().run() 
 




        
	    
	
			
	    
	    	    		

	
	
	
	



	
	

	











        
