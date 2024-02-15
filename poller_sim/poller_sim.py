import random
import heapq
import statistics
from collections import deque


POLL_SIZE_FROM_NS = 16
NUM_NAMESPACES = 60
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
    def __init__(self, ns, completion_time):
        self.ns = ns
        self.completion_time = completion_time


class Namespace(object):
    def __init__(self, system):
        self.batches_sizes = [0]
        self.inflights_q = deque()
        self.system = system

    def close_batch(self):
        self.batches_sizes.append(0)

    def get_new_sqe(self):
        last_io_comp_time = self.inflights_q[-1].completion_time if len(self.inflights_q) > 0 else self.system.T
        this_io_comp_time = last_io_comp_time + self.get_io_serve_time()
        self.batches_sizes[-1] += 1
        io = IO(self, this_io_comp_time)
        self.inflights_q.append(io)
        return (io, len(self.inflights_q))

    def return_sqe(self, io_to_return):
        next_io_to_finish = self.inflights_q.popleft()
        if next_io_to_finish.completion_time != io_to_return.completion_time:
            raise(Exception("BUGGGGGGGGGG"))
        return len(self.inflights_q)

    def get_io_serve_time(self):
        while True:
            t = random.gauss(200, 10)
            #t = random.expovariate(1 / 200.0)
            if t > 0:
                return t

class System(object):
    def __init__(self):
        self.namespaces = [Namespace(self) for _ in range(NUM_NAMESPACES)]
        self.active_namespace_indx = 0
        self.curr_working_set_size = 0
        self.q = PriorityQueue()
        self.T = 0
        self.io_returned(None)
        self.q.push(10000, 10000)
        self.active_working_sets = []

    def get_curr_ns_advnace_if_needed(self):
        if (self.namespaces[self.active_namespace_indx].batches_sizes[-1] >= POLL_SIZE_FROM_NS) and  (self.curr_working_set_size < MAX_WORKING_SET_SIZE):
            self.namespaces[self.active_namespace_indx].close_batch()
            self.active_namespace_indx = (self.active_namespace_indx + 1) % NUM_NAMESPACES 
        return self.namespaces[self.active_namespace_indx] 

        

    def io_returned(self, io):
        if io:
            if (io.ns.return_sqe(io) == 0):
                self.curr_working_set_size -= 1
        if self.q.num_elelments() <= MAX_INFLIGHTS_LWM:
            while self.q.num_elelments() < MAX_INFLIGHTS_HWM:
                active_ns = self.get_curr_ns_advnace_if_needed()
                (io, curr_ns_inflight) = active_ns.get_new_sqe()
                if curr_ns_inflight == 1:
                    self.curr_working_set_size += 1
                self.q.push(io, io.completion_time)

    def run(self):
        completed_ios = 0
        while completed_ios < IOS_PASSED_TILL_COMPLETE:
            io = self.q.pop()
            if isinstance(io, int):
                self.T = io
                self.active_working_sets.append(self.curr_working_set_size)
                self.q.push(self.T + 10000, self.T + 10000)
            else:
                self.T = io.completion_time
                self.io_returned(io)
                completed_ios += 1
        print([(statistics.mean(ns.batches_sizes[0:-1]), statistics.stdev(ns.batches_sizes[0:-1]),len(ns.batches_sizes)) for ns in self.namespaces])
        import ipdb; ipdb.set_trace()

System().run() 
 




        
	    
	
			
	    
	    	    		

	
	
	
	



	
	

	











        
