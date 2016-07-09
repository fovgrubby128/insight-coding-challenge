__author__ = 'Yanjun Zhu'
import json
import collections
import numpy as np
import datetime
import sys

def venmo_graph(filepath, outpath):
    ###
    # dict stores adjacency list of each node
    # dict_count stores the number of nodes connected to each node
    # dict_time stores the node pairs occurred in each certain timestamp
    # dict_tar_act stores the timestamp transaction happened for each pair
    ###
    
    dict = collections.defaultdict(set)
    dict_count = {}
    dict_time = collections.defaultdict(list)
    dict_tar_act = {}
    idx = 0

    def get_median(list):
        # return the median of the given list
        return np.median(np.array(list))

    def gap2seconds(time_l, time_s):
        # return the gap in seconds between two timestamps
        return (time_l - time_s).seconds

    file = open(outpath, "w")
    with open(filepath,'r') as f:
        for line in f:
            data = json.loads(line)
            timestamp = datetime.datetime.strptime(data['created_time'],"%Y-%m-%dT%H:%M:%SZ")
            actor = data['actor']
            target = data['target']
            # If any information is missing, then skip the record
            if (actor == "" or timestamp == "" or target == ""):
                continue
            # The graph is bi-directed
            if actor > target:
                swap = actor
                actor = target
                target = swap
            str = actor + target
            
            # The first transaction in input file
            if (idx == 0):
                global_maxtime = timestamp
                dict[actor].add(target)
                dict[target].add(actor)
                dict_count[actor] = 1
                dict_count[target] = 1
                dict_time[timestamp].append((actor,target))
                dict_tar_act[str] = timestamp
                rolling_median = ("%.2f" % 1)
                idx = idx + 1
            
            else:
                # The incoming transaction is "in-order"
                if (timestamp > global_maxtime):
                    global_maxtime = timestamp
                    key_list = dict_time.keys()
                    # Delete all records occurred out of 60-seconds window
                    for key in key_list:
                        if (gap2seconds(global_maxtime,key) > 60):
                            for pair in dict_time[key]:
                                a = pair[0]
                                b = pair[1]
                                dict[b].remove(a)
                                dict[a].remove(b)
                                dict_count[a] = dict_count[a] - 1
                                dict_count[b] = dict_count[b] - 1
                                if dict[b] == set():
                                    dict.pop(b)
                                    dict_count.pop(b)
                                if dict[a] == set():
                                    dict.pop(a)
                                    dict_count.pop(a)
                                dict_tar_act.pop(a+b)
                            dict_time.pop(key)
                    # If the (actor,target) pair of incoming transaction have transaction records still in the history (within 60-seconds window)
                    if (actor in dict[target]):
                        c_time = dict_tar_act[str]
                        dict_time[c_time].remove((actor,target))
                        if (dict_time[c_time] == []):
                            dict_time.pop(c_time)
                        dict_tar_act[str] = timestamp
                        dict_time[timestamp].append((actor,target))
                    # If the actor or target, either is the brand new guy
                    else:
                        dict[actor].add(target)
                        dict[target].add(actor)
                        dict_count[actor] = len(dict[actor])
                        dict_count[target] = len(dict[target])
                        dict_time[timestamp].append((actor,target))
                        dict_tar_act[str] = timestamp
                        count = dict_count.values()
                        rolling_median = ("%.2f" % get_median(count))
                # The incoming transaction is "out-order" (smaller than current global maximum time)
                else:
                    # Only the incoming transaction occurred within 60 seconds will change the result
                    if (gap2seconds(global_maxtime,timestamp) <= 60):
                        # If the (actor,target) pair of incoming transaction have transaction records still in the history (within 60-seconds window)
                        if (actor in dict[target]):
                            c_time = dict_tar_act[str]
                            if (c_time < timestamp):
                                dict_time[c_time].remove((actor,target))
                                if (dict_time[c_time] == []):
                                    dict_time.pop(c_time)
                                dict_tar_act[str] = timestamp
                                dict_time[timestamp].append((actor,target))
                        # If the actor or target, either is the brand new guy
                        else:
                            dict[actor].add(target)
                            dict[target].add(actor)
                            dict_count[actor] = len(dict[actor])
                            dict_count[target] = len(dict[target])
                            dict_time[timestamp].append((actor,target))
                            dict_tar_act[str] = timestamp
                            count = dict_count.values()
                            rolling_median = ("%.2f" % get_median(count))
            file.write(rolling_median)
            file.write('\n')

def main(argv):
    filepath=sys.argv[1]
    outpath=sys.argv[2]
    venmo_graph(filepath,outpath)

if __name__ == "__main__":
   main(sys.argv[1:])
