import numpy as np

# read query cost
def read_query_cost(filename):
    query_cost = []
    with open(filename) as file:
        for line in file:
            # read read cost [ms]
            if line.find("query cost") != -1:
                words = line.split(" ")
                time = words[-1]
                value = time[0:-4]
                query_cost.append(float(value))
    return query_cost

# ['crimes', 'nasdaq', 'job']
filenames = ['crimes', 'nasdaq', 'job']
for filename in filenames:
    if filename.find('crimes') != -1:
        print('------------------------crimes dataset------------------------')
        full_scan = read_query_cost('crimes_full_scan.txt')
        print('full scan average latency [ms]: ', sum(full_scan)/len(full_scan))

#         naive_index = read_query_cost('crimes_naive_index.txt')
#         print('naive index average latency [ms]: ', sum(naive_index)/len(naive_index))
#
#         interval_scan = read_query_cost('crimes_interval_scan.txt')
#         print('interval scan average latency [ms]: ', sum(interval_scan)/len(interval_scan))

        acer_index = read_query_cost('crimes_acer_Y1_Y2_A3.txt')
        # (optimized layout, enable truncation, simple8b)
        print('acer index average latency [ms]: ', sum(acer_index)/len(acer_index))

    elif filename.find('job') != -1:
        print('------------------------job dataset------------------------')
        full_scan = read_query_cost('job_full_scan.txt')
        print('full scan average latency [ms]: ', sum(full_scan)/len(full_scan))

#         naive_index = read_query_cost('job_naive_index.txt')
#         print('naive index average latency [ms]: ', sum(naive_index)/len(naive_index))
#
#         interval_scan = read_query_cost('job_interval_scan.txt')
#         print('interval scan average latency [ms]: ', sum(interval_scan)/len(interval_scan))

        acer_index = read_query_cost('job_acer_Y1_Y2_A3.txt')
        # (optimized layout, enable truncation, simple8b)
        print('acer index average latency [ms]: ', sum(acer_index)/len(acer_index))

    elif filename.find('nasdaq') != -1:
        print('------------------------nasdaq dataset------------------------')
        full_scan = read_query_cost('nasdaq_full_scan.txt')
        print('full scan average latency [ms]: ', sum(full_scan)/len(full_scan))

#         naive_index = read_query_cost('nasdaq_naive_index.txt')
#         print('naive index average latency [ms]: ', sum(naive_index)/len(naive_index))
#
#         interval_scan = read_query_cost('nasdaq_interval_scan.txt')
#         print('interval scan average latency [ms]: ', sum(interval_scan)/len(interval_scan))

        acer_index = read_query_cost('nasdaq_acer_Y1_Y2_A3.txt')
        # (optimized layout, enable truncation, simple8b)
        print('acer index average latency [ms]: ', sum(acer_index)/len(acer_index))

