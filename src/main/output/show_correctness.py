"""
original results run in Mac mini

please run below command to ensure acer is correct:
> python verify_results.py
or
> python3 verify_results.py
"""


# obtain the number of tuples for each query
def read_tuple_num(filename):
    results = []
    with open(filename) as file:
        for line in file:
            # read last string and convert it to integer value
            if line.find("number of tuples") != -1:
                words = line.split(" ")
                num = words[-1]
                results.append(int(num))
    return results


no_bug = True

# crimes dataset
full_scan = read_tuple_num('crimes_full_scan.txt')
# interval_scan = read_tuple_num('crimes_interval_scan.txt')
# naive_index = read_tuple_num('crimes_naive_index.txt')
acer_index = read_tuple_num('crimes_acer_Y1_Y2_A3.txt')
for i in range(500):
#     if full_scan[i] != interval_scan[i]:
#         print('interval_scan may have bug, query index: ', i)
#         no_bug = false
#     if full_scan[i] != naive_index[i]:
#         print('naive_index may have bug, query index: ', i)
#         no_bug = false
    if full_scan[i] != acer_index[i]:
        print('acer may have bug, query index: ', i)
        no_bug = false

# nasdaq dataset
full_scan = read_tuple_num('nasdaq_full_scan.txt')
# interval_scan = read_tuple_num('nasdaq_interval_scan.txt')
# naive_index = read_tuple_num('nasdaq_naive_index.txt')
acer_index = read_tuple_num('nasdaq_acer_Y1_Y2_A3.txt')
for i in range(500):
#     if full_scan[i] != interval_scan[i]:
#         print('interval_scan may have bug, query index: ', i)
#         no_bug = false
#     if full_scan[i] != naive_index[i]:
#         print('naive_index may have bug, query index: ', i)
#         no_bug = false
    if full_scan[i] != acer_index[i]:
        print('acer may have bug, query index: ', i)
        no_bug = false

# job dataset
full_scan = read_tuple_num('job_full_scan.txt')
# interval_scan = read_tuple_num('job_interval_scan.txt')
# naive_index = read_tuple_num('job_naive_index.txt')
acer_index = read_tuple_num('job_acer_Y1_Y2_A3.txt')
for i in range(500):
#     if full_scan[i] != interval_scan[i]:
#         print('interval_scan may have bug, query index: ', i)
#         no_bug = false
#     if full_scan[i] != naive_index[i]:
#         print('naive_index may have bug, query index: ', i)
#         no_bug = false
    if full_scan[i] != acer_index[i]:
        print('acer may have bug, query index: ', i)
        no_bug = false

# synthetic dataset
# full_scan = read_tuple_num('synthetic_full_scan.txt')
# interval_scan = read_tuple_num('synthetic_interval_scan.txt')
# naive_index = read_tuple_num('synthetic_naive_index.txt')
# acer_index = read_tuple_num('synthetic_acer_Y1_Y2_A3.txt')
# for i in range(500):
#     if full_scan[i] != interval_scan[i]:
#         print('interval_scan may have bug, query index: ', i)
#         no_bug = false
#     if full_scan[i] != naive_index[i]:
#         print('naive_index may have bug, query index: ', i)
#         no_bug = false
#     if full_scan[i] != acer_index[i]:
#         print('acer may have bug, query index: ', i)
#         no_bug = false
#
# if no_bug:
#     print('congratulations, no bugs...')
# else:
#     print('you need to check query...')

if no_bug:
    print('congratulations, no bugs...')
else:
    print('you need to check query...')
