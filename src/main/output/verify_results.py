"""
original results run in Mac mini

please run below command to ensure acer is correct:
> python verify_results.py | python3 verify_results.py
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

acer_nasdaq = read_tuple_num('acer_nasdaq_nfa.txt')
fullscan_nasdaq = read_tuple_num('fullscan_nasdaq_nfa.txt')
fullscanplus_nasdaq = read_tuple_num('fullscan+_nasdaq_nfa.txt')

acer_job = read_tuple_num('acer_job_nfa.txt')
fullscan_job = read_tuple_num('fullscan_job_nfa.txt')
# fullscanplus_job = read_tuple_num('fullscan+_job_nfa.txt')

acer_crimes = read_tuple_num('acer_crimes_nfa.txt')
fullscan_crimes = read_tuple_num('fullscan_crimes_nfa.txt')

for i in range(len(acer_nasdaq)):
    if acer_nasdaq[i] != fullscan_nasdaq[i]:
        print('acer occurs bug on nasdaq dataset, query index: ', i)
    if fullscan_nasdaq[i] != fullscanplus_nasdaq[i]:
            print('fullscan+ occurs bug on nasdaq dataset, query index: ', i)

    if acer_job[i] != fullscan_job[i]:
        print('acer occurs bug on cluster dataset, query index: ', i)
#     if fullscan_job[i] != fullscanplus_job[i]:
#             print('fullscan+ occurs bug on nasdaq dataset, query index: ', i)

    if acer_crimes[i] != fullscan_crimes[i]:
        print('acer occurs bug on crimes dataset, query index: ', i)

print('congratulations, no bugs...')