#!/usr/bin/python
# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import sys
import codecs


def processLine(line):
    step1 = line.split('\t')
    step2 = step1[1].split('@')
    totalTrips = int(len(step2) / 4)
    res = []

    for trip in range(totalTrips):
        currentTrip = []
        for num in range(4):
            if num == 0:
                if trip == 0:
                    currentTrip.append(step2[0].split('(')[1].strip())
                else:
                    currentTrip.append(step2[(4 * trip)].strip())
            elif num == 3:
                currentTrip.append(step2[(4 * trip) + 3].split(')')[0].strip())
            else:
                currentTrip.append(step2[(4 * trip) + num].strip())
        res.append(currentTrip)
    return res


'''
0 - start time
1 - end time
2 - start location
3 - end location
'''


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    res = []
    currentEnd = ""

    for line in my_input_stream:
        journeyArr = processLine(line)

        for journey in journeyArr:
            if currentEnd == "":
                currentEnd = journey
            else:
                if currentEnd[3] != journey[2]:
                    res.append([currentEnd[1], currentEnd[3], journey[0], journey[2]])

                currentEnd = journey
    print(res)
    for value in res:
        my_output_stream.write("By_Truck" + "\t" + '(' + ', '.join(value) + ')' + '\n')





# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    file_name = "sort_1.txt"

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        file_name = sys.argv[1]

    # 2. Local or Hadoop
    local_False_hadoop_True = False

    # 3. We set the path to my_dataset and my_result
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout

    if (local_False_hadoop_True == False):
        my_input_stream = "../../my_results/A01_Part6/2_my_sort_simulation/" + file_name
        my_output_stream = "../../my_results/A01_Part6/3_my_reduce_simulation/reduce_" + file_name[5:]

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
