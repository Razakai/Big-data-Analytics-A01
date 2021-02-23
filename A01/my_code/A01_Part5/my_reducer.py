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
    step2 = step1[1].split(',')
    step3 = int(step2[0].split('(')[1])
    step4 = int(step2[1].split(')')[0])
    return step1[0], step3, step4

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    res = {}

    for line in my_input_stream:
        bikeID, duration, numTrips = processLine(line)

        if bikeID in res:
            res[bikeID][0] += duration
            res[bikeID][1] += numTrips
        else:
            res[bikeID] = [duration, numTrips]

    res = dict(sorted(res.items(), key=lambda item: item[1][0], reverse=True))

    i = 0
    for key, value in res.items():
        if i < my_reducer_input_parameters[0]:
            my_output_stream.write(str(key) + "\t" + str(tuple(value)) + '\n')
        else:
            break
        i += 1

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
    top_n_bikes = 10

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        file_name = sys.argv[1]

    # 2. Local or Hadoop
    local_False_hadoop_True = False

    # 3. We set the path to my_dataset and my_result
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout

    if (local_False_hadoop_True == False):
        my_input_stream = "../../my_results/A01_Part5/2_my_sort_simulation/" + file_name
        my_output_stream = "../../my_results/A01_Part5/3_my_reduce_simulation/reduce_" + file_name[5:]

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []
    my_reducer_input_parameters.append( top_n_bikes )

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
