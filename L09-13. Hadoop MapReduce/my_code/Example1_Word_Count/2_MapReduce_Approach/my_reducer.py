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

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the word from the line
    word = None

    # 1.2. We ouptut the number of appearances
    num_appearances = None

    # 2. We assign both variables
    content = line.strip().split("\t")
    word = content[0]
    num_appearances = int(content[1][1:-1])

    # 3. We assign res
    res = (word, num_appearances)

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION write_line
# ------------------------------------------
def write_line(my_output_stream, current_word, current_num_appearances):
    # 1. We create the String
    my_str = current_word + "\t" + "(" + str(current_num_appearances) + ")" + "\n"

    # 2. We write it to the file
    my_output_stream.write(my_str)

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # 1. We unpack my_reducer_input_parameters
    pass

    # 2. We create a couple of variables to keep track of the current word being processed
    current_word = ""
    current_num_appearances = 0

    # 3. We read the file line by line
    for line in my_input_stream:
        # 3.1. We get the content from the line
        (word, num_appearances) = process_line(line)

        # 3.2. If we find a new word
        if (word != current_word):
            # 3.2.1. If current word was an actual word, we output the result for it
            if (current_word != ""):
                write_line(my_output_stream, current_word, current_num_appearances)

            # 3.2.2. We update current word to be the new one
            current_word = word
            current_num_appearances = 0

        # 3.3. We update current word appearances
        current_num_appearances += num_appearances

    # 4. We output the result for the last word
    if (current_word != ""):
        write_line(my_output_stream, current_word, current_num_appearances)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(local_False_Cloudera_True,
            my_reducer_input_parameters,
            input_file_example,
            output_file_example
           ):

    # 1. We select the input and output streams based on our working mode
    my_input_stream = None
    my_output_stream = None

    # 1.1: Local Mode --> We use the debug files
    if (local_False_Cloudera_True == False):
        my_input_stream = codecs.open(input_file_example, "r", encoding='utf-8')
        my_output_stream = codecs.open(output_file_example, "w", encoding='utf-8')

    # 1.2: Cloudera --> We use the stdin and stdout streams
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # 2. We trigger my_reducer
    my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters)

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
        my_input_stream = "../../../my_results/Example1_Word_Count/2_MapReduce_Approach/2_my_sort_simulation/" + file_name
        my_output_stream = "../../../my_results/Example1_Word_Count/2_MapReduce_Approach/3_my_reduce_simulation/reduce_" + file_name

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
