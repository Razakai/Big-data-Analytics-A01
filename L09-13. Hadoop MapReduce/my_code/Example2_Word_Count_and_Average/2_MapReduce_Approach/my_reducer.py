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

    # 1.1. We output the letter from the line
    letter = None

    # 1.2. We ouptut the num_words from the line
    num_words = None

    # 1.3. We ouptut the total_length_words from the line
    total_length_words = None

    # 2. We assign the variables
    content = line.strip().split("\t")
    letter = content[0]

    aux_content = content[1].split(", ")
    num_words = int(aux_content[0][1:])
    total_length_words = int(aux_content[1][:-1])

    # 3. We assign res
    res = (letter, num_words, total_length_words)

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION write_line
# ------------------------------------------
def write_line(my_output_stream, current_letter, current_num_words, current_total_length_words):
    # 1. We get the average length
    avg_length_words = round(current_total_length_words / (current_num_words * 1.0), 2)

    # 2. We create the String
    my_str = current_letter + "\t" + "(" + str(current_num_words) + ", " + str(current_total_length_words) + ", " + str(avg_length_words) + ")" + "\n"

    # 3. We write it to the file
    my_output_stream.write(my_str)

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # 1. We unpack my_reducer_input_parameters
    pass

    # 2. We create three variables to keep track of the current letter being processed
    current_letter = ""
    current_num_words = 0
    current_total_length_words = 0

    # 3. We read the file line by line
    for line in my_input_stream:
        # 3.1. We get the content from the line
        (letter, num_words, total_length_words) = process_line(line)

        # 3.2. If we find a new letter
        if (letter != current_letter):
            # 3.2.1. If current letter was an actual letter, we output the result for it
            if (current_letter != ""):
                write_line(my_output_stream, current_letter, current_num_words, current_total_length_words)

            # 3.2.2. We update current word to be the new one
            current_letter = letter
            current_num_words = 0
            current_total_length_words = 0

        # 3.3. We update current letter info
        current_num_words += num_words
        current_total_length_words += total_length_words

    # 4. We output the result for the last word
    if (current_letter != ""):
        write_line(my_output_stream, current_letter, current_num_words, current_total_length_words)

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
        my_input_stream = "../../../my_results/Example2_Word_Count_and_Average/2_MapReduce_Approach/2_my_sort_simulation/" + file_name
        my_output_stream = "../../../my_results/Example2_Word_Count_and_Average/2_MapReduce_Approach/3_my_reduce_simulation/reduce_" + file_name

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
