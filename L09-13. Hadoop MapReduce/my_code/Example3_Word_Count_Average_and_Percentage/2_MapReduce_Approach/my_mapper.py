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
import re

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = []

    # 2. We get the words from line
    words = line.strip().split(" ")

    # 3. We clean the characters from each word
    clean_words = [ re.sub(r"[^a-zA-Z]", "", w).lower() for w in words ]

    # 4. We get only the non-empty words
    res = [ x for x in clean_words if x != "" ]

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    # 1. We unpack my_mapper_input_parameters
    pass

    # 2. We create a dictionary to store all the words that have appeared so far
    my_letter_words_dict = {}

    # 3. We create an Integer to store the total amount of words
    total_amount_of_words = 0

    # 4. We read the file line by line
    for line in my_input_stream:
        # 4.1. We get the words from the line
        words = process_line(line)

        # 4.2. We insert each word in the dictionary
        for w in words:
            # 4.2.1. We get the first letter
            letter = w[0]

            # 4.2.2. We check if the letter has appeared in the dictionary before. If not, we enter the letter
            if (letter not in my_letter_words_dict):
                my_letter_words_dict[letter] = (0, 0)

            # 4.2.3. Update the dictionary with the new word
            my_letter_words_dict[letter] = (
            my_letter_words_dict[letter][0] + 1, my_letter_words_dict[letter][1] + len(w))

            # 4.2.4. We update the total amount of words
            total_amount_of_words += 1

    # 5. We write the content from my_words_dict into the result file
    for key in my_letter_words_dict:
        # 5.1. We get the value from the dictionary
        (num_words, total_length_words) = my_letter_words_dict[key]

        # 5.2. We create the String
        my_str = "universal" + "\t" + "(" + str(key) + ", " + str(num_words) + ", " + str(total_length_words) + ")" + "\n"

        # 5.3. We write it to the file
        my_output_stream.write(my_str)

    # 6. We output a last key with the total amount of words
    my_str = "universal" + "\t" + "(" + "Total" + ", " + str(total_amount_of_words) + ", " + "0" + ")" + "\n"
    my_output_stream.write(my_str)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(local_False_Cloudera_True,
            my_mapper_input_parameters,
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

    # 2. We trigger my_map
    my_map(my_input_stream, my_output_stream, my_mapper_input_parameters)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    file_name = "file1.txt"

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        file_name = sys.argv[1]

    # 2. Local or Hadoop
    local_False_hadoop_True = False

    # 3. We set the path to my_dataset and my_result
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout

    if (local_False_hadoop_True == False):
        my_input_stream = "../../../my_datasets/my_dataset_1/" + file_name
        my_output_stream = "../../../my_results/Example3_Word_Count_Average_and_Percentage/2_MapReduce_Approach/1_my_map_simulation/" + file_name[:-4] + ".txt"

    # 4. my_mapper.py input parameters
    my_mapper_input_parameters = []

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_mapper_input_parameters)