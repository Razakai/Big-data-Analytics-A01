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
import os
import codecs
import sys
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
    res = [ w for w in clean_words if w != "" ]

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(input_folder, output_file):
    # 1. We open the file for writing
    my_output_stream = codecs.open(output_file, "w", encoding="utf-8")

    # 2. We list the files from the directory input_folder
    list_of_files = os.listdir(input_folder)
    list_of_files.sort()

    # 3. We create a dictionary to store all the words that have appeared so far
    my_words_dict = {}

    # 4. We traverse the files one by one
    for file in list_of_files:
        # 4.1. We open the file for reading
        my_input_stream = codecs.open(input_folder + file, "r", encoding="utf-8")

        # 4.2. We read the file line by line
        for line in my_input_stream:
            # 4.2.1. We get the words from the line
            words = process_line(line)

            # 4.2.2. We insert each word in the dictionary
            for w in words:
                # I. We check if the word has appeared in the dictionary before. If not, we enter the word
                if (w not in my_words_dict):
                    my_words_dict[w] = 0

                # II. Update the dictionary with the new word
                my_words_dict[w] += 1

        # 4.3. We close the file
        my_input_stream.close()

    # 5. We write the content from my_words_dict into the result file
    for key in sorted(my_words_dict):
        # 5.1. We create the String
        my_str = key + "\t" + "(" + str(my_words_dict[key]) + ")" + "\n"

        # 5.2. We write it to the file
        my_output_stream.write(my_str)

    # 6. We close the file
    my_output_stream.close()

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    input_folder = "../../../my_datasets/my_dataset_1/"
    output_file = "../../../my_results/Example1_Word_Count/1_Sequential_Approach/result.txt"

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        input_folder = sys.argv[1]
        output_file = sys.argv[2]

    # 2. We call to my_main
    my_main(input_folder, output_file)
