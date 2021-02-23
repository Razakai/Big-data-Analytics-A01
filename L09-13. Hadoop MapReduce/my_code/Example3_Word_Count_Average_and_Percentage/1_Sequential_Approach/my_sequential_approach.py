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
    my_letter_words_dict = {}

    # 4. We create an Integer to store the total amount of words
    total_amount_of_words = 0

    # 5. We traverse the files one by one
    for file in list_of_files:
        # 5.1. We open the file for reading
        my_input_stream = codecs.open(input_folder + file, "r", encoding="utf-8")

        # 5.2. We read the file line by line
        for line in my_input_stream:
            # 5.2.1. We get the words from the line
            words = process_line(line)

            # 5.2.2. We insert each word in the dictionary
            for w in words:
                # I. We get the first letter
                letter = w[0]

                # II. We check if the letter has appeared in the dictionary before. If not, we enter the letter
                if (letter not in my_letter_words_dict):
                    my_letter_words_dict[letter] = (0, 0)

                # III. We update the dictionary with the new word
                my_letter_words_dict[letter] = ( my_letter_words_dict[letter][0] + 1, my_letter_words_dict[letter][1] + len(w) )

                # IV. We update the total amount of words
                total_amount_of_words += 1

        # 5.3. We close the file
        my_input_stream.close()

    # 6. We bulk the content to a list
    my_letter_words_list = []

    for letter in my_letter_words_dict:
        # 6.1. We get the value from the dictionary
        (num_words, total_length_words) = my_letter_words_dict[letter]

        # 6.2. We get the average length
        avg_length_words = round( total_length_words / (num_words * 1.0), 2 )

        # 6.3. We get the percentage of words
        percentage_words = round( (num_words / (total_amount_of_words * 1.0)) * 100.0, 3 )

        # 6.4. We add the entry to the list
        my_letter_words_list.append( (percentage_words, num_words, total_length_words, avg_length_words, letter) )

    # 7. We sort the list
    my_letter_words_list.sort(key=lambda item : item[0], reverse=True)

    # 8. We write the content from the list to the file
    for item in my_letter_words_list:
        # 8.1. We create the String
        my_str = item[4] + "\t" + "(" + str(item[0]) + ", " + str(item[1]) + ", " + str(item[2]) + ", " + str(item[3]) + ")" + "\n"

        # 8.2. We write it to the file
        my_output_stream.write(my_str)

    # 9. We close the file
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
    output_file = "../../../my_results/Example3_Word_Count_Average_and_Percentage/1_Sequential_Approach/result.txt"

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        input_folder = sys.argv[1]
        output_file = sys.argv[2]

    # 2. We call to my_main
    my_main(input_folder, output_file)
