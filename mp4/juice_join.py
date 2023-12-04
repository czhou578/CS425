
import ast
import os

def map_join_reduce(file_name, node_num, intermed_prefix):
    with open(f'MP3_FILE/{file_name}', 'r') as file:
        with open(f'MP3_FILE/result_{intermed_prefix}_{node_num}', 'a') as machine_result:
            for line in file:
                tuples = ast.literal_eval(f"[{line}]")

                # Extract the second entry from each tuple and strip '\n'
                second_entries = [entry[1].strip() for entry in tuples]

                # Create a new string by joining the second entries
                result_string = ','.join(second_entries)  

                machine_result.write(result_string + '\n')                  

if __name__ == "__main__":
    import sys
    # Access command-line arguments
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]

    # Call the function with the provided arguments
    map_join_reduce(arg1, arg2, arg3)