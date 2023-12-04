import string
import re

def map_join(file_names, to_match, node_num, intermed_prefix): #Europe/London
    file_names = file_names.split(', ')
    for file_name in file_names:
        with open(f'MP3_FILE/{file_name}', 'r') as file:
            for line in file:
                line_elements = line.split(',')

                if "dep" in line_elements[0] or "employee" in line_elements[0]: continue

                first_ele = line_elements[0]
                last_ele = line_elements[-1].strip()

                if last_ele.isalpha() or (last_ele.isalnum() and not last_ele.isdigit()): #from department table
                    
                    if int(line_elements[0]) == int(to_match):
                        prefix = line_elements[-1].strip()
                        with open(f"MP3_FILE/{intermed_prefix}_{prefix}", 'a') as file2: #individual files
                            file2.write(str((prefix, line)) + '\n')
                        
                        with open(f'MP3_FILE/map_department_{intermed_prefix}_result', 'a') as mapKeys: #aggregated department match
                            mapKeys.write(str((prefix, line)) + '\n')
                
                elif int(last_ele) == int(to_match): #from employee table
                    with open(f"MP3_FILE/{intermed_prefix}_{line_elements[0]}", 'a') as file2:
                        file2.write(str((line_elements[0], line)) + '\n')

                    with open(f'MP3_FILE/map_employee_{intermed_prefix}_result_{node_num}', 'a') as mapKeys: #aggregated employee match
                        mapKeys.write(str((line_elements[0], line)) + '\n')

if __name__ == "__main__":
    import sys
    # Access command-line arguments
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]
    arg4 = sys.argv[4]

    # Call the function with the provided arguments
    map_join(arg1, arg2, arg3, arg4)