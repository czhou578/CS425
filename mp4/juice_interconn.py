
#add up all the counts 
#total number of entries for the interconn. value passed in here
import ast

def juice_interconn(file_name, node_num, intermed_prefix, total_interconn_entries):
    total_interconn_entries = int(total_interconn_entries)
    entries_dict = {}

    with open(f"MP3_FILE/{file_name}") as file:
        composition = 0
        count_entries = 0
        detection_value = None
        for line in file:
            k_v = ast.literal_eval(line.strip())
            detection_value = k_v[0]
            if detection_value == "LoopVideo":
                detection_value = "Loop/Video"

            elif detection_value == "LoopNone":
                detection_value = "Loop/None"
            
            if detection_value not in entries_dict:
                entries_dict[detection_value] = 1
            else:
                entries_dict[detection_value] += 1
        
        with open(f'MP3_FILE/result_{intermed_prefix}_{node_num}', 'a') as machine_result:
            for key, val in entries_dict.items():
                composition = (val / total_interconn_entries) * 100
                formatted_percentage = "{:.1%}".format(composition / 100)
                machine_result.write(str((formatted_percentage, key)) + '\n')        

if __name__ == "__main__":
    import sys
    # Access command-line arguments
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]
    arg4 = sys.argv[4]

    # Call the function with the provided arguments
    juice_interconn(arg1, arg2, arg3, arg4)