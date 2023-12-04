
import ast

def map_filter_reduce(file_name, node_num, intermed_prefix):
    with open(f'MP3_FILE/{file_name}', 'r') as file:
        for line in file:
            with open(f'MP3_FILE/result_{intermed_prefix}_{node_num}', 'a') as machine_result:
                k_v = ast.literal_eval(line.strip())
                machine_result.write(k_v[1])    

if __name__ == "__main__":
    import sys
    # Access command-line arguments
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]

    # Call the function with the provided arguments
    map_filter_reduce(arg1, arg2, arg3)