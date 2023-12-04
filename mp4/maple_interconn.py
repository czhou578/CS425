
#interconn idx is the col idx of interconn
#interconn value is the value of the interconn column passed in

def maple_interconn(file_name, interconn_value, interconn_idx, intermed_prefix, node_num):
    interconn_idx = int(interconn_idx)
    with open(f"MP3_FILE/{file_name}") as file:
        for line in file:
            line_split = line.split(',')
            if line_split[interconn_idx] == interconn_value:
                detection_value = line_split[interconn_idx - 1]
                print(intermed_prefix)
                print(detection_value)

                if detection_value == "Loop/Video":
                    detection_value = "LoopVideo"
                
                if detection_value == "Loop/None":
                    detection_value = "LoopNone"

                with open(f"MP3_FILE/{intermed_prefix}_{detection_value}", 'a') as file2:
                    file2.write(str((detection_value, interconn_value))  + '\n')
                
                with open(f"MP3_FILE/map_{intermed_prefix}_result_{node_num}", "a") as resultFile:
                    resultFile.write(str((detection_value, interconn_value))  + '\n')


if __name__ == "__main__":
    import sys
    # Access command-line arguments
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]
    arg4 = sys.argv[4]
    arg5 = sys.argv[5]

    # Call the function with the provided arguments
    maple_interconn(arg1, arg2, arg3, arg4, arg5)

