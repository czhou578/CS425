import string
import re

def map_filter(file_name, regex, node_num, intermed_prefix):
    FLAG = False
    with open(f'MP3_FILE/{file_name}', 'r') as file:
        for line in file:
            matched_substring = re.search(regex, line, re.IGNORECASE)
            if matched_substring:
                FLAG = True
                # Split the string to get the first word
                first_word = line.split(',')[0]   
                # print('first word, ', first_word)

                with open(f"MP3_FILE/{intermed_prefix}_{first_word}", 'a') as file2: #intermediate key value pair
                        file2.write(str((first_word, line)) + '\n')


                with open(f"MP3_FILE/map_{intermed_prefix}_result_{node_num}", 'a') as regexMapFile: #aggregated file
                        regexMapFile.write(str((first_word, line)) + '\n')

    if FLAG == False:
        with open(f"MP3_FILE/map_{intermed_prefix}_result_{node_num}", 'a') as regexMapFile: #aggregated file
            pass
         

    # def champaign_map_filter(self, file_name, regex, node_num, intermed_prefix):
    #     with open(f'MP3_FILE/{file_name}', 'r') as file:
    #         for line in file:
    #             matched_substring = re.search(regex, line, re.IGNORECASE)
    #             if matched_substring:
    #                 # Split the string to get the first word
    #                 first_word = line.split(',')[0]   
    #                 # print('first word, ', first_word)

    #                 with open(f"MP3_FILE/{intermed_prefix}_{first_word}", 'a') as file2: #intermediate key value pair
    #                         file2.write(str((first_word, line)) + '\n')
 

    #                 with open(f"MP3_FILE/map_{intermed_prefix}_result_{node_num}", 'a') as regexMapFile: #aggregated file
    #                         regexMapFile.write(str((first_word, line)) + '\n')    

if __name__ == "__main__":
    import sys
    # Access command-line arguments
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]
    arg4 = sys.argv[4]

    # Call the function with the provided arguments
    map_filter(arg1, arg2, arg3, arg4)