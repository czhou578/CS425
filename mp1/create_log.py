import random
import string


def generate_string(length, numDigits):
    random_string = ''.join(random.choice(
        string.ascii_letters + string.digits) for _ in range(length))

    # Ensure that the random string has at most n digits
    while random_string.count(string.digits) > numDigits:
        random_string = ''.join(random.choice(
            string.ascii_letters + string.digits) for _ in range(length))
    return random_string

# have some machines run frequent patterns, and others run unfrequent patterns


'''
randomly generate number in range 0 to 10 exclusive
if < 2, then rare
if > 2, <= 5, somewhat frequent
if >= 6

rare, frequent, somewhat frequent.

'''

for i in range(1, 2):  # replace with number of VM's
    file_path = f'test_machine_{i}.log'

    with open(file_path, 'w') as file:
        random_num = random.randint(1, 10)
        for _ in range(1200000):  # how many lines to generate
            if random_num <= 2:
                file.write("Rare Pattern: " + generate_string(40, 3) + "\n")
            elif random_num > 2 and random_num <= 5:
                file.write("Somewhat Frequent: " +
                           generate_string(40, 8) + "\n")
            elif random_num >= 6:
                file.write("Frequent: " + generate_string(40, 15) + "\n")
