import random

with open('09.loc', 'w') as f:
    for i in range(1, 1001):
        s = str(i) + ' ' + str(random.randint(1, 100)) + '\n'
        f.write(s)