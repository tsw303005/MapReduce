'''
this code help you to check the correctness with ans file
'''

file_directory = "../result_file/"
num_reducer = int(input('num_reducer: '))
chunk = int(input('chunk: '))
testcase = input('testcase: ')

words = dict()

for i in range(1, num_reducer+1, 1):
    file = "TEST" + testcase + "-" + str(i) + ".out"
    with open(file_directory+file, 'r') as f:
        lines = f.readlines();
        for line in lines:
            tmp = line.split(' ')
            word = tmp[0]
            num = int(tmp[1])
            if word not in words:
                words[word] = num
            else:
                words[word] += num



ans = dict()
file_directory = "./" + testcase + "_sample_ans/"

for i in range(1, num_reducer+1, 1):
    file = "TEST" + testcase + "-" + str(i) + ".out"
    with open(file_directory+file, 'r') as f:
        lines = f.readlines();
        for line in lines:
            tmp = line.split(' ')
            word = tmp[0]
            num = int(tmp[1])
            if word not in ans:
                ans[word] = num
            else:
                ans[word] += num


items = ans.items()
tmp = words.items()
flag = 1
for i in items:
    if i not in tmp:
        flag = 0
        print(i)

if (flag):
    print("Pass !!!!!")
else:
    print("Fail TAT")