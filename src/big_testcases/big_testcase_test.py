'''
this file help you to check the correctness for big testcase
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
file_directory = "./"
file = testcase + ".word"
with open(file_directory+file, 'r') as f:
    for i in range(5000):
        line = f.readline()
        line = line[:-1]
        tmp = line.split(' ')
        for word in tmp:
            if word not in ans:
                ans[word] = 1
            else:
                ans[word] += 1



flag = 1
tmp = words.items()
ans  = ans.items()

for i in ans:
    if i not in tmp:
        flag = 0
        print(i)

if (flag):
    print("Pass !!!!!")
else:
    print("Fail TAT")
