
file_directory = "./result_file/"
num_reducer = int(input('num_reducer: '))
chunk = int(input('chunk: '))
testcase = input('testcase: ')

words = dict()

# for i in range(1, num_reducer+1, 1):
#     for j in range(1, chunk+1, 1):
#         file = str(j) + '_' + str(i) + '.txt'
#         with open(file_directory+file, 'r') as f:
#             lines = f.readlines()
#             for line in lines:
#                 tmp = line.split(' ')
#                 word = tmp[0]
#                 num = int(tmp[1])
#                 if word not in words:
#                     words[word] = num
#                 else:
#                     words[word] += num

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


# print(sorted(words.items(), key = lambda kv:(kv[1], kv[0]))) 

# ans = dict()
# file_directory = "./testcases/" + testcase + "_sample_ans/"

# for i in range(1, num_reducer+1, 1):
#     file = "TEST" + testcase + "-" + str(i) + ".out"
#     with open(file_directory+file, 'r') as f:
#         lines = f.readlines();
#         for line in lines:
#             tmp = line.split(' ')
#             word = tmp[0]
#             num = int(tmp[1])
#             if word not in ans:
#                 ans[word] = num
#             else:
#                 ans[word] += num

ans = dict()
file_directory = "./testcases/"
file = testcase + ".word"
with open(file_directory+file, 'r') as f:
    for i in range(10000):
        line = f.readline()
        tmp = line.split(' ')
        for word in tmp:
            if word not in ans:
                ans[word] = 1
            else:
                ans[word] += 1


# print(sorted(ans.items(), key = lambda kv:(kv[1], kv[0]))) 

# items = ans.items()
tmp = words.items()
# print(tmp)
# for i in items:
#     if i not in tmp:
#         print(i)
# print(len(items))
print(len(tmp))
print(len(ans))
print(tmp, end = '\n\n\n')
print(ans)