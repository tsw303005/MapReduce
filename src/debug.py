
file_directory = "./"
num_reducer = 12
chunk = 15;

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
    file = "TEST10-" + str(i) + ".txt"
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


print(sorted(words.items(), key = lambda kv:(kv[1], kv[0]))) 

ans = dict()
file_directory = "./testcases/10_sample_ans/"

for i in range(1, num_reducer+1, 1):
    file = "TEST10-" + str(i) + ".out"
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


print(sorted(ans.items(), key = lambda kv:(kv[1], kv[0]))) 

# items = ans.items()
# tmp = words.items()
# print(items)
# print(tmp)