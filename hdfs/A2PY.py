#importing hdfs
import hdfs
from hdfs import InsecureClient
import posixpath as psp

client = InsecureClient(url='http://localhost:9870', root='/')

#Task 1 to 10 starts from here

print('1. Creating directory dict1 and dict2')
client.makedirs('/dict1/')
client.makedirs('/dict1/question/')
client.makedirs('/dict1/answer/')
client.makedirs('/dict2/')
client.makedirs('/dict2/question/ ')

print('2. Adding the A2_Question.txt into HDFS as the path /dict1/question/')
client.upload('/dict1/question/', 'A2_Question.txt')

print("3. Listing  the content of dir /dict1/question/")
print(client.list('/dict1/'))

print("4. Reading content of question file from directory /dict1/question/")
with client.read('/dict1/question/A2_Question.txt', encoding="utf-8") as data:
    print(data.read())

print('5. Moving HDFS file /dict1/question/A2_Question.txt to /dict2/question/ and renaming it as A2_QuestionCopy.txt')
client.rename('/dict1/question/A2_Question.txt', '/dict2/question/A2_QuestionCopy.txt')

print("6. Putting the A2_Answer.txt into HDFS as the path: /dict1/answer/")
client.upload('/dict1/answer/', 'A2_Answer.txt')

print("7. Reading the contents of A2_Answer.txt")
with client.read('/dict1/answer/A2_Answer.txt', encoding="utf-8") as data:
    print(data.read())

print("8. Listing the disk space used by the HDFS directories /dict1/answer/ and /dict1/question/")
print(client.content('/dict1/answer/'))
print(client.content('/dict1/question/'))

print("9. Recursively listing the contents of the HDFS directories /dict1/")
fpaths = [psp.join(dpath, fname) for (dpath, _, fnames) in client.walk('/dict1/') for fname in fnames]
print(fpaths)

print("10. Removing all files/directories underneath the HDFS directories named /dict1/ and /dict2/")
client.delete('/dict1/', recursive=True)
client.delete('/dict2/', recursive=True)