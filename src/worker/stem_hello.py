from nltk.stem import LancasterStemmer
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize

lancaster = LancasterStemmer()
porter = PorterStemmer()
# #coba kata
# print(lancaster.stem("dilligent"))

# #coba kalimat
# sentence = "Pythoners are very intelligent and work very pythonly and now they are pythoning their way to success."
# def stemSentence(sentence):
#     token_words = word_tokenize(sentence)
#     token_words
#     stem_sentence=[]
#     for word in token_words:
#         stem_sentence.append(lancaster.stem(word))
#         stem_sentence.append(" ")
#     return "".join(stem_sentence)

# x = stemSentence(sentence)
# print(x)

#coba document
file = open("src/worker/file.txt")
content = file.read()
print(content)
my_lines_list=file.readlines()
my_lines_list
#print(my_lines_list)

def stemSentence(sentence):
    token_words = word_tokenize(sentence)
    token_words
    stem_sentence=[]
    for word in token_words:
        stem_sentence.append(porter.stem(word))
        stem_sentence.append(" ")
    return "".join(stem_sentence)

stem_file = open("src/worker/stemmedText.txt", mode="a+", encoding="utf-8")
for line in my_lines_list:
    stem_sentence = stemSentence(line)
    #stem_file.write(stem_sentence)
    print(stem_sentence)
stem_file.close()
