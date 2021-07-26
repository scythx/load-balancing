import requests

def readFiles(filePath):
    file = open(filePath)
    return file

content = readFiles("src/worker/file.txt").read()
#print(content)

requests.get(
    'http://localhost:5000', 
    params={
        'webhook': 'https://webhook.site/f7ae6bc8-6e24-47b7-8895-6f92612176fd'
    },
    data=content)
