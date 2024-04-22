import collections
def keyword_index(docs):
    # implement this
    d = collections.defaultdict(lambda:collections.defaultdict(int))
    for i, v in enumerate(docs):
        for w in v.split(' '):
            d[w][i] += 1
    return d

docs = ["Hello world", "world of python", "python is a snake"]
print(keyword_index(docs))

# Expected output: {'Hello': {0: 1}, 'world': {0: 1, 1: 1}, 'of': {1: 1}, 'python': {1: 1, 2: 1}, 'is': {2: 1}, 'a': {2: 1}, 'snake': {2: 1}}

d = defaultdict(dict)
for i,s in enumerate(docs):
    for j, w in enumerate(s.split(' ')):
        d[w][i]=j
print(d)
