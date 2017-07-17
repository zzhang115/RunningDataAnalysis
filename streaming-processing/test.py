test = (1,23, 'abc')
print(tuple(test)[1])

def abc(x, y):
    return 'hello', (x, y)

if __name__ == '__main__':
    print(tuple(abc(2, 3))[0])