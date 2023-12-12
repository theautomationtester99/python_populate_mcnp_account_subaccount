import numpy as np
import string
import random


def gen_rand(start1, end1, num1):
    res = []

    for j in range(num1):
        res.append(np.random.randint(start1, end1))

    return res


def gen_rand1(start1, end1):
    res = (random.randint(start1, end1))
    return res


def gen_rand_alpha(num_digits):
    res = ''.join(random.choices(string.ascii_uppercase + string.digits, k=num_digits))
    print("The generated random string : " + str(res))
    return str(res)
