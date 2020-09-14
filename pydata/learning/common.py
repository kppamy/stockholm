import numpy.random as random


def generate_list(start, end, size):
    return random.random_integers(start, end, size)


def generate_inputs():
    nums = [2, 7, 11, 15]
    target = 9
    s1 = [nums, target]

    nums = [3, 2, 4]
    target = 6
    s2 = [nums, target]

    nums = [3, 3]
    target = 6
    s3 = [nums, target]
    return [s1, s2, s3]


def format_test(func, inputs_func=None, print_func=None):
    if inputs_func is not None:
        cases = inputs_func()
    else:
        cases = generate_inputs()
    for case in cases:
        res = func(case[0], case[1])
        print(case)
        print(func.__name__ + ":")
        if print_func is not None:
            print_func(res)
        else:
            if isinstance(res, int):
                print(str(res))
            else:
                print(res)