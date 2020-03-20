
from functional import seq
from functools import reduce
import collections
from pprint import pprint
import concurrent.futures
import os
import time
import multiprocessing

# modified to be pickable...
class Worker():
    @staticmethod
    def transform(x):
        print(f'Process {os.getpid()} working record {x.name}')
        time.sleep(0.1)
        result = {'name': x.name, 'age': 2017 - x.born}
        print(f'Process {os.getpid()} done processing record {x.name}')
        return result

Scientist = collections.namedtuple('Scientist', [
    'name',
    'field',
    'born',
    'nobel',
])

scientists = (
    Scientist(name='Ada Lovelace', field='math', born=1815 , nobel=False),
    Scientist(name='Emmy Noether', field='math', born=1882 , nobel=False),
    Scientist(name='Marie Curie', field='physics', born=1867 , nobel=True),
    Scientist(name='Tu Youyou', field='chemistry', born=1930 , nobel=True),
    Scientist(name='Ada Yonauth', field='chemistry', born=1939 , nobel=True),
    Scientist(name='Vera Rubin', field='astronomy', born=1928 , nobel=False),
    Scientist(name='Sally Ride', field='physics', born=1951 , nobel=False),
)


def main():
    print('--- imutable data ---')
    pprint(scientists)
    print()

    print()
    print('--- filter ---')

    def nobel_filter(x):
        return x.nobel is True

    def born_in_1800(x):
        return x.born < 1900


    pprint(tuple(filter(nobel_filter, scientists)))
    print()

    # pprint(tuple(filter(born_in_1800, scientists)))
    # print()

    print()
    print('--- extra: chaining filters with PyFunctional ---')
    pprint(tuple(seq(scientists).filter(born_in_1800).filter(nobel_filter)))
    print()

    print()
    print('--- map ---')
    def calculate_names_and_ages(x):
        return {'name': x.name.upper(), 'age': 2017 - x.born}

    names_and_ages = tuple(map(calculate_names_and_ages, scientists))
    pprint(names_and_ages)
    print()

    print()
    print('--- reduce ---')
    def compute_age(acc, val):
        return acc + val['age']

    pprint(reduce(compute_age, names_and_ages, 0))

    print()
    print('--- reduce group by ---')
    def reducer(acc, val):
        acc[val.field].append(val.name)
        return acc

    scientist_by_field = reduce(reducer, scientists, collections.defaultdict(list))

    print()
    pprint(scientist_by_field)

    print()
    print('--- single code ---')
    start = time.time()
    result = tuple(map(Worker.transform, scientists))
    end = time.time()
    print(f"\nTime to complete: {end - start:.2f}s\n")
    # pprint(result)

    print()
    print('--- parallel ---')
    pool = multiprocessing.Pool()
    start = time.time()
    result = pool.map(Worker.transform, scientists)
    end = time.time()
    print(f"\nTime to complete: {end - start:.2f}s\n")
    # pprint(tuple(result))


    print()
    print('--- concurrent.futures ---')
    start = time.time()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        result = executor.map(Worker.transform, scientists)
    end = time.time()
    print(f"\nTime to complete: {end - start:.2f}s\n")
    # pprint(tuple(result))


if __name__ == "__main__":
    main()

