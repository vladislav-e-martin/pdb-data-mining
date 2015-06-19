__author__ = 'vlad'

# LIBRARIES AND PACKAGES

from pprint import pprint
from collections import Counter

# CONSTANTS

# FUNCTION DECLARATIONS
def identify_keywords(raw_list):
    counter = Counter(raw_list)

    key, word = counter.keys(), counter.values()
    pprint("Key: {0}    Word: {1}".format(key, word))

    # potential_keywords = []
    # for details in raw_list:
    #     for words in details.lower().split(" "):
    #         word = words.strip("!,.?1234567890-=@#$%^&*()_+")
    #         potential_keywords.append(word)
    # for index, word in enumerate(potential_keywords):
    #     counter = Counter(potential_keywords[index])
    #         print("\n".join("{} {}".format(*p) for p in counter.most_common()))