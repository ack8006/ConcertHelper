from difflib import SequenceMatcher


def ratio(item_one, item_two, ignore_case=False):
    if ignore_case:
        item_one, item_two = item_one.lower(), item_two.lower()
    return SequenceMatcher(None, item_one, item_two).ratio()

def partial_ratio(item_one, item_two, ignore_case=False):
    if len(item_one) <= len(item_two):
        shrt, lng = item_one, item_two
    else:
        shrt, lng = item_two, item_one
    diff = len(lng)-len(shrt)
    return max(map(lambda x: ratio(shrt, lng[x:x+len(shrt)]), xrange(diff+1)))

def token_sorted_ratio(item_one, item_two, ignore_case=False):
    item_one = ' '.join(sorted(item_one.split()))
    item_two = ' '.join(sorted(item_two.split()))
    return ratio(item_one, item_two, ignore_case)

def token_set_ratio(item_one, item_two, ignore_case=False):
    set_one, set_two = set(item_one.split()), set(item_two.split())
    intersection = set_one & set_two
    for token in intersection:
        set_one.discard(token)
        set_two.discard(token)

    intersection = ' '.join(intersection)
    set_one = ' '.join(set_one)
    set_two = ' '.join(set_two)
    #intersection vs. intersection + rest of item one
    #intersection vs. intersection + rest of item two
    #intersection vs. intersection + rest of both
    return max(ratio(intersection, intersection + ' ' + set_one, ignore_case),
               ratio(intersection, intersection + ' ' + set_two, ignore_case),
               ratio(intersection + ' ' + set_one, intersection+' '+set_two, ignore_case))

def all_techniques(item_one, item_two, ignore_case = False):
    return max(ratio(item_one, item_two, ignore_case),
               partial_ratio(item_one, item_two, ignore_case),
               token_sorted_ratio(item_one, item_two, ignore_case),
               token_set_ratio(item_one, item_two, ignore_case))

def avg_all_techniques(item_one, item_two, ignore_case = False):
    return sum([ratio(item_one, item_two, ignore_case),
               partial_ratio(item_one, item_two, ignore_case),
               token_sorted_ratio(item_one, item_two, ignore_case),
               token_set_ratio(item_one, item_two, ignore_case)])/4.0

#def get_best_scores(item_one, choices, limit, fn):
#    def wrapped():
#        match_scores = map(lambda x: (x, fn()(item_one,x)), choices)
#        return sorted(match_scores, key = lambda x:x[1], reverse=True)[:limit]
#    return wrapped

#***This feels like it could use decorators
#@get_best_scores(item_one, choices, limit)
def best_match_ratio(item_one, choices, limit=1):
    return get_best_scores(item_one, choices, limit, ratio)

def best_match_partial(item_one, choices, limit=1):
    return get_best_scores(item_one, choices, limit, partial_ratio)

def best_match_token(item_one, choices, limit=1):
    return get_best_scores(item_one, choices, limit, token_sorted_ratio)

def best_match_token_set(item_one, choices, limit = 1):
    return get_best_scores(item_one, choices, limit, token_set_ratio)

def best_match_all(item_one, choices, limit = 1):
    return get_best_scores(item_one, choices, limit, all_techniques)

def best_match_avg(item_one, choices, limit = 1):
    return get_best_scores(item_one, choices, limit, avg_all_techniques)


def get_best_scores(item_one, choices, limit, match_function):
    match_scores = map(lambda x: (x, match_function(item_one,x)), choices)
    return sorted(match_scores, key = lambda x:x[1], reverse=True)[:limit]



if __name__ == '__main__':
    #print best_match_ratio('yankees', ['new york yankees', 'mets', 'knicks', 'rangers'])
    #print best_match_partial('yankees', ['new york yankees', 'mets', 'knicks', 'rangers'], 3)
    #print best_match_token('yankees of new york', ['new york yankees', 'mets', 'knicks', 'rangers'], 3)
    print best_match_avg('yankees of new york', ['new york yankees', 'mets', 'knicks', 'rangers'], 3)





