import normality
from servicelayer.cache import make_key

from aleph.core import es, kv
from aleph.model import Entity
from aleph.index.indexes import entities_read_index


def get_total():
    key = make_key('names', '_total')
    total = kv.get(key)
    if total is not None:
        return int(str(total))
    query = {
        'size': 0,
        'query': {'match_all': {}}
    }
    index = entities_read_index(Entity.THING)
    res = es.search(index=index, body=query)
    count = res.get('hits', {}).get('total')
    kv.set(key, count, ex=3600)
    return count


def get_composite_frequency(name):
    name = normality.normalize(name, latinize=True)
    names = [n for n in name.split(' ') if len(n)]
    keys = [make_key('names', n) for n in names]
    counts = {n: c for n, c in zip(names, kv.mget(keys))}
    uncached = [n for (n, c) in counts.items() if c is None]
    queries = []
    for name in uncached:
        queries.append({'index': entities_read_index(Entity.THING)})
        queries.append({
            'size': 0,
            'query': {'term': {'names.text': name}}
        })
    if len(queries):
        res = es.msearch(body=queries)
        for name, resp in zip(uncached, res.get('responses', [])):
            total = resp.get('hits', {}).get('total')
            key = make_key('names', name)
            counts[name] = total
            kv.set(key, total, ex=3600)
    total = get_total()
    return counts, total
