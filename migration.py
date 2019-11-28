# -*- coding=UTF-8 -*-
# @date   2019-11-28 13:44
from elasticsearch import Elasticsearch
from elasticsearch import helpers

from_index = {
    "client": "http://localhost:9200/",
    "index": "search_sug",
    "doc_type": "sug"
}

to_index = {
    "client": "http://localhost:9201/",
    "index": "search_sug",
    "doc_type": "sug"
}

size_limit = 500


def migration(from_index, to_index, size_limit):
    from_client = Elasticsearch(from_index['client'])
    body = {
        "query": {
        },
        "from": 0,
        "size": size_limit,
    }

    index = from_index['index']
    doc_type = from_index['doc_type']
    size = size_limit
    scroll = '2m'

    # Check index exists
    if not from_client.indices.exists(index=index):
        raise Exception("Index %s not exists" % index)

    # Init scroll by search
    data = from_client.search(
        index=index,
        doc_type=doc_type,
        scroll=scroll,
        size=size,
        body=body
    )

    # Get the scroll ID
    scroll_id = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])

    while scroll_size > 0:
        print "Scrolling:%s" % scroll_id
        # Before scroll, process current batch of hits
        docs = data['hits']['hits']
        if len(docs) > 0:
            if docs and len(docs) > 0:
                index_dsl = []
                es_client_target = Elasticsearch(to_index['client'])
                for doc in docs:
                    index_dsl.append({
                        "_op_type": "index",
                        "_index": to_index['index'],
                        "_type": to_index['doc_type'],
                        "_id": doc['_id'],
                        "_source": doc['_source']
                    })
                print helpers.bulk(es_client_target, index_dsl)

        data = from_client.scroll(scroll_id=scroll_id, scroll=scroll)
        # Update the scroll ID
        scroll_id = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])


migration(from_index, to_index, size_limit)
