from elasticsearch import Elasticsearch
import Global


class ElasticObj(object):
    def __init__(self, index_name, index_type):
        """
        :param index_name: string
        :param index_type: string
        """
        self.index_name = index_name
        self.index_type = index_type
        self.es = Elasticsearch(
            [Global.ELASTIC_IP],
            http_auth=(Global.ELASTIC_USER, Global.ELASTIC_PASSWORD),
            port=Global.ELASTIC_PORT,
            use_ssl=False
        )

    def getSearchResult(self, body, scroll='5m', timeout='1m', size=1000):
        """
        :param body: json
        :param scroll: string
        :param timeout: string
        :param size: int
        :return hitData: json list
        """
        queryData = self.es.search(index=self.index_name, doc_type=self.index_type, scroll=scroll, timeout=timeout,
                                   size=size, body=body)
        hitData = queryData['hits']['hits']
        if not hitData:
            print('search result is empty!')
        scroll_id = queryData["_scroll_id"]
        total = queryData['hits']['total']
        print("hit data quantity: %d" % total)
        for i in range(int(total/size)):
            res = self.es.scroll(scroll_id=scroll_id, scroll=scroll)
            hitData = hitData + res['hits']['hits']
        return hitData


