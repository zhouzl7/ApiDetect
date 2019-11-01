from flask import Flask, request
import requests
import json
import datetime
from AnomalyDetection import AnomalyDetection
from ElasticSearch import ElasticObj

app = Flask(__name__)


@app.route('/api/v1/detect', methods=["GET", "POST"])
def detect():
    response = {"code": 200, "msg": "ok", "data": ""}
    if request.method == 'POST':
        # analyze request
        data = request.get_data()
        json_data = json.loads(data.decode("utf-8"))
        taskId = json_data.get("taskId")
        dataSource = json_data.get("dataSource")
        algorithms = json_data.get("algorithms")
        threshold = json_data.get("threshold")

        elasticIndex = dataSource['index']
        elasticType = dataSource['type']
        elasticRange = dataSource['range']
        elasticCompareRange = dataSource['compareRange']

        timeNow = datetime.datetime.now()
        timeNow.strftime('%Y-%m-%d %H:%M:%S')
        time60SecondsBefore = timeNow - datetime.timedelta(seconds=elasticRange)
        time60SecondsBefore.strftime('%Y-%m-%dT%H:%M:%S')
        time360SecondsBefore = timeNow - datetime.timedelta(seconds=elasticCompareRange)
        time360SecondsBefore.strftime('%Y-%m-%dT%H:%M:%S')
        # search elastic data set
        elasticSearch = ElasticObj(elasticIndex, elasticType)
        trainDataSetQuery = {
            "query": {
                "range": {
                    "timestamp": {
                        "lt": time60SecondsBefore,
                        # "gte": time360SecondsBefore
                    }
                }
            }
        }
        trainDataSet = elasticSearch.getSearchResult(trainDataSetQuery)
        if not trainDataSet:
            response['msg'] = 'training data set is empty!'
            return json.dumps(response)

        detectDataSetQuery = {
            "query": {
                "range": {
                    "timestamp": {
                        "lte": timeNow,
                        # "gte": time60SecondsBefore
                    }
                }
            }
        }
        detectDataSet = elasticSearch.getSearchResult(detectDataSetQuery)
        if not detectDataSet:
            response['msg'] = 'detecting data set is empty!'
            return json.dumps(response)

        # run anomaly detect algorithms
        anomalyQuantity = 0
        for algorithm in algorithms:
            anomalyDetection = AnomalyDetection(algorithm)
            status = anomalyDetection.run(trainDataSet, detectDataSet)
            if status:
                anomalyQuantity += 1

        result = 0
        if anomalyQuantity >= threshold:
            result = 1

        # report result
        reportRequest = json.dumps({"taskId": taskId,
                                    "dataSource": dataSource,
                                    "timeStamp": timeNow.strftime('%Y-%m-%dT%H:%M:%S'),
                                    "result": result,
                                    })
        print("reportRequest = %s" % reportRequest)
        headers = {'Content-Type': 'application/json'}
        reportResponse = requests.post("http://47.95.199.184/api/v1/report", data=reportRequest, headers=headers)
        print(reportResponse.json())
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
