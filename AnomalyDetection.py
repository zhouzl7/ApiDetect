import json
import numpy
import Global
import re
from MyThread import MyThread
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler


def elasticJson2NumberVector(jsonData):
    """
    :param jsonData: json
    :return numberVector: list
    """
    numberVector = []
    for key in jsonData.keys():
        value = jsonData[key]
        if key == "label" or key == "timestamp":
            continue
        elif key == "srcMAC" or key == "dstMAC":
            if value is None or len(value) == 0:
                numberVector.append(0)
            else:
                macList = value.split(':')
                for elem in macList:
                    numberVector.append(int(elem, 16))
        elif key == "srcIP" or key == "dstIP":
            if value is None or len(value) == 0:
                numberVector.append(0)
            else:
                numberVector += list(map(int, value.split('.')))
        else:
            if value is None or len(value) == 0 or value == "false":
                numberVector.append(0)
            elif value == "true":
                numberVector.append(1)
            else:
                rePattern = re.compile('[0-9]+')
                match = rePattern.findall(value)
                if len(match) == 0:
                    numberVector.append(hash(value) % 10000)
                else:
                    numberVector.append(int(match[0]))
    return numberVector


def isolationForest(trainDataSet, detectDataSet, threshold):
    """
    :param trainDataSet: json list
    :param detectDataSet: json list
    :param threshold: float
    :return: bool
    """

    # extract eigenvalue && 0-1 normalization
    scaler = MinMaxScaler()
    trainFeatureMatrix = []
    for hit in trainDataSet:
        trainFeatureMatrix.append(elasticJson2NumberVector(hit['_source']))
    trainFeatureMatrix = numpy.array(trainFeatureMatrix)
    trainFeatureMatrix = scaler.fit_transform(trainFeatureMatrix)
    detectFeatureMatrix = []
    for hit in detectDataSet:
        detectFeatureMatrix.append(elasticJson2NumberVector(hit['_source']))
    detectFeatureMatrix = numpy.array(detectFeatureMatrix)
    detectFeatureMatrix = scaler.fit_transform(detectFeatureMatrix)

    # Isolation Forest train and predict
    rng = numpy.random.RandomState(42)
    maxSamples = 256
    iForest = IsolationForest(max_samples=maxSamples, random_state=rng, contamination=0, behaviour='new')
    iForest.fit(trainFeatureMatrix)
    predictResult = iForest.predict(detectFeatureMatrix)
    anomalyQuantity = 0
    length = 0
    for predict in predictResult:
        length += 1
        if predict == -1:
            anomalyQuantity += 1
    anomalyRate = anomalyQuantity / length
    print("iForest anomalyRate: %f" % anomalyRate)
    if anomalyRate >= threshold:
        return True
    else:
        return False


def jsonField2NumberVector(key, value):
    """
    :param key: string
    :param value: string
    :return numberVector: list
    """
    numberVector = []
    if key == "label" or key == "timestamp":
        return numberVector
    elif key == "srcMAC" or key == "dstMAC":
        if value is None or len(value) == 0:
            numberVector.append(0)
        else:
            macList = value.split(':')
            for elem in macList:
                numberVector.append(int(elem, 16))
    elif key == "srcIP" or key == "dstIP":
        if value is None or len(value) == 0:
            numberVector.append(0)
        else:
            numberVector += list(map(int, value.split('.')))
    else:
        if value is None or len(value) == 0 or value == "false":
            numberVector.append(0)
        elif value == "true":
            numberVector.append(1)
        else:
            rePattern = re.compile('[0-9]+')
            match = rePattern.findall(value)
            if len(match) == 0:
                numberVector.append(hash(value) % 10000)
            else:
                numberVector.append(int(match[0]))
    return numberVector


def threeSigma(trainDataSet, detectDataSet, threshold, field):
    """
    :param trainDataSet: json list
    :param detectDataSet: json list
    :param threshold: float
    :param field: string
    :return: bool
    """

    # extract eigenvalue && 0-1 normalization
    scaler = MinMaxScaler()
    trainFeatureMatrix = []
    for hit in trainDataSet:
        trainFeatureMatrix.append(jsonField2NumberVector(field, hit['_source'][field]))
    trainFeatureMatrix = numpy.array(trainFeatureMatrix)
    trainFeatureMatrix = scaler.fit_transform(trainFeatureMatrix)
    detectFeatureMatrix = []
    for hit in detectDataSet:
        detectFeatureMatrix.append(jsonField2NumberVector(field, hit['_source'][field]))
    detectFeatureMatrix = numpy.array(detectFeatureMatrix)
    detectFeatureMatrix = scaler.fit_transform(detectFeatureMatrix)

    # 3 Sigma train and predict
    mean = numpy.mean(trainFeatureMatrix)
    std = numpy.std(trainFeatureMatrix)
    threshold1 = mean - 3 * std
    threshold2 = mean + 3 * std
    anomalyQuantity = 0
    length = 0
    for elem in detectFeatureMatrix:
        length += 1
        if elem[0] < threshold1 or elem[0] > threshold2:
            anomalyQuantity += 1
    anomalyRate = anomalyQuantity / length
    print("threeSigma anomalyRate: %f" % anomalyRate)
    if anomalyRate >= threshold:
        return True
    else:
        return False


class AnomalyDetection:
    def __init__(self, algorithm):
        """
        :param algorithm: json
        """
        self.algorithmName = algorithm['name']
        algorithmParam = json.loads(algorithm['param'])
        self.algorithmParamThreshold = algorithmParam['threshold']
        if 'field' in algorithmParam:
            self.algorithmParamField = algorithmParam['field']

    def run(self, trainDataSet, detectDataSet):
        """
        :param trainDataSet: json list
        :param detectDataSet: json list
        :return: bool
        """
        if self.algorithmName == Global.ISOLATION_FOREST:
            # new thread
            task = MyThread(isolationForest, (trainDataSet, detectDataSet, self.algorithmParamThreshold))
            task.start()
            return task.get_result()
        elif self.algorithmName == Global.THREE_SIGMA:
            # new thread
            task = MyThread(threeSigma, (trainDataSet, detectDataSet,
                                         self.algorithmParamThreshold, self.algorithmParamField))
            task.start()
            return task.get_result()
        else:
            return False
