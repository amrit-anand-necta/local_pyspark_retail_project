import configparser
from pyspark import SparkConf

#application configurations into python dictionary
def get_app_config(env):
    config = configparser.ConfigParser()
    config.read('configurations/application.conf')
    app_conf = {}
    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf


#pyspark configurations
def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read('configurations/pyspark.conf')
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf