import re
import sys

from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def computeContribs(urls, rank):
	num_urls = len(urls)
	tset = set()
	fset = set()
	for url in urls:
		if url[0] == 'T':
			tset.add(url[1:])
		elif url[0] == 'F':
			fset.add(url[1:])

	# to set is empty
	ret = []
	if len(tset) == 0:
		for url in fset:
			ret.append((url, rank / len(fset)))
	else:
		for url in tset:
			ret.append((url, rank / len(tset)))
		# yield (url, rank / num_urls)
	return ret

def parseNeighbors(urls):
	mt = re.match(r'"(\d+)"\t"(\d+)"', urls)
	ret = []
	if mt:
		ret.append((mt.group(1), "T" + mt.group(2)))
		ret.append((mt.group(2), "F" + mt.group(1)))
	# return mt.group(1), mt.group(2)
	return ret



if __name__ == "__main__":

	appName = 'pagerank'
	conf = SparkConf()
	conf.setAppName(appName)
	conf.setMaster('local')
	sc = SparkContext(conf=conf)
	inputs = sc.textFile('/Input/page_rank_data_small.txt')
	lines = inputs.map(lambda r: r[0])
	links = lines.flatMap(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
	ranks = links.flatMap(lambda url_neighbors: (url_neighbors[0], 1.0))
	for iteration in range(int(sys.argv[1])):
		contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

	# ranks.coalesce(1).saveAsTextFile('/output/result')
	for (url, rank) in ranks.collect():
		print(url, rank)
