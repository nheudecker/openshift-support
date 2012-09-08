#!/usr/bin/python

###############################################################################
# Copyright 2012 Nick Heudecker
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

"""Simple script to pull in Twitter search results and insert into MongoDB."""

from optparse import OptionParser
from pymongo import Connection

import urllib2
import json


def get_options():
    parser = OptionParser(usage="usage: python %prog [options]")
    parser.add_option("-t", "--target", default="localhost", dest="target", 
                        help="target MongoDB host")
    parser.add_option("-d", "--database", default="openshiftidxs", dest="database",
                        help="target MongoDB database")
    parser.add_option("-q", "--query", default="openshift", dest="query",
                        help="Twitter search string")
                        
    (options, args) = parser.parse_args()
    
    return options
    
    
def load_tweets(query, target, database):
    conn = Connection(target, 27017)
    db = conn[database]
    coll = db['tweets']
    
    for page in xrange(1, 16):
        print "Processing page " + str(page) + " for query " + query
        url = "http://search.twitter.com/search.json?include_entities=true&result_type=mixed&rpp=100&q={}&page={}".format(query, page)
        req = urllib2.Request(url)
        res = urllib2.urlopen(req)
        payload = json.load(res)
        if len(payload['results']) > 0: 
            coll.insert(payload['results'])
    

if __name__ == "__main__":
    options = get_options()
    load_tweets(options.query, options.target, options.database)
