# -*- coding: utf-8 -*-
import discogs_client as dc
import json
import pandas as pd
import sys
import time
import os

from discogs_client.exceptions import HTTPError
from optparse import OptionParser


parser = OptionParser(usage="usage: %prog [options] <dir of JSON with ids> <year> ",
                      version="%prog 1.0")


parser.add_option("-o", "--output", dest="filename",
                  help="write output to FILE", metavar="FILE")


(options, args) = parser.parse_args()

if len(args) != 2:
    parser.error("wrong number of arguments")

try:
    # make sure there is a config.py file
    import config
except:
    raise Exception('Could not import config.py -- please create this file!')

# try to access the consumer key and secret
ckey = config.consumer_key
csecret = config.consumer_secret
default_ckey = (ckey == 'your-consumer-key-here')
default_csecret = (csecret == 'your-consumer-secret-here')

if default_ckey and default_csecret:
    raise Exception('Please set variables consumer_key and '
                    'consumer_secret in config.py!\n'
                    '--obtain consumer key and secret at:'
                    ' https://www.discogs.com/settings/developers')

try:
    # make sure there is a access_tokens.py file
    import access_tokens
    access_token = access_tokens.access_token
    access_secret = access_tokens.access_secret
except:
    raise Exception('Could not import access_tokens.py -- please create this '
                    'file using the script consumer_key.py!')

uagent = ('consumerKey/2.0 +https://bitbucket.org/bigdata_famaf/tesis_illak/src/master/graph/tools/')

print('\n===\n'
      'user agent: {0}\n'
      'consumer key: {1}\n'
      'consumer secret: {2}\n'
      'access key: {3}\n'
      'access secret: {4}\n'
      '==='.format(uagent, ckey, csecret, access_token, access_secret))

# set key, secret when initializing Client
# -- also user access token & secret from previous validation
discogsClient = dc.Client(uagent, consumer_key=ckey, consumer_secret=csecret,
              token=access_token, secret=access_secret)

dirName = args[0] if args[0].endswith('/') else args[0] + "/"
year = args[1]

def getArtistImage(i):
    time.sleep(0.3)
    try:
        artist = discogsClient.artist(i)
        artist.name
        if('images' in artist.data.keys()):
            return artist.images[0]['uri150']
        else:
            return "https://s.discogs.com/images/default-release.png"
    except:
         print("Unexpected error: {0}, with id: {1}".format(sys.exc_info()[0],i))

def getReleaseImage(i):
    time.sleep(0.3)
    try:
        release = discogsClient.release(i)
        release.title
        if('images' in release.data.keys()):
            return release.images[0]['uri150']
        else:
            return "https://s.discogs.com/images/default-release.png"
    except:
        print("Unexpected error: {0}, with id: {1}".format(sys.exc_info()[0],i))
        return "error fetching image"

df = pd.read_json(dirName + "vertices_id_list_" + year + ".json" , lines=True)

artists_dir = "artists_images_" + year + ".csv" if not options.filename else options.filename + "/artists_images_" + year + ".csv"
releases_dir = "releases_images_" + year + ".csv" if not options.filename else options.filename + "/releases_images_" + year + ".csv"


if options.filename:
    os.makedirs(options.filename, exist_ok=True)


print("\n extracting images for artists, please wait...")

df['image_url'] = df['id'].apply(getArtistImage)
df.to_csv(artists_dir, encoding='utf-8', index=False)

print("DONE!")

df = pd.read_json(dirName + "edges_id_list_" + year + ".json" , lines=True)

print("\n extracting images for releases, please wait...")

df['image_url'] = df['id'].apply(getReleaseImage)
df.to_csv(releases_dir, encoding='utf-8', index=False)

print("DONE!")
