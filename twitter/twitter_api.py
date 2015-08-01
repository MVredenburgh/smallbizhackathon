import requests
import json
import os
import twitter
from time import sleep
from random import sample, shuffle
import unicodecsv as csv
import codecs
import nltk
import re
from nltk.tokenize import word_tokenize
from geopy.geocoders import Nominatim

def write_csv(header, rows, filename, header_order=[]):
    print "Writing CSV: "+filename
    header = [h for h in header_order if h in header] + [h for h in header if h not in header_order]
    with open(filename,"wb") as o:
        o.write(codecs.BOM_UTF8)
        writer = csv.writer(o,dialect=csv.excel)
        writer.writerow(header)
        for row in rows:
            writer.writerow([unicode(row.get(x,'')) for x in header])


def jdump(jval, filename):
    jdir = os.path.dirname(filename)
    if jdir and not os.path.exists(jdir):
        os.makedirs(jdir)
    with open(filename, 'w') as w:
        json.dump(jval, w, indent=2)

def jload(filename):
    with open(filename, 'r') as r:
        return json.load(r)

def jprint(jval):
    print json.dumps(jval, indent=2)

def batchify(func, items, batch_size, batch_delay=0):
    batch = []
    for i, item in enumerate(items):
        batch.append(item)
        if len(batch) >= batch_size:
            print "Processing",i+1,"items"
            for result in func(batch):
                yield result
            batch = []
            sleep(batch_delay)
    if batch:
        for result in func(batch):
            yield result


GEOLOCATOR = Nominatim()
GEO_CACHE_FILE = "geo_cache.json"
GEO_CACHE = {}
if os.path.exists(GEO_CACHE_FILE):
    GEO_CACHE = jload(GEO_CACHE_FILE)


def geocode(address):
    if address not in GEO_CACHE:
        try:
            location = geolocator.geocode(address)
            GEO_CACHE[address] = {'lat': location.latitude, 'lon':location.longitude, 'raw': location.raw}
            jdump(GEO_CACHE, GEO_CACHE_FILE)
            print "Updated cache with", address
        except:
            GEO_CACHE[address] = {}
            jdump(GEO_CACHE, GEO_CACHE_FILE)
            print "Updated cache with", address
    return GEO_CACHE[address]


TWITTER_API_KEY = os.environ['TWITTER_API_KEY']
TWITTER_API_SECRET = os.environ['TWITTER_API_SECRET']
TWITTER_ACCESS_TOKENS = jload('twitter_access_tokens.json')
TWITTER_APIS = [twitter.Api(consumer_key=TWITTER_API_KEY,
                            consumer_secret=TWITTER_API_SECRET,
                            access_token_key=t['access_token'],
                            access_token_secret=t['access_token_secret']) for t in TWITTER_ACCESS_TOKENS]

VALID_APIS = []
for api in TWITTER_APIS:
    try:
        rls = api.GetRateLimitStatus()
        if rls['rate_limit_context']:
            VALID_APIS.append(api)
            print rls['rate_limit_context']
    except:
        pass



def _get_profile_file(user_id):
    user_str = str(user_id)
    return 'profiles/'+user_str[-3:-1]+'/'+user_str+'.json'

def _get_also_follows_file(user_id):
    user_str = str(user_id)
    return 'also_follows/'+user_str[-3:-1]+'/'+user_str+'.json'

def fetch_profiles(fetch_profile_ids):
    for api in VALID_APIS:
        remaining_profile_ids = [uid for uid in fetch_profile_ids if not os.path.exists(_get_profile_file(uid))]
        shuffle(remaining_profile_ids)
        remaining_profile_ids = remaining_profile_ids[:1000]
        for i, user in enumerate(batchify(lambda x: api.UsersLookup(user_id=x),
                                          remaining_profile_ids,
                                          100)):
            user_json = user.AsDict()
            profile_file = _get_profile_file(user_json['id'])
            jdump(user_json, profile_file)
            print "saved", profile_file



def _get_followers_file(screen_name):
    return screen_name.replace('@','')+'_follower_ids.json'

def save_followers(screen_name):
    followers_file = _get_followers_file(screen_name)
    all_follower_ids = []
    for i, follower_id in enumerate(VALID_APIS[0].GetFollowerIDs(screen_name=screen_name)):
        all_follower_ids.append(follower_id)
        if i%100 == 0:
            jdump(all_follower_ids, followers_file)
            print "Saved",i,"follower_ids for",screen_name
    jdump(all_follower_ids, followers_file)


def get_followers(screen_name):
    followers_file = _get_followers_file(screen_name)
    if not os.path.exists(followers_file):
        save_followers(screen_name)
    return jload(followers_file)


def fetch_also_follows(all_follower_ids):
    remaining_follower_ids = [uid for uid in all_follower_ids if not os.path.exists(_get_also_follows_file(uid))]
    shuffle(remaining_follower_ids)
    for i, user_id in enumerate(remaining_follower_ids):
        follower_file = _get_also_follows_file(user_id)
        if not os.path.exists(follower_file):
            try:
                api = VALID_APIS[i%len(VALID_APIS)]
                user_follower_ids = list(api.GetFriendIDs(user_id=user_id, count=5000, total_count=50000))
                jdump(user_follower_ids, follower_file)
                print "Processed follower",i,":",follower_file
            except:
                print "Error with follower",i,":",user_id
            sleep(1)



def get_profile(screen_name):
    profile_file = 'profiles_by_name/'+screen_name.replace('@','')+'_follower_ids.json'
    if not os.path.exists(profile_file):
        profile = VALID_APIS[0].UsersLookup(screen_name=[screen_name])[0].AsDict()
        jdump(profile, profile_file)
    return jload(profile_file)


def my_followers_also_follow(my_profile_id, all_follower_ids, threshold=0.05):
    users_with_profiles = [uid for uid in all_follower_ids if os.path.exists(_get_profile_file(uid))]
    users_with_also_follows = [uid for uid in all_follower_ids if os.path.exists(_get_also_follows_file(uid))]
    
    sample_users = list(set(users_with_also_follows).intersection(set(users_with_profiles)))
    
    also_follows_map = {}
    for user_id in sample_users:
        user_follows = jload(_get_also_follows_file(user_id))
        for follower_id in user_follows:
            also_follows_map[follower_id] = also_follows_map.get(follower_id, 0)+1
    
    top_also_followers = [k for k,v in also_follows_map.iteritems() if float(v)/float(len(sample_users)) > threshold]
    top_also_followers.sort(key=lambda x: also_follows_map[x])
    top_also_followers.reverse()
    
    print "Fetching profiles for", len(top_also_followers), "top also-followed"
    fetch_profiles(top_also_followers)
    top_also_followers = [t for t in top_also_followers if os.path.exists(_get_profile_file(t))]
    
    followed_by_me = jload(_get_also_follows_file(my_profile_id))
    
    header = ['%_of_my_followers_who_follow_them',
              '%_of_their_followers_who_follow_me',
              'screen_name',
              'name',
              'total_followers_count',
              'other_accounts_followed_count',
              'follows_me',
              'followed_by_me',
              'description',
              'location',
              'url',
              'IsEvent',
              'IsMedia',
              'IsCelebrity',
              'IsSocialCause',
              ]
    
    def rows():
        for profile_id in top_also_followers:
            profile = jload(_get_profile_file(profile_id))
            my_followers_pct = float(also_follows_map[profile_id])/float(len(sample_users))
            their_followers_pct = my_followers_pct*float(len(all_follower_ids))/float(profile['followers_count']) if profile.get('followers_count','') else ''
            
            profile_row = { '%_of_my_followers_who_follow_them': my_followers_pct,
                            '%_of_their_followers_who_follow_me': their_followers_pct,
                            'screen_name': profile.get('screen_name',''),
                            'name': profile.get('name',''),
                            'total_followers_count': profile.get('followers_count',''),
                            'other_accounts_followed_count': profile.get('friends_count',''),
                            'follows_me': "yes" if profile_id in all_follower_ids else "",
                            'followed_by_me': "yes" if profile_id in followed_by_me else "",
                            'description': profile.get('description',''),
                            'location': profile.get('location',''),
                            'url': profile.get('url', '')
                           }
            yield profile_row
    
    return header, rows()


    
def get_profile_keywords(all_follower_ids):
    users_with_profiles = [uid for uid in all_follower_ids if os.path.exists(_get_profile_file(uid))]
    stop_words = [t.lower() for t in nltk.corpus.stopwords.words('english')]
    
    descriptions_map = {}
    description_count = 0
    for i, profile_id in enumerate(users_with_profiles):
        profile = jload(_get_profile_file(profile_id))
        description_tokens = [re.sub('[^a-z]+', '', t.lower()) for t in word_tokenize(profile.get('description',''))]
        description_tokens = [d for d in description_tokens if d not in ['http']]
        description_tokens = [t for t in description_tokens if len(t) > 1 and t not in stop_words]
        description_tuples = [ description_tokens[j]+" "+description_tokens[j+1] for j in range(len(description_tokens)-1)]
        if description_tokens:
            description_count = description_count+1
        for t in description_tokens + description_tuples:
            descriptions_map[t] = descriptions_map.get(t,0)+1
        if i%100 == 0:
            print "Processed",i,"profiles"
    
    top_descriptions = list(descriptions_map.keys())
    top_descriptions.sort(key=lambda x: descriptions_map[x])
    top_descriptions.reverse()
    
    top_descriptions_summary = top_descriptions[:200]
    top_descriptions_summary = top_descriptions_summary + [t for t in top_descriptions if t not in top_descriptions_summary and ' lover' in t][:20]
    top_descriptions_summary = top_descriptions_summary + [t for t in top_descriptions if t not in top_descriptions_summary and ' enthusiast' in t][:20]
    top_descriptions_summary.sort(key=lambda x: descriptions_map[x])
    top_descriptions_summary.reverse()
    
    header = ['description_keyword',
              'appears_in_profile_%',
              'appears_in_profile_count',
              'is_lover',
              'is_enthusiast',
              ]
    
    def rows():
        for description in top_descriptions_summary:
            row = { 'description_keyword': description,
                    'appears_in_profile_%': float(descriptions_map[description])/float(description_count),
                    'appears_in_profile_count': descriptions_map[description],
                    'is_lover': 'Y' if ' lover' in description else '',
                    'is_enthusiast': 'Y' if ' enthusiast' in description else '',
                   }
            yield row
    
    return header, rows()



def get_followers_info(followed_by_me):
    users_with_profiles = [uid for uid in all_follower_ids if os.path.exists(_get_profile_file(uid))]
    
    header = ['screen_name',
              'name',
              'total_followers_count',
              'other_accounts_followed_count',
              'followed_by_me',
              'description',
              'location',
              'url',
              'geo_lat',
              'geo_lon',
              ]
    
    def rows():
        for i, profile_id in enumerate(users_with_profiles):
            profile = jload(_get_profile_file(profile_id))
            profile_row = { 'screen_name': profile.get('screen_name',''),
                            'name': profile.get('name',''),
                            'total_followers_count': profile.get('followers_count',''),
                            'other_accounts_followed_count': profile.get('friends_count',''),
                            'followed_by_me': "yes" if profile_id in followed_by_me else "",
                            'description': profile.get('description',''),
                            'location': profile.get('location',''),
                            'url': profile.get('url', ''),
                            'geo_lat': geocode(profile.get('location','')).get('lat') or '',
                            'geo_lon': geocode(profile.get('location','')).get('lon') or '',
                            }
            yield profile_row
    
    return header, rows()



# BEGIN Profile Image Saver
def save_profile_photos(all_follower_ids):
    users_with_profiles = [uid for uid in all_follower_ids if os.path.exists(_get_profile_file(uid))]
    for i, profile_id in enumerate(users_with_profiles):
        profile = jload(_get_profile_file(profile_id))
        profile_image_url = profile.get('profile_image_url', '')
        if profile_image_url:
            extn = profile_image_url.split('.')[-1]
            if extn.lower() not in ['jpg', 'png', 'jpeg']:
                continue
            profile_image_filename = 'images/'+str(profile_id)+'.'+extn
            profile_image_filename = profile_image_filename.lower()
            try:
                resp = requests.get(profile_image_url)
                if resp.status_code == 200:
                    if not os.path.exists(os.path.dirname(profile_image_filename)):
                        os.makedirs(os.path.dirname(profile_image_filename))
                    with open(profile_image_filename, 'wb') as w:
                        w.write(resp.content)
                    print "Saved image",i,profile_image_filename
            except:
                pass

    
    

PROFILE_NAME = 'UrbnEarth'
my_profile_id = get_profile('UrbnEarth')['id']

all_follower_ids = get_followers(PROFILE_NAME)

fetch_profiles([my_profile_id]+all_follower_ids)
fetch_also_follows([my_profile_id]+all_follower_ids)

header, rows = my_followers_also_follow(my_profile_id, all_follower_ids)
write_csv(header, rows, 'twitter_'+PROFILE_NAME+'_followers_also_follow.csv')

header, rows = get_profile_keywords(all_follower_ids)
write_csv(header, rows, 'twitter_'+PROFILE_NAME+'_followers_self_describe_as.csv')

header, rows = get_followers_info(all_follower_ids)
write_csv(header, rows, 'twitter_'+PROFILE_NAME+'_followers.csv')

save_profile_photos([my_profile_id]+all_follower_ids)
