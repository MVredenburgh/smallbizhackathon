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
from wand.image import Image
from tempfile import mkstemp
import subprocess
from wordcloud import WordCloud
import sexmachine.detector as gender
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from collections import Counter

def write_csv(header, rows, filename, header_order=[]):
    print "Writing CSV: "+filename
    header = [h for h in header_order if h in header] + [h for h in header if h not in header_order]
    with open(filename,"wb") as o:
        o.write(codecs.BOM_UTF8)
        writer = csv.writer(o,dialect=csv.excel)
        writer.writerow(header)
        for row in rows:
            writer.writerow([unicode(row.get(x,'')) for x in header])


def dump(val, filename):
    jdir = os.path.dirname(filename)
    if jdir and not os.path.exists(jdir):
        os.makedirs(jdir)
    with open(filename, 'wb') as w:
        w.write(val)
    

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
for k,v in GEO_CACHE.iteritems():
    if v and v.get('raw').get('display_name'):
        v['display'] = v.get('raw').get('display_name')



def geocode(address):
    if address not in GEO_CACHE:
        try:
            location = GEOLOCATOR.geocode(address)
            GEO_CACHE[address] = {'lat': location.latitude, 'lon':location.longitude, 'display':location.raw.get('display_name'), 'raw': location.raw}
            jdump(GEO_CACHE, GEO_CACHE_FILE)
            print "Updated cache with", address
        except:
            GEO_CACHE[address] = {}
            jdump(GEO_CACHE, GEO_CACHE_FILE)
            print "Updated cache with", address
    return GEO_CACHE[address]


GENDER_CODER = gender.Detector()
def gender_code(name):
    gnd = GENDER_CODER.get_gender(name.split()[0].capitalize())
    if gnd in ['male','mostly_male']:
        return 'male'
    elif gnd in ['female','mostly_female']:
        return 'female'
    else:
        return None


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


MEDIA_ACCOUNTS = [u.replace('@','').lower().strip() for u in jload('media.list.json')]
CELEBRITY_ACCOUNTS = [u.replace('@','').lower().strip() for u in jload('celebrities.list.json')]
CAUSE_ACCOUNTS = [u.replace('@','').lower().strip() for u in jload('causes.list.json')]
ACTVITY_WORDS = list(set([f.strip().lower() for f in jload('activity.words.json')]))



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
              'id',
              'verified',
              'screen_name',
              'name',
              'gender',
              'total_followers_count',
              'other_accounts_followed_count',
              'follows_me',
              'followed_by_me',
              'description',
              'location',
              'url',
              'is_community',
              'is_celebrity',
              'is_media',
              'is_cause',
              ]
    
    def rows():
        for profile_id in top_also_followers:
            profile = jload(_get_profile_file(profile_id))
            my_followers_pct = float(also_follows_map[profile_id])/float(len(sample_users))
            their_followers_pct = my_followers_pct*float(len(all_follower_ids))/float(profile['followers_count']) if profile.get('followers_count','') else ''
            
            profile_row = { '%_of_my_followers_who_follow_them': my_followers_pct,
                            '%_of_their_followers_who_follow_me': their_followers_pct,
                            'id': profile.get('id',''),
                            'verified': 'Y' if profile.get('verified') else '',
                            'screen_name': profile.get('screen_name',''),
                            'name': profile.get('name',''),
                            'gender': gender_code(profile.get('name','')) or '',
                            'total_followers_count': profile.get('followers_count',''),
                            'other_accounts_followed_count': profile.get('friends_count',''),
                            'follows_me': "yes" if profile_id in all_follower_ids else "",
                            'followed_by_me': "yes" if profile_id in followed_by_me else "",
                            'description': profile.get('description',''),
                            'location': profile.get('location',''),
                            'url': profile.get('url', ''),
                            'is_community': 'community' in profile.get('description','').lower() and not gender_code(profile.get('name','')),
                            'is_celebrity': profile.get('screen_name').replace('@','').lower() in CELEBRITY_ACCOUNTS,
                            'is_media': profile.get('screen_name').replace('@','').lower() in MEDIA_ACCOUNTS,
                            'is_cause': profile.get('screen_name').replace('@','').lower() in CAUSE_ACCOUNTS,                            
                           }
            yield profile_row
    
    return header, rows()


    
def get_profile_keywords(all_follower_ids):
    users_with_profiles = [uid for uid in all_follower_ids if os.path.exists(_get_profile_file(uid))]
    stop_words = [t.lower() for t in nltk.corpus.stopwords.words('english')] + ['http']
    
    descriptions_map = {}
    description_count = 0
    for i, profile_id in enumerate(users_with_profiles):
        profile = jload(_get_profile_file(profile_id))
        description_tokens = [re.sub('[^a-z]+', '', t.lower()) for t in word_tokenize(profile.get('description',''))]
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
    
    header = ['id',
              'screen_name',
              'name',
              'total_followers_count',
              'other_accounts_followed_count',
              'followed_by_me',
              'description',
              'location',
              'url',
              'geo_lat',
              'geo_lon',
              'geo_display',
              'gender',
              'interest_list',
              ]
    
    def rows():
        for i, profile_id in enumerate(users_with_profiles):
            profile = jload(_get_profile_file(profile_id))
            profile_row = { 'id': profile.get('id',''),
                            'screen_name': profile.get('screen_name',''),
                            'name': profile.get('name',''),
                            'total_followers_count': profile.get('followers_count',''),
                            'other_accounts_followed_count': profile.get('friends_count',''),
                            'followed_by_me': "yes" if profile_id in followed_by_me else "",
                            'description': profile.get('description',''),
                            'location': profile.get('location',''),
                            'url': profile.get('url', ''),
                            'geo_lat': geocode(profile.get('location','')).get('lat') or '',
                            'geo_lon': geocode(profile.get('location','')).get('lon') or '',
                            'geo_display': geocode(profile.get('location','')).get('display') or '',
                            'gender': gender_code(profile.get('name','')) or '',
                            'interest_list': [aw for aw in ACTVITY_WORDS if aw in profile.get('description','').lower()]
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
            if not os.path.exists(profile_image_filename):
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

    
def normalize_images(follower_ids, dest_dir):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    for profile_id in follower_ids:
        for extn in ['jpg', 'png', 'jpeg']:
            src_file = 'images/'+str(profile_id)+'.'+extn
            dst_file = dest_dir.rstrip('/')+'/'+str(profile_id)+'.jpg'
            if os.path.exists(src_file) and not os.path.exists(dst_file):
                with open(src_file, 'rb') as r:
                    image = Image(blob=r.read())
                    image.format = 'jpg'
                    dim = min(image.width, image.height)
                    dimstr = str(dim)+'x'+str(dim)
                    image.transform(dimstr,'48x48')
                    with open(dst_file,'wb') as o:
                        image.save(o)


def _get_tmp_data(func, ext='.tmp'):
    handle, tmp_file = mkstemp(ext)
    os.close(handle)
    func(tmp_file)
    with open(tmp_file, 'rb') as r:
        file_data = r.read()
    os.remove(tmp_file)
    return file_data
    
    
    

def photo_montage(all_follower_ids, width, height):
    photo_files = ['pics_my_followers/'+str(f)+'.jpg' for f in all_follower_ids]
    available_photo_files = [p for p in photo_files if os.path.exists(p)]
    while len(available_photo_files) < (width * height):
        available_photo_files = available_photo_files + available_photo_files
    available_photo_files = available_photo_files[:(width * height)]
    shuffle(available_photo_files)
    montage_data_jpg = _get_tmp_data(lambda x: subprocess.call(["montage",
                                                 "-mode", "concatenate",
                                                 "-tile", str(width)+"x"+str(height),
                                                 ] + available_photo_files + [x]))
    return montage_data_jpg



def profile_wordcloud(all_follower_ids):
    users_with_profiles = [uid for uid in all_follower_ids if os.path.exists(_get_profile_file(uid))]
    stop_words = [t.lower() for t in nltk.corpus.stopwords.words('english')] + ['http']
    description_words = []
    
    for i, profile_id in enumerate(users_with_profiles):
        profile = jload(_get_profile_file(profile_id))
        description_tokens = [re.sub('[^a-z]+', '', t.lower()) for t in word_tokenize(profile.get('description',''))]
        description_tokens = [t for t in description_tokens if len(t) > 1 and t not in stop_words]
        description_words = description_words + description_tokens
    text = " ".join(description_words)
    wordcloud = WordCloud(background_color="white").generate(text)
    plt.imshow(wordcloud)
    plt.axis("off")
    plt_data_png = _get_tmp_data(lambda x: plt.savefig(x), ext='.png')
    plt.clf()
    return description_words, plt_data_png
    
def gender_piechart(all_follower_ids):
    users_with_profiles = [uid for uid in all_follower_ids if os.path.exists(_get_profile_file(uid))]
    genders = [gender_code(jload(_get_profile_file(uid)).get('name','')) for uid in users_with_profiles]
    male_pct = float(len([g for g in genders if g == 'male'])) / float(len(genders))
    female_pct = float(len([g for g in genders if g == 'female'])) / float(len(genders))
    unk_pct = float(len([g for g in genders if not g])) / float(len(genders))
    plt.pie([female_pct, unk_pct, male_pct],
            colors=['pink', 'lightgray', 'lightblue'],
            autopct='%1.1f%%',
            startangle=90)
    plt.axis('equal')
    plt_data_png = _get_tmp_data(lambda x: plt.savefig(x), ext='.png')
    plt.clf()
    gender_breakdown = {'M': male_pct, 'F': female_pct, '?': unk_pct}
    return gender_breakdown, plt_data_png
    

def save_faf_data(my_profile_id, faf_profile_id):
    my_profile = jload(_get_profile_file(my_profile_id))
    faf_profile = jload(_get_profile_file(faf_profile_id))
    my_followers = get_followers(my_profile['screen_name'])
    users_with_profiles = [uid for uid in my_followers if os.path.exists(_get_profile_file(uid))]
    users_with_also_follows = [uid for uid in my_followers if os.path.exists(_get_also_follows_file(uid))]
    sample_users = list(set(users_with_also_follows).intersection(set(users_with_profiles)))
    common_followers = [u for u in sample_users if faf_profile_id in jload(_get_also_follows_file(u))]
        
    my_followers_pct = float(len(common_followers))/float(len(my_followers))
    their_followers_pct = float(len(common_followers))/float(faf_profile['followers_count']) if faf_profile.get('followers_count','') else float(0)
    
    description_words, wordcloud_png = profile_wordcloud(common_followers)
    word_counts= [[k,v] for k,v in Counter(description_words).iteritems() if v > 1]
    word_counts.sort(key=lambda x: x[1])
    word_counts.reverse()
    word_counts = word_counts[:5]
    
    max_width= 10
    width = max_width if len(common_followers) > max_width else len(common_followers)
    height = (len(common_followers)/max_width) + 1
    profile_montage_jpg = photo_montage(common_followers, width, height)
    
    gender_breakdown, genderpie_png = gender_piechart(common_followers)
    
    info = { 'id': faf_profile['id'],
             'screen_name': faf_profile['screen_name'],
             'raw_profile': faf_profile,
             '%_of_my_followers_who_follow_them': my_followers_pct,
             '%_of_their_followers_who_follow_me': their_followers_pct,
             'profile_word_counts': word_counts,
             'gender_breakdown': gender_breakdown
             }
    
    str_id = str(faf_profile['id'])
    str_name = faf_profile['screen_name'].replace('@','').lower()
    jdump(info, 'assets/'+str_id+'.info.json')
    jdump(info, 'assets/'+str_name+'.info.json')
    
    dump(wordcloud_png, 'assets/'+str_id+'.wordcloud.png')
    dump(wordcloud_png, 'assets/'+str_name+'.wordcloud.png')
    
    dump(profile_montage_jpg, 'assets/'+str_id+'.profiles.jpg')
    dump(profile_montage_jpg, 'assets/'+str_name+'.profiles.jpg')
             
    dump(genderpie_png, 'assets/'+str_id+'.gender.png')
    dump(genderpie_png, 'assets/'+str_name+'.gender.png')
             




PROFILE_NAME = 'UrbnEarth'
my_profile_id = get_profile('UrbnEarth')['id']

all_follower_ids = get_followers(PROFILE_NAME)

#fetch_profiles([my_profile_id]+all_follower_ids)
#fetch_also_follows([my_profile_id]+all_follower_ids)
#save_profile_photos([my_profile_id]+all_follower_ids)
#normalize_images(all_follower_ids, 'pics_my_followers')

faf_header, faf_rows = my_followers_also_follow(my_profile_id, all_follower_ids)
faf_rows = list(faf_rows)
#write_csv(faf_header, faf_rows, 'twitter_'+PROFILE_NAME+'_followers_also_follow.csv')

fpk_header, fpk_rows = get_profile_keywords(all_follower_ids)
fpk_rows = list(fpk_rows)
#write_csv(fpk_header, fpk_rows, 'twitter_'+PROFILE_NAME+'_followers_self_describe_as.csv')

fol_header, fol_rows = get_followers_info(all_follower_ids)
fol_rows = list(fol_rows)
#write_csv(fol_header, fol_rows, 'twitter_'+PROFILE_NAME+'_followers.csv')

with open('twitter_'+PROFILE_NAME+'_montage.jpg', 'wb') as w:
    w.write(photo_montage(all_follower_ids, 50, 50))

with open('twitter_'+PROFILE_NAME+'_wordcloud.png', 'wb') as w:
    w.write(profile_wordcloud(all_follower_ids)[1])



"""
Twitter list saver:
divs = $.find('div.stream-item-header > a > span.username');
vals = [];
for(var i=0; i<divs.length; i++) {
 vals.push(divs[i].innerText.replace('@',''));
}

http://discoverahobby.com/listofhobbies
divs = $.find('div.hobbyholder > p');
vals = [];
for(var i=0; i<divs.length; i++) {
 vals.push(divs[i].innerText.replace('@',''));
 console.log(vals[i]);
}

"""

faf_communities = [faf for faf in faf_rows if faf.get('is_community')]
faf_communities.sort(key=lambda x: x.get('%_of_their_followers_who_follow_me',0))
faf_communities.reverse()
faf_communities = faf_communities[:5]

faf_celebrities = [faf for faf in faf_rows if faf.get('is_celebrity')]
faf_celebrities.sort(key=lambda x: x.get('%_of_their_followers_who_follow_me',0))
faf_celebrities.reverse()
faf_celebrities = faf_celebrities[:5]

faf_media = [faf for faf in faf_rows if faf.get('is_media')]
faf_media.sort(key=lambda x: x.get('%_of_their_followers_who_follow_me',0))
faf_media.reverse()
faf_media = faf_media[:5]

faf_causes = [faf for faf in faf_rows if faf.get('is_cause')]
faf_causes.sort(key=lambda x: x.get('%_of_their_followers_who_follow_me',0))
faf_causes.reverse()
faf_causes = faf_causes[:5]


overall_interest_cluster = Counter()
male_interest_cluster = Counter()
female_interest_cluster = Counter()
geo_cluster = Counter()
geo_interest_cluster = {}

for fol in fol_rows:
    overall_interest_cluster.update(fol.get('interest_list') or [])
    if fol.get('gender') == 'male':
        male_interest_cluster.update(fol.get('interest_list') or [])
    elif fol.get('gender') == 'female':
        female_interest_cluster.update(fol.get('interest_list') or [])
    if fol.get('geo_display'):
        if not fol.get('geo_display') in geo_interest_cluster:
            geo_interest_cluster[fol.get('geo_display')] = Counter()
        geo_cluster.update([fol.get('geo_display')])
        geo_interest_cluster[fol.get('geo_display')].update(fol.get('interest_list') or [])
    

overall_interests = [c[0] for c in overall_interest_cluster.most_common(3)]
male_interests = [c[0] for c in male_interest_cluster.most_common(3)]
female_interests = [c[0] for c in female_interest_cluster.most_common(3)]

overall_themes = [c[0] for c in Counter(profile_wordcloud([fol['id'] for fol in fol_rows if len([m for m in overall_interests if m in fol.get('interest_list')]) > 0])[0]).most_common(5)]
male_themes = [c[0] for c in Counter(profile_wordcloud([fol['id'] for fol in fol_rows if len([m for m in male_interests if m in fol.get('interest_list') and fol.get('gender') == 'male']) > 0])[0]).most_common(5)]
female_themes = [c[0] for c in Counter(profile_wordcloud([fol['id'] for fol in fol_rows if len([m for m in female_interests if m in fol.get('interest_list') and fol.get('gender') == 'female']) > 0])[0]).most_common(5)]

overall_geo = [c[0] for c in geo_cluster.most_common(2)]


render_profiles = [f['id'] for f in faf_communities] + [f['id'] for f in faf_media] + [f['id'] for f in faf_celebrities] + [f['id'] for f in faf_causes]
for i, faf_profile_id in enumerate(render_profiles):
    save_faf_data(my_profile_id, faf_profile_id)
    print "Rendered profile",i+1,"of",len(render_profiles),"id:",faf_profile_id

master_data = {}
master_data['advertising'] = {'communities': [str(f['id']) for f in faf_communities],
                              'media': [str(f['id']) for f in faf_media],
                              'celebrities': [str(f['id']) for f in faf_celebrities],
                              }
master_data['marketing'] = {'everyone': {'interests': overall_interests, 'themes': overall_themes},
                            'male': {'interests': male_interests, 'themes': male_themes},
                            'female': {'interests': female_interests, 'themes': female_themes},
                            'geo': overall_geo,
                            }
master_data['social_impact'] = {'causes': [str(f['id']) for f in faf_causes],                     }
master_data['raw_profile'] = jload(_get_profile_file(my_profile_id))

save_faf_data(my_profile_id, my_profile_id)
str_id = str(my_profile_id)
str_name = PROFILE_NAME.replace('@','').lower()

jdump(master_data, 'assets/'+str_id+'.master.json')
jdump(master_data, 'assets/'+str_name+'.master.json')
