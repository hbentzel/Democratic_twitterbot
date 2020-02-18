import twython
import itertools
import time
import pandas as pd
import os
import datetime

dir_path = os.path.dirname(os.path.realpath(__file__))
save_path = dir_path + '\\output\\'
file_name = 'tweets.csv'

app_key = '1nIkehOLknyWCNdbWaYElpzG0'
app_secret = 'SaodSndpQyv302Pc0gMERxE4LzLIlHHRsJnh7Ei44FFWybVPeS'
oauth_token = '982835970880954368-k74xV0ojdekEKV2BarfSXsIXm4L7z1f'
oauth_token_secret = 'fABwhyBHUPknQO8sWgR3bvjP06UnP9kx0SzGMTXWnM7lW'

#twitter = twython.Twython(app_key, app_secret, oauth_token, oauth_token_secret)

queries = {'Sanders': "'sanders' OR 'bernie sanders' or 'bernie'",
         'Biden': "'biden' OR 'joe biden'",
         'Bloomberg': "'bloomberg' OR 'mike bloomberg'",
         'Warren': "'warren' OR 'elizabeth warren'",
         'Buttigieg': "'buttigeg' OR 'pete buttigeg' OR 'mayor pete'",
         'Klobuchar': "'klobuchar' OR 'amy klobuchar'"}

def query_search(query, twitter, lg='en', mode='extended', rt=False, pl='United States') -> dict:
    search = twitter.search(q=query, lang=lg, place=pl, tweet_mode=mode, retweeted=rt)
    result = search['statuses'][0]
    return(result)

def result_filter_add(json: dict, query: str, dataframe):
    try:
        dict = {'query_type': query,
               'screen_name': json['user']['screen_name'].encode('utf-8'),
               'bio': json['user']['description'].encode('utf-8'),
               'location': json['user']['location'],
               'following': json['user']['friends_count'],
               'followers': json['user']['followers_count'],
               'tweet': json['retweeted_status']['full_text'].encode('utf-8'),
               'date': json['retweeted_status']['created_at'],
               'device': json['source'],
               }
    except:
        dict = {'query_type': query,
               'screen_name': json['user']['screen_name'].encode('utf-8'),
               'bio': json['user']['description'].encode('utf-8'),
               'location': json['user']['location'],
               'following': json['user']['friends_count'],
               'followers': json['user']['followers_count'],
               'tweet': json['full_text'].encode('utf-8'),
               'date': json['created_at'],
               'device': json['source'],
               }

    new_df = dataframe.append(dict, ignore_index=True)
    return(new_df)

def save_tweets(df, filename=file_name, path=save_path):
        # this will need to be fixed as some tweets will have other query types (reduces amount of unique searches)
        df = df.drop_duplicates('tweet')
        df.to_csv(save_path+filename, index=False)

def check_existence(file=file_name):
    try:
        df = pd.read_csv(save_path + file)
        return(df)
    except:
        df = pd.DataFrame()
        return(df)

def indicator(instance, last_saved):
        print("Tweets aggregated: {0}, Last saved: {1}".format(instance, last_saved), end="\r")

def sleeping(time):
    minutes = time / 60 % 60
    print("Rate Limit Reached! Sleeping for %.2f minutes...\t\t\t"%minutes, end="\r")

def main():
    api = twython.Twython(app_key, app_secret, oauth_token, oauth_token_secret)
    df = check_existence()
    last_saved = None
    instance = 0

    for query in itertools.cycle(queries):
        try:
                result = query_search(query, twitter=api)
                df = result_filter_add(result, query, df)
                instance += 1
                indicator(instance, last_saved)

                if instance % 179 == 0:
                    save_tweets(df)
                    last_saved = datetime.datetime.fromtimestamp(time.time())
                else:
                    continue

        except twython.TwythonRateLimitError as error1:
                remainder = float(api.get_lastfunction_header(header='x-rate-limit-reset')) - time.time()
                sleeping(remainder)
                continue

if __name__ == "__main__":
    main()
