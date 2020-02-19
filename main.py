import twython
import itertools
import time as tm
import pandas as pd
import os
import datetime
import config
import sys

# Setting up global variables for directory paths
dir_path = os.path.dirname(os.path.realpath(__file__))
save_path = dir_path + '\\output\\'
file_name = 'tweets.csv'

# Queries used for query_search function, key = "query_type" value = "search"
queries = {'Sanders': "'sanders' OR 'bernie sanders' or 'bernie'",
         'Biden': "'biden' OR 'joe biden'",
         'Bloomberg': "'bloomberg' OR 'mike bloomberg'",
         'Warren': "'warren' OR 'elizabeth warren'",
         'Buttigieg': "'buttigeg' OR 'pete buttigeg' OR 'mayor pete'",
         'Klobuchar': "'klobuchar' OR 'amy klobuchar'"}

# Searches using api and returns JSON (dictionary)
def query_search(query, twitter, lg='en', mode='extended', rt=False, pl='United States') -> dict:
    search = twitter.search(q=query, lang=lg, place=pl, tweet_mode=mode, retweeted=rt)
    result = search['statuses'][0]
    return(result)

# Looks up keys that we want and stores them. Accounts for different search modes (extended), appends them to pandas dataframe
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

# this saves the file, and returns when it was saved
def save_tweets(df, filename=file_name, path=save_path):
        # pandas function that drops duplicates
        df = df.drop_duplicates('tweet')
        df.to_csv(save_path+filename, index=False)
        last_saved = datetime.datetime.fromtimestamp(tm.time())
        return(last_saved)

# Checks if there is an existing csv file to convert into a df (creates one if there isn't)
def check_existence(file=file_name):
    try:
        df = pd.read_csv(save_path + file)
        return(df)
    except:
        df = pd.DataFrame()
        return(df)

# Command prompt message that shows how many tweets have been collected on session
def indicator(instance):
        print("Tweets aggregated: {0}".format(instance) + "{:<50}".format(" "), end="\r")

# Displays response code and displays countdown timer
def sleeping(response_code, time, last_saved):
    print("{0}, Sleeping for {1} seconds, Last save: {2}".format(response_code, int(time), last_saved), end="\r")
    tm.sleep(time)

# helper function that returns difference in opening row count and current row count
def row_diff(orow, crow):
    return(crow-orow)

def main():
    # this part sets up the initial variables used in the for loop, df = dataframe, orow = opening row count
    api = twython.Twython(config.app_key, config.app_secret,
                         config.oauth_token, config.oauth_token_secret)
    df = check_existence()
    orow = df.shape[0]
    last_save = ""

    # creates 5 brand new lines
    print("\n" * 5)

# reiterates through query dictionary (forever)
    for query in itertools.cycle(queries):
        try:
                result = query_search(query, twitter=api)
                df = result_filter_add(result, query, df)
                last_save = save_tweets(df)
                indicator(row_diff(orow, df.shape[0]))

# rate limit error, sleeps for 15 minutes
        except twython.TwythonRateLimitError as error1:
                remainder = abs(float(api.get_lastfunction_header(header='x-rate-limit-reset')) - tm.time())
                sleeping(error1, remainder, last_save)
                continue
# any other error, sleeps for 5 minutes
        except twython.TwythonError as error2:
                remainder = 300
                sleeping(error2, remainder, last_save)
                continue

if __name__ == "__main__":
    main()
