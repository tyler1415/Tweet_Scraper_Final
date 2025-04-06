from datetime import datetime as dt
import datetime
import tweepy
import pyodbc

# Keys needed to access twitter API
API_Key = "*******"
API_Key_Secret = "********"
Bearer_Token = "********" \
               "********"
Access_Token = "********"
Access_Token_Secret = "*******"


#Connect to Twitter through API
def User_Authorization(API_Key, API_Key_Secret, Access_Token, Access_Token_Secret):
    auth = tweepy.OAuthHandler(API_Key, API_Key_Secret)
    auth.set_access_token(Access_Token, Access_Token_Secret)

    api = tweepy.API(auth)

    try:
        api.verify_credentials()
        print("Authentication OK \n")
    except tweepy.TweepError as err:
        print(f"Authentication error: {err}")

    return api


#Go to twitter account and scrape up to 200 tweets
def Return_Tweets(api):
    #username to search for
    userName = "User_Name"

    # make initial request for most recent tweets (200 is the maximum allowed count)
    tweets = api.user_timeline(screen_name=userName, count=200)

    return tweets


#Filter tweets for tweets created today (e.g - 12/22/2020)
def Filter_Tweets(tweets):

    today = dt.today()
    current_date = dt.combine(today, dt.min.time())

    Filtered_Tweets = []

    for tweet in tweets:
        if tweet.created_at > current_date:
            Filtered_Tweets.append(tweet)
        else:
            pass

    return Filtered_Tweets


#Load filtered tweets into SQL Server
def Load_Into_SQL_Server(Filtered_Tweets):
    conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};'
                          'SERVER=Server;'
                          'DATABASE=WRB_Tweets;'
                          'Trusted_Connection=yes')

    cursor = conn.cursor()

    try:
        for index, tweet in enumerate(Filtered_Tweets):
            print(index, tweet.id_str, tweet.created_at, tweet.text, tweet.retweet_count, tweet.favorite_count)
            cursor.execute('''INSERT INTO dbo.WRB (ID, Create_Date, Tweet, Retweets, Favorites) 
                              VALUES (?, ?, ?, ?, ?);''', tweet.id_str, tweet.created_at, tweet.text, tweet.retweet_count, tweet.favorite_count)
        conn.commit()
    except pyodbc.DatabaseError as err:
        cursor.rollback()
        print(f"Database error: {err}")
    finally:
        cursor.close()
        conn.close()


Returned_Api_Auth = User_Authorization(API_Key, API_Key_Secret, Access_Token, Access_Token_Secret)
Returned_Tweets = Return_Tweets(Returned_Api_Auth)
Returned_Filtered_Tweets = Filter_Tweets(Returned_Tweets)
Load_Into_SQL_Server(Returned_Filtered_Tweets)
