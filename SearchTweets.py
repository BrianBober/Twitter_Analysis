import tweepy
import pandas as pd
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class Gather_Data:
    '''
    Class to connect to twitter API, search a particular search term,
    and push results to a database.
    '''
    def __init__(self):
        
        '''
        Putting API info here.
        '''
		######################################
		##Insert keys here to run the code!!##
		######################################
		
        self.consumer_key = 
        self.consumer_secret = 
        self.access_token = 
        self.access_token_secret = 
        
    def connect_to_twitter(self):
        '''
        Connection to twitter API
        '''
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api_connection = tweepy.API(self.auth)

    def search_term(self,term, ntweets = 10):
        
        '''
        Input: search term, api connection, and number of tweets(max 1000 to avoid
        rate limiting). Searches twitter for term.
        
        Output: iterable containing tweet text and info
        
        '''
        if ntweets > 1000:
            raise ValueError('ntweets too large - decrease number to <= 1000')
            
        self.search_results = tweepy.Cursor(self.api_connection.search,
                                       q = term, lang = 'en', ).items(ntweets)
        
        
        
    def search_to_dataframe(self):
        '''
        Takes the search term results returned by twitter and
        turns them into a pandas dataframe.
        '''
        
        aggregated_results = [[tweet.id,
                                       tweet.created_at,
                                       tweet.lang,
                                       True if 'retweeted_status' in dir(tweet) else False,
                                       #Retweeted status exists as an attribute of tweet 
                                       #if its a retweet, otherwise it is not a retweet.
                                       tweet.author.screen_name,
                                       tweet.author.followers_count,
                                       tweet.author.location,
                                       tweet.author.time_zone,
                                       tweet.favorite_count,
                                       tweet.text] for tweet in self.search_results]

    
        self.tweet_df = pd.DataFrame(aggregated_results, columns = ['ID',
                                                                    'Datetime',
                                                                    'Language',                                                                    
                                                                    'IsRetweet',
                                                                    'AuthorScreenName',
                                                                    'AuthorFollowersCount',
                                                                    'AuthorLocation',
                                                                    'AuthorTimeZone',
                                                                    'TweetFavoriteCount',
                                                                    'TweetText'])
    
    
class Score_Sentiment:
    '''
    The score sentiment class takes a dataframe with a column containing
    text and performs sentiment analysis on that column.
    '''
    def __init__(self, pandas_dataframe, col_to_score):
        self.data = pandas_dataframe.copy()
        self.col_to_score = col_to_score
        
    def clean_data(self):
        '''
        Cleans the data to remove @ callout & hyperlinks.
        '''
        self.data.loc[:,self.col_to_score] = self.data.loc[:,self.col_to_score].\
        str.replace("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)", " ")
    
    @staticmethod  
    def score_sentiment_single_entry(text_entry):
        '''
        Scores sentiment of a single text entry using VADER VADER
        '''
        text_analyzer = SentimentIntensityAnalyzer()
        polarity = list(text_analyzer.polarity_scores(text_entry).values())
        if polarity[-1] > 0.05:
            polarity.append('Positive')
        elif polarity[-1] > -0.05:
            polarity.append('Neutral/Unsure')
        else:
            polarity.append('Negative')
        return polarity
        
    def add_sentiment_to_df(self):
        '''
        Appends sentiment and raw polarity score to dataframe
        '''
        self.data[['neg_score','neutral_score','pos_score','compound_score','Sentiment']] = self.data.loc[:,self.col_to_score].apply(
                lambda text: pd.Series(self.score_sentiment_single_entry(text)))
        
 
def push_df_to_database(pandas_dataframe, database_loc,table_name):
    '''
    Writes dataframe to an SQL database defined in init for storage
    '''
    try:
        con = sqlite3.connect(database_loc)
        print('connected')
        pandas_dataframe.to_sql(table_name, con, 
                             if_exists = 'append', index = False)
        print('successfully pushed to database')
    except Exception as e:
        print('unable to push to database')
        print(e)
    finally:
        con.close()
        print('closed connection')
        
    return


if __name__ == "__main__":

    gather_tweets = Gather_Data()
    gather_tweets.connect_to_twitter()
    gather_tweets.search_term('Trump',ntweets=400)
    gather_tweets.search_to_dataframe()
    push_df_to_database(gather_tweets.tweet_df,
                        'D:\\Github\\Twitter_Analysis\\Data\\Tweets.db',
                        'TrumpTweets')


    sentiment_scorer = Score_Sentiment(gather_tweets.tweet_df, 'TweetText')
    sentiment_scorer.clean_data()
    sentiment_scorer.add_sentiment_to_df()
    push_df_to_database(sentiment_scorer.data,
                        'D:\\Github\\Twitter_Analysis\\Data\\Tweets.db',
                        'TrumpTweetsScored')
    
    gather_tweets = Gather_Data()
    gather_tweets.connect_to_twitter()
    gather_tweets.search_term('Happy',ntweets=400)
    gather_tweets.search_to_dataframe()
    push_df_to_database(gather_tweets.tweet_df,
                        'D:\\Github\\Twitter_Analysis\\Data\\Tweets.db',
                        'HappyTweets')


    sentiment_scorer = Score_Sentiment(gather_tweets.tweet_df, 'TweetText')
    sentiment_scorer.clean_data()
    sentiment_scorer.add_sentiment_to_df()
    push_df_to_database(sentiment_scorer.data,
                        'D:\\Github\\Twitter_Analysis\\Data\\Tweets.db',
                        'HappyTweetsScored')
    
    gather_tweets = Gather_Data()
    gather_tweets.connect_to_twitter()
    gather_tweets.search_term('Disgusting',ntweets=400)
    gather_tweets.search_to_dataframe()
    push_df_to_database(gather_tweets.tweet_df,
                        'D:\\Github\\Twitter_Analysis\\Data\\Tweets.db',
                        'DisgustingTweets')


    sentiment_scorer = Score_Sentiment(gather_tweets.tweet_df, 'TweetText')
    sentiment_scorer.clean_data()
    sentiment_scorer.add_sentiment_to_df()
    push_df_to_database(sentiment_scorer.data,
                        'D:\\Github\\Twitter_Analysis\\Data\\Tweets.db',
                        'DisgustingTweetsScored')
    