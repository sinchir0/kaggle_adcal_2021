# http://ailaby.com/twitter_api/

import json
import os
import sys
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime as dt

from ipdb import set_trace as st
from requests_oauthlib import OAuth1Session

CK = os.environ["TWITTER_API_KEY_CORP"]  # Consumer Key
CS = os.environ["TWITTER_API_SECRET_KEY_CORP"]  # Consumer Secret
AT = os.environ["TWITTER_ACCESS_TOKEN_CORP"]  # Access Token
AS = os.environ["TWITTER_ACCESS_TOKEN_SECRET_CORP"]  # Accesss Token Secert


class TweetsGetter(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.session = OAuth1Session(CK, CS, AT, AS)

    @abstractmethod
    def specifyUrlAndParams(self, keyword):
        """
        呼出し先 URL、パラメータを返す
        """

    @abstractmethod
    def pickupTweet(self, res_text, includeRetweet):
        """
        res_text からツイートを取り出し、配列にセットして返却
        """

    @abstractmethod
    def getLimitContext(self, res_text):
        """
        回数制限の情報を取得 （起動時）
        """

    def collect(self, total=-1, onlyText=False, includeRetweet=False):
        """
        ツイート取得を開始する
        """

        # ----------------
        # 回数制限を確認
        # ----------------
        self.checkLimit()

        # ----------------
        # URL、パラメータ
        # ----------------
        url, params = self.specifyUrlAndParams()
        params["include_rts"] = str(includeRetweet).lower()
        # include_rts は statuses/user_timeline のパラメータ。search/tweets には無効

        # ----------------
        # ツイート取得
        # ----------------
        cnt = 0
        unavailableCnt = 0
        while True:
            res = self.session.get(url, params=params)
            if res.status_code == 503:
                # 503 : Service Unavailable
                if unavailableCnt > 10:
                    raise Exception("Twitter API error %d" % res.status_code)

                unavailableCnt += 1
                print("Service Unavailable 503")
                self.waitUntilReset(time.mktime(dt.now().timetuple()) + 30)
                continue

            unavailableCnt = 0

            if res.status_code != 200:
                raise Exception("Twitter API error %d" % res.status_code)

            tweets = self.pickupTweet(json.loads(res.text))
            if len(tweets) == 0:
                # len(tweets) != params['count'] としたいが
                # count は最大値らしいので判定に使えない。
                # ⇒  "== 0" にする
                # https://dev.twitter.com/discussions/7513
                break

            for tweet in tweets:
                if ("retweeted_status" in tweet) and (includeRetweet is False):
                    pass
                else:
                    if onlyText is True:
                        yield tweet["text"]
                    else:
                        yield tweet

                    cnt += 1
                    if cnt % 100 == 0:
                        print("%d件 " % cnt)

                    if total > 0 and cnt >= total:
                        return

            params["max_id"] = tweet["id"] - 1

            # ヘッダ確認 （回数制限）
            # X-Rate-Limit-Remaining が入ってないことが稀にあるのでチェック
            if (
                "X-Rate-Limit-Remaining" in res.headers
                and "X-Rate-Limit-Reset" in res.headers
            ):
                if int(res.headers["X-Rate-Limit-Remaining"]) == 0:
                    self.waitUntilReset(int(res.headers["X-Rate-Limit-Reset"]))
                    self.checkLimit()
            else:
                print("not found  -  X-Rate-Limit-Remaining or X-Rate-Limit-Reset")
                self.checkLimit()

    def checkLimit(self):
        """
        回数制限を問合せ、アクセス可能になるまで wait する
        """
        unavailableCnt = 0
        while True:
            url = "https://api.twitter.com/1.1/application/rate_limit_status.json"
            res = self.session.get(url)

            if res.status_code == 503:
                # 503 : Service Unavailable
                if unavailableCnt > 10:
                    raise Exception("Twitter API error %d" % res.status_code)

                unavailableCnt += 1
                print("Service Unavailable 503")
                self.waitUntilReset(time.mktime(dt.now().timetuple()) + 30)
                continue

            unavailableCnt = 0

            if res.status_code != 200:
                raise Exception("Twitter API error %d" % res.status_code)

            remaining, reset = self.getLimitContext(json.loads(res.text))
            if remaining == 0:
                self.waitUntilReset(reset)
            else:
                break

    def waitUntilReset(self, reset):
        """
        reset 時刻まで sleep
        """
        seconds = reset - time.mktime(dt.now().timetuple())
        seconds = max(seconds, 0)
        print("\n     =====================")
        print("     == waiting %d sec ==" % seconds)
        print("     =====================")
        sys.stdout.flush()
        time.sleep(seconds + 10)  # 念のため + 10 秒

    @staticmethod
    def bySearch(keyword):
        return TweetsGetterBySearch(keyword)

    @staticmethod
    def byUser(screen_name):
        return TweetsGetterByUser(screen_name)


class TweetsGetterBySearch(TweetsGetter):
    """
    キーワードでツイートを検索
    """

    def __init__(self, keyword):
        super(TweetsGetterBySearch, self).__init__()
        self.keyword = keyword

    def specifyUrlAndParams(self):
        """
        呼出し先 URL、パラメータを返す
        """
        url = "https://api.twitter.com/1.1/search/tweets.json"
        params = {"q": self.keyword, "count": 100}
        return url, params

    def pickupTweet(self, res_text):
        """
        res_text からツイートを取り出し、配列にセットして返却
        """
        results = []
        for tweet in res_text["statuses"]:
            results.append(tweet)

        return results

    def getLimitContext(self, res_text):
        """
        回数制限の情報を取得 （起動時）
        """
        remaining = res_text["resources"]["search"]["/search/tweets"]["remaining"]
        reset = res_text["resources"]["search"]["/search/tweets"]["reset"]

        return int(remaining), int(reset)


class TweetsGetterByUser(TweetsGetter):
    """
    ユーザーを指定してツイートを取得
    """

    def __init__(self, screen_name):
        super(TweetsGetterByUser, self).__init__()
        self.screen_name = screen_name

    def specifyUrlAndParams(self):
        """
        呼出し先 URL、パラメータを返す
        """
        url = "https://api.twitter.com/1.1/statuses/user_timeline.json"
        params = {
            "screen_name": self.screen_name,
            "count": 200,
            "exclude_replies": True,
        }
        return url, params

    def pickupTweet(self, res_text):
        """
        res_text からツイートを取り出し、配列にセットして返却
        """
        results = []
        for tweet in res_text:
            results.append(tweet)

        return results

    def getLimitContext(self, res_text):
        """
        回数制限の情報を取得 （起動時）
        """
        remaining = res_text["resources"]["statuses"]["/statuses/user_timeline"][
            "remaining"
        ]
        reset = res_text["resources"]["statuses"]["/statuses/user_timeline"]["reset"]

        return int(remaining), int(reset)


# if __name__ == '__main__':

#     # 引数を取得
#     today = sys.argv[1]

#     # キーワードで取得
#     # getter = TweetsGetter.bySearch(u'渋谷')

#     # ユーザーを指定して取得 （screen_name）
#     getter = TweetsGetter.byUser('SansanDSOC')

#     # todayの時刻を朝9時ということにする
#     JST = tz.gettz('Asia/Tokyo')
#     now = dt.strptime(today,'%Y-%m-%d') + datetime.timedelta(hours=9)
#     now = now.astimezone(JST)

#     all_info = []
#     for tweet in getter.collect(total=100):
#         # print ('------ %d' % cnt)
#         # print ('{} {} {}'.format(tweet['id'], tweet['created_at'], '@'+tweet['user']['screen_name']))
#         # print (tweet['text'])
#         created_at = tweet['created_at']
#         created_at_datetime = created_at_to_datetime(created_at).astimezone(JST)
#         # １週間のTweetに限定する
#         if (now - created_at_datetime) > datetime.timedelta(7):
#             break
#         created_at_str = dt.strftime(created_at_datetime, '%Y-%m-%d %H:%M:%S')
#         tweet_text = tweet['text']
#         tweet_user = tweet['user']
#         retweet_count = tweet['retweet_count']
#         favorite_count = tweet['favorite_count']
#         url = tweet['entities']['urls'][0]['url']
#         point = 10 * retweet_count + favorite_count

#         all_info.append((
#             created_at_str,
#             tweet_text,
#             url,
#             retweet_count,
#             favorite_count,
#             point
#         ))

#     # dfに変換し、CSV出力
#     df_tweet = pd.DataFrame(
#         all_info,
#         columns=['created_at', 'tweet_text', 'url', 'retweet_count', 'favorite_count', 'point']
#     )

#     df_tweet.to_csv(f'data/{today}_weekly_tweet.csv', index=False)
