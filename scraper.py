import argparse
import os
import praw
import pandas as pd
import numpy as np
import traceback
from datetime import datetime, timedelta
from dotenv import load_dotenv
from psaw import PushshiftAPI


load_dotenv()

to_hr_date = lambda ts: datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
to_filesafe_date = lambda ts: datetime.fromtimestamp(ts).strftime("%Y-%m-%d-%H-%M-%S")

POST = "POST"
COMMENT = "CMNT"
DATA_FOLDER = "data"


NEW = "new"
TIME = "time"


def get_query_data(
    api,
    query,
    subreddit,
    start_time,
    end_time,
    approximate_limit,
    max_comment_depth=np.inf,
    order_by=NEW,
):
    """
    Inputs:
        api: PushshiftAPI object to use
        query: string representing search query
        subreddit: string representing subreddit name to be queried
        start_time: int representing utc timestamp of beginning of search interval
        end_time: int representing utc timestamp of end of search interval
        approximate_limit: int representing approximate number of results returned
                           total value returned may be greater due to number of comments
                           in last queried post

    Outputs: pd.DataFrame containing the scraped data, and timestamp of next iteration.
             dataframe will have the following headers:
                - query
                - submission_type
                - id
                - parent_id
                - author
                - subreddit
                - title
                - url
                - body
                - created_utc
                - created_hr_date
                - depth
                - upvotes
                - downvotes
    """
    data = {
        "query": [],
        "submission_type": [],
        "id": [],
        "parent_id": [],
        "author": [],
        "subreddit": [],
        "title": [],
        "url": [],
        "body": [],
        "created_utc": [],
        "created_hr_date": [],
        "depth": [],
        "upvotes": [],
        "downvotes": [],
    }

    while len(list(data.values())[0]) < approximate_limit:
        args = {
            "q": query,
            "after": start_time,
            "before": end_time,
            "subreddit": subreddit,
            "filter": ["url", "title", "selftext", "comments"],
            "limit": 10,
        }
        if order_by == TIME:
            args["sort"] = "desc"
            args["sort_type"] = "created_utc"
        gen = api.search_submissions(**args)

        lgen = list(gen)
        if not lgen:
            break

        for submission in lgen:
            data["query"].append(query)
            data["submission_type"].append(POST)
            data["id"].append(submission.id)
            data["parent_id"].append(None)
            data["author"].append(submission.author)
            data["subreddit"].append(subreddit)
            data["title"].append(submission.title.replace("\n", "\\n"))
            data["body"].append(submission.selftext.replace("\n", "\\n"))
            data["url"].append(submission.url)
            data["created_utc"].append(submission.created_utc)
            data["created_hr_date"].append(to_hr_date(submission.created_utc)),
            data["depth"].append(None),
            data["upvotes"].append(submission.ups)
            data["downvotes"].append(submission.downs)

            submission.comments.replace_more()  # using default limit of 32 due to rate issues
            for comment in submission.comments.list():
                # .list() returns comments in a breadth first search, so this guarentees
                # breaking here returns only and all comments depth <= max_comment_depth
                if comment.depth > max_comment_depth:
                    break

                data["query"].append(query)
                data["submission_type"].append(COMMENT)
                data["id"].append(comment.id)
                data["parent_id"].append(submission.id)
                data["author"].append(comment.author)
                data["subreddit"].append(subreddit)
                data["title"].append(None)
                data["body"].append(comment.body.replace("\n", "\\n"))
                data["url"].append(None)
                data["created_utc"].append(comment.created_utc)
                data["created_hr_date"].append(to_hr_date(comment.created_utc)),
                data["depth"].append(comment.depth),
                data["upvotes"].append(comment.ups)
                data["downvotes"].append(comment.downs)
            end_time = int(submission.created_utc - 1)

    df = pd.DataFrame(data)
    return df, end_time


def main(subreddit, query, end_date, start_date, tag):
    program_start_time = datetime.now().timestamp()
    folder = "%s/%s" % (DATA_FOLDER, to_filesafe_date(program_start_time))
    if not tag is None:
        folder += "_%s" % tag
    csv_folder = "%s/csv" % folder
    pkl_folder = "%s/pkl" % folder
    os.mkdir(folder)
    os.mkdir(csv_folder)
    os.mkdir(pkl_folder)
    master_filename = "master_%s_%s_%s_%s" % (
        query,
        subreddit,
        to_filesafe_date(start_date.timestamp()),
        to_filesafe_date(end_date.timestamp()),
    )
    if not tag is None:
        master_filename += "_%s" % tag

    def save_df(df, filename):
        print(df)
        df.to_csv("%s/%s.csv" % (csv_folder, filename), na_rep="(none)", sep="\t")
        df.to_pickle("%s/%s.pkl" % (pkl_folder, filename))

    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_APP_NAME")
    username = os.getenv("REDDIT_USERNAME")
    password = os.getenv("REDDIT_PASSWORD")

    api = PushshiftAPI(
        praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username,
            password=password,
        )
    )

    dfs = []

    increment_end_time = lambda ts: int(
        (
            min(
                datetime.fromtimestamp(ts) + timedelta(hours=2),
                end_date,
            )
        ).timestamp()
    )

    try:
        start_time = int(start_date.timestamp())
        end_time = increment_end_time(start_time)
        _round = 0
        while start_time < end_date.timestamp():
            print(
                "starting round %s: %s - %s"
                % (_round, to_hr_date(start_time), to_hr_date(end_time))
            )
            _round += 1

            df, _ = get_query_data(
                api=api,
                query=query,
                subreddit=subreddit,
                start_time=start_time,
                end_time=end_time,
                approximate_limit=1,  # force only one subround of querying to run if limiting data gathered
                max_comment_depth=5,
                order_by=NEW,
            )
            dfs.append(df)
            filename = "%s_%s_%s_%s" % (
                query,
                subreddit,
                to_filesafe_date(start_time),
                to_filesafe_date(end_time),
            )
            save_df(df=df, filename=filename)
            start_time = end_time
            end_time = increment_end_time(end_time)
    except Exception as e:
        print("\n%s%s%s" % ("-" * 80, e, "-" * 80))
        print(traceback.format_exc())
        print("%s\n" % ("-" * 80))
    finally:
        master_df = pd.concat(dfs)
        save_df(df=master_df, filename=master_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
This version of the scraper only selects 10 posts per 2 hours from the date range given.\n\n
It only adds comments to a max depth of 5 levels.\n\n
[CS 486 Project F20 - Team 35]""",
    )
    parser.add_argument(
        "--subreddit",
        "-sr",
        type=str,
        help="subreddit to search",
        required=True,
        dest="subreddit",
    )
    parser.add_argument(
        "--query",
        "-q",
        type=str,
        help="query to search with",
        required=True,
        dest="query",
    )
    parser.add_argument(
        "--start-date",
        "-sd",
        type=datetime.fromisoformat,
        help="start date for search (input in iso format)",
        required=True,
        dest="start_date",
    )
    parser.add_argument(
        "--end-date",
        "-ed",
        type=datetime.fromisoformat,
        help="end date for search (input in iso format)",
        required=True,
        dest="end_date",
    )
    parser.add_argument(
        "--tag",
        "-t",
        type=str,
        help="optional tag for data folder and master file",
        dest="tag",
    )

    args = parser.parse_args()
    main(**vars(args))