# stunning-eureka
_University of Waterloo CS 486 F20 Team 35 Code Repository_
_Contributors: @lliepert, @AvyayAgarwal, @hrak109_

This repository contains the code used to predict the 2020 US Presidential Election result via sentiment analysis on Reddit submissions. It was submitted by Team 35 as part of the University of Waterloo's CS 486 F20 project.

## Description

This repo has two main components: a scraper program, and a Jupyter notebook containing the analysis and prediction. To reproduce, follow the included instructions to set up and run the scraper, then repeat the analysis and prediction using the notebook as a guide.

## Scraper Setup and Usage

1. Before running the scraper, you must first populate your own `.env` file. See `.env.template` for the required format, and see PRAW's documentation [here](https://praw.readthedocs.io/en/latest/getting_started/configuration/options.html#basic-configuration-options) for how to generate the required information. _At a high level, you must create a Reddit account and corresponding application which is passed to the PRAW instance in order to query the Reddit API._

2. Before you run the scraper, first create the directory `data/` in the same directory where `scraper.py` is located. All data scraped will be placed here.
_It is recommended to **add this directory to your `.gitignore`**, as the generated files are quite large and will likely exceed Github's recommended file size._

3. To run the scraper, run the command
    ```
    python scraper.py --subreddit politics --query trump --start-date 2016-09-01 --end-date 2016-11-01
    ```
    Note you must be running Python 3.8+.
    The scraper will create a new directory in `data/` tagged with the current time, and will save both `.csv` and `.pkl` copies of the data. Both incremental and a final master dataset are generated.
    Run `python scraper.py -h` for more information.
