from decimal import Decimal
import boto3
import pandas as pd
from datetime import datetime, date
import re


def convert_to_id(link):
    app_id = re.findall("(?<=\/)\d.+?(?=\/)", link)
    return int(app_id[0])


def date_to_timestamp(deal_day, deal_month):
    todays_date = date.today()
    todays_year = todays_date.year
    todays_month = todays_date.month
    todays_day = todays_date.day
    if todays_month > deal_month or (todays_month == deal_month and todays_day > deal_day):
        todays_year += 1
    dt = datetime(year=todays_year, day=deal_day, month=deal_month)
    return dt.isoformat()


def conver_offerend_to_timestamp(offer_ends):
    split_ends = offer_ends.split()
    if (split_ends[-1] != "in"):
        day = int(datetime.strptime(split_ends[-1], '%d').strftime('%d'))
        month = int(datetime.strptime(split_ends[-2], '%B').strftime('%m'))
        return date_to_timestamp(day, month)
    return offer_ends


def date_to_dateTime(date):
    try:
        dt = list(map(int, datetime.strptime(
            date, '%b %d, %Y').strftime("%Y-%m-%d").split("-")))
        return datetime(dt[0], dt[1], dt[2]).isoformat()
    except ValueError:
        try:
            dt = list(map(datetime.strptime(
                date, '%b %Y').strftime("%Y-%m-%d").split("-")))
            return datetime(dt[0], dt[1], dt[2]).isoformat()
        except:
            return date
    except TypeError:
        return date


def convert_isoformat(date):
    try:
        date = int(round(date.timestamp()))
        return datetime.fromtimestamp(date).isoformat()
    except:
        return date


def create_table(table_name, partition_name, sort_name):
    # Table defination
    dynamodb = create_dynamodb()
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': partition_name, 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': sort_name, 'KeyType': 'RANGE'}  # Sort key
        ],
        AttributeDefinitions=[
            # AttributeType defines the data type. 'S' is string type and 'N' is number type
            {'AttributeName': partition_name, 'AttributeType': 'S'},
            {'AttributeName': sort_name, 'AttributeType': 'S'},
        ],
        ProvisionedThroughput={
            # ReadCapacityUnits set to 10 strongly consistent reads per second
            # WriteCapacityUnits set to 10 writes per second
            'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
    )
    return table


def pre_data_deal(df_deal):

    df_deal_drop_name = df_deal.dropna(subset=["name"])

    df_deal_drop_name["review"] = df_deal_drop_name["review"].fillna(
        "No Review")

    df_deal_drop_name["recent review"] = df_deal_drop_name["recent review"].fillna(
        "No Review")

    df_deal_drop_name["offer ends"] = df_deal_drop_name["offer ends"].fillna(
        "Offer ends in")

    df_deal_drop_name['original price'] = df_deal_drop_name['original price'].replace(
        r'^\$', '', regex=True)

    df_deal_drop_name['discounted price'] = df_deal_drop_name['discounted price'].replace(
        r'^\$', '', regex=True)

    df_deal_drop_name['timestamp'] = df_deal_drop_name['timestamp'].fillna(
        df_deal_drop_name['offer ends'].apply(conver_offerend_to_timestamp))

    df_deal_drop_name['release date'] = df_deal_drop_name['release date'].apply(
        date_to_dateTime)

    df_deal_drop_name['timestamp'] = df_deal_drop_name['timestamp'].apply(
        convert_isoformat)

    df_deal_drop_name['app_id'] = df_deal_drop_name['link'].apply(
        convert_to_id)

    df_deal_drop_name.columns = df_deal_drop_name.columns.str.replace(' ', '_')

    return df_deal_drop_name


def pre_data_reviews(df_reviews):

    df_reviews['last_play_time'] = df_reviews['last_play_time'].apply(
        convert_isoformat)
    df_reviews['created_time'] = df_reviews['created_time'].apply(
        convert_isoformat)
    df_reviews['last_updated'] = df_reviews['last_updated'].apply(
        convert_isoformat)

    df_reviews.columns = df_reviews.columns.str.replace(' ', '_')

    return df_reviews


def pre_data_link(df_link):
    df_link.columns = df_link.columns.str.replace(' ', '_')
    return df_link


def create_dynamodb(name='dynamodb',
                    endpoint_url='http://localhost:8000',
                    region_name='dummy',
                    aws_access_key_id='dummy',
                    aws_secret_access_key='dummy'):

    dynamodb = boto3.resource(name,
                              endpoint_url=endpoint_url,
                              region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
    return dynamodb


def load_data_deal(deals, dynamodb=None):
    dynamodb = create_dynamodb()

    deals_table = dynamodb.Table('deal')
    for _, deal in deals.iterrows():
        app_id = re.findall("(?<=\/)\d.+?(?=\/)", deal['link'])
        app_id = app_id[0]
        deals_table.put_item(Item={
            'app_id': str(app_id),
            'discounted_price': str(deal['discounted_price']),
            'end_date': deal['timestamp']
        }
        )


def load_data_reviews(reviews):
    dynamodb = create_dynamodb()

    review_table = dynamodb.Table('reviews')

    for _, review in reviews.iterrows():
        review_table.put_item(Item={
            'app_id': str(review['appid']),
            'steam_id': str(review['steamid']),
            'total_playtime': review['total_playtime'],
            'playtime_at_review': review['playtime_at_review'],
            'last_play_time': review['last_play_time'],
            'recommended':  review['recommended'],
            'helpful_vote': review['helpful_vote'],
            'funny_vote': review['funny_vote'],
            'weighted_vote_score': Decimal(str(review['weighted_vote_score'])),
            'content': str(review['content']),
            'created_time': review['created_time'],
            'last_updated': review['last_updated']
        }
        )


def load_data_game(games):
    dynamodb = create_dynamodb()

    game_table = dynamodb.Table('game')

    for _, game in games.iterrows():
        item = {'app_id': str(game['app_id']),
                'name': str(game['name']),
                'release_date': game['release_date'],
                'tag': game['tag'],
                'category': game['category'],
                'developer':  game['developer'],
                'review': game['review'],
                'recent_review': game['recent_review'],
                'original_price': game['original_price'],
                'support_windows': game['support_windows'],
                'support_mac':  game['support_mac'],
                'support_linux': game['support_linux'],
                'support_vr': game['support_vr']
                }
        game_table.put_item(Item=item)


def Extract(input_deal, input_link, input_reviews):

    df_deal = pd.read_json(input_deal)
    df_link = pd.read_json(input_link)
    df_reviews = pd.read_json(input_reviews)

    return df_deal, df_link, df_reviews


def Transform(df_reviews, df_link, df_deal):

    df_reviews = pre_data_reviews(df_reviews)
    df_link = pre_data_link(df_link)
    df_deal = pre_data_deal(df_deal)
    df_game = df_deal.merge(df_link, on=['app_id', 'link', 'name'], how='left')

    return df_reviews, df_deal, df_game


def Load(df_reviews, df_deal, df_game):
    load_data_game(df_game)
    load_data_deal(df_deal)
    load_data_reviews(df_reviews)


def create_tables():
    deal_table = create_table('deal', 'app_id', 'discounted_price')
    game_table = create_table('game', 'app_id', 'name')
    reviews_table = create_table('reviews', 'app_id', 'steam_id')


def main():
    input_deal = "../Data_Crawl/deal.json"
    input_link = "../Data_Crawl/link.json"
    input_reviews = "../Data_Crawl/reviews.json"

    create_tables()

    df_deal, df_link, df_reviews = Extract(
        input_deal, input_link, input_reviews)
    df_reviews, df_deal, df_game = Transform(df_reviews, df_link, df_deal)
    Load(df_reviews, df_deal, df_game)


if __name__ == '__main__':
    main()
