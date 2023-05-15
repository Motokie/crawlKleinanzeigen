import datetime
import logging
import os
import time
import re
from io import StringIO

import boto3
import pandas
import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from pandas.core.frame import DataFrame

REGIONS = os.environ.setdefault('REGIONS',
                                'olsberg,sundern-%28sauerland%29,iserlohn,plettenberg,moehnesee,edertal,solingen') \
    .split(
    ",")
KAT_REGION_CODE = os.environ.setdefault('KAT_REGION_CODES',
                                        'c208l1354,c208l1412,c208l1735,c208l1415,c208l16255,c208l10306,c208l2117') \
    .split(",")
PRICE_MIN = os.environ.setdefault('PRICE_MIN', '')
PRICE_MAX = os.environ.setdefault('PRICE_MAX', '')
DISTANCE = os.environ.setdefault('DISTANCE', '40')
GROUND_SIZE_MIN = os.environ.setdefault('GROUND_SIZE_MIN', '')
GROUND_SIZE_MAX = os.environ.setdefault('GROUND_SIZE_MAX', '')

AWS_REGION = "eu-central-1"
SUCCESS_SUBJECT = "Neue Immobilien gefunden"
ERROR_SUBJECT = "Fehler beim Laden der Immobilien"
NEW_OFFERS_TEXT = "Neue Angebote: \r\n"
CHARSET = "UTF-8"
sesV2Client = boto3.client('sesv2', region_name=AWS_REGION)
BASIC_URL = "https://www.ebay-kleinanzeigen.de/s-haus-kaufen/PH_REGION/preis:PH_PRICE_MIN:PH_PRICE_MAX/PH_KAT_REGIONrPH_DISTANCE+haus_kaufen.grundstuecksflaeche_d:PH_GROUND_SIZE_MIN%2CPH_GROUND_SIZE_MAX"

timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
s3 = boto3.resource('s3')
s3Object = s3.Object('hotokie-immo', 'crawld.csv')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(event, context):
    handler = Handler()
    process()


def build_urls():
    urls = []

    for region, kat_region_code in zip(REGIONS, KAT_REGION_CODE):
        urls.append(
            BASIC_URL
            .replace("PH_REGION", region)
            .replace("PH_PRICE_MIN", PRICE_MIN)
            .replace("PH_PRICE_MAX", PRICE_MAX)
            .replace("PH_KAT_REGION", kat_region_code)
            .replace("PH_DISTANCE", DISTANCE)
            .replace("PH_GROUND_SIZE_MIN", GROUND_SIZE_MIN)
            .replace("PH_GROUND_SIZE_MAX", GROUND_SIZE_MAX))
    logger.info(urls)
    return urls


def find_new_offers(already_crawled: DataFrame, html_lines, html_prices, html_distances):
    already_seen = {}
    for title in already_crawled["title"]:
        title = title.strip()
        already_seen[title.strip()] = True

    offers = []
    for i in range(len(html_lines)):
        line = html_lines[i].text
        price = html_prices[i].text.strip()
        url = "https://www.ebay-kleinanzeigen.de" + html_lines[i]['href']
        if line not in already_seen and int(html_distances[i]) <= int(DISTANCE):
            offers.append(Offer(timestamp, line, url, price))
            already_seen[line] = True
    return offers


def offers_to_df(offers):
    if len(offers) > 0:
        return pandas.DataFrame([offer.__dict__ for offer in offers])
    return None


def crawl_immo_sales(url):
    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    html = BeautifulSoup(req.content, 'html.parser')
    return html


def send_ses_mail(body, subject):
    SENDER = os.environ.setdefault("SENDER", "daniel-harders@t-online.de")
    RECIPIENT1 = os.environ.setdefault("RECIPIENT_1", "daniel-harders@t-online.de")
    RECIPIENT2 = os.environ.setdefault("RECIPIENT_2", "harders-janssen@t-online.de")
    recipients = [RECIPIENT2, RECIPIENT1]
    try:
        # Provide the content of the email.
        logger.info("Trying to send email")
        response = sesV2Client.send_email(FromEmailAddress=SENDER,
                                          Destination={
                                              'ToAddresses': recipients
                                          },
                                          Content={
                                              'Simple': {
                                                  'Subject': {
                                                      'Data': subject,
                                                      'Charset': CHARSET
                                                  },
                                                  'Body': {
                                                      'Html': {
                                                          'Data': body,
                                                          'Charset': CHARSET
                                                      }
                                                  }
                                              }
                                          }
                                          )
    # Display an error if something goes wrong.
    except ClientError as e:
        logger.info(e.response['Error']['Message'])
    else:
        logger.info("Email sent! Message ID:" + response['MessageId'])


def write_to_s3(df: DataFrame):
    s3Object.put(Body=bytes(
        df.to_csv(sep=';', index=False).encode('utf-8-sig')))


def read_s3_immo_file():
    df = pandas.DataFrame()
    try:
        logger.info("Loading S3 object..")
        s3Object.load()
        logger.info("Loaded S3 object: " + str(s3Object.get()))
    except ClientError as e:
        errorCode = e.response['Error']['Code']
        if errorCode == "404" or errorCode == "403":
            logger.warning("Object does not exist")
            df = pandas.DataFrame(data={"timestamp": [], "price": [], "title": [], "url": []})
            write_to_s3(df)
    else:
        body = s3Object.get()['Body']
        csv_string = body.read().decode('utf-8')
        df = pandas.read_csv(StringIO(csv_string), delimiter=";", encoding='utf-8-sig')
    return df


def process():
    crawl_urls = build_urls()
    for URL in crawl_urls:
        already_crawled: DataFrame = read_s3_immo_file()
        html = crawl_immo_sales(URL)
        logger.info("Crawled: " + URL)

        html_lines = html.find_all("a", {"class": "ellipsis"})
        html_prices = html.find_all("p", {"class": "aditem-main--middle--price-shipping--price"})
        html_distances = html.find_all("div", {"class": "aditem-main--top--left"})

        distances = []
        for distance in html_distances:
            distances.append(int(extract_number_from_string(str(distance))))
        logger.info(distances)

        if len(html_lines) != 0:
            new_offers = offers_to_df(find_new_offers(already_crawled, html_lines, html_prices, distances))

            already_crawled = pandas.concat([already_crawled, new_offers], ignore_index=True, sort=False)

            if new_offers is not None:
                write_to_s3(already_crawled)
                body = new_offers.to_html()
                send_ses_mail(body, SUCCESS_SUBJECT)
                for new_offer in new_offers:
                    logger.info(new_offer)
            else:
                logger.info("No new offers found!")


def extract_number_from_string(string):
    pattern = r'\d+'  # Regex, to find one or more following numbers
    matches = re.findall(pattern, string)
    if matches:
        number = int(matches[-1])
        return number
    else:
        return None


class Handler:
    def __init__(self):
        pass


class Offer:
    def __init__(self, timestamp, title, url, price):
        self.timestamp = timestamp
        self.title = title
        self.url = url
        self.price = price
