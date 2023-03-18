import datetime
import logging
import time
from io import StringIO

import boto3
import pandas
import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from pandas.core.frame import DataFrame

SENDER = "daniel-harders@t-online.de"
RECIPIENT1 = "daniel-harders@t-online.de"
RECIPIENT2 = "harders-janssen@t-online.de"
AWS_REGION = "eu-central-1"
SUCCESS_SUBJECT = "Neue Immobilien gefunden"
ERROR_SUBJECT = "Fehler beim Laden der Immobilien"
NEW_OFFERS_TEXT = "Neue Angebote: \r\n"
CHARSET = "UTF-8"
sesV2Client = boto3.client('sesv2', region_name=AWS_REGION)

CRAWL_URLS = [
    "https://www.ebay-kleinanzeigen.de/s-haus-kaufen/olsberg/preis::200000/c208l1354r20+haus_kaufen.grundstuecksflaeche_d:800.00%2C",
    "https://www.ebay-kleinanzeigen.de/s-haus-kaufen/sundern-%28sauerland%29/preis::200000/c208l1412r20+haus_kaufen.grundstuecksflaeche_d:800.00%2C",
    "https://www.ebay-kleinanzeigen.de/s-haus-kaufen/iserlohn/preis::200000/c208l1735r20+haus_kaufen.grundstuecksflaeche_d:800.00%2C",
    "https://www.ebay-kleinanzeigen.de/s-haus-kaufen/plettenberg/preis::200000/c208l1415r20+haus_kaufen.grundstuecksflaeche_d:800.00%2C",
    "https://www.ebay-kleinanzeigen.de/s-haus-kaufen/moehnesee/preis::200000/c208l16255r20+haus_kaufen.grundstuecksflaeche_d:800.00%2C",
    "https://www.ebay-kleinanzeigen.de/s-haus-kaufen/edertal/preis::200000/c208l10306r20+haus_kaufen.grundstuecksflaeche_d:800.00%2C",
    "https://www.ebay-kleinanzeigen.de/s-haus-kaufen/solingen/preis::200000/c208l2117r20+haus_kaufen.grundstuecksflaeche_d:800.00%2C"]

timestamp = datetime.datetime.fromtimestamp(
    time.time()).strftime('%Y-%m-%d %H:%M:%S')
s3 = boto3.resource('s3')
s3Object = s3.Object('hotokie-immo', 'crawld.csv')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(event, context):
    handler = Handler()
    process()


def find_new_offers(already_crawled: DataFrame, html_lines, html_prices):
    already_seen = {}
    for title in already_crawled["title"]:
        title = title.strip()
        already_seen[title] = title

    offers = []
    for i in range(len(html_lines)):
        line = html_lines[i].text
        price = html_prices[i].text.strip()
        url = "https://www.ebay-kleinanzeigen.de" + html_lines[i]['href']
        if line not in already_seen:
            offers.append(Offer(timestamp, line, url, price))
    return offers


def offers_to_df(offers):
    if len(offers) > 0:
        return pandas.DataFrame([offer.__dict__ for offer in offers])
    return None


def crawl_immo_sales(url):
    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    html = BeautifulSoup(req.content, 'html.parser')
    return html


def send_ses_mail(body, subject, recipients):
    try:
        # Provide the content of the email.
        logger.warning("Trying to send email")
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
    for URL in CRAWL_URLS:
        already_crawled: DataFrame = read_s3_immo_file()
        crawled_immo_sales = crawl_immo_sales(URL)
        logger.info("Crawled: " + URL)

        html_lines = crawled_immo_sales.find_all("a", {"class": "ellipsis"})
        html_prices = crawled_immo_sales.find_all("p", {"class": "aditem-main--middle--price-shipping--price"})

        logger.info("Found " + str(len(html_lines)) + " entries")
        if len(html_lines) != 0:
            new_offers = offers_to_df(find_new_offers(already_crawled, html_lines, html_prices))

            already_crawled = pandas.concat([already_crawled, new_offers], ignore_index=True, sort=False)

            if new_offers is not None:
                write_to_s3(already_crawled)
                body = new_offers.to_html()
                send_ses_mail(body, SUCCESS_SUBJECT, [RECIPIENT2, RECIPIENT1])
            else:
                logger.info("No new offers found!")


class Handler:
    def __init__(self):
        pass


class Offer:
    def __init__(self, timestamp, title, url, price):
        self.timestamp = timestamp
        self.title = title
        self.url = url
        self.price = price
