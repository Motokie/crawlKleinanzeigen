import os

from src.CrawlKleinanzeigen import build_urls
from src.CrawlKleinanzeigen import process


def test_build_urls():
    # Test with one region
    os.environ['REGIONS'] = 'region1'
    expected_urls = ['https://www.ebay-kleinanzeigen.de/s-haus-kaufen/region1/preis::/c208l1354r20+haus_kaufen.grundstuecksflaeche_d:%2C']
    assert build_urls() == expected_urls

    # Test with multiple regions
    os.environ['REGIONS'] = 'region1,region2,region3'
    os.environ['PRICE_MAX'] = '300000'
    os.environ['DISTANCE'] = '27'
    os.environ['GROUND_SIZE_MIN'] = '2000'
    os.environ['GROUND_SIZE_MAX'] = '50000'
    expected_urls = [
        'https://www.ebay-kleinanzeigen.de/s-haus-kaufen/region1/preis::300000/c208l1354r27+haus_kaufen.grundstuecksflaeche_d:2000%2C50000',
        'https://www.ebay-kleinanzeigen.de/s-haus-kaufen/region2/preis::300000/c208l1354r27+haus_kaufen.grundstuecksflaeche_d:2000%2C50000',
        'https://www.ebay-kleinanzeigen.de/s-haus-kaufen/region3/preis::300000/c208l1354r27+haus_kaufen.grundstuecksflaeche_d:2000%2C50000'
    ]
    assert build_urls() == expected_urls


def test_process():
    process()
