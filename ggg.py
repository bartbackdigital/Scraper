
import random
from collections import OrderedDict
import json
import pandas as pd
import scraper_helper
from scrapy import Selector
import requests
import os
from PIL import Image
import time
import openai
import uuid

# Load the API key from an environment variable for security
# openai.api_key = os.getenv("OPENAI_API_KEY")

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (iPad; CPU OS 13_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    # Add more user-agents as needed
]
user_agent = random.choice(user_agents)
# if not openai.api_key:
#     raise ValueError("The OPENAI_API_KEY environment variable is not set.")

get_cwd = os.getcwd()
os.makedirs(f'{get_cwd}/funda_images', exist_ok=True)
os.makedirs(f'{get_cwd}/funda_images_processed', exist_ok=True)
image_int = 1
switch = True
sess = requests.Session()
headers = """User-Agent: """+ user_agent+"""
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br, zstd
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Cache-Control: max-age=0
Cookie: INLB=fundawonenFrontends; SNLB=fundawonenFrontend010; .ASPXANONYMOUS=PgLtRYIPJu7IXHyVf8JRoQytdW3TBt5L6AXmKMXSxgZa6dHV_HVSzlpyTVNPXMHsQPm0fJJ6MBLenuYdjCXOt7QRwSwn_taXX6pe_lG3ZAQvfFAU_eo4nSoFfzCnNFnFzyjA3sVsy8JIC8lbZo0ii0Yj7dc1; sr=0%7cfalse; i18n=nl; bm_mi=8683880AB8B16C952B61F3A34622D650~YAAQHURDULlX/M6OAQAAIX5+0RcfABMjf1eraTA2Ppinb41MDKOu+QsB+vxmL+R6JBmiDBeL2wrJBqM+w0AAXzZZgr92tH0MoGnDPKyIrB99izZtdTNW0iITvrcuif+Xyrr+kAnhN0/YtWEwIkrUFoJk5VnLUW+CrM2lq3Jps7ktxUzX0lKEK0qdCG2yv9mdYLEeIXeqa9gbz12J7e4JNySin1O9KB9UzCv0H5VorKVfOQZ1DLFglO3WPR+WFtatW4ZhyJOAra1vbXGbPOHjmjPb38P8imP6JHcij96p9TukKKEV6CZyTk33TachSKBtksVFP+DtQXg=~1; ak_bmsc=93B0D2DD85599507DED1127C5D6A531F~000000000000000000000000000000~YAAQHURDUMdX/M6OAQAAp4l+0RfrIkp0UtRvZ5h2zjWjE1CZ3VCiCqeRv979c5CBRxXAg8s9xTEWcMttxmVCk/HNVylpeJxC8dyiTWE1zku6Q6eewNCW6ACI0PysawxhapfwiL/D18+csuXDFlqsz11xM9VtuZ+SfTcE7FATRI3qe3wovQohSfkdFi9wrrhpCAmHbkd0jNyiKkJCAZysqCiuGY5mmCIvTop6KDoSRnnUdfjQBh24BiLAr0rsfVMqFCfhcaVCgsHABLepnGdXo08IQjKKKCgUA0dtASwy7QA/LocvFiFkXegRaGm8Az3Hu1u9EaFgYtz4fp/0MuZhNBtUQ2E7USuHkjZqbMgtlVazEgGDZrpNweE+9tzlqQ7UKSapTOH3i1fUDvzmTDOCo3d1V5zPD8Cfoe5IrM1hftOAag0+o2ejzAub4PJMMWFH3gySLlRRVa3oRJCtb58kLHGXtNfVheQbrt9xPKf+UAF7yvlXhFhAADKyFA==; last_search=koop=Nederland%20%20%2B%201%20filter|L3pvZWtlbi9rb29wLz9zZWxlY3RlZF9hcmVhPSU1QiUyMm5sJTIyJTVEJmFtZW5pdGllcz0lNUIlMjJmaXhlcl91cHBlciUyMiU1RA==; wib=; didomi_token=eyJ1c2VyX2lkIjoiMThlZDE3ZTgtOGUyZi02MGFiLTliNjgtZGE1NTNhZmZjNGJhIiwiY3JlYXRlZCI6IjIwMjQtMDQtMTJUMDg6NDg6MzQuNTMwWiIsInVwZGF0ZWQiOiIyMDI0LTA0LTEyVDEwOjM1OjI4LjM0NVoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzpmdW5kYWZ1bmMtQUhoRDJZcTMiLCJjOnNlZ21lbnQtR1FwV1BRaEsiLCJjOmZ1bmRhcGFydC1pQk5nWFd3WCIsImM6ZGF0YWRvZy1yZHRZN0FtRSIsImM6Z29vZ2xlYW5hLWphZ242Tm5nIiwiYzpvcHRpbWl6ZWx5LTgyMkE2cU5nIiwiYzpjbG91ZGZsYXJlLThmbmEzRUhBIiwiYzpnb29nbGV0YWctZUVZUGozMloiLCJjOmhvdGphci1UODJqdENVcSIsImM6Z2V0ZmVlZGJhLUJ0alhGTDJaIiwiYzpzYWxlc2ZvcmNlLWtLN3JwZXhZIiwiYzppbmctanFYNmQzdFkiXX0sInB1cnBvc2VzIjp7ImVuYWJsZWQiOlsicGVyc29uYWxpc2F0aW9uLWdyb3VwIiwiYWR2ZXJ0aXNlbWVudC1ncm91cCJdfSwidmVyc2lvbiI6MiwiYWMiOiJFdC1BSUJFa0VEQUtNQWx2Z0FBQS5BQUFBIn0=; euconsent-v2=CP89L8AP89L8AAHABBENAvEgAP7gAAAAABpYGgNBzC5dRAFCAD5wYNsAOQQVoNCABEQgAAIAAgABwAKAIAQCkEAAFADgAAACAAAAIAIBAAJAEAAAAQAAAAAAAAAAQAAAAAIIIAAAgAIBCAAIAAAAAAAAQAAAgAACAAAAkAAAAIIAQEAABAAAAMQAAwABBQglABgACChBSADAAEFCB0AGAAIKEBIAMAAQUILQAYAAgoQA.f9wAAAAAAAAA; didomi_consent=functional-group,personalisation-group,advertisement-group,analytical-group,cookies,measure_ad_performance,market_research,improve_products,select_basic_ads,create_ads_profile,select_personalized_ads,create_content_profile,select_personalized_content,use_limited_data_to_select_content,; userConsentPersonalization=true; optimizelyEndUserId=oeu1712918131274r0.8423867102491414; _pubcid=003993b7-0d00-46d1-bd67-7a9597dc1e6a; _pubcid_cst=PSz0LK4s%2Bw%3D%3D; ajs_anonymous_id=0a16ce47-1c1b-4622-ba2f-cfb82746994a; _hjSession_2869769=eyJpZCI6ImRmYmFhMjI4LTQwNWUtNDJmYS1hMTI2LTE2MzBkYmI2YTJhMiIsImMiOjE3MTI5MTgxMzY0NDAsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxfQ==; _lr_retry_request=true; _lr_env_src_ats=false; panoramaId_expiry=1713004541019; pbjs-unifiedid=%7B%22TDID%22%3A%22bc25c883-345d-4587-bdbe-3043c345d099%22%2C%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222024-04-12T10%3A35%3A41%22%7D; pbjs-unifiedid_cst=PSz0LK4s%2Bw%3D%3D; __gads=ID=f7ecb411d84dad85:T=1712918494:RT=1712918494:S=ALNI_MZ5N8BtaF1bRc923IbjNyDKA1KbTg; __gpi=UID=00000dec716f9e6b:T=1712918494:RT=1712918494:S=ALNI_MZ8cIwY5oaDiO4xlfKUTl80AyyctA; _hjSessionUser_2869769=eyJpZCI6IjMzOWY1NjFlLWY2NWQtNWE3Yi1hNTY5LTJmNGYwNzg1NTQyOCIsImNyZWF0ZWQiOjE3MTI5MTgxMzY0NDAsImV4aXN0aW5nIjp0cnVlfQ==; _gcl_au=1.1.1416522339.1712918508; sessionstarted=true; cto_bundle=XdiWtl9saDBGJTJGMmZ2a3pmb2JyR2lDUndXZXBHNkVmRTBOcXpLS0E5TXBvWUhFUVg2JTJGZjRrMSUyQkxZcWo4Z2paZ0VKY0FqMGo0M0p1eDJaUWNXJTJCcUYwM3dyNTAycFRzdkhxVHBlYlhLbkh3WjhFbW1XN0EzUng5MiUyQjBBU25Cbks4UHZkajE; cto_bidid=dZ6JdV9YNGtVMU9oQ2VrbDNacDdEQXlld2VTJTJGVXBEQ05tT2xZWWthOUFyUnFGaW9mOVFsU1NHSU53S3lxeWFxZzk3ekZ2b214MURLZmpoTHkwQ1h6OFQ3d3l3JTNEJTNE; __eoi=ID=7a02abff3abe3b15:T=1712918521:RT=1712918521:S=AA-AfjY0FyVx2-13S4S6t1OJXfXv; _dd_s=logs=1&id=fa76a1e6-fb57-4e4f-a8d2-e19301961a1c&created=1712918128338&expire=1712919420085; bm_sv=3490A83321FBB62ACEBE3A7E4302B382~YAAQbLlNaG8P7MiOAQAAnfDm0RfwCaONn+v7hN2lTz2J7lSW37yvD3/lFbEgibwdfoWYm87+cIVvrM9nlkI9EiqPfmevxmE7VOqmAHUbGVZyjEvAolLMCHifZd9EwIxeEMd55IgJMXkRVIB6NswynCf3xSBRXUr+mOpms6IwmnoLx9aQ3ZvOASPq8A4UDkVmAfCOgOi/r8MRLkS6OQOCineW0J8ulTpXxfYGfK3ItZ3xjIQ+L7jK+5EwtyvWfeE=~1
Dnt: 1
Referer: https://www.funda.nl/zoeken/koop/?selected_area=%5B%22nl%22%5D&amenities=%5B%22fixer_upper%22%5D
Sec-Ch-Ua: "Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: same-origin
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"""
headers = scraper_helper.get_dict(headers,strip_cookie=True)



# Your Smartproxy credentials and endpoint
username = 'spb2g1f0zg'
password = 'K73JIwcrmfy2qelh1U'
endpoint = 'eu.smartproxy.com:10000'

def proxy_request(url):
    # Correct the proxy format to match the documentation you provided
    proxy = f"http://{username}:{password}@{endpoint}"
    proxies = {
        "http": proxy,
        "https": proxy
    }
    # proxies = {
    #     "http": 'http://194.180.238.238:40938',
    #     "https": 'http://194.180.238.238:40938'
    # }
    try:
        # Random delay to mimic human behavior and possibly avoid detection.
        time.sleep(random.uniform(1, 5))
        
        # Perform the HTTP request through the proxy.
        response = requests.get(url, proxies=proxies, headers=headers)  # Ensure 'headers' is defined and includes User-Agent, etc.
        print(response.status_code, end=': ', flush=True)
        
        # If the request was successful, print success and return both the response and a Selector for scraping.
        if response.status_code == 200:
            print("Request Successful")
            return response, Selector(text=response.text)
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None, None
    except Exception as e:
        print(f"An error occurred: {e}")
        # Delay a bit longer in case of an error before retrying or ending the attempt.
        time.sleep(random.uniform(5, 10))
        return None, None


def openAITextGeneration(content):
    print("Original content:", content)
    try:
        completion = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "Je bent een helpvolle assistent die texten herschrijft in het nederlands op een spannende manier en in correct Nederlands. Zorg ervoor dat iemand in een Doe Het Zelf project t.o.v. vastgoed het snapt. Het gaat om een klusvastgoed project."
                },
                {
                    "role": "user",
                    "content": f"Herschrijf de tekst hieronder op een spannende manier, ongeveer 350 woorden en laat het door een mens geschreven lijken. Zorg dat het goed leesbaar is en duidelijk voor iemand die geïnteresseerd is in klusvastgoed. Zorg er ook voor dat het realistische tekst is en klopt met de input van de tekst.: {content}"
                },
            ],
        )
        # Printing the rewritten text
        print(completion.choices[0].message.content)
        return completion.choices[0].message.content
    except Exception as e:
        return content
        # print("An error occurred:", e)




def generateImageNames(streetAddress, location):
    keywords = ['verbouwzelf-renoveren', 'verbouw-zelf-vergroten', 'verbouwingswerkzaamheden',
                'projectwoning', 'verbouw-advies', 'verbouw-zelf-verkleinen', 'verbouwzelf-aankleden',
                'verbouw-zelf-ontwerpen', 'verbouwzelf-maken', 'verbouw-zelf-verwijderen',
                'klusbedrijf', 'verbouwplan', 'verbouw-zelf-verrassen', 'verbouwingsproject', 'verbouwontwerp',
                'verbouwbedrijven', 'verbouwzelf-verbouwen', 'verbouw-zelf-doen', 'verbouwzelf-bouwen',
                'onderhanden-woning', 'verbouw-zelf-vervangen', 'verbouw-plannen', 'verbouw-budget',
                'verbouw-timing', 'verbouwers', 'kluswoning', 'verbouw-zelf-vermaken', 'verbouw-stappenplan',
                'verbouw-zelf-maken', 'verbouwinspiratie', 'verbouw-vergunningen', 'verbouw-zelf-versterken',
                'verbouwingstechnieken', 'fixer-upper', 'dhz-huis', 'verbouw', 'klus-woning',
                'verbouwing', 'verbouwzelf-verfraaien', 'eigenhandig-verbouwen', 'verbouwtekenaar', 'verbouw-zelf-bouwen',
                'verbouwzelf-vermaken', 'verfraaien', 'verbouw-zelf-verbouwen',  'verbouwarchitect',
                'in-opbouw-woning', 'verbouwingskosten', 'verbouw-zelf-veranderen', 'verbouwzelf-verbeteren', 'onderhoudswerk',
                'verbouwplannen', 'schilderwerk', 'verbouwadvies', 'opknappen', 'verbouwbedrijf', 'handige-mannenproject',
                'verbouwzelf-verrassen', 'verbouw-kosten', 'doe-het-zelf-woning', 'verbouw-do-it-yourself', 'aanpakken',
                'verbouw-zelf-renoveren', 'verbouwzelf-ontwerpen', 'verbouwer', 'verbouwzelf-veranderen', 'verbouwzelf-vergroten',
                'renoveren', 'verbouw-zelf-verzorgen', 'verbouw-inspiratie', 'verbouw-zelf-verfraaien', 'verbouwen', 'verbouw-ideeën',
                'verbouwdo-it-yourself', 'renovatieproject', 'verbouwzelf-verkleinen', 'verbouw-zelf-verbeteren', 'verbouwingen',
                'sleutelklare-woning', 'verbouw-zelf-aankleden', 'verbouw-zelf-versieren', 'werk-aan-huis',
                'verbouw-aannemer', 'verbeter-je-huis', 'verbouwzelf-doen', 'verbouwingsbedrijf', 'klushuis', 'klus-pand', 'kluspand',
                'verbouwing-project', 'verbouw-project', 'huis-te-verbouwen', 'woning-klusklaar', 'verbouw-woning', 'verbouw-huis', 'dhz-woning']

    keyword = random.choice(keywords)
    format = random.choice([1, 2, 3])
    if format == 1:
        newImageName = keyword + '-' + location + '-' + streetAddress
    elif format == 2:
        newImageName = location + '-' + streetAddress + '-' + keyword
    else:
        newImageName = location + '-' + keyword + '-' + streetAddress

    location = location.replace(' ', '-')
    streetAddress = streetAddress.replace(' ', '-')
    directory = get_cwd+"/funda_images_processed/"+location+"/"+streetAddress

    while os.path.exists(os.path.join(directory, newImageName+'.jpg')):
        newImageName = newImageName + "-" + str(image_int)
        image_int += 1

    return newImageName


def imageProcessor(img_id, streetAddress, location):
    im = Image.open(f'{get_cwd}/funda_images/{img_id}.jpg')
    w, h = im.size
    im = im.crop((random.randint(5, 10), random.randint(5, 10),
                 w-random.randint(5, 10), h-random.randint(20, 30)))

    imageName = generateImageNames(streetAddress, location).replace(' ', '-')
    location = location.replace(' ', '-')
    streetAddress = streetAddress.replace(' ', '-')
    os.makedirs(
        f'{get_cwd}/funda_images_processed/{location}/{streetAddress}', exist_ok=True)
    os.makedirs(
        f'{get_cwd}/funda_images_processed/{location}/{streetAddress}', exist_ok=True)
    im.save(
        f'{get_cwd}/funda_images_processed/{location}/{streetAddress}/{imageName}.jpg')
    imageDir = location+"/"+streetAddress+"/"+imageName
    return imageDir



def urlScraper(main_url):
    # Adjusted to unpack the tuple returned by proxy_request
    req, resp = proxy_request(main_url)
    url = req.url
    print(req.status_code,'page 1')
    # Use resp for further scraping if needed
    # Note: resp is a Selector object based on your proxy_request function's implementation
    
    listing_urls = resp.xpath(
        '//div[@class="search-result-main-promo"]/a/@href | '
        '//div[@class="search-result__header-title-col"]/a[1]/@href | '
        '//h2/parent::a/@href').getall()
    print(len(listing_urls))
    
    for listing_url in listing_urls:
        print(listing_url)
        if listing_url not in scraped_data:
            listingScraper(listing_url)
    
    total_pages = resp.xpath('//ul[@class="pagination"]/li[last()-1]/a/text()').get()
    print(f'total pages : {total_pages}')
    if total_pages:
        for x in range(2,int(total_pages)+1):
            print(f'{url}&search_result={x}')
            req, resp = proxy_request(f'{url}&search_result={x}')
            print(req.status_code,f'page: {x}')
            listing_urls = resp.xpath(
                '//div[@class="search-result-main-promo"]/a/@href | '
                '//div[@class="search-result__header-title-col"]/a[1]/@href | '
                '//h2/parent::a/@href').getall()
            print(len(listing_urls))
            
            for listing_url in listing_urls:
                print(listing_url)
                if listing_url not in scraped_data:
                    listingScraper(listing_url)
    
            



def exporter(row, file_name):
    global switch
    if switch:
        switch = False
        pd.DataFrame(row, index=[0]).to_csv(file_name, index=False, mode='a')
    else:
        pd.DataFrame(row, index=[0]).to_csv(
            file_name, index=False, mode='a', header=False)


def getListingId(url):
    url = url.split('/')[-2].split('-')
    if len(url) > 1:
        return url[1]
    else:
        return url[0]


def dictParser(dic, keys):
    d = dic
    sw = True
    for k in keys:
        try:
            d = d[k]
        except:
            sw = False
    if sw:
        return d
    else:
        return None


def listingScraper(url):
    time.sleep(random.uniform(1, 5))  # Mimic human-like request interval
    req, resp = proxy_request(url)  # Adjusted to match the return value of `proxy_request`
    if not req:  # Check if the request was unsuccessful
        print(f"Failed to scrape {url}")
        return

    print(url, req.status_code)

    raw_js = resp.xpath('//script[@type="application/ld+json" and contains(text(),"address")]/text()').get()
    if raw_js:
        print('scraping data')
        js = json.loads(raw_js)
        data = {
            'url': url,
            'listing_id': getListingId(url),
            'listingName': js.get('name', '')
        }

        description_texts = resp.xpath('//div[@class="object-description-body"]/text() | //h2[contains(text(),"Omschrijving")]/following-sibling::div[1]//text()').getall()
        description = ' '.join(description_texts) if description_texts else ''
        description = scraper_helper.cleanup(description)
        # data['description'] = openAITextGeneration(description)

        # Extract and process various fields from the property listing
        data['shortDescription'] = js.get('description', '')
        data['price'] = dictParser(js, ['offers', 'price'])
        data['streetAddress'] = dictParser(js, ['address', 'streetAddress'])
        data['addressLocality'] = dictParser(js, ['address', 'addressLocality'])
        data['addressRegion'] = dictParser(js, ['address', 'addressRegion'])

        # Process additional property attributes
        data['living'] = resp.xpath('//span[contains(text(),"wonen")]/preceding-sibling::span[1]/text()').get(default='')
        data['plot'] = resp.xpath('//span[contains(text(),"perceel")]/preceding-sibling::span[1]/text()').get(default='')
        data['bedrooms'] = resp.xpath('//span[contains(text(),"slaapkamer")]/preceding-sibling::span[1]/text()').get(default='')
        data['brokerPhone'] = resp.xpath('//a[@data-track-click="Phone Called"]/@href | //div[@data-interaction="Object.Tel"]//a[contains(@href,"tel:")]/@href').get(default='')
        data['brokerName'] = resp.xpath('//h3[@class="object-contact-aanbieder-name"]/a/text() | //h3/a/@title').get(default='')
        data['Asking price per m²'] = resp.xpath('//dt[contains(text(),"Vraagprijs per")]/following-sibling::dd[1]//text()').get(default='')
        data['offeredSince'] = ' '.join(resp.xpath('//dt[contains(text(),"Aangeboden sinds")]/following-sibling::dd[1]//text()').getall())
        data['status'] = ' '.join(resp.xpath('//dt[contains(text(),"Status")]/following-sibling::dd[1]//text()').getall())
        data['acceptance'] = ' '.join(resp.xpath('//dt[contains(text(),"Aanvaarding")]/following-sibling::dd[1]//text()').getall())
        # Add additional fields here as needed...
        # Additional property details extraction
        data['typeOfHouse'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Soort woonhuis") or contains(text(),"Type woning")]/following-sibling::dd[1]//text()').getall()))
        data['typeOfConstruction'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Soort bouw")]/following-sibling::dd[1]//text()').getall()))
        data['constructionYear'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Bouwjaar")]/following-sibling::dd[1]//text()').getall()))
        data['specifically'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Specifiek")]/following-sibling::dd[1]//text()').getall()))
        data['typeOfRoof'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Soort dak")]/following-sibling::dd[1]//text()').getall()))
        data['numberOfRooms'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Aantal kamers") or contains(text(),"Kamers")]/following-sibling::dd[1]//text()').getall()))
        data['numberOfBathroom'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Aantal badkamers") or contains(text(),"Badkamers")]/following-sibling::dd[1]//text()').getall()))
        data['numberOfFloors'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Aantal woonlagen") or contains(text(),"Woonlagen")]/following-sibling::dd[1]//text()').getall()))
        data['energyLabel'] = scraper_helper.cleanup(' '.join(resp.xpath('//dt[contains(text(),"Energielabel")]/following-sibling::dd[1]//text()').getall()))
        # Handle image processing and link gathering
        image_links = []
        is_first_image = True
        if len(resp.xpath('//div[@class="media-viewer-fotos__item relative w-full"]/img').getall()) > 0:
            for srow in resp.xpath('//div[@class="media-viewer-fotos__item relative w-full"]/img'):
                img_url = srow.xpath('./@data-lazy').get()
                img_id = srow.xpath('./@data-media-id').get()
                if img_url and img_id:
                    save_image(img_url, img_id, data['streetAddress'], data['addressLocality'], is_first_image)
                    if is_first_image:  # After saving the first image, set is_first_image to False
                        is_first_image = False
                    image_links.append(img_url)
        else:
            for srow in js['photo']:
                i = srow['contentUrl']
                o = str(uuid.uuid4())
                if i:
                    req = sess.get(i, stream=True)
                    save_image(i,o,data['streetAddress'], data['addressLocality'], is_first_image)
                    if is_first_image:  # After saving the first image, set is_first_image to False
                        is_first_image = False
                    image_links.append(i)
    
        data['imageLinks'] = ','.join(image_links)
        
        # Assuming `exporter` function handles data correctly for CSV export
        exporter(data, f'{get_cwd}/funda.csv')


def save_image(img_url, img_id, streetAddress, addressLocality, is_first_image):
    response = sess.get(img_url, stream=True)
    if response.status_code == 200:
        # Define the base path for saving images
        base_image_path = f'{get_cwd}/funda_images_processed/{addressLocality}/{streetAddress}'
        os.makedirs(base_image_path, exist_ok=True)

        # Define the full path for the original image
        original_image_path = os.path.join(base_image_path, f'{img_id}.jpg')

        with open(original_image_path, 'wb') as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        
        print(f"Image saved at {original_image_path}")

        # Generate and save thumbnail only for the first image
        if is_first_image:
            try:
                img = Image.open(original_image_path)
                #ANTIALIAS DEPRECIATED
                # img.thumbnail((500, 500), Image.ANTIALIAS)  # Resize image for thumbnail
                img.thumbnail((500, 500), Image.LANCZOS)  # Resize image for thumbnail
                thumbnail_image_path = os.path.join(base_image_path, f'{img_id}_thumbnail.jpg')
                img.save(thumbnail_image_path)  # Save the resized image as a thumbnail
                print(f"Thumbnail saved at {thumbnail_image_path}")
            except Exception as e:
                print(f"Error creating thumbnail: {e}")
    else:
        print(f"Failed to download image: {img_url}")




if __name__ == '__main__':
    if os.path.exists(f'{get_cwd}/funda.csv'):
        scraped_data = pd.read_csv(f'{get_cwd}/funda.csv')['url'].to_list()
    else:
        scraped_data = []

    target_url = 'https://www.funda.nl/koop/heel-nederland/kluswoning/'
    listing_urls = urlScraper(target_url)
        