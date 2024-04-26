import os
import pandas as pd
import requests
from requests.exceptions import SSLError
from scrapy import Selector
import random
import json
import ftplib
import scraper_helper
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# FTP credentials and server information
FTP_HOST = 's243.webhostingserver.nl'
FTP_USER = 'theuserof@klusvastgoed.nl'
FTP_PASS = 'vsavwjhN7uSS8h59rSae@!'

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (iPad; CPU OS 13_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    # Add more user-agents as needed
]
user_agent = random.choice(user_agents)

get_cwd = os.getcwd()
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
Upgrade-Insecure-Requests: 1"""
headers = scraper_helper.get_dict(headers,strip_cookie=True)



# Your Smartproxy credentials and endpoint
username = 'spb2g1f0zg'
password = 'K73JIwcrmfy2qelh1U'
endpoint = 'eu.smartproxy.com:10000'


def upload_file_ftp(filename):
    """
    Upload a file to an FTP server and delete the file locally after upload.
    """
    try:
        with ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS) as ftp:
            ftp.cwd('/update/')  # Change to the appropriate directory on FTP
            with open(filename, 'rb') as file:
                ftp.storbinary(f'STOR {filename}', file)
            print(f"Successfully uploaded {filename} to FTP.")
    except ftplib.all_errors as e:
        print(f"FTP error: {e}")
    finally:
        os.remove(filename)
        print(f"Deleted local file {filename}.")



def proxy_request(url, retry=False):
    """ Function to manage proxy requests, with optional retry on SSL error """
    proxy = f"http://{username}:{password}@{endpoint}"
    proxies = {"http": proxy, "https": proxy}
    try:
        response = requests.get(url, proxies=proxies, headers=headers, timeout=10)  # Added a timeout for safety
        if response.status_code == 200:
            return response, Selector(text=response.text)
        else:
            print(f"Failed to fetch {url}, status code: {response.status_code}")

    except SSLError as ssl_error:
        print(f"SSL error occurred: {ssl_error}")
        if not retry:  # Retry once if it's the first SSL error
            print("Retrying request...")
            return proxy_request(url, retry=True)
        else:
            print("Retry failed.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return None, None





def read_csv(filename):
    """ Read CSV and process each row """
    df = pd.read_csv(filename)
    return df




def scrape_data(row):
    """ Scrape data from each URL and update availability """
    # Initialize data dictionary at the start of each scrape session
    data = {'status': ''}
    
    response, selector = proxy_request(row['FD URL'])
    if not selector:  # Check if the request was unsuccessful
        print(f"Failed to scrape {row['FD URL']}")
        return row

    print(row['FD URL'], response.status_code)
    
    # Check if the URL contains '/detail/', potentially fetching additional details
    if '/detail/' in row['FD URL']:
        img_response, img_selector = proxy_request(f"{row['FD URL']}/overzicht")
        print(f"{row['FD URL']}/overzicht", img_response.status_code if img_response else "Request failed")
    
    # Check for specific "Verkocht" status
    verkocht_status = selector.xpath('//li[contains(@class, "label-transactie-definitief")][contains(text(), "Verkocht")]/text()').get()
    if verkocht_status:
        data['status'] = 'Verkocht'
    else:
        # Extract status using original method if "Verkocht" is not found
        status_list = selector.xpath('//dt[contains(text(),"Status")]/following-sibling::dd[1]//text()').getall()
        cleaned_status = ' '.join([text.strip() for text in status_list if text.strip()])
        data['status'] = ' '.join(cleaned_status.split())  # Normalize spaces

    if data['status']:
        row['Available'] = data['status']
        print("new status:", data['status'])

    return row



def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f'outputs-{date_str}.csv'
    #filename = f'outputs-2024-04-22.csv'
    if not os.path.exists(filename):
        print(f"No file found for {filename}")
        return

    df = pd.read_csv(filename)
    updated_rows = []

    # Using ThreadPoolExecutor to scrape data concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(scrape_data, row): row for index, row in df.iterrows()}
        for future in as_completed(future_to_row):
            result = future.result()
            updated_rows.append(result)

    new_filename = 'updated-' + filename
    new_df = pd.DataFrame(updated_rows)
    new_df.to_csv(new_filename, index=False)

    # Upload the updated file to FTP and delete it
    upload_file_ftp(new_filename)

if __name__ == '__main__':
    main()

