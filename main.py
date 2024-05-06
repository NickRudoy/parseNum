import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def normalize_phone_number(phone_number):
    return re.sub(r'\D', '', phone_number)

def find_phone_numbers(url, session):
    phone_numbers = set()

    if pd.isna(url):
        return list(phone_numbers)

    try:
        response = session.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=re.compile(r'tel:'))

        for link in links:
            phone_number = re.findall(r'tel:(.*)', link['href'])[0]
            phone_number = normalize_phone_number(phone_number)
            phone_numbers.add(phone_number)
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")

    return list(phone_numbers)

def main():
    file_path = input("Please enter the path to your CSV file: ")
    df = pd.read_csv(file_path)
    
    df['Address'] = df.get('Address', pd.Series(dtype='str'))
    df['number'] = pd.Series(dtype='str', index=df.index)

    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    unique_numbers = {}
    excluded_number = '74957888333'

    for i, url in enumerate(df['Address']):
        print(f"Processing URL {i+1}: {url}")
        phone_numbers = find_phone_numbers(url, session)
        df.loc[i, 'number'] = ', '.join(phone_numbers)
        print(f"Found phone numbers: {phone_numbers}")
        print(df)
        df.to_csv("new_file3.csv", index=False)

        for number in phone_numbers:
            if number not in unique_numbers and number != excluded_number:
                unique_numbers[number] = url
                df_unique = pd.DataFrame(list(unique_numbers.items()), columns=['number', 'Address'])
                df_unique.to_csv("unique_numbers.csv", index=False)

    

    

if __name__ == "__main__":
    main()
