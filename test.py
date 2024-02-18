import requests
from bs4 import BeautifulSoup
import csv

import aiohttp
import asyncio
import async_timeout

async def fetch_url(session, url):
    try:
        with async_timeout.timeout(10):
            async with session.get(url) as response:
                return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return None
    
async def fetch_all_urls(infos):
    urls = []
    for info in infos:
        # geoid = "60750"+ info

        url = f"https://censusreporter.org/profiles/14000US0{info['geoid']}-census-tract-{info['tractce']}-san-francisco-ca/"

        urls.append(url)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls]
        return await asyncio.gather(*tasks)

# async def fetch_url(session, url):
#     try:
#         async with aiohttp.ClientTimeout(total=10):
#             async with session.get(url) as response:
#                 return await response.text()
#     except Exception as e:
#         print(f"Error fetching {url}: {str(e)}")
#         return None

# async def fetch_all_urls(infos):
#     urls = [f"https://censusreporter.org/profiles/14000US0{info['geoid']}-census-tract-{info['tractce']}-san-francisco-ca/" for info in infos]
    
#     async with aiohttp.ClientSession() as session:
#         tasks = [fetch_url(session, url) for url in urls]
#         return await asyncio.gather(*tasks)
# import asyncio
# import aiohttp
# import async_timeout
# from aiohttp import ClientSession

# async def fetch_url(session, url, delay=0):
#     await asyncio.sleep(delay)  # Introduce delay before each request
#     try:
#         with async_timeout.timeout(10):
#             async with session.get(url) as response:
#                 return await response.text()
#     except Exception as e:
#         print(f"Error fetching {url}: {str(e)}")
#         return None

# async def fetch_all_urls(infos, batch_size=20, delay=2):
#     urls = [f"https://censusreporter.org/profiles/14000US0{info['geoid']}-census-tract-{info['tractce']}-san-francisco-ca/" for info in infos]
    
#     async with ClientSession() as session:
#         tasks = []
#         for i, url in enumerate(urls):
#             # Introduce delay for each task based on its order and a given delay
#             task = fetch_url(session, url, delay=i * delay / batch_size)
#             tasks.append(task)
#             # Batch tasks to limit the number of concurrent requests
#             if (i + 1) % batch_size == 0 or i + 1 == len(urls):
#                 results = await asyncio.gather(*tasks)
#                 tasks = []  # Reset tasks list for the next batch
#                 yield from results  # Use yield to handle results as they are ready
#         if tasks:  # Ensure any remaining tasks are also processed
#             results = await asyncio.gather(*tasks)
#             yield from results
    
def parse_html_content(html_content, xpaths):
    # Parse the HTML content with BeautifulSoup using lxml parser for better CSS selector support
    if html_content is None:
        print("Empty content received, skipping parsing.")
        return {}
   
    from lxml import etree
    from lxml.etree import tostring
    parser = etree.HTMLParser()
    tree = etree.fromstring(html_content, parser)

    # Define a dictionary to hold the extracted information
    extracted_info = {}
    for xpath in xpaths:
            elements = tree.xpath(xpath['val'])

            if elements:
                # If the XPath ends with text(), elements will be a list of strings
                if xpath['val'].endswith('text()'):
                    extracted_info[xpath['name']] = (elements[0].strip())
                # Otherwise, elements is a list of elements, and you can extract text using .text
                else:
                    # Ensure the element has text before stripping it
                    if elements[0].text:
                        extracted_info[xpath['name']] = (elements[0].text.strip())
                    else:
                        extracted_info[xpath['name']] = "N/A"
            else:
                extracted_info[xpath['name']] = "N/A"
   

    # print(extracted_info)
    # Using CSS Selector to extract the population
    # population_selector = "#cover-profile > article > div:nth-child(2) > div > span > span.value"
    # population_element = soup.select_one(population_selector)
    # if population_element:
    #     extracted_info['Population'] = population_element.text.strip()
    # else:
    #     extracted_info['Population'] = 'N/A'
    # population_xpath = "/html/body/div[1]/div[2]/article/div[1]/div/span/span[1]"
    # population_elements = tree.xpath(population_xpath)

    # if population_elements:
    #     extracted_info['Population'] = population_elements[0].text.strip()
    # else:
    #     extracted_info['Population'] = 'N/A'
    
    # ma_xpath = "/html/body/div[2]/article[1]/div/section[1]/div[1]/a/span/span[1]/text()"
    # ma_element = tree.xpath(ma_xpath)
    # print(ma_element)
    # if ma_xpath:
    #     extracted_info['ma'] = ma_element[0].text.strip()
    # else:
    #     extracted_info['ma'] = 'N/A'
    
    # female_rate_xpath = "/html/body/div[2]/article[1]/div/section[2]/div[1]/div/div/div[2]/div/span[2]"
    # fr_element = tree.xpath(female_rate_xpath)
    # # print(fr_element)
    # if fr_element:
    #     extracted_info['female_rate'] = fr_element.text.strip()
    # else:
    #     extracted_info['female_rate'] = 'N/A'

    # income_xpath = "/html/body/div[2]/article[1]/div/section[2]/div[1]/div/div/div[2]/div/span[2]"
    # income_element = tree.xpath(female_rate_xpath)
    # # print(fr_element)
    # if fr_element:
    #     extracted_info['female_rate'] = fr_element.text.strip()
    # else:
    #     extracted_info['female_rate'] = 'N/A'

    #     /html/body/div[2]/article[2]/div/section[1]/div[1]/a/span/span[1]/text()

    # /html/body/div[2]/article[1]/div/section[1]/div[1]/a/span/span[1]
    
    # population_selector = "#cover-profile > article > div:nth-child(2) > div > span > span.value"
    # population_element = soup.select_one(population_selector)
    # if population_element:
    #     extracted_info['Population'] = population_element.text.strip()
    # else:
    #     extracted_info['Population'] = 'N/A'
    
    # population_selector = "#cover-profile > article > div:nth-child(2) > div > span > span.value"
    # population_element = soup.select_one(population_selector)
    # if population_element:
    #     extracted_info['Population'] = population_element.text.strip()
    # else:
    #     extracted_info['Population'] = 'N/A'

    

    # Add more selectors here for other pieces of information you need
    # For example, median age, household income, etc., each following the same pattern:
    # 1. Define the selector
    # 2. Use soup.select_one(selector) to find the element
    # 3. Extract the text or attribute you need
    # 4. Add it to the extracted_info dictionary

    return extracted_info

def extract_number(input_string):
    import re
    # Find all occurrences of numeric patterns in the string
    numbers = re.findall(r'\d+\.?\d*', input_string)
    # This regex pattern \d+\.?\d* will match integers or decimals

    if numbers:
        # Convert the first found number to float if decimal, else int
        first_number = float(numbers[0]) if '.' in numbers[0] else int(numbers[0])
        return first_number
    else:
        return None  # or raise an exception, or handle as you see fit
    




xpaths = [
        {
            "name": "population",
            "val": "/html/body/div[1]/div[2]/article/div[1]/div/span/span[1]"
        },
        {
            "name": "female_rate",
            "val": "/html/body/div[2]/article[1]/div/section[2]/div[1]/div/div/div[2]/div/span[2]/text()"
        },
        {
            "name": "median_age",
            "val": "/html/body/div[2]/article[1]/div/section[1]/div[1]/a/span/span[1]/text()"
        },
        {
            "name": "per_cap_income",
            "val": "/html/body/div[2]/article[2]/div/section[1]/div[1]/a/span/span[1]/text()"
        },
        {
            "name": "median_household_income",
            "val": "/html/body/div[2]/article[2]/div/section[1]/div[2]/a/span/span[1]/text()"
        },
        {
            "name": "poverty",
            "val": "/html/body/div[2]/article[2]/div/section[2]/div[1]/a/span/span[1]/text()"
        },
        {
            "name": "household",
            "val": "/html/body/div[2]/article[3]/div/section[1]/div[1]/a/span/span[1]/text()"
        },
        {
            "name": "person_per_household",
            "val": "/html/body/div[2]/article[3]/div/section[1]/div[2]/a/span/span[1]/text()"
        },
        {
            "name": "non_family_percentage",
            "val": "/html/body/div[2]/article[3]/div/section[1]/div[3]/div/div[2]/div/span[2]/text()"
        },
        {
            "name": "married",
            "val": "/html/body/div[2]/article[3]/div/section[2]/div[1]/div/div[2]/div/span[2]/text()"
        },
        {
            "name": "number_of_house_unit",
            "val": " /html/body/div[2]/article[4]/div/section[1]/div[1]/a/span/span[1]/text()"
        },
        {
            "name": "occupied",
            "val": "/html/body/div[2]/article[4]/div/section[1]/div[2]/div/div[2]/div/span[2]"
        },
        {
            "name": "renter_occupied",
            "val": "/html/body/div[2]/article[4]/div/section[1]/div[3]/div/div[2]/div/span[2]"
        },
        {
            "name": "highschool",
            "val": "/html/body/div[2]/article[5]/div/section[1]/div[1]/div[1]/a/span/span[1]/text()"
        },
         {
            "name": "bachelor",
            "val": "/html/body/div[2]/article[5]/div/section[1]/div[1]/div[2]/a/span/span[1]/text()"
        },
         {
            "name": "language",
            "val": "/html/body/div[2]/article[5]/div/section[2]/div[1]/a/span/span[1]/text()"
        },
         {
            "name": "placeofbirth",
            "val": "/html/body/div[2]/article[5]/div/section[3]/div[1]/a/span/span[1]/text()"
        },
         {
            "name": "veteran",
            "val": "/html/body/div[2]/article[5]/div/section[4]/div[1]/a/span/span[1]/text()"
        },

        # /html/body/div[2]/article[4]/div/section[1]/div[1]/a/span/span[1]/text()


    ]

# async def main():
#     import pandas as pd
#     df = pd.read_csv("t.csv")
#     infos = df[['TRACTCE', 'GEOID']].apply(lambda row: {'tractce': str(row['TRACTCE']), 'geoid': row['GEOID']}, axis=1).to_list()
    
#     res = []  # Make sure this is defined to collect results
#     async for results in fetch_all_urls(infos):
#         for result in results:
#             if result:  # Check if result is not None
#                 parsed_content = parse_html_content(result, xpaths)
#                 res.append(parsed_content)
    
#     # After collecting all data, write to CSV
#     pd.DataFrame(res).to_csv('output.csv', index=False)

async def main():
    # tracts = [{"geoid": "6075980501", "tractce": "980501"}]  # Your tract pairs here

    import pandas as pd
    # df = pd.read_csv("t.csv")
    # infos = df[['TRACTCE', 'GEOID']].apply(lambda row: {'tractce': str(row['TRACTCE']), 'geoid': row['GEOID']}, axis=1).to_list()
   
    infos = [
    # {"tractce": "20101", "geoid": "6075020101"},
    # {"tractce": "18000", "geoid": "6075018000"},
    # {"tractce": "61501", "geoid": "6075061501"},
    # {"tractce": "61503", "geoid": "6075061503"},
    # {"tractce": "61507", "geoid": "6075061507"},
    # {"tractce": "33203", "geoid": "6075033203"},
    # {"tractce": "31000", "geoid": "6075031000"},
    # {"tractce": "22600", "geoid": "6075022600"},
    # {"tractce": "60702", "geoid": "6075060702"},
    # {"tractce": "12002", "geoid": "6075012002"},
    # {"tractce": "60502", "geoid": "6075060502"},
    # {"tractce": "16802", "geoid": "6075016802"},
    # {"tractce": "25300", "geoid": "6075025300"},
    # {"tractce": "980600", "geoid": "60750980600"},
    # {"tractce": "16000", "geoid": "6075016000"},
    # {"tractce": "61502", "geoid": "6075061502"},
    # {"tractce": "12406", "geoid": "6075012406"},
    # {"tractce": "980900", "geoid": "60750980900"},
    {"tractce": "20202", "geoid": "6075020202"},
    {"tractce": "22901", "geoid": "6075022901"},
]

    # Fetch all URLs
    results = await fetch_all_urls(infos)
    # res = []
    
    # res = []
    # for result in results:
    #     res.append(parse_html_content(result, xpaths, result))
    # df = pd.DataFrame(res)
    # df.to_csv('output.csv', index=False)

    res = []
    # Since results are in order, you can zip infos and results directly
    for info, result in zip(infos, results):
        if result:  # Check if result is not None
            parsed_content = parse_html_content(result, xpaths)

            # Append 'geoid' and 'tractce' to parsed_content
            parsed_content['geoid'] = info['geoid']
            parsed_content['tractce'] = info['tractce']
            res.append(parsed_content)
        else:
            # Append a record indicating a failed fetch or parse
            res.append({'geoid': info['geoid'], 'tractce': info['tractce'], 'error': 'Failed to fetch or parse content'})
    
    df = pd.DataFrame(res)
    df.to_csv('output2_3.csv', index=False)

# Write to CSV
    # print(results)

# Run the main function
if __name__ == "__main__":
    

    
    asyncio.run(main())

# Open a CSV file to write the data
# with open('census_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
#     writer = csv.DictWriter(csvfile, fieldnames=columns)
#     writer.writeheader()
    
#     # Example URL structure, replace the tract number as needed
#     base_url = "https://censusreporter.org/profiles/14000US{}-census-tract-{}-san-francisco-ca/"
#     tracts = [("06075980501", "980501")]  # Add your 244 tract pairs here
    
#     for tract_pair in tracts:
#         url = base_url.format(*tract_pair)
#         response = requests.get(url)
        
#         if response.status_code == 200:
#             soup = BeautifulSoup(response.content, 'html.parser')
            
#             # Initialize a dictionary to hold the data for this row
#             row_data = {'Census Tract': tract_pair[1], 'URL': url}
            
#             # Extract data points using BeautifulSoup based on the observed structure of the webpage
#             # This is a placeholder example; you'll need to adjust selectors based on actual content
#             row_data['Population'] = soup.find('some_selector_for_population').text
#             row_data['Median Age'] = soup.find('some_selector_for_median_age').text
#             row_data['Household Income'] = soup.find('some_selector_for_household_income').text
#             row_data['Housing Units'] = soup.find('some_selector_for_housing_units').text
            
#             # Write the row to the CSV file
#             writer.writerow(row_data)
#         else:
#             print(f"Failed to fetch data for URL: {url}")
