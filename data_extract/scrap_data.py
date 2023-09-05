


import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import tika
from tika import parser


# scrap sporting events
def scrap_sport(url, filename):
    response = requests.get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')
    events_table = soup.find('table', {'class': 'list'})
    with open(filename, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['Date', 'Sport','Event', 'Location'])
        for row in events_table.find_all('tr')[1:]:
            cols = row.find_all('td')
            date = cols[0].text.strip()
            sport = cols[1].text.strip()
            event = cols[2].text.strip()
            location = cols[3].text.strip()
            writer.writerow([date, sport, event, location])

# scrap movies
def scrap_movie_22(url):

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    table1 = soup.find('div',attrs={"class":"sqs-block html-block sqs-block-html", "id": "block-6ac6ab807903b6cc76b4"})
    table2 = soup.find('div',attrs={"class":"sqs-block html-block sqs-block-html", "id": "block-yui_3_17_2_1_1643130479299_9491"})
    table3 = soup.find('div',attrs={"class":"sqs-block html-block sqs-block-html", "id": "block-yui_3_17_2_1_1635595753017_12688"})

    p_tags_one = table1.find_all('p')
    p_tags_two = table2.find_all('p')
    p_tags_three = table3.find_all('p')

    with open("fest/festival_2022.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Festival', 'Date'])
        for i in range(4, len(p_tags_one)-1, 3):
            writer.writerow([p_tags_one[i].text.strip(), p_tags_one[i+1].text.strip()])
        for i in range(0, len(p_tags_two)-1, 3):
            writer.writerow([p_tags_two[i].text.strip(), p_tags_two[i+1].text.strip()])
        for i in range(1, len(p_tags_three)-4, 3):
            writer.writerow([p_tags_three[i].text.strip(), p_tags_three[i+1].text.strip()])

def scrap_movie_21(path):
    parsed_pdf = parser.from_file(path, service='text')
    text = parsed_pdf['content']
    index_first = text.find('Previous events')
    index_last = text.find('Coronavirus')
    text = text[index_first:index_last]
    for data in text:
        data = text.split('\n\n')
    for d in data:
        if 'http' in d:
            data.remove(d)
    data = list(filter(lambda x: '\n' not in x, data))
    data = data[1:-1]
    with open("fest/festival_2021.csv", mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['Festival', 'Date', 'Location'])
        for element in data:
            fields = element.split(',')
            if ',' in fields:
                fields = fields.split(',')
            writer.writerow(fields)



def scrap_adl(url):
    text = ""
    num_pages = 11
    for i in range(0, num_pages + 1):
        page_url = f"{url}{i}"
        response = requests.get(page_url)
        soup = BeautifulSoup(response.content, "html.parser")
        raw_text = soup.get_text()
        parsed_text = parser.from_buffer(raw_text)
        text_content = parsed_text["content"]
        text += text_content
        index_first = text.find('100%')
        index_last = text.find('Read more about Zyklon B')
        text = text[index_first:index_last]
        for data in text:
            data = text.split('\n\n\n')
        ls = []
        for i in range(len(data)):
            if "\n\nHate Symbol" in data[i]:
                ls.append(data[i])
        for i in range(len(ls)):
            ls[i] = ls[i].split('\n')[0]
        with open("hate_speech/ADL.csv", mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['ADL_Symbol'])
            for sym in ls:
                if sym == "":
                    continue
                writer.writerow([sym])



if __name__ == '__main__':

    url_sport_2022 = "https://www.topendsports.com/events/calendar-2022.htm"
    url_sport_2021 = "https://www.topendsports.com/events/calendar-2021.htm"
    url_sport_2020 = "https://www.topendsports.com/events/calendar-2020.htm"
    url_fes_2022 = 'https://www.film-fest-report.com/home/film-festivals-2022'
    filesport_2022 = 'sport/sporting_events_2022.csv'
    filesport_2021 = 'sport/sporting_events_2021.csv'
    filesport_2020 = 'sport/sporting_events_2020.csv'
    path_fest_2021 = 'pdf/2021_film_festivals.pdf'
    url_adl = "https://www.adl.org/resources/hate-symbols/search?keywords=&sort_by=title&page="


    scrap_sport(url_sport_2022, filesport_2022)
    scrap_sport(url_sport_2021, filesport_2021)
    scrap_sport(url_sport_2020, filesport_2020)
    scrap_movie_22(url_fes_2022)
    scrap_movie_21(path_fest_2021)
    scrap_adl(url_adl)



    

