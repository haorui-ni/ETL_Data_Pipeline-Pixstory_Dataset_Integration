import pandas as pd
import numpy as np
import json
import glob
import os
from datetime import datetime

# Sport and Festival
def combine(path):
    path = r'./' + path
    all_files = glob.glob(os.path.join(path , "./*.csv"))
    frame = pd.DataFrame()
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        frame = pd.concat([frame,df], axis=0, ignore_index=True)
    frame.to_csv(path + '.tsv', sep="\t", index=False)

def active_sport(sport_data):
    sport_data.drop(sport_data[sport_data['Date'].str.contains('postponed until')].index, inplace=True)
    sport_data.drop(sport_data[sport_data['Date'].str.contains('postponed to')].index, inplace=True)
    sport_data.drop(sport_data[sport_data['Date'].str.contains('postponed\)')].index, inplace=True)
    sport_data.drop(sport_data[sport_data['Date'].str.contains('postponed again')].index, inplace=True)
    sport_data.drop(sport_data[sport_data['Date'].str.contains('canceled')].index, inplace=True)
    return sport_data

def end_date(df_row):
    try:
        if df_row['End_date'] == None:
            return
        elif df_row['End_date'].isdigit():
            return f"{df_row['Start_date'][:8]}{df_row['End_date'].zfill(2)}"
        else:
            month, day = df_row['End_date'].split()
            return f"{df_row['Start_date'][:5]}{month[:3]}-{day.zfill(2)}"
    except:
        return "error"

def date_format(df_date):
    date = datetime.strptime(df_date, '%m/%d/%Y')
    formatted_date = date.strftime('%Y-%m-%d')
    return formatted_date


if __name__ == '__main__':
    combine("init")
    ini_data = pd.read_csv('combined_tsv.tsv', delimiter='\t')
    ini_data['Account Created Date'] = pd.to_datetime(ini_data['Account Created Date']).dt.date

    # adding sport and fest
    sport_2020 = pd.read_csv("sport/sporting_events_2020.csv")
    sport_2021 = pd.read_csv("sport/sporting_events_2021.csv")
    sport_2022 = pd.read_csv("sport/sporting_events_2022.csv")

    sport_2022['Date'] = sport_2022['Date'].str.replace('â', ' - ')
    sport_2021['Date'] = sport_2021['Date'].str.replace('â', ' - ')
    sport_2021['Date'] = sport_2021['Date'].str.replace('Â', ' - ')
    sport_2020['Date'] = sport_2020['Date'].str.replace('â', ' - ')
    sport_2020['Date'] = sport_2020['Date'].str.replace('Â', ' -')

    sport_2022['Date'] = sport_2022['Date'].str.replace(' - ', '-')
    sport_2021['Date'] = sport_2021['Date'].str.replace(' - ', '-')
    sport_2020['Date'] = sport_2020['Date'].str.replace(' - ', '-')
    sport_2022['Date'] = sport_2022['Date'].str.replace('-', ' - ')
    sport_2021['Date'] = sport_2021['Date'].str.replace('-', ' - ')
    sport_2020['Date'] = sport_2020['Date'].str.replace('-', ' - ')

    sport_2022 = active_sport(sport_2022)
    sport_2021 = active_sport(sport_2021)
    sport_2020 = active_sport(sport_2020)

    sport_2022['Date'] = sport_2022['Date'].str.replace(r'(\w+)-(\d+)', r'\1 \2')
    sport_2021['Date'] = sport_2021['Date'].str.replace(r'(\w+)-(\d+)', r'\1 \2')
    sport_2020['Date'] = sport_2020['Date'].str.replace(r'(\w+)-(\d+)', r'\1 \2')

    sport_2022 = sport_2022.assign(Year=2022)
    sport_2021 = sport_2021.assign(Year=2021)
    sport_2020 = sport_2020.assign(Year=2020)

    sport_2022[['Start_date', 'End_date']] = sport_2022['Date'].str.split(' - ', expand=True)
    sport_2022 = sport_2022.drop(['Date'], axis=1)

    sport_2021[['Start_date', 'end_date', 'append']] = sport_2021['Date'].str.split(' - ', expand=True)
    sport_2021['append'] = sport_2021['append'].astype(str).replace('None', '')
    sport_2021['End_date'] = sport_2021['end_date'] + str(sport_2021['append'])
    sport_2021['End_date'] = sport_2021['End_date'].str.split('0        \n1').str[0]
    sport_2021 = sport_2021.drop(['Date', 'end_date', 'append'], axis=1)

    sport_2020[['Start_date', 'End_date', 'append']] = sport_2020['Date'].str.split(' - ', expand=True)
    sport_2020 = sport_2020.drop(['Date', 'append'], axis=1)

    sport_data = pd.concat([sport_2020, sport_2021, sport_2022], axis=0)
    sport_data = sport_data.reset_index(drop=True)
    sport_data['Start_date'] = sport_data['Start_date'].str.split(' \(').str[0]
    sport_data['End_date'] = sport_data['End_date'].str.split(' \(').str[0]

    sport_data['Start_date'] = pd.to_datetime(sport_data['Year'].astype(str) + ' ' + sport_data['Start_date'])
    sport_data['Start_date'] = sport_data['Start_date'].dt.strftime('%Y-%m-%d')
    sport_data = sport_data.drop(['Year'], axis=1)

    sport_data['End_date'] = sport_data.apply(end_date, axis=1)

    sport_data['End_date'] = pd.to_datetime(sport_data['End_date'], errors='coerce')
    sport_data.loc[sport_data['End_date'].dt.strftime('%b').notnull(), 'End_date'] = sport_data.loc[
        sport_data['End_date'].dt.strftime('%b').notnull(), 'End_date'].dt.strftime('%Y-%m-%d')
    sport_data['End_date'] = sport_data['End_date'].dt.strftime('%Y-%m-%d')
    sport_data = sport_data.loc[~sport_data['End_date'].fillna('').str.contains('error')]
    sport_data['End_date'].fillna(sport_data['Start_date'], inplace=True)
    sport_data.to_csv('sport/sport.csv', index=False)

    festival_2022 = pd.read_csv("fest/festival_2022.csv", on_bad_lines='skip')

    festival_2022['Date'] = festival_2022['Date'].str.split(' 2022').str[0]
    festival_2022['Date'] = festival_2022['Date'].str.replace('-', ' - ')

    festival_2022['Date'] = festival_2022['Date'].str.replace(r'(\d+)\s+([A-Za-z]+)\s*-\s*(\d+)\s+([A-Za-z]+)',
                                                              r'\2 \1 - \4 \3')
    festival_2022['Date'] = festival_2022['Date'].str.replace(r'(\d+)\s*-\s*(\d+)\s+([A-Za-z]+)', r'\3 \1 - \3 \2')
    festival_2022['Date'] = festival_2022['Date'].str.replace(r'(\d+)\s+([A-Za-z]+)', r'\2 \1')

    festival_2022[['Start_date', 'End_date']] = festival_2022['Date'].str.split(' - ', expand=True)
    festival_2022.drop('Date', axis=1, inplace=True)

    festival_2022['Start_date'] = festival_2022['Start_date'] + ', 2022'
    festival_2022['Start_date'] = pd.to_datetime(festival_2022['Start_date'], format='%B %d, %Y')
    festival_2022['Start_date'] = festival_2022['Start_date'].dt.strftime('%Y-%m-%d')

    festival_2022['End_date'] = festival_2022['End_date'] + ', 2022'
    festival_2022['End_date'] = pd.to_datetime(festival_2022['End_date'], format='%B %d, %Y')
    festival_2022['End_date'] = festival_2022['End_date'].dt.strftime('%Y-%m-%d')

    festival_2021 = pd.read_csv("fest/festival_2021.csv", on_bad_lines='skip')
    festival_2021 = festival_2021[["Festival", "Date"]]
    festival_2021['Date'] = festival_2021['Date'].str.replace(' – ', ' - ')
    festival_2021['Date'] = festival_2021['Date'].str.split(' - ').str[1]

    festival_2021[['Start_date', 'End_date']] = festival_2021['Date'].str.split('-', expand=True)
    festival_2021.drop('Date', axis=1, inplace=True)
    festival_2021 = festival_2021.assign(Year=2021)

    festival_2021['Start_date'] = pd.to_datetime(festival_2021['Year'].astype(str) + ' ' + festival_2021['Start_date'])
    festival_2021['Start_date'] = festival_2021['Start_date'].dt.strftime('%Y-%m-%d')

    festival_2021['End_date'] = festival_2021.apply(end_date, axis=1)
    festival_2021['End_date'] = pd.to_datetime(festival_2021['End_date'], errors='coerce')
    festival_2021.loc[festival_2021['End_date'].dt.strftime('%b').notnull(), 'End_date'] = festival_2021.loc[
        festival_2021['End_date'].dt.strftime('%b').notnull(), 'End_date'].dt.strftime('%Y-%m-%d')
    festival_2021['End_date'] = festival_2021['End_date'].dt.strftime('%Y-%m-%d')
    festival_2021 = festival_2021.drop(['Year'], axis=1)

    festival_2020 = pd.read_csv("fest/festival_2020.csv", on_bad_lines='skip')

    festival_2020 = festival_2020[["Festival Name", "Start Date", "End Date"]]
    festival_2020 = festival_2020.rename(
        columns={"Festival Name": "Festival", "Start Date": "Start_date", "End Date": "End_date"})
    needed = festival_2020["Start_date"] != "tbd"
    festival_2020 = festival_2020[needed]
    festival_2020["Start_date"] = festival_2020["Start_date"].apply(date_format)

    festival_2020 = festival_2020[~festival_2020['End_date'].str.contains('`')]
    festival_2020["End_date"] = festival_2020["End_date"].apply(date_format)

    festival_data = pd.concat([festival_2020, festival_2021, festival_2022], axis=0)
    festival_data = festival_data.reset_index(drop=True)
    festival_data.to_csv('fest/festival.csv', index=False)

    ini_data['Account Created Date'] = pd.to_datetime(ini_data['Account Created Date'])

    sport_data['Start_date'] = pd.to_datetime(sport_data['Start_date'])
    sport_data['End_date'] = pd.to_datetime(sport_data['End_date'])
    festival_data['Start_date'] = pd.to_datetime(festival_data['Start_date'])
    festival_data['End_date'] = pd.to_datetime(festival_data['End_date'])

    merged_sport = pd.merge(ini_data, sport_data, how='left', left_on='Account Created Date', right_on='Start_date')
    merged_sport = merged_sport[((merged_sport['Account Created Date'] <= merged_sport['End_date'])) &
                                (merged_sport['Account Created Date'] >= merged_sport['Start_date'])]
    merged_festival = pd.merge(ini_data, festival_data, how='left', left_on='Account Created Date',
                               right_on='Start_date')
    merged_festival = merged_festival[((merged_festival['Account Created Date'] <= merged_festival['End_date'])) &
                                      (merged_festival['Account Created Date'] >= merged_festival['Start_date'])]

    ini_data['Sport Event'] = merged_sport['Event']
    ini_data['Festival Event'] = merged_festival['Festival']
    
    ini_data.to_csv('combination/added_ini_data.tsv', sep='\t', index=False)
    os.remove("init.tsv")

# STEP 2
## Read the whole dataset
df = pd.read_csv('combination/added_ini_data.tsv', sep='\t', header = 0)
df["Account Created Date"] = pd.to_datetime(df["Account Created Date"])

## FOOTBALL MATCH
f = open("add_dataset/football_parsed.json")
json = json.load(f)

data = []
for json_data in json:
    if 'WinTeam' in json_data:
        data.append({
        'date': json_data['date'],
        'WinTeam': json_data['WinTeam'],
        'LoseTeam': json_data['LoseTeam'],
        'ScoreGap': json_data['ScoreGap']
        })

df_football = pd.DataFrame(data)
df_fb_grouped = df_football.groupby('date').agg({
    'WinTeam': lambda x: ', '.join(x.dropna()),
    'LoseTeam': lambda x: ', '.join(x.dropna()),
    'ScoreGap': lambda x: ', '.join([str(val) for val in x.dropna()])
})
df_fb_grouped.reset_index(inplace=True)
df_fb_grouped['date'] = pd.to_datetime(df_fb_grouped['date'])

## MERGE FOOTBALL MATCH
df_merged1 = pd.merge(df, df_fb_grouped, how="left", left_on='Account Created Date', right_on='date')
df_merged1.drop('date', axis=1, inplace=True)

## MOVIES
df_movies = pd.read_csv("add_dataset/movies.csv")
df_movies['year_month'] = pd.to_datetime(df_movies['Year'].astype(str) + '-' + df_movies['Month'], format='%Y-%B').dt.to_period('M')
df_movies_grouped = df_movies.groupby('year_month').agg({'MovieTitle': lambda x: ', '.join(x), 'Rating': 'mean', 'Genre': lambda x: ', '.join(x)})
df_movies_grouped.reset_index(inplace=True)

## MERGE MOVIES
df_merged1['year_month'] = pd.to_datetime(df_merged1['Account Created Date']).dt.to_period('M')
df_merged2 = pd.merge(df_merged1, df_movies_grouped, how='left', left_on='year_month', right_on='year_month')
df_merged2.drop('year_month', axis=1, inplace=True)

## MOVIE INFO
### NetFlix
df_merged2['total_netflix'] = 264
df_merged2['highq_pct_netflix'] = 29/264
df_merged2['highq_netflix'] = 29
### HBO
df_merged2['total_hbo'] = 173
df_merged2['highq_pct_hbo'] = 19/173
df_merged2['highq_hbo'] = 19
### Prime
df_merged2['total_prime'] = 777
df_merged2['highq_pct_prime'] = 45/777
df_merged2['highq_prime'] = 45

df_merged2.to_csv("combination/combined_result.tsv", sep="\t", index=False)