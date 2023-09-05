import requests
import pandas as pd

df = pd.read_csv('pixstory_dataset_v2.tsv', sep='\t')

for i, row in df.iloc[:].iterrows():
    url = row['Media']
    base_url, filename = url.rsplit('/', 1)
    base_url = base_url + "/optimized"
    new_url = base_url + "/" + filename
    response = requests.get(new_url)
    with open(f'images/{i+1}.png', 'wb') as f:
        f.write(response.content)
