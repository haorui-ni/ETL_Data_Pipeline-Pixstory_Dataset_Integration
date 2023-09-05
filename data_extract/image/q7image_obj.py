import os
import requests
from PIL import Image
import pandas as pd
import io

TIKA_URL = 'http://localhost:8764/inception/v4/classify/image'

input_dir = 'image10'
#output_file = 'output_8.csv'

topn = 2
min_confidence = 0.03

results = []

for i, filename in enumerate(sorted(os.listdir(input_dir))):
    if filename.endswith('.jpg') or filename.endswith('.jpeg') or filename.endswith('.png'):
        print(filename)

        file_path = os.path.join(input_dir, filename)
        
        img = Image.open(file_path)
        
        img_bytes = io.BytesIO()
        img.save(img_bytes, format='JPEG')
        img_bytes = img_bytes.getvalue()
        
        try:
            response = requests.post(TIKA_URL, params={'topn': topn, 'min_confidence': min_confidence}, data=img_bytes)
            data = response.json()
            classnames = data['classnames']
            print(classnames)
        except:
            classnames = ''
        
        results.append({'filename': filename, 'Object Recognition': classnames})
# print(results)
results = sorted(results, key=lambda x: int(x['filename'].split('.')[0]))
'''df = pd.read_csv(output_file)
df.insert(loc= len(df.columns), column='Object Recognition', value=[result['Object Recognition'] for result in results])
df.to_csv(output_file, index=False)
# Write the results to a CSV file using pandas'''
df_2 = pd.DataFrame(results)
df_2.to_csv("output_10_obj.csv", index=False)
