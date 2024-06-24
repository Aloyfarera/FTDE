import math
import pandas as pd
import requests
import time

# buat save semua dict
all_data_list = []

# define chunk buat save setiap 100 ability id
chunk_size = 100

for id_ in range(1, 1000):

    BASE_URL = f"https://pokeapi.co/api/v2/ability/{id_}"
    print(BASE_URL)
    
    # error handling koneksi
    try:
        for ulang in range(5):
            response = requests.get(BASE_URL)
            break
    except:
        print(f"Retry {ulang}")
        time.sleep(5) 
    
    # parsing 'effect_entries'
    try:
        all_data = response.json()['effect_entries']
    except:
        print("efek kosong ?")
        continue  # Skip jika 'effect_entries' kosong di API
    
    for data in all_data:
        if data == []:
            continue
        
        effect = data['effect']
        short_effect = data['short_effect']
        language = data['language']
        pk_ability_id = id_

        # simpan ke sebuah dict
        data_dict = {
            'pokemon_ability_id': pk_ability_id,
            'short_effect': short_effect,
            'language': str(language),
            'effect': " ".join(effect.split())  # hapus whitespace \t,\n etc
        }
        
        # append dict ke list
        all_data_list.append(data_dict)

    # jadikan csv setiap chunk_size = 100 ability id
    if id_ % chunk_size == 0:
        df = pd.DataFrame(all_data_list)
        df = df.drop_duplicates()  
        start_id = (math.ceil(id_ / 100) - 1) * 100 + 1  # buat penamaan filename
        end_id = id_  # End ID untuk filename
        df.to_csv(f"csv/result_{start_id}_{end_id}.csv", index=False)  
        all_data_list = []  # Reset listnya per-100 ablity id

if all_data_list:
    df = pd.DataFrame(all_data_list)
    df = df.drop_duplicates()  
    start_id = (math.ceil(id_ / 100) - 1) * 100 + 1 
    end_id = id_ + 1  
    df.to_csv(f"csv/result_{start_id}_{end_id}.csv", index=False)  
