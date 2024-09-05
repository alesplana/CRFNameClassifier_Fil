import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

df = pd.read_csv('ext_5.csv', sep='|', header=None)
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

surnames_df = df[1].drop_duplicates().copy()

# List of suffixes and surname indicators
suffixes = {'JR', 'III', 'IV', 'V', 'VI', 'VII', 'VIII'}
surname_indicators = {'DE LA', 'DELA', 'DEL', 'SAN'}

def process_name(name):
    words = name.split()
    n_words = len(words)
    is_surname_in_list = False

    # Rule 2: Handle single-word case (automatically true)
    if n_words == 1:
        return words[0], np.nan, True

    # Rule 1: Check for suffix in the middle of the string
    for i in range(n_words - 1):
        if words[i] in suffixes:
            potential_middle = ' '.join(words[i+1:])
            if potential_middle in surnames_df.values:
                is_surname_in_list = True
            return ' '.join(words[:i+1]), potential_middle, is_surname_in_list

    # Rule 3: Check for surname indicators
    for i in range(1, n_words - 1):
        if f"{words[i-1]} {words[i]}" in surname_indicators:
            potential_middle = ' '.join(words[i-1:])
            if potential_middle in surnames_df.values:
                is_surname_in_list = True
            return ' '.join(words[:i-1]), potential_middle, is_surname_in_list

    # Optimized Rule 4: Loop through potential surname segments
    for i in range(1, n_words):
        potential_surname = ' '.join(words[i:])
        if potential_surname in surnames_df.values:
            is_surname_in_list = True
            return ' '.join(words[:i]), potential_surname, is_surname_in_list

    # Additional Logic: Handle case where the last word is a single character
    last_word = words[-1]
    if len(last_word) == 1:
        return ' '.join(words[:-1]), np.nan, False

    # Handle cases where B is initially null
    # Check if the last word minus last character is a surname
    if n_words > 1 and len(last_word) > 1:
        potential_surname = last_word[:-1]
        if potential_surname in surnames_df.values:
            is_surname_in_list = True
            return ' '.join(words[:-1]), potential_surname, is_surname_in_list

    # If no conditions matched, return the name as the first name, B is NaN
    return name, np.nan, is_surname_in_list

# Step 2: Multi-threaded application of the process_name function
def apply_multithreaded(df):
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(process_name, df[2]))  # Adjust the column index if necessary

    # Unpacking the results into DataFrame columns
    df['A'], df['B'], df['is_surname_in_list'] = zip(*results)
    df['A'] = df['A'].str.strip()
    df['B'] = df['B'].str.strip()
    return df

# Apply the processing function in a multi-threaded manner
df = apply_multithreaded(df)

df.to_csv('clean_x.csv', index=False, sep='|')