import re
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

def clean_text_after_parsing(text):
    # Replace <br/> with a unique separator (e.g., '|||END|||')
    text = re.sub(r'</p>', '<br/></p>', text)
    text = re.sub(r'<br\s*/?>', '|||END|||', text)
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def process_file(file_name):
    result = []
    if file_name.endswith('html'):
        with open(os.path.join('dump', file_name), 'r', encoding='utf-8') as file:                                  # Location of the html files
            content = file.read()
            
            #####################################
            content = clean_text_after_parsing(content)
            #####################################

            # Use BeautifulSoup to parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')
            text = soup.get_text()

            # Apply the regex for name extraction, considering only lines that match
            name_pattern = re.compile(r"(\d+)\s+([A-ZÑ\s-]+),\s+([A-ZÑ]+\.?\s+[A-ZÑ\s-]+[A-ZÑ])")
            
            matches = name_pattern.findall(text)
            formatted_names = []

            for num, last, first_middle in matches:
                first_middle = re.sub(r'\s+', ' ', first_middle.strip()).upper().replace('NOTHING FOLLOWS', '')
                formatted_names.append(f"{num} | {last.upper()} | {first_middle}")
            
            result = (file_name, formatted_names)
    
    return result

def save_results(results):
    with open('../data/ext_5.csv', 'a+') as f:                                                                      # Location of the output file
        for file_name, names in results:
            for name in names:
                print(name)
                f.write(name + '\n')

def extract_names_from_html_files():
    files = os.listdir('dump')                                                                                      # Location of the html files
    results = []

    # Using ThreadPoolExecutor for multithreading
    with ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        future_to_file = {executor.submit(process_file, file_name): file_name for file_name in files}
        
        # Collect results as they are completed
        for future in as_completed(future_to_file):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as exc:
                print(f'{future_to_file[future]} generated an exception: {exc}')
    
    # Save the results after all threads have completed
    save_results(results)

# Example usage:
if __name__ == '__main__':
    extract_names_from_html_files()
