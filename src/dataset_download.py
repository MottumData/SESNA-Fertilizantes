import requests
from src import scrape_urls

def download_datasets(urls, destination_folder, progress_callback):
    failed_urls = []
    good_urls = []
    total_urls = len(urls)

    for i, url in enumerate(urls):
        response = requests.get(url)
        if response.status_code == 200:
            # Create a file path based on the URL
            filename = url.split("/")[-1]
            destination_path = f"{destination_folder}/{filename}"
            with open(destination_path, 'wb') as file:
                file.write(response.content)
            good_urls.append(url)
        else:
            failed_urls.append(url)

        # Calculate and update progress after each URL is processed
        current_progress = (i + 1) / total_urls
        progress_callback(current_progress)

    # Return results
    return {
        'good_count': len(good_urls),
        'good_urls': good_urls,
        'failed_count': len(failed_urls),
        'failed_urls': failed_urls
    }
# Example usage:
'''
download_urls = []
url = "https://www.datos.gob.mx/busca/dataset/programa-de-fertilizantes-2023-listados-autorizados"
urls = scrape_urls.scrape_urls(url)
for url in urls:
    download_urls.append(url)

download_destination_folder = "data/productores_autorizados"
download_datasets(download_urls, download_destination_folder)
'''