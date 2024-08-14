from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import os

# Get the absolute path of the data directory
data_dir = os.path.join(os.getcwd(), 'data')

chrome_options = Options()
chrome_options.add_experimental_option('prefs', {
    "download.default_directory": data_dir, # Change default directory for downloads
    "download.prompt_for_download": False, # To auto download the file
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True # To download PDFs
})

# Setup webdriver
s = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=s, options=chrome_options)

try:
    # Navigate to the page
    driver.get('https://www.inegi.org.mx/app/ageeml/#')
    
    driver.maximize_window()

    # Wait for the first element to be clickable and click it
    first_element = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div[1]/div[2]/div[3]/div/div/div[4]/div[3]/div[2]/div[2]/div/button'))
    )
    first_element.click()

    second_element = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div[1]/div[2]/div[3]/div/div/div[4]/div[3]/div[2]/div[2]/div/ul/li[3]/p/a[2]/span'))
    )
    second_element.click()

    # Print the second element
    print(second_element.text)
    # Wait for the download to complete
    while True:
        time.sleep(1)
        if not any(fname.endswith('.crdownload') for fname in os.listdir(data_dir)):
            break

    time.sleep(10)

finally:
    # Close the driver
    driver.quit()

# TODO: Revisión para descargar los datos de INEGI