import requests
from bs4 import BeautifulSoup
from datetime import datetime

BASE_URL = "https://www.storia.ro/ro/rezultate/vanzare/apartament/iasi/iasi"
NUMAR_PAGINI = 80
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,ro;q=0.8"
}

def scrape_page(pagina: int) -> list[dict]:
    """
    Extrage anunturile brute de pe o pagina de pe Storia
    """
    url = f"{BASE_URL}?page={pagina}"
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code != 200:
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    anunturi = soup.find_all('article')
    
    results = []
    for card in anunturi:
        titlu_tag = card.find('h3') or card.find('span', {'data-cy': 'listing-item-title'})
        link_tag  = card.find('a', href=True)
        
        pret_raw = "N/A"
        for span in card.find_all('span'):
            if '€' in span.get_text(strip=True):
                pret_raw = span.get_text(strip=True)
                break
        
        results.append({
            "title_raw":       titlu_tag.get_text(strip=True) if titlu_tag else None,
            "price_raw":       pret_raw,
            "description_raw": card.get_text(separator=' | '),
            "url":             "https://www.storia.ro" + link_tag['href'] if link_tag else None,
            "scraped_at":      datetime.utcnow(),
        })
    
    return results
