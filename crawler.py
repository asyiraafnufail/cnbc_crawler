import requests
from bs4 import BeautifulSoup
import json
import os
import re
from datetime import datetime
from pymongo import MongoClient

MONGO_URI = "mongodb+srv://Rafa:123@cluster1.hbzhgmr.mongodb.net/?appName=Cluster1"
DB_NAME = "ucp1_crawling_cnbc_news"
COLLECTION_NAME = "ucp1"

def crawl_cnbc_environment():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    
    # Koneksi ke MongoDB Atlas
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print("Koneksi ke MongoDB Atlas Berhasil!")
    except Exception as e:
        print(f"Gagal koneksi ke MongoDB: {e}")
        return

    search_url = "https://www.cnbcindonesia.com/tag/sustainability"
    print(f"Mencari artikel di: {search_url}")
    
    try:
        response = requests.get(search_url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        article_urls = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if "cnbcindonesia.com" in href and re.search(r'/\d{6,}', href):
                if href not in article_urls:
                    article_urls.append(href)

        if not article_urls:
            print("Link artikel tidak ditemukan.")
            return

        print(f"Menemukan {len(article_urls)} link. Memproses 5 artikel terbaru...")

        for url in article_urls[:5]:
            try:
                res = requests.get(url, headers=headers, timeout=10)
                article_soup = BeautifulSoup(res.text, 'html.parser')

                # Ekstraksi Data (Meta Tags)
                meta_title = article_soup.find('meta', property='og:title')
                judul = meta_title['content'] if meta_title else "Judul tidak ditemukan"

                meta_date = article_soup.find('meta', attrs={'name': 'publishdate'}) or article_soup.find('meta', attrs={'name': 'dtk:publishdate'})
                tanggal = meta_date['content'] if meta_date else "Tanggal tidak ditemukan"

                meta_author = article_soup.find('meta', attrs={'name': 'author'})
                author = meta_author['content'] if meta_author else "Author tidak ditemukan"

                meta_img = article_soup.find('meta', property='og:image')
                thumbnail = meta_img['content'] if meta_img else "Thumbnail tidak ditemukan"

                content_div = article_soup.find('div', class_='detail_text')
                isi_berita = ""
                if content_div:
                    paragraphs = content_div.find_all('p')
                    isi_berita = " ".join([p.text.strip() for p in paragraphs if p.text.strip() != ""])
                
                # Menyiapkan dokumen untuk MongoDB
                document = {
                    "url": url,
                    "judul": judul,
                    "tanggal_publish": tanggal,
                    "author": author,
                    "tag_kategori": "Environmental Sustainability",
                    "isi_berita": isi_berita[:1500], # Mengambil lebih banyak teks untuk database
                    "thumbnail": thumbnail,
                    "updated_at": datetime.now()
                }

                # PROSES UPSERT: Gunakan URL sebagai kunci unik
                # Jika URL sudah ada, data akan diupdate. Jika belum, data baru akan ditambah.
                collection.update_one(
                    {"url": url}, 
                    {"$set": document}, 
                    upsert=True
                )
                print(f" -> Berhasil Simpan/Update: {judul[:40]}...")

            except Exception as e:
                print(f" -> Gagal memproses {url}: {e}")

        print(f"\nSemua data berhasil disinkronisasi ke MongoDB Atlas!")

    except Exception as e:
        print(f"Terjadi kesalahan: {e}")

if __name__ == "__main__":
    crawl_cnbc_environment()