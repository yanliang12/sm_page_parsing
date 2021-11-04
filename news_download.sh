####news_download.sh####
while true; do
   python3 thenationalnews_download.py
   python3 gulfnews_download.py
   python3 khaleejtimes_download.py
   sleep $[60 * 60]
done
####news_download.sh####
