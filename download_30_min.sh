####download_one_min.sh####
while true; do
   python3 gulfnews_download.py
   python3 khaleejtimes_download.py
   python3 thenationalnews_download.py
   ###
   python3 bayt_download.py 
   python3 indeed_download.py
   ###
   python3 dubizzle_download.py
   sleep $[30 * 60]
done
####download_one_min.sh####
