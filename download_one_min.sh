####download_one_min.sh####
while true; do
   python3 opensooq_download.py
   python3 bayut_download.py
   python3 propertyfinder_download.py
   sleep $[1 * 60]
done
####download_one_min.sh####
