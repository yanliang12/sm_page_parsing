####job_download.sh####
while true; do
   python3 bayt_download.py 
   python3 indeed_download.py
   sleep $[60 * 60]
done
####job_download.sh####
