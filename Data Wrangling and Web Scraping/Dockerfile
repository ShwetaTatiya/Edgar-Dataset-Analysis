FROM python

#Code Source
COPY edgar_scraping.py /edgar_scraping.py

Run pip3 install pandas
Run pip3 install boto
Run pip3 install requests
Run pip3 install bs4

CMD [ "python", "./edgar_scraping.py" ]

ENTRYPOINT  ["python","./edgar_scraping.py"]

CMD myinput