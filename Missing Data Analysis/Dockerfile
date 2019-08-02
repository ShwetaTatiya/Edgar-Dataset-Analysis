FROM python

#Code Source
COPY edgar_scraping_year.py /edgar_scraping_year.py

Run pip3 install pandas
Run pip3 install boto
Run pip3 install requests
Run pip3 install bs4

CMD [ "python", "./edgar_scraping_year.py" ]

ENTRYPOINT  ["python","./edgar_scraping_year.py"]

CMD myinput