
# Graph Network Backcloth Visualizer

This work is part of a research project for Summer 2023
This is an extension of the work carried out as a part of the MITACS Globalink Research Internship 2022 @ 
British Columbia, Canada.

The project aims to simulate the street networks of the city of Vancouver, and 
derive research results based on the different factors that influence the 
susceptibility of a street segment or junction to crime.

## Tech Stack

**Frontend:** GraphXR

**Backend:** Neo4j Database

**Language used for scripts:** Python
## Screenshots

![App Screenshot](https://i.imgur.com/WMHF2JN.png)

![App Screenshot](https://i.imgur.com/ISKksp2.png)

![App Screenshot](https://i.imgur.com/X6tyhVn.png)

## Setup

In order to setup the project on a new system:
1. Create a Neo4j account and database making sure to download the file that includes the database password
2. Change DATABASE_INFO_FILEPATH in driver.py to point to the downloaded database info file
3. From the data_loading folder run pip install -r requirements.txt
4. From the data_loading folder run python driver.py to load the data into the database. This could take a while.