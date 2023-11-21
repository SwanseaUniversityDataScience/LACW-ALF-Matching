# Introduction
Within the Children Looked After (CLA) dataset only 37% of the children have an Anonymised Linkage Field (ALF). This prevents researchers linking to other datasets, and as a result, limits potential research. There are also other known data issues, including discrepancies with the week of births, duplicate identifiers and year-on-year changes in identifiers. 

# Objectives
To improve accuracy and availability of the Anonymised Linkage Fields (ALFs) in the CLA dataset in order to improve overall research quality.
Methods: Utilising several datasets within the Secure Anonymised Information Linkage (SAIL), we developed a two-stage CLA matching algorithm to improve the ALF matching rate and correct for any data errors. In order to determine our algorithms performance, we benchmarked against ALFs already identified via the algorithm currently used by SAIL.

# Results
Our algorithm increased ALF matching by 25%, assigning 62% of individuals an ALF. Inconsistent weeks of birth, incorrect and duplicate identifiers were resolved. When benchmarking against the current ALF assigning algorithm used by SAIL, 90% of individuals ALFs were correctly replicated, with only 1% mis-matched. The remaining 9% could not be allocated an ALF via our algorithm. 

# Conclusion
We have developed an algorithm which demonstrates comparable ALF matching performance to the current algorithm used within SAIL, as well as greatly improving the ALF matching in the CLA dataset. This algorithm has the potential to overcome potential bias in missing data, as well as enabling increasing the opportunity to link other datasets. Further development and refinement could result in the algorithm being applied to other datasets in SAIL.

# How to Use 
1. Use the requirements.txt file to install all neccessary packages
2. Open the table_inventory.json file in the helper_files folder and change the dates on the created tables
3. Open the lac-alf matching.py file and on line 17 enter your username and password to connect to DB2 
4. On line 20 change the schema to where you want the tables to be saved 
5. Save the file and then run
