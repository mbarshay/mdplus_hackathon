# Medicure Misfits MD++ Repo
This repository contains all of the working code that we developed for the MD+ Hackathon. Just to orient visitors to the layout of this repository:

## Code Files
### data_pipeline.py

This is the main dataframe generation file which contains the logic for transforming mimic data. In simple terms, it used the EDstays file in the ED module to generate a range of data based on ED visits, including:
* demographics (e.g. insurance, gender, race, primary language, marital status) 
* ED visit metadata (e.g. index of visit, primary ICD, number of home meds)
* hospitalization data (e.g. death time, length of stay)
* aggregation data (e.g. total length of stay for subsequent ED visits/hospitalizations, total cost for subsequent ED visits/hospitalizations, total number of visits, was the patient ever on HTN medications)

### readabilityAnalysis.ipynb

This jupyter notebook was used to run the readability analysis. It would filter by patients who underwent any type of cholesystectomy or appendectomy (open, percutaneous, etc...), retrieved those specific discharge notes, and filtered for the patient instructions within the discharge note. Flesch-kincaid score was then calculated. For these patients, we analyzed patients who came back to the hospital within 30 days. We did a subanalysis of patients who came in with surgical site infections within 30 days. 

## Data Files 
### MIMIC Data Transformation Outputs

This folder contains the different transformation outputs that were done to the original MIMIC data files, including the hydration of data from publicly available sources of payor price per DRG code.
Different versions reflect changes to what outputs were tracked/produced. 

### Blood Thinner Cost Savings Preparation Work

This folder contains all of the preparation work required in order to identify visits that meet adverse blood events (based both on ICD codes as well as DRGs) as well as drg price data. 

### Readability Outputs

This folder contains the results of the readability analysis. Of note for this specific analysis, it includes the flesch-kincaid score, insurance, language, and race.
