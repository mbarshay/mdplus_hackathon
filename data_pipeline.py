from datetime import datetime

import pandas as pd
import csv
import sys
import os


# Fun edge cases:
# (a) 10000032 -> have an ED d/c followed by immediate re-admission both linked to the same admission ID
# (b) what does it mean to have a hospital admission id with dispo to home?
# 29079034.0  32952584  2180-07-22 16:24:00  2180-07-23 05:54:00      F                     WHITE         AMBULANCE                     HOME
# (c) Of note, the stay_id seems odd and not fully reliable in the sense that numbers don't increment in the way I had wanted them to 
# (d) about 600 pts have hadm_id repeated which is just kinda interesting .... connects to the point (a) - might be worth dropping entirely
# (e) what are the relevant dx codes we want to include

base_ed_dir = "/Users/meilakhbarshay/Downloads/mimic-iv-ed-2.2/ed/"

dx_filename = "diagnosis.csv"
ed_stays_filename = "edstays.csv"
output_filename = "output.csv"

dx_path = os.path.join(base_ed_dir, dx_filename)
ed_stays_path = os.path.join(base_ed_dir, ed_stays_filename)
output_path = os.path.join(base_ed_dir, output_filename)

base_icu_dir = "/Users/meilakhbarshay/Downloads/mimic-iv-2.2/icu"
icu_stays_filename = "icustays.csv"
icu_stays_path = os.path.join(base_icu_dir, icu_stays_filename)



mimic_date_format = "%Y-%m-%d %H:%M:%S"
num_days_revisit_window = 30
relevant_dx_for_visits = []
DISPO_EXPIRED = 'EXPIRED'


df_dx = pd.read_csv(dx_path)
## for further performance considerations, may be worth merging the dataframes up-front and reducing data volume (in terms of cols)
df_primary_dx = df_dx[df_dx['seq_num'] == 1]


df_ed_stays = pd.read_csv(ed_stays_path)
# Explicitly sort the dataframe columns by subject_id (aka patient), then time of arrival, and finally stay_id as possible
df_ed_stays = df_ed_stays.sort_values(by=['subject_id', 'intime', 'stay_id'], ascending=[True, True, True])
df_ed_stays = df_ed_stays.reset_index(drop=True)
df_ed_stays['visit_num'] = df_ed_stays.groupby('subject_id').cumcount() + 1

df_ed_stays_by_pt = df_ed_stays.groupby('subject_id').count().reset_index().set_index('subject_id')

# print(df_ed_stays.head(5))

master_dict = {}
for index, row in df_ed_stays.iterrows():
	current_pt = row['subject_id']
	# print(current_pt)
	current_ed_visit = row['stay_id']

	current_visit_date = datetime.strptime(row['intime'], mimic_date_format)

	next_index = index + 1
	if (next_index < len(df_ed_stays)):
		next_row = df_ed_stays.loc[next_index]

		## number of subequent ED encounters within N days
		num_subseq_ed_encounters = 0 

		## number of total visits 
		specific_subject_id = current_pt
		num_total_visits = df_ed_stays_by_pt.loc[specific_subject_id]['stay_id']

		## encounter # for that patient 
		encounter_num = row['visit_num']

		## primary dx code 
		relevant_dx_row = df_primary_dx[(df_primary_dx['stay_id'] == current_ed_visit)]
		if not relevant_dx_row.empty:
			primary_icd_code = relevant_dx_row['icd_code'].iloc[0]
			primary_icd_version = relevant_dx_row['icd_version'].iloc[0]
			primary_icd_title = relevant_dx_row['icd_title'].iloc[0]
		else:
			primary_icd_code = ''
			primary_icd_version = ''
			primary_icd_title = ''

		## death within any visit 
		death_subseq_ed_encounters = False
		## cost of all "n" day encounters
		## # of ICU admissions within "n" day
		
		while (next_row['subject_id'] == current_pt):
			print(next_row['subject_id'])
			next_visit_date = datetime.strptime(next_row['intime'], mimic_date_format)
			time_delta = next_visit_date - current_visit_date
			if (time_delta.days <= num_days_revisit_window):
				num_subseq_ed_encounters += 1 
				if (next_row['disposition'] == DISPO_EXPIRED):
					death_subseq_ed_encounters = True
			else:
				break
			next_index += 1 
			next_row = df_ed_stays.loc[next_index]	

		master_dict[current_ed_visit] = {
			'subject_id' : current_pt, 
			'hadm_id' : row['hadm_id'],
			'num_subseq_ed_encounters' : num_subseq_ed_encounters,
			'num_total_visits' : num_total_visits,
			'encounter_num' : encounter_num,
			'primary_icd_code': primary_icd_code,
			'primary_icd_version' : primary_icd_version,
			'primary_icd_title' : primary_icd_title,
			'death_subseq_ed_encounters' : death_subseq_ed_encounters
		}
	# if index > 100:
	# 	break
	# sys.exit(0)


df = pd.DataFrame(master_dict).T  
df.index.name = 'stay_id'
df.to_csv(output_path)
