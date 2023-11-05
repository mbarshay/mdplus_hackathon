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
# (f) are we missing any direct admits? 
# (g) related to the above, there are many rows with a hadm_id in hosp that do NOT exist in the ed_stays, suggesting either lots of direct admits or
#     something else skewing the data:
#	  There are 431231 total rows and this is the breakdown of the admission location (aka where admitted from)
# admission_location
# EMERGENCY ROOM                            232595
# PHYSICIAN REFERRAL                        114963
# TRANSFER FROM HOSPITAL                     35974
# WALK-IN/SELF REFERRAL                      15816
# CLINIC REFERRAL                            10008
# PROCEDURE SITE                              7804
# PACU                                        5479
# INTERNAL TRANSFER TO OR FROM PSYCH          4205
# TRANSFER FROM SKILLED NURSING FACILITY      3843
# INFORMATION NOT AVAILABLE                    359
# AMBULATORY SURGERY TRANSFER                  185
# (h) was the best way to figure out if a subsequent visit went to the icu by just looking at the hadm_id associated with the ED encounter and looking it up in the 
#     icu stays table? I think so but ... 


# Constants
mimic_date_format = "%Y-%m-%d %H:%M:%S"
num_days_revisit_window = 30
relevant_dx_for_visits = []
DISPO_EXPIRED = 'EXPIRED'




base_ed_dir = "/Users/meilakhbarshay/Downloads/mimic-iv-ed-2.2/ed/"
base_icu_dir = "/Users/meilakhbarshay/Downloads/mimic-iv-2.2/icu"
base_hosp_dir = "/Users/meilakhbarshay/Downloads/mimic-iv-2.2/hosp"


ed_dx_filename = "diagnosis.csv"
ed_stays_filename = "edstays.csv"
ed_med_rec_filename = "medrecon.csv"

ed_dx_path = os.path.join(base_ed_dir, ed_dx_filename)
ed_stays_path = os.path.join(base_ed_dir, ed_stays_filename)
ed_med_rec_path = os.path.join(base_ed_dir, ed_med_rec_filename)

icu_stays_filename = "icustays.csv"
icu_stays_path = os.path.join(base_icu_dir, icu_stays_filename)

hosp_admissions_filename = "admissions.csv"
hosp_admissions_path = os.path.join(base_hosp_dir, hosp_admissions_filename)


# Generate Pandas Dataframes
ed_df_dx = pd.read_csv(ed_dx_path)
## for further performance considerations, may be worth merging the dataframes up-front and reducing data volume (in terms of cols)
ed_df_primary_dx = ed_df_dx[ed_df_dx['seq_num'] == 1]


df_ed_stays = pd.read_csv(ed_stays_path)
# Explicitly sort the dataframe columns by subject_id (aka patient), then time of arrival, and finally stay_id as possible
df_ed_stays = df_ed_stays.sort_values(by=['subject_id', 'intime', 'stay_id'], ascending=[True, True, True])
df_ed_stays = df_ed_stays.reset_index(drop=True)
# Line below should generate the index of that ED visit for that patient, based on the intime (aka first vs second visit)
df_ed_stays['visit_num'] = df_ed_stays.groupby('subject_id').cumcount() + 1
df_ed_stays_by_pt = df_ed_stays.groupby('subject_id').count().reset_index().set_index('subject_id')
df_ed_stays['hadm_id'] = pd.to_numeric(df_ed_stays['hadm_id'], errors='coerce', downcast='integer')

df_ed_med_rec = pd.read_csv(ed_med_rec_path)
df_ed_med_rec_by_ed_stay = df_ed_med_rec.groupby('stay_id').count().reset_index().set_index('stay_id')


hosp_admissions = pd.read_csv(hosp_admissions_path)

icu_stays = pd.read_csv(icu_stays_path)


def look_up_admission_by_hadm(hadm_id, admission_df):
	relevant_hadm_row = admission_df[admission_df['hadm_id'] == hadm_id]
	return relevant_hadm_row

print("Start time is: ", datetime.now())

master_dict = {}
for index, row in df_ed_stays.iterrows():
	
	current_pt = row['subject_id']
	current_pt_ed_gender = row['gender']
	current_pt_ed_race = row['race']
	current_pt_ed_hadm_id = row['hadm_id']

	current_pt_adm_death_time = ''
	current_pt_adm_insurance = ''
	current_pt_adm_language = ''
	current_pt_adm_marital_status = ''
	current_pt_adm_race = ''

	if index % 300 == 0:
		print(current_pt)

	if not pd.isna(current_pt_ed_hadm_id):
		current_pt_ed_hadm_id = int(current_pt_ed_hadm_id)

		current_pt_hosp_admissions_row = look_up_admission_by_hadm(row['hadm_id'],hosp_admissions)
		current_pt_adm_death_time = current_pt_hosp_admissions_row['deathtime'].iloc[0]
		current_pt_adm_insurance = current_pt_hosp_admissions_row['insurance'].iloc[0]
		current_pt_adm_language = current_pt_hosp_admissions_row['language'].iloc[0]
		current_pt_adm_marital_status = current_pt_hosp_admissions_row['marital_status'].iloc[0]
		current_pt_adm_race = current_pt_hosp_admissions_row['race'].iloc[0]


	# print(current_pt)
	current_ed_visit = row['stay_id']

	current_visit_date = datetime.strptime(row['intime'], mimic_date_format)

	next_index = index + 1
	if (next_index < len(df_ed_stays)):
		next_row = df_ed_stays.loc[next_index]

		## number of total meds on file 
		ed_num_meds = 0 
		if current_ed_visit in df_ed_med_rec_by_ed_stay.index:
		    ed_num_meds = df_ed_med_rec_by_ed_stay.loc[current_ed_visit]['name']

		## number of subequent ED encounters within N days
		num_subseq_ed_encounters = 0 

		## number of total visits 
		specific_subject_id = current_pt
		num_total_visits = df_ed_stays_by_pt.loc[specific_subject_id]['stay_id']

		## encounter # for that patient 
		encounter_num = row['visit_num']

		## primary dx code 
		relevant_dx_row = ed_df_primary_dx[(ed_df_primary_dx['stay_id'] == current_ed_visit)]
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
		num_subseq_icu_admissions = 0
		
		while (next_row['subject_id'] == current_pt):
			next_visit_date = datetime.strptime(next_row['intime'], mimic_date_format)
			time_delta = next_visit_date - current_visit_date
			if (time_delta.days <= num_days_revisit_window):
				num_subseq_ed_encounters += 1 
				if (next_row['disposition'] == DISPO_EXPIRED):
					death_subseq_ed_encounters = True
				if not pd.isna(next_row['hadm_id']):
					next_row_hadm_id = int(next_row['hadm_id'])
					next_row_icu_admissions_row = look_up_admission_by_hadm(next_row_hadm_id,icu_stays)
					if not next_row_icu_admissions_row.empty:
						num_subseq_icu_admissions += 1
			else:
				break
			next_index += 1 
			next_row = df_ed_stays.loc[next_index]	

		master_dict[current_ed_visit] = {
			'subject_id' : current_pt, 
			'hadm_id' : current_pt_ed_hadm_id,
			'ed_encounter_num' : encounter_num,
			'ed_primary_icd_code': primary_icd_code,
			'ed_primary_icd_version' : primary_icd_version,
			'ed_primary_icd_title' : primary_icd_title,
			'ed_num_meds' : ed_num_meds,
			'ed_gender' : current_pt_ed_gender,
			'ed_race' : current_pt_ed_race,
			'hosp_adm_death_time' : current_pt_adm_death_time,
			'hosp_adm_insurance' : current_pt_adm_insurance,
			'hosp_adm_language' : current_pt_adm_language,
			'hosp_adm_marital_status' : current_pt_adm_marital_status,
			'hosp_adm_race' : current_pt_adm_race, 
			'death_subseq_ed_encounters' : death_subseq_ed_encounters,
			'num_subseq_ed_encounters' : num_subseq_ed_encounters,
			'num_total_visits' : num_total_visits,
			'num_subseq_icu_admissions' : num_subseq_icu_admissions, 
		}

print("End time is: ", datetime.now())


output_filename = "output.csv"
output_path = os.path.join(base_ed_dir, output_filename)

df = pd.DataFrame(master_dict).T  
df.index.name = 'stay_id'
df.to_csv(output_path)
