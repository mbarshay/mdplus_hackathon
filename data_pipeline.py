from datetime import datetime
import pandas as pd
import csv
import sys
import os
import re

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

ELIQUIS_MED = 'Eliquis'
WARFARIN_MED = 'Warfarin'
APIXABAN_MED = 'apixaban'
RIVAROXABAN_MED = 'rivaroxaban'
XARELTO_MED = 'Xarelto'
SAVAYSA_MED = 'Savaysa'
EDOXABAN_MED = 'Edoxaban'

MAX_SBP = 139

BLOOD_THINNER_ANALYSIS_ELIGIBLE = 'Blood Thinner Analysis Eligible'
BLOOD_THINNER_CATEGORY = 'Blood Thinner Category'

output_dir = "/Users/meilakhbarshay/Documents/mdplus_hackathon/"

base_ed_dir = "/Users/meilakhbarshay/Downloads/mimic-iv-ed-2.2/ed/"
base_icu_dir = "/Users/meilakhbarshay/Downloads/mimic-iv-2.2/icu"
base_hosp_dir = "/Users/meilakhbarshay/Downloads/mimic-iv-2.2/hosp"


ed_dx_filename = "diagnosis.csv"
ed_stays_filename = "edstays.csv"
ed_med_rec_filename = "medrecon.csv"
ed_vital_sgins_filename = "vitalsign.csv"

ed_dx_path = os.path.join(base_ed_dir, ed_dx_filename)
ed_stays_path = os.path.join(base_ed_dir, ed_stays_filename)
ed_med_rec_path = os.path.join(base_ed_dir, ed_med_rec_filename)
ed_vital_sgins_path = os.path.join(base_ed_dir, ed_vital_sgins_filename)

icu_stays_filename = "icustays.csv"
icu_stays_path = os.path.join(base_icu_dir, icu_stays_filename)

hosp_admissions_filename = "admissions.csv"
hosp_admissions_path = os.path.join(base_hosp_dir, hosp_admissions_filename)

hosp_drgcodes_filename = "drgcodes.csv"
hosp_drgcodes_path = os.path.join(base_hosp_dir, hosp_drgcodes_filename)

hosp_patients_filename = 'patients.csv'
hosp_patients_path = os.path.join(base_hosp_dir, hosp_patients_filename)

hosp_dx_filename = "diagnoses_icd.csv"
hosp_dx_path = os.path.join(base_hosp_dir, hosp_dx_filename)

hosp_dx_desc_filename = "d_icd_diagnoses.csv"
hosp_dx_desc_path = os.path.join(base_hosp_dir, hosp_dx_desc_filename)

blood_thinner_drgs_filename = 'drgs_clean_v1_hcfa_only.csv'
blood_thinner_drgs_path = os.path.join(output_dir, blood_thinner_drgs_filename)

blood_thinner_icds_of_interest_filename = 'blood_adverse_events_icds.csv'
blood_thinner_icds_of_interest_path = os.path.join(output_dir, blood_thinner_icds_of_interest_filename)

# Generate Pandas Dataframes
ed_df_dx = pd.read_csv(ed_dx_path)
## for further performance considerations, may be worth merging the dataframes up-front and reducing data volume (in terms of cols)
ed_df_primary_dx = ed_df_dx[ed_df_dx['seq_num'] == 1]
ed_df_dx_icd10_htn = ed_df_dx[(ed_df_dx['icd_version'] == 10) & (ed_df_dx['icd_code'].str.startswith('I10'))]

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

df_ed_med_rec_htn_meds = df_ed_med_rec[((df_ed_med_rec['etcdescription'] == 'ACE Inhibitors') | 
			(df_ed_med_rec['etcdescription'] == 'Angiotensin II Receptor Blockers (ARBs)') | 
			(df_ed_med_rec['etcdescription'] == '*HTN med') | 
			(df_ed_med_rec['etcdescription'] == '*HTN medication') | (df_ed_med_rec['etcdescription'] == '*HTN meds.') | 
			(df_ed_med_rec['etcdescription'] == '*HTN') | 
			(df_ed_med_rec['etcdescription'] == '*something for HTN') | 
			(df_ed_med_rec['etcdescription'] == '*a HTN med'))]

df_ed_med_rec_htn_meds_full = df_ed_med_rec[(df_ed_med_rec['etcdescription'] == 'Alpha-Beta Blockers') | 
											(df_ed_med_rec['etcdescription'] == 'Beta Blockers Non-Cardiac Selective') | 
											(df_ed_med_rec['etcdescription'] == 'ACE Inhibitors') | 
											(df_ed_med_rec['etcdescription'] == 'Angiotensin II Receptor Blockers (ARBs)') |
											(df_ed_med_rec['etcdescription'] == 'Diuretic - Loop') |
											(df_ed_med_rec['etcdescription'] == 'Angiotensin II Receptor Blocker (ARB)-Diuretic Combinations') |
											(df_ed_med_rec['etcdescription'] == 'Diuretic - Thiazides and Related') ]


df_ed_vitals = pd.read_csv(ed_vital_sgins_path)
df_ed_vitals_non_missing = df_ed_vitals[df_ed_vitals['sbp'].notna()]
df_ed_vitals_non_missing = df_ed_vitals_non_missing.sort_values(by=['subject_id', 'stay_id', 'charttime'], ascending=[True, True, True])
df_ed_vitals_non_missing_first = df_ed_vitals_non_missing.drop_duplicates(subset='stay_id', keep='first')

hosp_admissions = pd.read_csv(hosp_admissions_path)
hosp_drgcodes = pd.read_csv(hosp_drgcodes_path)

blood_thinner_drgs = pd.read_csv(blood_thinner_drgs_path)
blood_thinner_drgs_codes = blood_thinner_drgs['drg_code'].tolist()

blood_thinner_icds_of_interest = pd.read_csv(blood_thinner_icds_of_interest_path)
blood_thinner_icds_of_interest_9 = blood_thinner_icds_of_interest['icd_9_codes'].tolist()
blood_thinner_icds_of_interest_10 = blood_thinner_icds_of_interest['icd_10_codes'].tolist()

icu_stays = pd.read_csv(icu_stays_path)

hosp_patients = pd.read_csv(hosp_patients_path)

# Initial helper method I used to try to generate which medications we wanted to consider for eliquis vs. heparin 
# results saved down in csvs that were checked into source control 

def generate_relevant_blood_thinning_medications(df_ed_med_rec):
	# isolate only the columns that actually matter for identifying drug names - codes usually correlate to doses, not to
	# core drug 

	columns_to_drop = ['subject_id', 'stay_id', 'charttime', 'ndc', 'gsn', 'etc_rn', 'etccode']
	df_ed_med_rec_blood_thiners = df_ed_med_rec.drop(columns=columns_to_drop)

	etc_pattern = '.*warfarin.*|.*Direct Factor Xa Inhibitors.*'
	name_pattern = '.*warfarin.*|.*apixaban.*|.*coumadin.*'
	filtered_df = df_ed_med_rec_blood_thiners[
						df_ed_med_rec_blood_thiners['etcdescription'].str.contains(etc_pattern, case=False, na=False, regex=True) | 
						df_ed_med_rec_blood_thiners['name'].str.contains(name_pattern, case=False, na=False, regex=True)
					]
	filtered_df = filtered_df.drop_duplicates()

	filtered_df.to_csv(os.path.join(output_dir, 'blood_thinners_v2.csv'), index=False)







def generate_patient_blood_thinner_status():
	# get the relevant blood thinning medications
	blood_thinner_meds = pd.read_csv(os.path.join(output_dir,'blood_thinners_v2_clean.csv'))
	merged_df = df_ed_med_rec.merge(blood_thinner_meds, on=['name', 'etcdescription'], how='inner')
	pt_medication_mapping = {}

	# Build the initial dictionary mapping patients -> visit -> med list (e.g eliquis vs. xarelto)
	for index, row in merged_df.iterrows():
		if row['subject_id'] not in pt_medication_mapping:
			pt_medication_mapping[row['subject_id']] = {}
		pt_medication_mapping[row['subject_id']][row['stay_id']] = []
		if row['name'] == ELIQUIS_MED or row['name'] == APIXABAN_MED:
			pt_medication_mapping[row['subject_id']][row['stay_id']].append(ELIQUIS_MED)
		elif row['name'] == RIVAROXABAN_MED or row['name'] == XARELTO_MED:
			pt_medication_mapping[row['subject_id']][row['stay_id']].append(XARELTO_MED)
		elif row['name'] == SAVAYSA_MED or row['name'] == EDOXABAN_MED:
			pt_medication_mapping[row['subject_id']][row['stay_id']].append(SAVAYSA_MED)
		else: 
			pt_medication_mapping[row['subject_id']][row['stay_id']].append(WARFARIN_MED)

	# Evaluate if patients are eligible (aka have only ever been on eliquis OR heparin across all visits)
	for subject, visit_level in pt_medication_mapping.items():
		eliquis = 0
		warfarin = 0
		xarelto = 0
		savaysa = 0
		for visit_id, med_given in visit_level.items():
			if ELIQUIS_MED in med_given:
				eliquis = 1
			if WARFARIN_MED in med_given:
				warfarin = 1
			if XARELTO_MED in med_given:
				xarelto = 1
			if SAVAYSA_MED in med_given:
				savaysa = 1
		if eliquis + warfarin + savaysa + xarelto > 1:
			pt_medication_mapping[subject][BLOOD_THINNER_ANALYSIS_ELIGIBLE] = False
		else:
			pt_medication_mapping[subject][BLOOD_THINNER_ANALYSIS_ELIGIBLE] = True
			if eliquis == 1:
				pt_medication_mapping[subject][BLOOD_THINNER_CATEGORY] = ELIQUIS_MED
			if warfarin == 1:
				pt_medication_mapping[subject][BLOOD_THINNER_CATEGORY] = WARFARIN_MED
			if xarelto == 1:
				pt_medication_mapping[subject][BLOOD_THINNER_CATEGORY] = XARELTO_MED
			if savaysa == 1:
				pt_medication_mapping[subject][BLOOD_THINNER_CATEGORY] = SAVAYSA_MED
	return pt_medication_mapping

def look_up_admission_by_hadm(hadm_id, admission_df):
	relevant_hadm_row = admission_df[admission_df['hadm_id'] == hadm_id]
	return relevant_hadm_row

print("Start time is: ", datetime.now())

pt_blood_thinner_look_up = generate_patient_blood_thinner_status()

# This helps identify the first ED visit where a patient was on a blood thinner to establish t=0
pts_assessed_for_blood_thinner = []
master_dict = {}

# Master file is based on going through every single ED visit logged
for index, row in df_ed_stays.iterrows():	
	current_pt = row['subject_id']
	current_pt_ed_gender = row['gender']
	current_pt_ed_gender_coded = 1 if current_pt_ed_gender == 'M' else 0

	pt_metadata = hosp_patients[hosp_patients['subject_id'] == current_pt]
	if (len(pt_metadata) > 1):
		print("There was more than one row for this patient!")
		sys.exit(0)

	current_pt_pt_anchor_age = ''
	current_pt_pt_anchor_year = ''
	current_pt_anchor_year_group = ''

	if (len(pt_metadata) == 1):
		pt_metadata = pt_metadata.iloc[0]
		current_pt_pt_anchor_age = pt_metadata['anchor_age']
		current_pt_pt_anchor_year = pt_metadata['anchor_year']
		current_pt_anchor_year_group = pt_metadata['anchor_year_group']

	current_pt_ed_race = row['race']
	current_pt_ed_race_coded = ''

	if current_pt_ed_race in ['WHITE', 'WHITE - RUSSIAN', 'WHITE - BRAZILIAN','PORTUGUESE', 'WHITE - OTHER EUROPEAN','WHITE - EASTERN EUROPEAN']:
	    current_pt_ed_race_coded = 0
	elif current_pt_ed_race in ['BLACK/AFRICAN AMERICAN', 'BLACK/CAPE VERDEAN','BLACK/CARIBBEAN ISLAND','BLACK/AFRICAN']:
	    current_pt_ed_race_coded = 1
	elif current_pt_ed_race in ['HISPANIC/LATINO - DOMINICAN', 'HISPANIC/LATINO - SALVADORAN','HISPANIC/LATINO - PUERTO RICAN', 
								'HISPANIC/LATINO - GUATEMALAN', 'HISPANIC OR LATINO', 'HISPANIC/LATINO - MEXICAN','HISPANIC/LATINO - COLUMBIAN',
								'SOUTH AMERICAN','HISPANIC/LATINO - CUBAN','HISPANIC/LATINO - HONDURAN','HISPANIC/LATINO - CENTRAL AMERICAN']:
	    current_pt_ed_race_coded = 2
	elif current_pt_ed_race in ['ASIAN', 'ASIAN - SOUTH EAST ASIAN', 'ASIAN - CHINESE','ASIAN - ASIAN INDIAN','ASIAN - KOREAN']:
	    current_pt_ed_race_coded = 3
	elif current_pt_ed_race in ['MULTIPLE RACE/ETHNICITY']:
		current_pt_ed_race_coded = 4
	elif current_pt_ed_race in ['AMERICAN INDIAN/ALASKA NATIVE', 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER']:
		current_pt_ed_race_coded = 5
	elif current_pt_ed_race in ['OTHER','UNKNOWN','UNABLE TO OBTAIN','PATIENT DECLINED TO ANSWER']:
		current_pt_ed_race_coded = 6
	else:
	    print("Unexpectedly, there was a race that had not been pre-categorized: ", race)
	    sys.exit(0)

	current_pt_ed_hadm_id = row['hadm_id']

	current_pt_adm_death_time = ''
	current_pt_adm_insurance = ''
	current_pt_adm_language = ''
	current_pt_adm_marital_status = ''
	current_pt_adm_race = ''

	current_ed_visit = row['stay_id']

	current_visit_date = datetime.strptime(row['intime'], mimic_date_format)

	pt_vitals = df_ed_vitals_non_missing_first[df_ed_vitals_non_missing_first['subject_id'] == current_pt]
	pt_vitals_htn = pt_vitals[pt_vitals['sbp'] > MAX_SBP]
	current_pt_ever_htn_vs = len(pt_vitals_htn) > 0

	pt_htn_meds_full = df_ed_med_rec_htn_meds_full[df_ed_med_rec_htn_meds_full['subject_id'] == current_pt]
	current_pt_ever_htn_meds_full = len(pt_htn_meds_full) > 0 

	current_pt_ever_htn_icd = len(ed_df_dx_icd10_htn[ed_df_dx_icd10_htn['subject_id'] == current_pt]) > 0
	

	## number of total meds on file for the current ED visit 
	ed_num_meds = 0 
	if current_ed_visit in df_ed_med_rec_by_ed_stay.index:
	    ed_num_meds = df_ed_med_rec_by_ed_stay.loc[current_ed_visit]['name']

	## number of total ED visits for all time for this pt [will be the same for all ED visits]
	specific_subject_id = current_pt
	num_total_visits = df_ed_stays_by_pt.loc[specific_subject_id]['stay_id']

	## ED encounter # for that patient relative to other ED visits 
	encounter_num = row['visit_num']

	## primary dx code for that ED visit 
	relevant_dx_row = ed_df_primary_dx[(ed_df_primary_dx['stay_id'] == current_ed_visit)]
	if (len(relevant_dx_row) > 1):
		print("There can not be more than primary dx for an ED visit")
		sys.exit(0)

	icd_dxes_blood_thinner_target = None

	if not relevant_dx_row.empty:
		relevant_dx_row = relevant_dx_row.iloc[0]

		primary_icd_code = relevant_dx_row['icd_code']
		primary_icd_version = relevant_dx_row['icd_version']
		primary_icd_title = relevant_dx_row['icd_title']

		primary_icd_is_icd_of_interest = False
		matching_prefixes = None

		if (primary_icd_version == 9):
			matching_prefixes = [prefix for prefix in blood_thinner_icds_of_interest_9 if primary_icd_code.startswith(str(prefix))]
		if (primary_icd_version == 10):
			matching_prefixes = [prefix for prefix in blood_thinner_icds_of_interest_10 if primary_icd_code.startswith(str(prefix))]

		if matching_prefixes:
		    primary_icd_is_icd_of_interest = True
		    icd_dxes_blood_thinner_target = matching_prefixes

	else:
		primary_icd_code = ''
		primary_icd_version = ''
		primary_icd_title = ''

	current_pt_htn_not_on_meds = False

	current_visit_vitals = df_ed_vitals_non_missing_first[df_ed_vitals_non_missing_first['stay_id'] == current_ed_visit]
	if (len(current_visit_vitals) == 1):
		current_visit_vitals = current_visit_vitals.iloc[0]
		if (current_visit_vitals['sbp'] > MAX_SBP):
			df_ed_med_rec_htn = df_ed_med_rec_htn_meds[(df_ed_med_rec_htn_meds['stay_id'] == current_ed_visit)]
			if(len(df_ed_med_rec_htn) == 0):
				current_pt_htn_not_on_meds = True

	elif len(current_visit_vitals) > 1:
		print("There was more than one vitals sign row")
		sys.exit(0)

	if index % 300 == 0:
		print(current_pt,"-",current_ed_visit)

	next_index = index + 1
	if (next_index < len(df_ed_stays)):
		next_row = df_ed_stays.loc[next_index]

		## number of subequent ED encounters within N days
		num_subseq_ed_encounters = 0 

		## death within any visit 
		death_subseq_ed_encounters = False

		## # of ICU admissions within "n" day
		num_subseq_icu_admissions = 0

		# pt was HTN and still not on meds for subsequent visit
		not_on_htn_subseq_ed_encounter = False
		not_on_htn_subseq_ed_encounter_num_visits = 0

		# pt was HTN and got on ACEs/ARBs in a subsequent visit
		on_htn_subseq_ed_encounter_num_visits = 0


		# placeholder variable that tells me if this patient AS of this visit was first on a blood thinner - this drives
		# whether downstream summation takes place
		blood_thinner = None
		blood_thinner_pt = None
		xa_inh = None

		# Logic = pt was ever on a blood thinner, this is first visit we've noted for them, and they are only ever on one type of blood thinner
		if (current_pt in pt_blood_thinner_look_up and  
			pt_blood_thinner_look_up[current_pt][BLOOD_THINNER_ANALYSIS_ELIGIBLE] == True):
			blood_thinner_pt = pt_blood_thinner_look_up[current_pt][BLOOD_THINNER_CATEGORY]
			
			# Need to also confirm that pt was on a blood thinner during THIS ED visit 
			if (current_ed_visit in pt_blood_thinner_look_up[current_pt] and current_pt not in pts_assessed_for_blood_thinner):
				pts_assessed_for_blood_thinner.append(current_pt)
				blood_thinner = pt_blood_thinner_look_up[current_pt][BLOOD_THINNER_CATEGORY]
				if blood_thinner == WARFARIN_MED:
					xa_inh = 0
				else:
					xa_inh = 1

		total_hours_inpt_post_blood_thinner_drg_primary = 0
		total_hours_inpt_post_blood_thinner_drg_secondary = 0
		
		total_hours_inpt_post_blood_thinner_icd = 0 
		total_hours_inpt_post_blood_thinner_icd_primary = 0
		total_hours_inpt_post_blood_thinner_icd_secondary = 0

		total_visits_inpt_post_blood_thinner_icd = 0 
		total_visits_inpt_post_blood_thinner_icd_primary = 0
		total_visits_inpt_post_blood_thinner_icd_secondary = 0

		drg_national_payment_rate_primary = 0 
		drg_national_payment_rate_secondary = 0 


		if not pd.isna(current_pt_ed_hadm_id):

			# Some metadata has to be obtained from the hospitalizations table 
			current_pt_ed_hadm_id = int(current_pt_ed_hadm_id)

			current_pt_hosp_admissions_row = look_up_admission_by_hadm(row['hadm_id'],hosp_admissions)
			if (len(current_pt_hosp_admissions_row) > 1):
				print("There should not be more than one hospital admission per id.")
				sys.exit(0)

			current_pt_hosp_admissions_row = current_pt_hosp_admissions_row.iloc[0]

			current_pt_adm_death_time = current_pt_hosp_admissions_row['deathtime']
			current_pt_adm_insurance = current_pt_hosp_admissions_row['insurance']
			current_pt_adm_language = current_pt_hosp_admissions_row['language']
			current_pt_adm_marital_status = current_pt_hosp_admissions_row['marital_status']
			current_pt_adm_race = current_pt_hosp_admissions_row['race']

			if current_pt_adm_race in ['WHITE', 'WHITE - RUSSIAN', 'WHITE - BRAZILIAN','PORTUGUESE', 'WHITE - OTHER EUROPEAN','WHITE - EASTERN EUROPEAN']:
			    current_pt_adm_race_coded = 0
			elif current_pt_adm_race in ['BLACK/AFRICAN AMERICAN', 'BLACK/CAPE VERDEAN','BLACK/CARIBBEAN ISLAND','BLACK/AFRICAN']:
			    current_pt_adm_race_coded = 1
			elif current_pt_adm_race in ['HISPANIC/LATINO - DOMINICAN', 'HISPANIC/LATINO - SALVADORAN','HISPANIC/LATINO - PUERTO RICAN', 
										'HISPANIC/LATINO - GUATEMALAN', 'HISPANIC OR LATINO', 'HISPANIC/LATINO - MEXICAN','HISPANIC/LATINO - COLUMBIAN',
										'SOUTH AMERICAN','HISPANIC/LATINO - CUBAN','HISPANIC/LATINO - HONDURAN','HISPANIC/LATINO - CENTRAL AMERICAN']:
			    current_pt_adm_race_coded = 2
			elif current_pt_adm_race in ['ASIAN', 'ASIAN - SOUTH EAST ASIAN', 'ASIAN - CHINESE','ASIAN - ASIAN INDIAN','ASIAN - KOREAN']:
			    current_pt_adm_race_coded = 3
			elif current_pt_adm_race in ['MULTIPLE RACE/ETHNICITY']:
				current_pt_adm_race_coded = 4
			elif current_pt_adm_race in ['AMERICAN INDIAN/ALASKA NATIVE', 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER']:
				current_pt_adm_race_coded = 5
			elif current_pt_adm_race in ['OTHER','UNKNOWN','UNABLE TO OBTAIN','PATIENT DECLINED TO ANSWER']:
				current_pt_adm_race_coded = 6
			else:
			    print("Unexpectedly, there was a race that had not been pre-categorized: ", race)
			    sys.exit(0)


			# Per discussion, we want to add the cost of the current visit to the hospitalizations
			if blood_thinner is not None:
				admittime = datetime.strptime(current_pt_hosp_admissions_row['admittime'], mimic_date_format)
				dischtime = datetime.strptime(current_pt_hosp_admissions_row['dischtime'], mimic_date_format)
				los_hours = (dischtime - admittime).total_seconds() / 3600

				# ICD-based LOS 
				current_visit_dxs = ed_df_dx[ed_df_dx['stay_id'] == current_ed_visit]
				current_visit_dxs_icd9 = current_visit_dxs[current_visit_dxs['icd_version'] == 9]['icd_code'].tolist()
				current_visit_dxs_icd10 = current_visit_dxs[current_visit_dxs['icd_version'] == 10]['icd_code'].tolist()

				matching_prefixes_9 = [prefix for code in current_visit_dxs_icd9 for prefix in blood_thinner_icds_of_interest_9 if code.startswith(str(prefix))]
				matching_prefixes_10 = [prefix for code in current_visit_dxs_icd10 for prefix in blood_thinner_icds_of_interest_10 if code.startswith(str(prefix))]

				if matching_prefixes_9 or matching_prefixes_10:
				    total_hours_inpt_post_blood_thinner_icd += los_hours
				    total_visits_inpt_post_blood_thinner_icd += 1 


				current_visit_dxs_primary = ed_df_dx[(ed_df_dx['stay_id'] == current_ed_visit) & (ed_df_dx['seq_num'] == 1)]
				current_visit_dxs_primary_icd9 = current_visit_dxs_primary[current_visit_dxs_primary['icd_version'] == 9]['icd_code'].tolist()
				current_visit_dxs_primary_icd10 = current_visit_dxs_primary[current_visit_dxs_primary['icd_version'] == 10]['icd_code'].tolist()

				matching_prefixes_9 = [prefix for code in current_visit_dxs_primary_icd9 for prefix in blood_thinner_icds_of_interest_9 if code.startswith(str(prefix))]
				matching_prefixes_10 = [prefix for code in current_visit_dxs_primary_icd10 for prefix in blood_thinner_icds_of_interest_10 if code.startswith(str(prefix))]

				if matching_prefixes_9 or matching_prefixes_10:
				    total_hours_inpt_post_blood_thinner_icd_primary += los_hours
				    total_visits_inpt_post_blood_thinner_icd_primary +=1 


				current_visit_primary_sec_dxs = ed_df_dx[(ed_df_dx['stay_id'] == current_ed_visit) & ((ed_df_dx['seq_num'] == 1) | (ed_df_dx['seq_num'] == 2))]
				current_visit_primary_sec_dxs_icd9 = current_visit_primary_sec_dxs[current_visit_primary_sec_dxs['icd_version'] == 9]['icd_code'].tolist()
				current_visit_primary_sec_dxs_icd10 = current_visit_primary_sec_dxs[current_visit_primary_sec_dxs['icd_version'] == 10]['icd_code'].tolist()

				matching_prefixes_9 = [prefix for code in current_visit_primary_sec_dxs_icd9 for prefix in blood_thinner_icds_of_interest_9 if code.startswith(str(prefix))]
				matching_prefixes_10 = [prefix for code in current_visit_primary_sec_dxs_icd10 for prefix in blood_thinner_icds_of_interest_10 if code.startswith(str(prefix))]

				if matching_prefixes_9 or matching_prefixes_10:
				    total_hours_inpt_post_blood_thinner_icd_secondary += los_hours
				    total_visits_inpt_post_blood_thinner_icd_secondary += 1 




				# DRG-based LOS and cost assessment
				relevant_hcfa_drg = hosp_drgcodes[(hosp_drgcodes['hadm_id'] == current_pt_ed_hadm_id) & (hosp_drgcodes['drg_type'] == 'HCFA')]
				drg_code = None 
				if not relevant_hcfa_drg.empty:
					drg_code = relevant_hcfa_drg['drg_code'].iloc[0]

					if drg_code in blood_thinner_drgs_codes: 
						drg_row = blood_thinner_drgs[blood_thinner_drgs['drg_code'] == drg_code]
						if len(drg_row) == 1:
							drg_row = drg_row.iloc[0]
						else:
							print("This is unexpected!")
							sys.exit(0)

						if (drg_row['primary_outcomes'] == 1):
							total_hours_inpt_post_blood_thinner_drg_primary += los_hours
							drg_national_payment_rate_primary += drg_row['national_payment_rate']
						elif (drg_row['secondary_outcomes'] == 1):
							total_hours_inpt_post_blood_thinner_drg_secondary += los_hours
							drg_national_payment_rate_secondary += drg_row['national_payment_rate']
						else:
							print("This should not happen")
							sys.exit(0)


###################### NEXT VISIT ITERATION BEGINS HERE ###################### 
		
		while (next_row['subject_id'] == current_pt):
			next_visit_date = datetime.strptime(next_row['intime'], mimic_date_format)
			time_delta = next_visit_date - current_visit_date
			next_row_hadm_id = None
			if not pd.isna(next_row['hadm_id']):
				next_row_hadm_id = int(next_row['hadm_id'])

			# For metrics where we want to look within a visit window:
			if (time_delta.days <= num_days_revisit_window):
				num_subseq_ed_encounters += 1 
				if (next_row['disposition'] == DISPO_EXPIRED):
					death_subseq_ed_encounters = True
				if next_row_hadm_id:
					next_row_icu_admissions_row = look_up_admission_by_hadm(next_row_hadm_id,icu_stays)
					if not next_row_icu_admissions_row.empty:
						num_subseq_icu_admissions += 1


				if (current_pt_htn_not_on_meds):
					next_visit_vitals = df_ed_vitals_non_missing_first[df_ed_vitals_non_missing_first['stay_id'] == next_row['stay_id']]
					if (len(next_visit_vitals) == 1):
						next_visit_vitals = next_visit_vitals.iloc[0]
						if (next_visit_vitals['sbp'] > MAX_SBP):
							df_ed_med_rec_htn = df_ed_med_rec_htn_meds[(df_ed_med_rec_htn_meds['stay_id'] == next_row['stay_id'])]
							if (len(df_ed_med_rec_htn) == 0):
								not_on_htn_subseq_ed_encounter = True
								not_on_htn_subseq_ed_encounter_num_visits += 1
							else:
								on_htn_subseq_ed_encounter_num_visits += 1

			if next_row_hadm_id:
				next_row_hosp_admissions_row = look_up_admission_by_hadm(next_row_hadm_id,hosp_admissions)
				if not next_row_hosp_admissions_row.empty:
					if blood_thinner is not None: 
						admittime = datetime.strptime(next_row_hosp_admissions_row['admittime'].iloc[0], mimic_date_format)
						dischtime = datetime.strptime(next_row_hosp_admissions_row['dischtime'].iloc[0], mimic_date_format)
						los_hours = (dischtime - admittime).total_seconds() / 3600

						# ICD-based LOS [for now brute forcing three different levels of calcs - to-reivist]
						current_visit_dxs = ed_df_dx[ed_df_dx['stay_id'] == next_row['stay_id']]
						current_visit_dxs_icd9 = current_visit_dxs[current_visit_dxs['icd_version'] == 9]['icd_code'].tolist()
						current_visit_dxs_icd10 = current_visit_dxs[current_visit_dxs['icd_version'] == 10]['icd_code'].tolist()

						matching_prefixes_9 = [prefix for code in current_visit_dxs_icd9 for prefix in blood_thinner_icds_of_interest_9 if code.startswith(str(prefix))]
						matching_prefixes_10 = [prefix for code in current_visit_dxs_icd10 for prefix in blood_thinner_icds_of_interest_10 if code.startswith(str(prefix))]

						if matching_prefixes_9 or matching_prefixes_10:
						    total_hours_inpt_post_blood_thinner_icd += los_hours
						    total_visits_inpt_post_blood_thinner_icd += 1

						
						current_visit_dxs_primary = ed_df_dx[(ed_df_dx['stay_id'] == next_row['stay_id']) & (ed_df_dx['seq_num'] == 1)]
						current_visit_dxs_primary_icd9 = current_visit_dxs_primary[current_visit_dxs_primary['icd_version'] == 9]['icd_code'].tolist()
						current_visit_dxs_primary_icd10 = current_visit_dxs_primary[current_visit_dxs_primary['icd_version'] == 10]['icd_code'].tolist()

						matching_prefixes_9 = [prefix for code in current_visit_dxs_primary_icd9 for prefix in blood_thinner_icds_of_interest_9 if code.startswith(str(prefix))]
						matching_prefixes_10 = [prefix for code in current_visit_dxs_primary_icd10 for prefix in blood_thinner_icds_of_interest_10 if code.startswith(str(prefix))]

						if matching_prefixes_9 or matching_prefixes_10:
						    total_hours_inpt_post_blood_thinner_icd_primary += los_hours
						    total_visits_inpt_post_blood_thinner_icd_primary += 1

						
						current_visit_primary_sec_dxs = ed_df_dx[(ed_df_dx['stay_id'] == next_row['stay_id']) & ((ed_df_dx['seq_num'] == 1) | (ed_df_dx['seq_num'] == 2))]
						current_visit_primary_sec_dxs_icd9 = current_visit_primary_sec_dxs[current_visit_primary_sec_dxs['icd_version'] == 9]['icd_code'].tolist()
						current_visit_primary_sec_dxs_icd10 = current_visit_primary_sec_dxs[current_visit_primary_sec_dxs['icd_version'] == 10]['icd_code'].tolist()

						matching_prefixes_9 = [prefix for code in current_visit_primary_sec_dxs_icd9 for prefix in blood_thinner_icds_of_interest_9 if code.startswith(str(prefix))]
						matching_prefixes_10 = [prefix for code in current_visit_primary_sec_dxs_icd10 for prefix in blood_thinner_icds_of_interest_10 if code.startswith(str(prefix))]

						if matching_prefixes_9 or matching_prefixes_10:
						    total_hours_inpt_post_blood_thinner_icd_secondary += los_hours
						    total_visits_inpt_post_blood_thinner_icd_secondary +=1

						# DRG-based LOS and cost assessment
						relevant_hcfa_drg = hosp_drgcodes[(hosp_drgcodes['hadm_id'] == next_row['hadm_id']) & (hosp_drgcodes['drg_type'] == 'HCFA')]
						drg_code = None 
						if not relevant_hcfa_drg.empty:
							drg_code = relevant_hcfa_drg['drg_code'].iloc[0]

						if drg_code in blood_thinner_drgs_codes: 
							drg_row = blood_thinner_drgs[blood_thinner_drgs['drg_code'] == drg_code]
							if len(drg_row) == 1:
								drg_row = drg_row.iloc[0]
							else:
								print("This is unexpected!")
								sys.exit(0)
							
							if (drg_row['primary_outcomes'] == 1):
								total_hours_inpt_post_blood_thinner_drg_primary += los_hours
								drg_national_payment_rate_primary += drg_row['national_payment_rate']
							elif (drg_row['secondary_outcomes'] == 1):
								total_hours_inpt_post_blood_thinner_drg_secondary += los_hours
								drg_national_payment_rate_secondary += drg_row['national_payment_rate']
							else:
								print("This should not happen")
								sys.exit(0)
			next_index += 1 
			next_row = df_ed_stays.loc[next_index]

		master_dict[current_ed_visit] = {
			'subject_id' : current_pt, # Patient level identifier
			'hadm_id' : current_pt_ed_hadm_id, # Identifier for the hospitalization, if there was one
			'ed_encounter_num' : encounter_num, # Index of which ED visit this was for the patient (first vs. second)
			'ed_primary_icd_code': primary_icd_code, # Primary DX code for the ED encounter (actual ICD code)
			'ed_primary_icd_version' : primary_icd_version, # Primary DX code for the ED encounter (icd 9 vs 10)
			'ed_primary_icd_title' : primary_icd_title, # Primary DX code for the ED encounter (ICD description plain text)
			'ed_primary_icd_title_is_of_interest' : primary_icd_is_icd_of_interest, # Per discussion with GF, notes if the primary dx is a blood thinner
																					# dx of interest
			'ed_primary_icd_dxes_blood_thinner_target' : icd_dxes_blood_thinner_target, # Indicates what ICD prefix the given ED visit matched on for ICD of interest
			'ed_num_meds' : ed_num_meds, # Number of home meds pt is on as of the current ED visit 
			'ed_gender' : current_pt_ed_gender, # Gender noted for current ED visit 
			'ed_race' : current_pt_ed_race, # Race noted for current ED visit 
			'ed_race_coded' : current_pt_ed_race_coded, # Race coded for current ED visit [see above]
			'hosp_adm_death_time' : current_pt_adm_death_time, # Time of death if patient was admitted and died - of note, this will miss pts who 
																# died in the emergency department prior to an admission
			'hosp_adm_insurance' : current_pt_adm_insurance, # Insurance type at the time of ED visit as noted by hosp admission
			'hosp_adm_language' : current_pt_adm_language, # Primary language at the time of ED visit as noted by hosp admission
			'hosp_adm_marital_status' : current_pt_adm_marital_status, # Marital status at the time of ED visit as noted by hosp admission
			'hosp_adm_race' : current_pt_adm_race,  # Race at the time of ED visit as noted by hosp admission
			'hosp_adm_race_coded' : current_pt_adm_race_coded,  # Race coded at the time of ED visit as noted by hosp admission [see above]
			'death_subseq_ed_encounters' : death_subseq_ed_encounters, # Notes if any subsequent ED visits within the window ended in death (T/F)
			'num_subseq_ed_encounters' : num_subseq_ed_encounters, # This is total number of subsequent ED visits within the window, no other excl/incl criteria
			'num_total_visits' : num_total_visits, # This is total number of ED visits all time, with no excl/incl criteria
			'num_subseq_icu_admissions' : num_subseq_icu_admissions, # This is calculated number of ICU admissions within the window
			'blood_thinner' : blood_thinner, # Notes type of blood thinner pt was on if this was ED first visit on blood thinner 
			'blood_thinner_pt' : blood_thinner_pt, # Notes if this pt is generally on just one blood thinner (regardless of if this is the first visit)
			'total_hours_inpt_post_blood_thinner_drg_primary' : total_hours_inpt_post_blood_thinner_drg_primary, # Sums total inpt los for all hos
																												# that meet pre-screened DRG primary
																												# criteria and were for pts on blood
																												# thinner 
			'total_hours_inpt_post_blood_thinner_drg_secondary' : total_hours_inpt_post_blood_thinner_drg_secondary, # Sums total inpt los for all hos
																												# that meet pre-screened DRG secondary
																												# criteria and were for pts on blood
																												# thinner
			'drg_national_payment_rate_primary' : drg_national_payment_rate_primary, # Sums total inpt cost for all hos
																					# that meet pre-screened DRG primary
																					# criteria and were for pts on blood
																					# thinner based on 
																					# https://www.optumcoding.com/upload/docs/2021%20DRG_National%20Average%20Payment%20Table_Update.pdf
			'drg_national_payment_rate_secondary' : drg_national_payment_rate_secondary, # Sums total inpt cost for all hos
																					# that meet pre-screened DRG secondary
																					# criteria and were for pts on blood
																					# thinner based on 
																					# https://www.optumcoding.com/upload/docs/2021%20DRG_National%20Average%20Payment%20Table_Update.pdf
			'total_hours_inpt_post_blood_thinner_icd' : total_hours_inpt_post_blood_thinner_icd,  # Sums total inpt los for all hos
																					# that meet pre-screened ICD 
																					# criteria and were for pts on blood
																					# thinner
			'total_visits_inpt_post_blood_thinner_icd' : total_visits_inpt_post_blood_thinner_icd, # total vist number equivalent of the above
			'total_hours_inpt_post_blood_thinner_icd_primary' : total_hours_inpt_post_blood_thinner_icd_primary, # Similar to the above but this only
																					# sums total inpt los for all hos where primary dx meets
																					# pre-screened ICD criteria and were for pts on blood thinner
			'total_visits_inpt_post_blood_thinner_icd_primary' : total_visits_inpt_post_blood_thinner_icd_primary, # total visit number equivalent of the above
			'total_hours_inpt_post_blood_thinner_icd_secondary' : total_hours_inpt_post_blood_thinner_icd_secondary, # Similar to the above but this only
																					# sums total inpt los for all hos where primary dx meets
																					# pre-screened ICD criteria and were for pts on blood thinner
			'total_visits_inpt_post_blood_thinner_icd_secondary' : total_visits_inpt_post_blood_thinner_icd_secondary, # total vist number equivalent of the above
			'not_on_htn_subseq_ed_encounter' : not_on_htn_subseq_ed_encounter, # Indicates if a pt originally presented with HTN and was not on a ACE/ARB
																				# and then returns for a subsequent visit within 30 days and is still HTN
																				# and still with no ACE/ARB
			'not_on_htn_subseq_ed_encounter_num_visits' : not_on_htn_subseq_ed_encounter_num_visits, # equivalent of the above - tracks number of
																				# visits within 30 days where pt was "HTN" but still no ace/arb
			'on_htn_subseq_ed_encounter_num_visits' : on_htn_subseq_ed_encounter_num_visits, # This tracks the number of ED visits within 30 days
																				# that a pt who originally presented with HTN but no ACE/ARB
																				# had where the pt was put on an ace/arb
			'current_pt_ed_gender_coded' : current_pt_ed_gender_coded, # per GF request: 0: if female, 1: if male
			'xa_inh' : xa_inh, # per GF request: 0: Non-Xainh [aka Warfarin] 1: Xarelto, Eliquis, etc,
			'current_pt_ever_htn_vs' : current_pt_ever_htn_vs, # notes if the pt was ever HTN during any ED visit's based on vitals
			'current_pt_ever_htn_icd' : current_pt_ever_htn_icd, # notes if the pt was ever HTN during any ED visit's based on ICD
			'current_pt_ever_htn_meds_full' : current_pt_ever_htn_meds_full, # notes if the pt was ever on any HTN meds as noted by Tripp's groups
			'current_pt_pt_anchor_age' : current_pt_pt_anchor_age, # data from hosp patients table
			'current_pt_pt_anchor_year' : current_pt_pt_anchor_year, # data from hosp patients table
			'current_pt_anchor_year_group' : current_pt_anchor_year_group, # data from hosp patients table
		}
	# if index > 10000:
	# 	break



print("End time is: ", datetime.now())
# sys.exit(0)

output_filename = "output.csv"
output_path = os.path.join(output_dir, output_filename)

df = pd.DataFrame(master_dict).T  
df.index.name = 'stay_id'
df.to_csv(output_path)

# filter all medications by Apixiban / Eliquis or heparin 
# identify which patients have ONLY A or B 
# Go through every single visit and see if as of that visit one of the meds was being taken
# Assuming that it's the first visit (based on flag), then iterate through all subsequent visits
# 	Find every hospital stay by the hadm in the admissions file based on admittime	dischtime 
#	Sum up total number of days 
# Prep work - might need to merge d_icd_diagnoses and diagnoses_icd 
# Alternatively, used drgcodes - do all pts have these? 

