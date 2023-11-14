clear

use "C:\Users\tripp\Downloads\mimic_output_w_htn.dta" 

* input: not_on_htn_subseq_ed_encounter_n
*output files: num_subseq_ed_encounters, num_subseq_icu_admissions
* controls: current_pt_ever_htn_vs, current_pt_ever_htn_icd, current_pt_ever_htn_meds_full

*** Characteristics ****
tab not_on_htn_subseq_ed_encounter_n
tab num_subseq_ed_encounters if not_on_htn_subseq_ed_encounter_n == 1
tab num_subseq_icu_admissions if not_on_htn_subseq_ed_encounter_n == 1

tab current_pt_ever_htn_icd num_subseq_ed_encounters
tab current_pt_ever_htn_icd num_subseq_ed_encounters if not_on_htn_subseq_ed_encounter_n == 1
tab current_pt_ever_htn_vs 
tab current_pt_ever_htn_vs if not_on_htn_subseq_ed_encounter_n == 1 
tab current_pt_ever_htn_meds_full
tab current_pt_ever_htn_meds_full if not_on_htn_subseq_ed_encounter_n == 1 

tab not_on_htn_subseq_ed_encounter_n num_subseq_ed_encounters, chi2
tab not_on_htn_subseq_ed_encounter_n num_subseq_icu_admissions, chi2

ranksum num_subseq_ed_encounters, by(not_on_htn_subseq_ed_encounter)
ranksum num_subseq_icu_admissions, by(not_on_htn_subseq_ed_encounter)


tab current_pt_ever_htn_icd if not_on_htn_subseq_ed_encounter_n == 1 
tab not_on_htn_subseq_ed_encounter_n num_subseq_icu_admissions if current_pt_ever_htn_vs == "TRUE", chi2 

tab not_on_htn_subseq_ed_encounter_n num_subseq_ed_encounters if current_pt_ever_htn_icd == "TRUE", chi2
tab not_on_htn_subseq_ed_encounter_n num_subseq_icu_admissions if current_pt_ever_htn_icd == "TRUE", chi2 

tab not_on_htn_subseq_ed_encounter_n num_subseq_ed_encounters if current_pt_ever_htn_meds_full == "TRUE", chi2
tab not_on_htn_subseq_ed_encounter_n num_subseq_icu_admissions if current_pt_ever_htn_meds_full == "TRUE", chi2 