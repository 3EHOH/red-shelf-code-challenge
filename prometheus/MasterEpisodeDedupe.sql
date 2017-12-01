DROP TABLE IF EXISTS association_dedupe;

CREATE TABLE association_dedupe AS
SELECT DISTINCT
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_start_day,
association_end_day
FROM association;

TRUNCATE association;

INSERT INTO association
(parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_start_day,
association_end_day)
SELECT
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_start_day,
association_end_day
FROM association_dedupe;

DROP TABLE association_dedupe;





CREATE TABLE assignment_dedupe AS
SELECT DISTINCT
member_id,
master_claim_id,
master_episode_id,
claim_source,
assigned_type,
assigned_count,
rule,
pac,
pac_type,
episode_period,
IP_period
FROM assignment;

TRUNCATE assignment;

INSERT INTO assignment
(member_id,
master_claim_id,
master_episode_id,
claim_source,
assigned_type,
assigned_count,
rule,
pac,
pac_type,
episode_period,
IP_period)

SELECT
member_id,
master_claim_id,
master_episode_id,
claim_source,
assigned_type,
assigned_count,
rule,
pac,
pac_type,
episode_period,
IP_period
FROM assignment_dedupe;

DROP TABLE assignment_dedupe;




CREATE TABLE episode_dedupe AS
SELECT DISTINCT
member_id,
claim_id,
claim_line_id,
master_episode_id,
master_claim_id,
episode_id,
episode_type,
episode_begin_date,
episode_end_date,
episode_length_days,
trig_begin_date,
trig_end_date,
associated,
association_count,
association_level,
truncated,
#attrib_cost_physician,
#attrib_cost_facility,
#attrib_visit_physician,
#attrib_visit_facility,
attrib_default_physician,
attrib_default_facility
FROM episode;

TRUNCATE episode;

INSERT INTO episode
(member_id,
claim_id,
claim_line_id,
master_episode_id,
master_claim_id,
episode_id,
episode_type,
episode_begin_date,
episode_end_date,
episode_length_days,
trig_begin_date,
trig_end_date,
associated,
association_count,
association_level,
truncated,
#attrib_cost_physician,
#attrib_cost_facility,
#attrib_visit_physician,
#attrib_visit_facility,
attrib_default_physician,
attrib_default_facility)

SELECT
member_id,
claim_id,
claim_line_id,
master_episode_id,
master_claim_id,
episode_id,
episode_type,
episode_begin_date,
episode_end_date,
episode_length_days,
trig_begin_date,
trig_end_date,
associated,
association_count,
association_level,
truncated,
#attrib_cost_physician,
#attrib_cost_facility,
#attrib_visit_physician,
#attrib_visit_facility,
attrib_default_physician,
attrib_default_facility
FROM episode_dedupe;

DROP TABLE episode_dedupe;



CREATE TABLE triggers_dedupe AS
SELECT
DISTINCT
member_id,
claim_id,
claim_line_id,
master_episode_id,
master_claim_id,
episode_id,
episode_type,
trig_begin_date,
trig_end_date,
episode_begin_date,
episode_end_date,
orig_episode_begin_date,
orig_episode_end_date,
look_back,
look_ahead,
req_conf_claim,
conf_claim_id,
conf_claim_line_id,
min_code_separation,
max_code_separation,
trig_by_episode_id,
trig_by_master_episode_id,
dx_pass_code,
px_pass_code,
em_pass_code,
conf_dx_pass_code,
conf_px_pass_code,
conf_em_pass_code,
dropped,
truncated,
win_claim_id,
win_master_claim_id
FROM triggers;



truncate triggers;


insert into triggers
(
member_id,
claim_id,
claim_line_id,
master_episode_id,
master_claim_id,
episode_id,
episode_type,
trig_begin_date,
trig_end_date,
episode_begin_date,
episode_end_date,
orig_episode_begin_date,
orig_episode_end_date,
look_back,
look_ahead,
req_conf_claim,
conf_claim_id,
conf_claim_line_id,
min_code_separation,
max_code_separation,
trig_by_episode_id,
trig_by_master_episode_id,
dx_pass_code,
px_pass_code,
em_pass_code,
conf_dx_pass_code,
conf_px_pass_code,
conf_em_pass_code,
dropped,
truncated,
win_claim_id,
win_master_claim_id
)
select
member_id,
claim_id,
claim_line_id,
master_episode_id,
master_claim_id,
episode_id,
episode_type,
trig_begin_date,
trig_end_date,
episode_begin_date,
episode_end_date,
orig_episode_begin_date,
orig_episode_end_date,
look_back,
look_ahead,
req_conf_claim,
conf_claim_id,
conf_claim_line_id,
min_code_separation,
max_code_separation,
trig_by_episode_id,
trig_by_master_episode_id,
dx_pass_code,
px_pass_code,
em_pass_code,
conf_dx_pass_code,
conf_px_pass_code,
conf_em_pass_code,
dropped,
truncated,
win_claim_id,
win_master_claim_id
FROM triggers_dedupe;

drop table triggers_dedupe;


#


DROP TABLE IF EXISTS claim_line_dedupe;

CREATE TABLE claim_line_dedupe AS
SELECT DISTINCT
master_claim_id, member_id, claim_id, claim_line_id, sequence_key, final_version_flag, provider_npi, provider_id, physician_id, facility_id, allowed_amt, facility_type_code, begin_date, end_date, place_of_svc_code, claim_line_type_code, assigned, assigned_count, quantity, standard_payment_amt, charge_amt, paid_amt, prepaid_amt, copay_amt, coinsurance_amt, deductible_amt, insurance_product, plan_id, admission_date, admission_src_code, admit_type_code, discharge_status_code, discharge_date, type_of_bill, rev_count, drg_version, ms_drg_code, apr_drg_code, readmission, office_visit, `trigger`, ed_visit, ed_visit_id, core_service, pas
FROM claim_line;

TRUNCATE claim_line;

INSERT INTO claim_line
(master_claim_id, member_id, claim_id, claim_line_id, sequence_key, final_version_flag, provider_npi, provider_id, physician_id, facility_id, allowed_amt, facility_type_code, begin_date, end_date, place_of_svc_code, claim_line_type_code, assigned, assigned_count, quantity, standard_payment_amt, charge_amt, paid_amt, prepaid_amt, copay_amt, coinsurance_amt, deductible_amt, insurance_product, plan_id, admission_date, admission_src_code, admit_type_code, discharge_status_code, discharge_date, type_of_bill, rev_count, drg_version, ms_drg_code, apr_drg_code, readmission, office_visit, `trigger`, ed_visit, ed_visit_id, core_service, pas)
SELECT
master_claim_id, member_id, claim_id, claim_line_id, sequence_key, final_version_flag, provider_npi, provider_id, physician_id, facility_id, allowed_amt, facility_type_code, begin_date, end_date, place_of_svc_code, claim_line_type_code, assigned, assigned_count, quantity, standard_payment_amt, charge_amt, paid_amt, prepaid_amt, copay_amt, coinsurance_amt, deductible_amt, insurance_product, plan_id, admission_date, admission_src_code, admit_type_code, discharge_status_code, discharge_date, type_of_bill, rev_count, drg_version, ms_drg_code, apr_drg_code, readmission, office_visit, `trigger`, ed_visit, ed_visit_id, core_service, pas
FROM claim_line_dedupe;

DROP TABLE claim_line_dedupe;



DROP TABLE IF EXISTS claim_line_rx_dedupe;

CREATE TABLE claim_line_rx_dedupe AS
SELECT DISTINCT
master_claim_id, member_id, claim_id, sequence_key, final_version_flag, allowed_amt, assigned, assigned_count, line_counter, charge_amt, paid_amt, prepaid_amt, copay_amt, coinsurance_amt, deductible_amt, drug_nomen, drug_code, drug_name, builder_match_code, days_supply_amt, quantityDispensed, rx_fill_date, prescribing_provider_id, prescribing_provider_npi, prescribing_provider_dea, pharmacy_zip_code, insurance_product, genericDrugIndicator, national_pharmacy_Id, orig_adj_rev, plan_id
FROM claim_line_rx;

TRUNCATE claim_line_rx;

INSERT INTO claim_line_rx
(master_claim_id, member_id, claim_id, sequence_key, final_version_flag, allowed_amt, assigned, assigned_count, line_counter, charge_amt, paid_amt, prepaid_amt, copay_amt, coinsurance_amt, deductible_amt, drug_nomen, drug_code, drug_name, builder_match_code, days_supply_amt, quantityDispensed, rx_fill_date, prescribing_provider_id, prescribing_provider_npi, prescribing_provider_dea, pharmacy_zip_code, insurance_product, genericDrugIndicator, national_pharmacy_Id, orig_adj_rev, plan_id)
SELECT
master_claim_id, member_id, claim_id, sequence_key, final_version_flag, allowed_amt, assigned, assigned_count, line_counter, charge_amt, paid_amt, prepaid_amt, copay_amt, coinsurance_amt, deductible_amt, drug_nomen, drug_code, drug_name, builder_match_code, days_supply_amt, quantityDispensed, rx_fill_date, prescribing_provider_id, prescribing_provider_npi, prescribing_provider_dea, pharmacy_zip_code, insurance_product, genericDrugIndicator, national_pharmacy_Id, orig_adj_rev, plan_id
FROM claim_line_rx_dedupe;

DROP TABLE claim_line_rx_dedupe;



DROP TABLE IF EXISTS claims_combined_dedupe;

CREATE TABLE claims_combined_dedupe AS
SELECT DISTINCT
master_claim_id, member_id, allowed_amt, assigned_count, claim_line_type_code, begin_date, end_date
FROM claims_combined;

TRUNCATE claims_combined;

INSERT INTO claims_combined
(master_claim_id, member_id, allowed_amt, assigned_count, claim_line_type_code, begin_date, end_date)
SELECT
master_claim_id, member_id, allowed_amt, assigned_count, claim_line_type_code, begin_date, end_date
FROM claims_combined_dedupe;

DROP TABLE claims_combined_dedupe;



DROP TABLE IF EXISTS code_dedupe;

CREATE TABLE code_dedupe AS
SELECT DISTINCT
u_c_id, master_claim_id, function_code, code_value, nomen, principal
FROM code;

TRUNCATE code;

INSERT INTO code
(u_c_id, master_claim_id, function_code, code_value, nomen, principal)
SELECT
u_c_id, master_claim_id, function_code, code_value, nomen, principal
FROM code_dedupe;

DROP TABLE code_dedupe;



DROP TABLE IF EXISTS member_dedupe;

CREATE TABLE member_dedupe AS
SELECT DISTINCT
member_id, gender_code, race_code, zip_code, birth_year, age, enforced_provider_id, primary_care_provider_id, primary_care_provider_npi, pcp_effective_date, date_of_death, insurance_type, insurance_carrier, dual_eligible, months_eligible_total, cost_of_care, unassigned_cost, assigned_cost, ed_visits, ed_cost, ip_stay_count, ip_stay_cost, bed_days, alos, claim_lines, claim_lines_t, claim_lines_tc, claim_lines_c, ip_readmit_count, ip_readmit_cost, designated_pcp, plan_id, rf_count, st_count
FROM member;

TRUNCATE member;

INSERT INTO member
(member_id, gender_code, race_code, zip_code, birth_year, age, enforced_provider_id, primary_care_provider_id, primary_care_provider_npi, pcp_effective_date, date_of_death, insurance_type, insurance_carrier, dual_eligible, months_eligible_total, cost_of_care, unassigned_cost, assigned_cost, ed_visits, ed_cost, ip_stay_count, ip_stay_cost, bed_days, alos, claim_lines, claim_lines_t, claim_lines_tc, claim_lines_c, ip_readmit_count, ip_readmit_cost, designated_pcp, plan_id, rf_count, st_count)
SELECT
member_id, gender_code, race_code, zip_code, birth_year, age, enforced_provider_id, primary_care_provider_id, primary_care_provider_npi, pcp_effective_date, date_of_death, insurance_type, insurance_carrier, dual_eligible, months_eligible_total, cost_of_care, unassigned_cost, assigned_cost, ed_visits, ed_cost, ip_stay_count, ip_stay_cost, bed_days, alos, claim_lines, claim_lines_t, claim_lines_tc, claim_lines_c, ip_readmit_count, ip_readmit_cost, designated_pcp, plan_id, rf_count, st_count
FROM member_dedupe;

DROP TABLE member_dedupe;



DROP TABLE IF EXISTS enrollment_dedupe;

CREATE TABLE enrollment_dedupe AS
SELECT DISTINCT
member_id, begin_date, end_date, age_at_enrollment, insurance_product, coverage_type, isGap
FROM enrollment;

TRUNCATE enrollment;

INSERT INTO enrollment
(member_id, begin_date, end_date, age_at_enrollment, insurance_product, coverage_type, isGap)
SELECT
member_id, begin_date, end_date, age_at_enrollment, insurance_product, coverage_type, isGap
FROM enrollment_dedupe;

DROP TABLE enrollment_dedupe;





DROP TABLE IF EXISTS provider_dedupe;

CREATE TABLE provider_dedupe AS
SELECT DISTINCT
provider_id, npi, dea_no, group_name, practice_name, provider_name, system_name, tax_id, medicare_id, zipcode, pilot_name, aco_name, provider_type, provider_attribution_code, provider_aco, provider_health_system, provider_medical_group, facility_id, member_count, episode_count, severity_score, performance_score
FROM provider;

TRUNCATE provider;

INSERT INTO provider
(provider_id, npi, dea_no, group_name, practice_name, provider_name, system_name, tax_id, medicare_id, zipcode, pilot_name, aco_name, provider_type, provider_attribution_code, provider_aco, provider_health_system, provider_medical_group, facility_id, member_count, episode_count, severity_score, performance_score)
SELECT
provider_id, npi, dea_no, group_name, practice_name, provider_name, system_name, tax_id, medicare_id, zipcode, pilot_name, aco_name, provider_type, provider_attribution_code, provider_aco, provider_health_system, provider_medical_group, facility_id, member_count, episode_count, severity_score, performance_score
FROM provider_dedupe;

DROP TABLE provider_dedupe;
