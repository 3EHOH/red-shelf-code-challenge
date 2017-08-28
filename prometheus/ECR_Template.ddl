
CREATE SCHEMA ECR_Template;


CREATE TABLE ECR_Template.assignment
(
    id int NOT NULL,
    member_id varchar(50) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    master_episode_id varchar(73) DEFAULT NULL,
    claim_source varchar(12) DEFAULT NULL,
    assigned_type varchar(2) DEFAULT NULL,
    assigned_count int DEFAULT NULL::int,
    rule varchar(7) DEFAULT NULL,
    pac int DEFAULT 0,
    pac_type varchar(2) DEFAULT NULL,
    episode_period int DEFAULT NULL::int,
    IP_period int DEFAULT NULL::int
);

ALTER TABLE ECR_Template.assignment ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.association
(
    id int NOT NULL,
    parent_master_episode_id varchar(73) DEFAULT NULL,
    child_master_episode_id varchar(73) DEFAULT NULL,
    association_type varchar(24) DEFAULT NULL,
    association_level int DEFAULT NULL::int,
    association_count int DEFAULT NULL::int,
    association_start_day varchar(15) DEFAULT NULL,
    association_end_day varchar(15) DEFAULT NULL
);

ALTER TABLE ECR_Template.association ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.build_episode_reference
(
    EPISODE_ID varchar(6) NOT NULL,
    NAME varchar(6) DEFAULT NULL,
    TYPE varchar(45) NOT NULL,
    STATUS varchar(45) DEFAULT NULL,
    DESCRIPTION varchar(37) DEFAULT NULL,
    CREATED_DATE timestamp(0) NOT NULL,
    MODIFIED_DATE timestamp(0) DEFAULT NULL::timestamp,
    USERS_USER_ID varchar(45) DEFAULT NULL,
    MDC_CATEGORY varchar(2) NOT NULL,
    PARM_SET int DEFAULT 1,
    TRIGGER_TYPE int DEFAULT NULL::int,
    TRIGGER_NUMBER int DEFAULT NULL::int,
    SEPARATION_MIN int DEFAULT NULL::int,
    SEPARATION_MAX int DEFAULT NULL::int,
    BOUND_OFFSET int DEFAULT NULL::int,
    BOUND_LENGTH int DEFAULT NULL::int,
    CONDITION_MIN int DEFAULT NULL::int,
    VERSION varchar(45) DEFAULT NULL,
    END_OF_STUDY int DEFAULT NULL::int
);

ALTER TABLE ECR_Template.build_episode_reference ADD CONSTRAINT C_PRIMARY PRIMARY KEY (EPISODE_ID) DISABLED;

CREATE TABLE ECR_Template.claim_line
(
    id int NOT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    member_id varchar(50) DEFAULT NULL,
    claim_id varchar(50) DEFAULT NULL,
    claim_line_id varchar(22) DEFAULT NULL,
    sequence_key varchar(11) DEFAULT NULL,
    final_version_flag varchar(2) DEFAULT NULL,
    provider_npi varchar(12) DEFAULT NULL,
    provider_id varchar(24) DEFAULT NULL,
    physician_id varchar(24) DEFAULT NULL,
    facility_id varchar(24) DEFAULT NULL,
    allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    facility_type_code varchar(8) DEFAULT NULL,
    begin_date date DEFAULT NULL::date,
    end_date date DEFAULT NULL::date,
    place_of_svc_code varchar(25) DEFAULT NULL,
    claim_line_type_code varchar(12) DEFAULT NULL,
    assigned int DEFAULT 0,
    assigned_count int DEFAULT 0,
    quantity int DEFAULT NULL::int,
    standard_payment_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    charge_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    paid_amt numeric(15,4) DEFAULT NULL::numeric(1,0),
    prepaid_amt numeric(15,4) DEFAULT NULL::numeric(1,0),
    copay_amt numeric(15,4) DEFAULT NULL::numeric(1,0),
    coinsurance_amt numeric(15,4) DEFAULT NULL::numeric(1,0),
    deductible_amt numeric(15,4) DEFAULT NULL::numeric(1,0),
    insurance_product varchar(24) DEFAULT NULL,
    plan_id varchar(24) DEFAULT NULL,
    admission_date date DEFAULT NULL::date,
    admission_src_code varchar(12) DEFAULT NULL,
    admit_type_code varchar(12) DEFAULT NULL,
    discharge_status_code varchar(12) DEFAULT NULL,
    discharge_date date DEFAULT NULL::date,
    type_of_bill varchar(12) DEFAULT NULL,
    rev_count int DEFAULT NULL::int,
    drg_version varchar(12) DEFAULT NULL,
    ms_drg_code varchar(12) DEFAULT NULL,
    apr_drg_code varchar(12) DEFAULT NULL,
    readmission int DEFAULT 0,
    office_visit int DEFAULT 0,
    trigger int DEFAULT 0,
    ed_visit int DEFAULT 0,
    ed_visit_id varchar(12) DEFAULT NULL,
    core_service int DEFAULT 0,
    pas int DEFAULT 0
);

ALTER TABLE ECR_Template.claim_line ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.claim_line_rx
(
    id int NOT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    member_id varchar(50) DEFAULT NULL,
    claim_id varchar(50) DEFAULT NULL,
    sequence_key varchar(11) DEFAULT NULL,
    final_version_flag varchar(5) DEFAULT NULL,
    allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    assigned int DEFAULT 0,
    assigned_count int DEFAULT NULL::int,
    line_counter int DEFAULT NULL::int,
    charge_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    paid_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    prepaid_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    copay_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    coinsurance_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    deductible_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    drug_nomen varchar(10) DEFAULT NULL,
    drug_code varchar(25) DEFAULT NULL,
    drug_name varchar(50) DEFAULT NULL,
    builder_match_code varchar(50) DEFAULT NULL,
    days_supply_amt int DEFAULT NULL::int,
    quantityDispensed numeric(40,20) DEFAULT NULL::numeric(1,0),
    rx_fill_date date DEFAULT NULL::date,
    prescribing_provider_id varchar(24) DEFAULT NULL,
    prescribing_provider_npi varchar(12) DEFAULT NULL,
    prescribing_provider_dea varchar(20) DEFAULT NULL,
    pharmacy_zip_code varchar(16) DEFAULT NULL,
    insurance_product varchar(24) DEFAULT NULL,
    genericDrugIndicator varchar(6) DEFAULT NULL,
    national_pharmacy_Id varchar(20) DEFAULT NULL,
    orig_adj_rev varchar(12) DEFAULT NULL,
    plan_id varchar(20) DEFAULT NULL
);

ALTER TABLE ECR_Template.claim_line_rx ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.claims_combined
(
    id int NOT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    member_id varchar(50) DEFAULT NULL,
    allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    assigned_count int DEFAULT NULL::int,
    claim_line_type_code varchar(12) DEFAULT NULL,
    begin_date date DEFAULT NULL::date,
    end_date date DEFAULT NULL::date
);

ALTER TABLE ECR_Template.claims_combined ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.code
(
    id int NOT NULL,
    u_c_id int NOT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    function_code varchar(10) DEFAULT NULL,
    code_value varchar(12) DEFAULT NULL,
    nomen varchar(7) DEFAULT NULL,
    principal int DEFAULT 0
);

ALTER TABLE ECR_Template.code ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.episode
(
    id int NOT NULL,
    member_id varchar(50) DEFAULT NULL,
    claim_id varchar(50) DEFAULT NULL,
    claim_line_id varchar(22) DEFAULT NULL,
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    episode_id varchar(10) DEFAULT NULL,
    episode_type varchar(1) DEFAULT NULL,
    episode_begin_date date DEFAULT NULL::date,
    episode_end_date date DEFAULT NULL::date,
    episode_length_days int DEFAULT NULL::int,
    trig_begin_date date DEFAULT NULL::date,
    trig_end_date date DEFAULT NULL::date,
    associated int DEFAULT 0,
    association_count int DEFAULT NULL::int,
    association_level int DEFAULT NULL::int,
    truncated int DEFAULT 0,
    attrib_cost_physician varchar(30) DEFAULT NULL,
    attrib_cost_facility varchar(30) DEFAULT NULL,
    attrib_visit_physician varchar(30) DEFAULT NULL,
    attrib_visit_facility varchar(30) DEFAULT NULL,
    attrib_default_physician varchar(30) DEFAULT NULL,
    attrib_default_facility varchar(30) DEFAULT NULL
);

ALTER TABLE ECR_Template.episode ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.episode_aggregates
(
    episode_id varchar(10) DEFAULT NULL,
    filter_id int DEFAULT NULL::int,
    level int DEFAULT NULL::int,
    episode_count int DEFAULT 0,
    total_cost_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    total_cost_split numeric(40,20) DEFAULT 0.00000000000000000000,
    avg_cost_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    avg_cost_split numeric(40,20) DEFAULT 0.00000000000000000000,
    standard_deviation_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    standard_deviation_split numeric(40,20) DEFAULT 0.00000000000000000000,
    cv_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    cv_split numeric(40,20) DEFAULT 0.00000000000000000000,
    percent_costs_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    percent_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    typical_costs_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    typical_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    typ_w_comp_costs_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    typ_w_comp_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    pac_costs_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    pac_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    complication_rate_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    complication_rate_split numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_1_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_80_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_99_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_1_ann_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_80_ann_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_99_ann_costs_split numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_1_costs_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_99_costs_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_1_ann_costs_unsplit numeric(40,20) DEFAULT 0.00000000000000000000,
    percentile_99_ann_costs_unsplit numeric(40,20) DEFAULT 0.00000000000000000000
);


CREATE TABLE ECR_Template.filtered_episodes
(
    filter_id int DEFAULT NULL::int,
    master_episode_id varchar(73) DEFAULT NULL,
    filter_fail varbinary(80) DEFAULT '0'::varbinary(1),
    age_limit_lower varbinary(80) NOT NULL DEFAULT '0'::varbinary(1),
    age_limit_upper varbinary(80) NOT NULL DEFAULT '0'::varbinary(1),
    episode_cost_upper varbinary(80) NOT NULL DEFAULT '0'::varbinary(1),
    episode_cost_lower varbinary(80) NOT NULL DEFAULT '0'::varbinary(1),
    coverage_gap varbinary(80) NOT NULL DEFAULT '0'::varbinary(1),
    episode_complete varbinary(80) NOT NULL DEFAULT '0'::varbinary(1),
    drg varbinary(80) NOT NULL DEFAULT '0'::varbinary(1),
    proc_ep_orphan varbinary(80) NOT NULL DEFAULT '0'::varbinary(1),
    proc_ep_orph_triggered varbinary(80) NOT NULL DEFAULT '0'::varbinary(1)
);


CREATE TABLE ECR_Template.filter_params
(
    filter_id int NOT NULL,
    episode_id varchar(11) DEFAULT NULL,
    lower_age_limit int DEFAULT NULL::int,
    upper_age_limit int DEFAULT NULL::int
);


CREATE TABLE ECR_Template.filters
(
    filter_id int NOT NULL,
    fiilter_name varchar(73) DEFAULT NULL
);

ALTER TABLE ECR_Template.filters ADD CONSTRAINT C_PRIMARY PRIMARY KEY (filter_id) DISABLED;

CREATE TABLE ECR_Template.master_epid_code
(
    id int NOT NULL,
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    code_value varchar(12) DEFAULT NULL,
    nomen varchar(7) DEFAULT NULL,
    pas int DEFAULT 0,
    core_service int DEFAULT 0,
    pac int DEFAULT 0,
    pac_type int DEFAULT NULL::int,
    risk_factor varchar(14) DEFAULT NULL,
    sub_type varchar(14) DEFAULT NULL,
    betos varchar(24) DEFAULT NULL
);

ALTER TABLE ECR_Template.master_epid_code ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.master_epid_level
(
    id int NOT NULL,
    filter_id int DEFAULT NULL::int,
    master_episode_id varchar(73) DEFAULT NULL,
    claim_type varchar(2) DEFAULT NULL,
    level int DEFAULT NULL::int,
    split int DEFAULT 0,
    annualized int DEFAULT 0,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_t numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_tc numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_c numeric(40,20) DEFAULT NULL::numeric(1,0),
    risk_factor_count int DEFAULT NULL::int,
    sub_type_count int DEFAULT NULL::int,
    probability_of_complications int DEFAULT NULL::int,
    IP_stay_count int DEFAULT NULL::int,
    IP_stay_facility_costs numeric(40,20) DEFAULT NULL::numeric(1,0),
    IP_stay_prof_costs numeric(40,20) DEFAULT NULL::numeric(1,0),
    IP_stay_total_costs numeric(40,20) DEFAULT NULL::numeric(1,0),
    IP_stay_bed_days int DEFAULT NULL::int,
    IP_stay_avg_length int DEFAULT NULL::int
);

ALTER TABLE ECR_Template.master_epid_level ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.mdc_desc
(
    mdc varchar(5) NOT NULL DEFAULT '',
    mdc_description varchar(255) DEFAULT NULL
);

ALTER TABLE ECR_Template.mdc_desc ADD CONSTRAINT C_PRIMARY PRIMARY KEY (mdc) DISABLED;

CREATE TABLE ECR_Template.member
(
    id int NOT NULL,
    member_id varchar(50) DEFAULT NULL,
    gender_code varchar(2) DEFAULT NULL,
    race_code varchar(2) DEFAULT NULL,
    zip_code varchar(16) DEFAULT NULL,
    birth_year int DEFAULT NULL::int,
    age int DEFAULT NULL::int,
    enforced_provider_id varchar(24) DEFAULT NULL,
    primary_care_provider_id varchar(24) DEFAULT NULL,
    primary_care_provider_npi varchar(11) DEFAULT NULL,
    pcp_effective_date date DEFAULT NULL::date,
    date_of_death date DEFAULT NULL::date,
    insurance_type varchar(24) DEFAULT NULL,
    insurance_carrier varchar(24) DEFAULT NULL,
    dual_eligible int DEFAULT NULL::int,
    months_eligible_total int DEFAULT NULL::int,
    cost_of_care numeric(40,20) DEFAULT NULL::numeric(1,0),
    unassigned_cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    assigned_cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    ed_visits int DEFAULT NULL::int,
    ed_cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    ip_stay_count int DEFAULT NULL::int,
    ip_stay_cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    bed_days int DEFAULT NULL::int,
    alos int DEFAULT NULL::int,
    claim_lines int DEFAULT NULL::int,
    claim_lines_t int DEFAULT NULL::int,
    claim_lines_tc int DEFAULT NULL::int,
    claim_lines_c int DEFAULT NULL::int,
    ip_readmit_count int DEFAULT NULL::int,
    ip_readmit_cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    designated_pcp varchar(24) DEFAULT NULL,
    plan_id varchar(24) DEFAULT NULL,
    rf_count int DEFAULT NULL::int,
    st_count int DEFAULT NULL::int
);

ALTER TABLE ECR_Template.member ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.member_epid_level
(
    id int NOT NULL,
    member_id varchar(50) DEFAULT NULL,
    episode_id varchar(12) DEFAULT NULL,
    level int DEFAULT NULL::int,
    split int DEFAULT 0,
    annualized int DEFAULT 0,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_t numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_tc numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_c numeric(40,20) DEFAULT NULL::numeric(1,0)
);

ALTER TABLE ECR_Template.member_epid_level ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.member_master_epid_level
(
    id int NOT NULL,
    member_id varchar(50) DEFAULT NULL,
    master_episode_id varchar(73) DEFAULT NULL,
    level int DEFAULT NULL::int,
    split int DEFAULT 0,
    annualized int DEFAULT 0,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_t numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_tc numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_c numeric(40,20) DEFAULT NULL::numeric(1,0)
);

ALTER TABLE ECR_Template.member_master_epid_level ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.ndc_to_multum
(
    id int NOT NULL,
    multum varchar(10) DEFAULT NULL,
    ndc varchar(12) DEFAULT NULL,
    drug_category varchar(14) DEFAULT NULL
);

ALTER TABLE ECR_Template.ndc_to_multum ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.percentiles
(
    Filter_ID int DEFAULT NULL::int,
    Episode_ID varchar(6) DEFAULT NULL,
    Level int DEFAULT NULL::int,
    Split_1stPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Split_99thPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Split_80thPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_1stPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_99thPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_80thPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Unsplit_1stPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Unsplit_99thPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_1stPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_99thPercentile_Cost numeric(20,2) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE ECR_Template.pmp_su
(
    pmp_su_id int NOT NULL,
    begin_date date DEFAULT NULL::date,
    end_date date DEFAULT NULL::date,
    period varchar(10) DEFAULT NULL,
    member_count int DEFAULT NULL::int,
    total_costs numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_costs_IP numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_costs_OP numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_costs_PB numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_costs_RX numeric(40,20) DEFAULT NULL::numeric(1,0),
    pmp_costs numeric(40,20) DEFAULT NULL::numeric(1,0),
    pmp_costs_IP numeric(40,20) DEFAULT NULL::numeric(1,0),
    pmp_costs_OP numeric(40,20) DEFAULT NULL::numeric(1,0),
    pmp_costs_PB numeric(40,20) DEFAULT NULL::numeric(1,0),
    pmp_costs_RX numeric(40,20) DEFAULT NULL::numeric(1,0),
    member_count_IP_stay int DEFAULT NULL::int,
    member_count_ED_visit int DEFAULT NULL::int,
    member_percent_IP_stay numeric(10,5) DEFAULT NULL::numeric(1,0),
    member_percent_ED_visit numeric(10,5) DEFAULT NULL::numeric(1,0),
    member_count_SU int DEFAULT NULL::int,
    member_count_SU_IP int DEFAULT NULL::int,
    member_count_SU_ED int DEFAULT NULL::int,
    member_count_SU_ED_and_IP int DEFAULT NULL::int,
    member_percent_SU numeric(10,5) DEFAULT NULL::numeric(1,0),
    member_percent_SU_IP numeric(10,5) DEFAULT NULL::numeric(1,0),
    member_percent_SU_ED numeric(10,5) DEFAULT NULL::numeric(1,0),
    member_percent_SU_ED_and_IP numeric(10,5) DEFAULT NULL::numeric(1,0),
    costs_ED_and_IP numeric(40,20) DEFAULT NULL::numeric(1,0),
    costs_SU_ED_or_IP numeric(40,20) DEFAULT NULL::numeric(1,0),
    costs_SU_ED numeric(40,20) DEFAULT NULL::numeric(1,0),
    costs_SU_IP numeric(40,20) DEFAULT NULL::numeric(1,0),
    costs_SU_ED_and_IP numeric(40,20) DEFAULT NULL::numeric(1,0),
    percent_costs_SU_ED_or_IP numeric(10,5) DEFAULT NULL::numeric(1,0),
    percent_ED_costs_SU_ED numeric(10,5) DEFAULT NULL::numeric(1,0),
    percent_IP_costs_SU_IP numeric(10,5) DEFAULT NULL::numeric(1,0),
    percent_costs_SU_ED_and_IP numeric(10,5) DEFAULT NULL::numeric(1,0),
    ED_SU_avg_ED_count int DEFAULT NULL::int,
    ED_SU_avg_IP_count int DEFAULT NULL::int,
    ED_SU_avg_ED_LOS int DEFAULT NULL::int,
    IP_SU_avg_ED_count int DEFAULT NULL::int,
    IP_SU_avg_IP_count int DEFAULT NULL::int,
    IP_SU_avg_ED_LOS int DEFAULT NULL::int,
    ED_and_IP_SU_avg_ED_count int DEFAULT NULL::int,
    ED_and_IP_SU_avg_IP_count int DEFAULT NULL::int,
    ED_and_IP_SU_avg_ED_LOS int DEFAULT NULL::int,
    ED_SU_avg_ED_cost numeric(15,4) DEFAULT NULL::numeric(1,0),
    IP_SU_avg_IP_cost numeric(15,4) DEFAULT NULL::numeric(1,0),
    ED_and_IP_SU_avg_ED_and_IP_cost numeric(15,4) DEFAULT NULL::numeric(1,0)
);

ALTER TABLE ECR_Template.pmp_su ADD CONSTRAINT C_PRIMARY PRIMARY KEY (pmp_su_id) DISABLED;

CREATE TABLE ECR_Template.pmp_su_metric_desc
(
    pmp_su_metric_id int NOT NULL,
    metric_name varchar(30) DEFAULT NULL,
    metric_desc varchar(80) DEFAULT NULL
);

ALTER TABLE ECR_Template.pmp_su_metric_desc ADD CONSTRAINT C_PRIMARY PRIMARY KEY (pmp_su_metric_id) DISABLED;

CREATE TABLE ECR_Template.pmp_su_ranks
(
    id int NOT NULL,
    pmp_su_id int DEFAULT NULL::int,
    pmp_su_metric_id int DEFAULT NULL::int,
    rank int DEFAULT NULL::int,
    count int DEFAULT NULL::int,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0)
);

ALTER TABLE ECR_Template.pmp_su_ranks ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.provider
(
    id int NOT NULL,
    provider_id varchar(24) DEFAULT NULL,
    npi varchar(12) DEFAULT NULL,
    dea_no varchar(20) DEFAULT NULL,
    group_name varchar(75) DEFAULT NULL,
    practice_name varchar(75) DEFAULT NULL,
    provider_name varchar(75) DEFAULT NULL,
    system_name varchar(75) DEFAULT NULL,
    tax_id varchar(11) DEFAULT NULL,
    medicare_id varchar(20) DEFAULT NULL,
    zipcode varchar(12) DEFAULT NULL,
    pilot_name varchar(75) DEFAULT NULL,
    aco_name varchar(75) DEFAULT NULL,
    provider_type varchar(24) DEFAULT NULL,
    provider_attribution_code varchar(24) DEFAULT NULL,
    provider_aco varchar(24) DEFAULT NULL,
    provider_health_system varchar(75) DEFAULT NULL,
    provider_medical_group varchar(75) DEFAULT NULL,
    facility_id varchar(30) DEFAULT NULL,
    member_count int DEFAULT NULL::int,
    episode_count int DEFAULT NULL::int,
    severity_score int DEFAULT NULL::int,
    performance_score int DEFAULT NULL::int
);

ALTER TABLE ECR_Template.provider ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.provider_epid
(
    id int NOT NULL,
    provider_id varchar(30) DEFAULT NULL,
    episode_id varchar(10) DEFAULT NULL,
    member_count int DEFAULT NULL::int,
    episode_count int DEFAULT NULL::int,
    severity_score int DEFAULT NULL::int
);

ALTER TABLE ECR_Template.provider_epid ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.provider_epid_level
(
    id int NOT NULL,
    provider_id varchar(30) DEFAULT NULL,
    episode_id varchar(10) DEFAULT NULL,
    level int DEFAULT NULL::int,
    split int DEFAULT 0,
    annualized int DEFAULT 0,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_t numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_tc numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_c numeric(40,20) DEFAULT NULL::numeric(1,0)
);

ALTER TABLE ECR_Template.provider_epid_level ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.provider_master_epid_level
(
    id int NOT NULL,
    provider_id varchar(24) DEFAULT NULL,
    master_episode_id varchar(73) DEFAULT NULL,
    level int DEFAULT NULL::int,
    split int DEFAULT 0,
    annualized int DEFAULT 0,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_t numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_tc numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_c numeric(40,20) DEFAULT NULL::numeric(1,0)
);

ALTER TABLE ECR_Template.provider_master_epid_level ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.provider_specialty
(
    id int NOT NULL,
    provider_id varchar(30) DEFAULT NULL,
    specialty_id varchar(24) DEFAULT NULL,
    specialty_position int DEFAULT NULL::int
);

ALTER TABLE ECR_Template.provider_specialty ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.report_episode_detail
(
    Filter_ID int DEFAULT NULL::int,
    Member_ID varchar(50) DEFAULT NULL,
    Master_Episode_ID varchar(255) DEFAULT NULL,
    Episode_ID varchar(6) DEFAULT NULL,
    Episode_Name varchar(6) DEFAULT NULL,
    Episode_Description varchar(255) DEFAULT NULL,
    Episode_Type varchar(50) DEFAULT NULL,
    MDC int DEFAULT NULL::int,
    MDC_Description varchar(255) DEFAULT NULL,
    Episode_Begin_Date varchar(10) DEFAULT NULL,
    Episode_End_Date varchar(10) DEFAULT NULL,
    Episode_Length int DEFAULT NULL::int,
    Level int DEFAULT NULL::int,
    Split_Total_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_1stPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_99thPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_80thPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_Total_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_Total_Typical_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_Total_TypicalwPAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_1stPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_99thPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_80thPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_Typical_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_TypicalwPAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_1stPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_99thPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_Typical_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_TypicalwPAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_1stPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_99thPercentile_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_Typical_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_TypicalwPAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Facility_ID varchar(50) DEFAULT NULL,
    Physician_ID varchar(50) DEFAULT NULL,
    Physician_Specialty varchar(2) DEFAULT NULL,
    Split_Expected_Total_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_Expected_Typical_IP_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_Expected_Typical_Other_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_Expected_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Expected_Total_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Expected_Typical_IP_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Expected_Typical_Other_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Expected_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE ECR_Template.risk_factors
(
    id int NOT NULL,
    code_value varchar(12) DEFAULT NULL,
    code_nomen varchar(7) DEFAULT NULL,
    risk_factor_id varchar(14) DEFAULT NULL,
    risk_factor_name varchar(80) DEFAULT NULL
);

ALTER TABLE ECR_Template.risk_factors ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.run
(
    uid int NOT NULL,
    submitter_name varchar(75) DEFAULT NULL,
    submitted_date date DEFAULT NULL::date,
    run_name varchar(75) DEFAULT NULL,
    data_start_date date DEFAULT NULL::date,
    data_end_date date DEFAULT NULL::date,
    data_latest_begin_date date DEFAULT NULL::date,
    enrolled_population int DEFAULT NULL::int,
    run_date date DEFAULT NULL::date,
    data_approved_date date DEFAULT NULL::date,
    metadata_version varchar(100) DEFAULT NULL,
    metadata_custom int DEFAULT 0,
    episode_count int DEFAULT NULL::int,
    episode_cost numeric(15,4) DEFAULT NULL::numeric(1,0),
    unassigned_cost numeric(15,4) DEFAULT NULL::numeric(1,0),
    jobUid int DEFAULT NULL::int
);

ALTER TABLE ECR_Template.run ADD CONSTRAINT C_PRIMARY PRIMARY KEY (uid) DISABLED;

CREATE TABLE ECR_Template.tmp_annids
(
    master_episode_id varchar(73) DEFAULT NULL,
    parent_child_id varchar(2) DEFAULT NULL
);


CREATE TABLE ECR_Template.tmp_annids_c
(
    master_episode_id varchar(73) DEFAULT NULL
);


CREATE TABLE ECR_Template.tmp_ann_mel
(
    master_episode_id varchar(73) DEFAULT NULL,
    level int DEFAULT NULL::int,
    parent_child_id varchar(2) DEFAULT NULL,
    cost numeric(15,4) DEFAULT 0.0000,
    cost_t numeric(15,4) DEFAULT 0.0000,
    cost_tc numeric(15,4) DEFAULT 0.0000,
    cost_c numeric(15,4) DEFAULT 0.0000
);


CREATE TABLE ECR_Template.tmp_enroll_gap
(
    episode_begin_date date DEFAULT NULL::date,
    episode_end_date date DEFAULT NULL::date,
    gap_begin_date date DEFAULT NULL::date,
    gap_end_date date DEFAULT NULL::date,
    episode_length int DEFAULT NULL::int,
    gap_length int DEFAULT NULL::int,
    master_episode_id varchar(73) DEFAULT NULL,
    member_id varchar(50) DEFAULT NULL,
    gap_length_in_episode int DEFAULT NULL::int,
    filter_fail varbinary(80) DEFAULT '0'::varbinary(1)
);


CREATE TABLE ECR_Template.tmp_filt_proc_orp_trig
(
    master_episode_id varchar(73) DEFAULT NULL
);


CREATE TABLE ECR_Template.tmp_hlow
(
    episode_id varchar(10) DEFAULT NULL,
    low numeric(26,10) DEFAULT NULL::numeric(1,0),
    high numeric(26,10) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE ECR_Template.tmp_proc_orph
(
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    claim_line_type_code varchar(12) DEFAULT NULL,
    begin_date date DEFAULT NULL::date,
    end_date date DEFAULT NULL::date,
    confirmed int DEFAULT 0
);


CREATE TABLE ECR_Template.tmp_sub_association
(
    uber_master_episode_id varchar(73) DEFAULT NULL,
    parent_master_episode_id varchar(73) DEFAULT NULL,
    child_master_episode_id varchar(73) DEFAULT NULL,
    association_type varchar(24) DEFAULT NULL,
    association_level int DEFAULT NULL::int,
    association_count int DEFAULT NULL::int,
    association_sub_level int DEFAULT NULL::int,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_t numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_tc numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_c numeric(40,20) DEFAULT NULL::numeric(1,0),
    association_sub_level_inv int DEFAULT NULL::int
);


CREATE TABLE ECR_Template.tmp_total_cost
(
    level int DEFAULT NULL::int,
    total_cost_unsplit numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_cost_split numeric(40,20) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE ECR_Template.tmp_uber
(
    id int NOT NULL,
    parent_master_episode_id varchar(73) DEFAULT NULL,
    child_master_episode_id varchar(73) DEFAULT NULL,
    association_type varchar(24) DEFAULT NULL,
    association_level int DEFAULT NULL::int,
    association_count int DEFAULT NULL::int,
    association_start_day varchar(15) DEFAULT NULL,
    association_end_day varchar(15) DEFAULT NULL
);

ALTER TABLE ECR_Template.tmp_uber ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.triggers
(
    id int NOT NULL,
    member_id varchar(50) DEFAULT NULL,
    claim_id varchar(50) DEFAULT NULL,
    claim_line_id varchar(22) DEFAULT NULL,
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    episode_id varchar(10) DEFAULT NULL,
    episode_type varchar(1) DEFAULT NULL,
    trig_begin_date date DEFAULT NULL::date,
    trig_end_date date DEFAULT NULL::date,
    episode_begin_date date DEFAULT NULL::date,
    episode_end_date date DEFAULT NULL::date,
    orig_episode_begin_date date DEFAULT NULL::date,
    orig_episode_end_date date DEFAULT NULL::date,
    look_back varchar(4) DEFAULT NULL,
    look_ahead varchar(4) DEFAULT NULL,
    req_conf_claim int DEFAULT 0,
    conf_claim_id varchar(50) DEFAULT NULL,
    conf_claim_line_id varchar(10) DEFAULT NULL,
    min_code_separation int DEFAULT NULL::int,
    max_code_separation varchar(6) DEFAULT NULL,
    trig_by_episode_id varchar(10) DEFAULT NULL,
    trig_by_master_episode_id varchar(73) DEFAULT NULL,
    dx_pass_code varchar(12) DEFAULT NULL,
    px_pass_code varchar(12) DEFAULT NULL,
    em_pass_code varchar(12) DEFAULT NULL,
    conf_dx_pass_code varchar(12) DEFAULT NULL,
    conf_px_pass_code varchar(12) DEFAULT NULL,
    conf_em_pass_code varchar(12) DEFAULT NULL,
    dropped int DEFAULT 0,
    truncated int DEFAULT 0,
    win_claim_id varchar(50) DEFAULT NULL,
    win_master_claim_id varchar(73) DEFAULT NULL
);

ALTER TABLE ECR_Template.triggers ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE ECR_Template.unassigned_episodes
(
    member_id varchar(50) DEFAULT NULL,
    episode_id varchar(10) DEFAULT NULL,
    episode_begin_date date DEFAULT NULL::date,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0)
);



CREATE PROJECTION ECR_Template.build_episode_reference /*+createtype(L)*/ 
(
 EPISODE_ID,
 NAME,
 TYPE,
 STATUS,
 DESCRIPTION,
 CREATED_DATE,
 MODIFIED_DATE,
 USERS_USER_ID,
 MDC_CATEGORY,
 PARM_SET,
 TRIGGER_TYPE,
 TRIGGER_NUMBER,
 SEPARATION_MIN,
 SEPARATION_MAX,
 BOUND_OFFSET,
 BOUND_LENGTH,
 CONDITION_MIN,
 VERSION,
 END_OF_STUDY
)
AS
 SELECT build_episode_reference.EPISODE_ID,
        build_episode_reference.NAME,
        build_episode_reference.TYPE,
        build_episode_reference.STATUS,
        build_episode_reference.DESCRIPTION,
        build_episode_reference.CREATED_DATE,
        build_episode_reference.MODIFIED_DATE,
        build_episode_reference.USERS_USER_ID,
        build_episode_reference.MDC_CATEGORY,
        build_episode_reference.PARM_SET,
        build_episode_reference.TRIGGER_TYPE,
        build_episode_reference.TRIGGER_NUMBER,
        build_episode_reference.SEPARATION_MIN,
        build_episode_reference.SEPARATION_MAX,
        build_episode_reference.BOUND_OFFSET,
        build_episode_reference.BOUND_LENGTH,
        build_episode_reference.CONDITION_MIN,
        build_episode_reference.VERSION,
        build_episode_reference.END_OF_STUDY
 FROM ECR_Template.build_episode_reference
 ORDER BY build_episode_reference.EPISODE_ID
SEGMENTED BY hash(build_episode_reference.EPISODE_ID) ALL NODES KSAFE 1;

CREATE PROJECTION ECR_Template.filter_params /*+createtype(L)*/ 
(
 filter_id,
 episode_id,
 lower_age_limit,
 upper_age_limit
)
AS
 SELECT filter_params.filter_id,
        filter_params.episode_id,
        filter_params.lower_age_limit,
        filter_params.upper_age_limit
 FROM ECR_Template.filter_params
 ORDER BY filter_params.filter_id,
          filter_params.episode_id,
          filter_params.lower_age_limit,
          filter_params.upper_age_limit
SEGMENTED BY hash(filter_params.filter_id, filter_params.lower_age_limit, filter_params.upper_age_limit, filter_params.episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION ECR_Template.filters /*+createtype(L)*/ 
(
 filter_id,
 fiilter_name
)
AS
 SELECT filters.filter_id,
        filters.fiilter_name
 FROM ECR_Template.filters
 ORDER BY filters.filter_id
SEGMENTED BY hash(filters.filter_id) ALL NODES KSAFE 1;

CREATE PROJECTION ECR_Template.mdc_desc /*+createtype(L)*/ 
(
 mdc,
 mdc_description
)
AS
 SELECT mdc_desc.mdc,
        mdc_desc.mdc_description
 FROM ECR_Template.mdc_desc
 ORDER BY mdc_desc.mdc
SEGMENTED BY hash(mdc_desc.mdc) ALL NODES KSAFE 1;



