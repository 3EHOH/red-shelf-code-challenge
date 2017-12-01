CREATE SCHEMA epbuilder;

CREATE SEQUENCE epbuilder.mel_seq ;
CREATE SEQUENCE epbuilder.assignmnt_seq ;
CREATE SEQUENCE epbuilder.pa_seq ;

CREATE TABLE epbuilder.duplciates
(
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    assigned_count int DEFAULT NULL::int,
    count int NOT NULL DEFAULT 0,
    allowed_amt numeric(24,18) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.duplciates_check
(
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    count int NOT NULL DEFAULT 0,
    allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.duplciates_test
(
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    count int NOT NULL DEFAULT 0
);


CREATE TABLE epbuilder.DYO_Savings_Calc_Summary
(
    episode_id varchar(6) DEFAULT NULL,
    episode_name varchar(6) DEFAULT NULL,
    level int DEFAULT NULL::int,
    episode_volume numeric(18,0) DEFAULT NULL::numeric(1,0),
    split_total_cost numeric(18,2) DEFAULT NULL::numeric(1,0),
    average_split_costs numeric(18,6) DEFAULT NULL::numeric(1,0),
    average_annualized_split_costs numeric(18,6) DEFAULT NULL::numeric(1,0),
    average_expected_split_costs numeric(18,6) DEFAULT NULL::numeric(1,0),
    "50thPercentile_RATIO" numeric(4,2) DEFAULT NULL::numeric(1,0),
    "80thPercentile_RATIO" numeric(4,2) DEFAULT NULL::numeric(1,0),
    "98thPercentile_RATIO" numeric(4,2) DEFAULT NULL::numeric(1,0),
    Reduction_to_50th_Percentile numeric(18,4) DEFAULT NULL::numeric(1,0),
    PERCENT_SAVINGS_AT_50th numeric(18,8) DEFAULT NULL::numeric(1,0),
    Reduction_to_80th_Percentile numeric(18,4) DEFAULT NULL::numeric(1,0),
    PERCENT_SAVINGS_AT_80th numeric(18,8) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.episode_orig
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

ALTER TABLE epbuilder.episode_orig ADD CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED;

CREATE TABLE epbuilder.PMPY_Dataset_Costs
(
    claim_line_type_code varchar(12) DEFAULT NULL,
    Year varchar(4) DEFAULT NULL,
    Total_Cost numeric(40,20) DEFAULT NULL::numeric(1,0),
    Unique_member_id_count int NOT NULL DEFAULT 0,
    Average_Cost numeric(40,24) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.RED_NO_DUP
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
    Unsplit_Expected_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    IP_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    OP_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    PB_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    RX_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    LEVEL_1 varchar(73) DEFAULT NULL,
    LEVEL_2 varchar(73) DEFAULT NULL,
    LEVEL_3 varchar(73) DEFAULT NULL,
    LEVEL_4 varchar(73) DEFAULT NULL,
    LEVEL_5 varchar(73) DEFAULT NULL
);


CREATE TABLE epbuilder.savings_calc_epis
(
    filter_id int DEFAULT NULL::int,
    member_id varchar(50) DEFAULT NULL,
    master_episode_id varchar(255) DEFAULT NULL,
    episode_id varchar(6) DEFAULT NULL,
    episode_name varchar(6) DEFAULT NULL,
    episode_type varchar(50) DEFAULT NULL,
    MDC int DEFAULT NULL::int,
    mdc_description varchar(255) DEFAULT NULL,
    level int DEFAULT NULL::int,
    episode_begin_date varchar(10) DEFAULT NULL,
    episode_end_date varchar(10) DEFAULT NULL,
    Split_Total_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_Total_Typical_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_Typical_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_Average_Typical_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Average_Typical_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Potential_Typical_Savings numeric(14,2) DEFAULT NULL::numeric(1,0),
    Potential_PAC_Savings numeric(17,6) DEFAULT NULL::numeric(1,0),
    level_1 varchar(73) DEFAULT NULL,
    level_2 varchar(73) DEFAULT NULL,
    level_3 varchar(73) DEFAULT NULL,
    level_4 varchar(73) DEFAULT NULL,
    level_5 varchar(73) DEFAULT NULL
);


CREATE TABLE epbuilder.tmp_mel1_gtzero_t
(
    master_episode_id varchar(73) DEFAULT NULL,
    cost_gtzero_t numeric(15,4) DEFAULT 0.0000,
    cost_t_gtzero_t numeric(15,4) DEFAULT 0.0000,
    cost_c_gtzero_t numeric(15,4) DEFAULT 0.0000
);


CREATE TABLE epbuilder.tmp_mel1_gtzero_tnot
(
    master_episode_id varchar(73) DEFAULT NULL,
    cost_gtzero_tnot numeric(15,4) DEFAULT 0.0000,
    cost_t_gtzero_tnot numeric(15,4) DEFAULT 0.0000,
    cost_c_gtzero_tnot numeric(15,4) DEFAULT 0.0000
);


CREATE TABLE epbuilder.tmp_mel1_zero_t
(
    master_episode_id varchar(73) DEFAULT NULL,
    cost_zero_t numeric(15,4) DEFAULT 0.0000,
    cost_t_zero_t numeric(15,4) DEFAULT 0.0000,
    cost_c_zero_t numeric(15,4) DEFAULT 0.0000
);


CREATE TABLE epbuilder.tmp_mel1_zero_tnot
(
    master_episode_id varchar(73) DEFAULT NULL,
    cost_zero_tnot numeric(15,4) DEFAULT 0.0000,
    cost_t_zero_tnot numeric(15,4) DEFAULT 0.0000,
    cost_c_zero_tnot numeric(15,4) DEFAULT 0.0000
);


CREATE TABLE epbuilder.omh_harp
(
    ENCRYPTED_ID varchar(11),
    OMH_HARP_IND char(1),
    OASAS_HARP_IND char(1),
    PREV_SENT_IND char(1),
    RECIP_COUNTY_CODE char(2),
    RECIP_COUNTY_NAME varchar(12),
    NYC_ROS_IND char(3),
    LESS2M_MC_PLAN_ID varchar(8),
    LESS2M_MC_PLAN_NAME varchar(35),
    LESS2M_MC_PLAN_TYPE varchar(4),
    LESS2M_MCAID_ELIG_IND char(1),
    LESS2M_MCARE_ELIG_IND char(1),
    LESS2M_HH_OUTREACH_IND char(1),
    LESS2M_HH_ENGAGE_IND char(1),
    LESS1M_MC_PLAN_ID varchar(8),
    LESS1M_MC_PLAN_NAME varchar(35),
    LESS1M_MC_PLAN_TYPE varchar(4),
    LESS1M_MCAID_ELIG_IND char(1),
    LESS1M_MCARE_ELIG_IND char(1),
    LESS1M_HH_OUTREACH_IND char(1),
    LESS1M_HH_ENGAGE_IND char(1),
    CURR_MC_PLAN_ID varchar(8),
    CURR_MC_PLAN_NAME varchar(35),
    CURR_MC_PLAN_TYPE varchar(4),
    CURR_MCAID_ELIG_IND char(1),
    CURR_MCARE_ELIG_IND char(1),
    CURR_HH_OUTREACH_IND char(1),
    CURR_HH_ENGAGE_IND char(1),
    LESS2M_ENROLL_MC_DOH_OMC_IND varchar(7),
    LESS2M_ENROLL_MC_PLAN_ID varchar(8),
    LESS2M_ENROLL_MC_PLAN_NAME varchar(53),
    LESS2M_ENROLL_MC_PLAN_TYPE varchar(18),
    LESS1M_ENROLL_MC_DOH_OMC_IND varchar(7),
    LESS1M_ENROLL_MC_PLAN_ID varchar(8),
    LESS1M_ENROLL_MC_PLAN_NAME varchar(53),
    LESS1M_ENROLL_MC_PLAN_TYPE varchar(18),
    CURR_ENROLL_MC_DOH_OMC_ID varchar(7),
    CURR_ENROLL_MC_PLAN_ID varchar(8),
    CURR_ENROLL_MC_PLAN_NAME varchar(53),
    CURR_ENROLL_MC_PLAN_TYPE varchar(18),
    CURR_RECIP_AGE varchar(3),
    RECIP_DOB date,
    RECIP_MONTH_OF_BIRTH varchar(2),
    OMC_OR_PLAN_ID varchar(8),
    MMC_PLAN_NAME_AGG varchar(35),
    TRANS_DIST_78_IND char(1),
    CURR_COMB_PLAN_ID varchar(8),
    CURR_COMB_PLAN_NAME varchar(53),
    CURR_COMB_PLAN_TYPE varchar(18),
    HARP_LIST_ELIGIBLE_IND char(1),
    ENROLLED_IND char(1),
    WAS_ASSESSED_IND char(1),
    FROM_ENROLL_H_CODE varchar(2),
    FROM_ENROLL_BEG_DT date,
    FROM_ENROLL_END_DT date,
    EXCEPTION_CHANGE_DATE date,
    ASSESSMENT_DATE date,
    ASSESSMENT_TIER varchar(9),
    LAST_SENT_BEG_DT date,
    LAST_SENT_END_DT date,
    LAST_SENT_HAS_ERROR char(1),
    LAST_SENT_CODE varchar(2),
    LAST_SENT_SUBMITTED_DT date,
    CURR_ACTUAL_H_CODE varchar(2),
    CURR_CALCULATED_H_CODE varchar(2),
    Sent_or_elegible varchar(2)
);


CREATE TABLE epbuilder.ccs_grouping
(
    epi_id varchar(10),
    mdc varchar(10),
    ccs_group varchar(200),
    code varchar(10),
    code_desc varchar(500),
    subgroups varchar(100)
);


CREATE TABLE epbuilder.enrollment_raw_2012
(
    year int,
    month int,
    member_id varchar(50),
    dob varchar(20),
    sex varchar(5),
    dod varchar(500)
);


CREATE TABLE epbuilder.enrollment_raw_2013
(
    year int,
    month int,
    member_id varchar(50),
    dob varchar(20),
    sex varchar(5),
    dod varchar(500)
);


CREATE TABLE epbuilder.tmp_provider_attribution_em
(
    master_episode_id varchar(73) DEFAULT NULL,
    physician_id varchar(24) DEFAULT NULL,
    phys_count int NOT NULL DEFAULT 0,
    phys_cost numeric(62,20) DEFAULT NULL::numeric(1,0),
    max_count binary(1) DEFAULT NULL::binary,
    max_cost binary(1) DEFAULT NULL::binary
);


CREATE TABLE epbuilder.tmp_prov_attr
(
    master_episode_id varchar(73) DEFAULT NULL,
    physician_id varchar(24) DEFAULT NULL,
    max_count int NOT NULL DEFAULT 0,
    max_cost int NOT NULL DEFAULT 0
);


CREATE TABLE epbuilder.episode_to_code
(
    EPISODE_ID varchar(80) NOT NULL,
    FUNCTION_ID varchar(2) NOT NULL,
    CODE_VALUE varchar(10) NOT NULL DEFAULT '',
    CODE_TYPE_ID varchar(4) NOT NULL,
    CODE_MULTUM_CATEGORY varchar(3) NOT NULL,
    SUBTYPE_ID char(6) DEFAULT NULL::"char",
    COMPLICATION int DEFAULT NULL::int,
    SUFFICIENT int DEFAULT NULL::int,
    CORE int DEFAULT NULL::int,
    VETTED int DEFAULT NULL::int,
    PAS int DEFAULT NULL::int,
    CORE_QUANTITY float DEFAULT NULL::float,
    IS_POS int DEFAULT NULL::int,
    QUALIFYING_DIAGNOSIS int DEFAULT 0,
    RX_FUNCTION varchar(10) DEFAULT NULL
);


CREATE TABLE epbuilder.Descriptive_Stats
(
    Field varchar(100) DEFAULT NULL,
    Data varchar(100) DEFAULT NULL
);


CREATE TABLE epbuilder.enrollment_raw_new
(
    year numeric(18,0),
    month int,
    date date,
    member_id varchar(50),
    insurance_product varchar(24),
    birth_year int,
    sex varchar(2),
    dod date
);


CREATE TABLE epbuilder.claims_dsrp_info
(
    id int,
    master_claim_id varchar(100),
    member_id varchar(50),
    allowed_amt numeric(40,20),
    assigned_count int,
    claim_line_type_code varchar(12),
    begin_date date,
    end_date date,
    claim_id varchar(50),
    type_of_bill varchar(12),
    facility_type_code varchar(8),
    place_of_svc_code varchar(25),
    function_code varchar(10),
    code_value varchar(12),
    nomen varchar(7),
    principal int,
    master_episode_id varchar(73),
    assigned_type varchar(2),
    LEVEL_5 varchar(73),
    LEVEL_5_association_type varchar(4),
    BETOS_CATEGORY varchar(3),
    PB_OTHER int,
    OP_ED int,
    PAS int,
    Chronic_bundle int,
    IPC int,
    PREVENT int
);


CREATE TABLE epbuilder.preg_assign_analysis
(
    master_episode_id varchar(73),
    allowed_amt numeric(40,20),
    ASSIGNED_TO_PREGN int,
    ASSIGNED_TO_VAGDEL int,
    ASSIGNED_TO_CSECT int,
    ASSIGNED_TO_PREVENT int
);


CREATE TABLE epbuilder.QCRG_DETAIL
(
    QACRG1_CODE char(6) NOT NULL,
    QACRG2_CODE char(4) NOT NULL,
    QACRG3_CODE char(2) NOT NULL,
    QACRG1_DESC varchar(105) DEFAULT NULL,
    QACRG2_DESC varchar(150) DEFAULT NULL,
    QACRG3_DESC varchar(92) DEFAULT NULL,
    QCRG_CODE char(5) NOT NULL,
    QCRG_DESC varchar(108) DEFAULT NULL
);


CREATE TABLE epbuilder.tmp_enroll_gap1
(
    master_episode_id varchar(73),
    member_id varchar(50),
    episode_length int,
    gap_length int,
    filter_fail int
);


CREATE TABLE epbuilder.tmp_proc_orph_conf
(
    master_episode_id varchar(73),
    master_claim_id varchar(100),
    claim_line_type_code varchar(12),
    begin_date date,
    end_date date,
    confirmed int
);


CREATE TABLE epbuilder.tmp_proc_orph_conf2
(
    master_episode_id varchar(73),
    master_claim_id varchar(100),
    claim_line_type_code varchar(12),
    begin_date date,
    end_date date,
    confirmed int
);


CREATE TABLE epbuilder.filtered_episodes_TESTING
(
    filter_id int,
    master_episode_id varchar(73),
    filter_fail int,
    age_limit_lower int,
    age_limit_upper int,
    coverage_gap int,
    proc_ep_orphan int,
    proc_ep_orph_triggered int,
    episode_complete int,
    drg int,
    episode_cost_lower int,
    episode_cost_upper int
);


CREATE TABLE epbuilder.enrollment_raw_old
(
    year int,
    month int,
    member_id varchar(50),
    dob varchar(20),
    sex varchar(5),
    dod varchar(500)
);


CREATE TABLE epbuilder.MTRNTY_nborn_delivery
(
    del_meid varchar(73),
    nborn_meid varchar(73)
);


CREATE TABLE epbuilder.report_episode_detail_revised_justincase
(
    Filter_ID int,
    Member_ID varchar(50),
    Master_Episode_ID varchar(255),
    Episode_ID varchar(6),
    Episode_Name varchar(6),
    Episode_Description varchar(255),
    Episode_Type varchar(50),
    MDC varchar(2),
    MDC_Description varchar(255),
    Episode_Begin_Date varchar(10),
    Episode_End_Date varchar(10),
    Episode_Length int,
    Level int,
    Split_Total_Cost numeric(13,2),
    Split_1stPercentile_Cost numeric(13,2),
    Split_99thPercentile_Cost numeric(13,2),
    Split_80thPercentile_Cost numeric(13,2),
    Split_Total_PAC_Cost numeric(13,2),
    Split_Total_Typical_Cost numeric(13,2),
    Split_Total_TypicalwPAC_Cost numeric(13,2),
    Annualized_Split_Total_Cost numeric(13,2),
    Annualized_Split_1stPercentile_Cost numeric(13,2),
    Annualized_Split_99thPercentile_Cost numeric(13,2),
    Annualized_Split_80thPercentile_Cost numeric(13,2),
    Annualized_Split_Total_PAC_Cost numeric(13,2),
    Annualized_Split_Total_Typical_Cost numeric(13,2),
    Annualized_Split_Total_TypicalwPAC_Cost numeric(13,2),
    Unsplit_Total_Cost numeric(13,2),
    Unsplit_1stPercentile_Cost numeric(13,2),
    Unsplit_99thPercentile_Cost numeric(13,2),
    Unsplit_Total_PAC_Cost numeric(13,2),
    Unsplit_Total_Typical_Cost numeric(13,2),
    Unsplit_Total_TypicalwPAC_Cost numeric(13,2),
    Annualized_Unsplit_Total_Cost numeric(13,2),
    Annualized_Unsplit_1stPercentile_Cost numeric(13,2),
    Annualized_Unsplit_99thPercentile_Cost numeric(13,2),
    Annualized_Unsplit_Total_PAC_Cost numeric(13,2),
    Annualized_Unsplit_Total_Typical_Cost numeric(13,2),
    Annualized_Unsplit_Total_TypicalwPAC_Cost numeric(13,2),
    Facility_ID varchar(50),
    Physician_ID varchar(50),
    Physician_Specialty varchar(2),
    Split_Expected_Total_Cost numeric(13,2),
    Split_Expected_Typical_IP_Cost numeric(13,2),
    Split_Expected_Typical_Other_Cost numeric(13,2),
    Split_Expected_PAC_Cost numeric(13,2),
    Unsplit_Expected_Total_Cost numeric(13,2),
    Unsplit_Expected_Typical_IP_Cost numeric(13,2),
    Unsplit_Expected_Typical_Other_Cost numeric(13,2),
    Unsplit_Expected_PAC_Cost numeric(13,2),
    IP_PAC_Count numeric(13,2),
    OP_PAC_Count numeric(13,2),
    PB_PAC_Count numeric(13,2),
    RX_PAC_Count numeric(13,2)
);


CREATE TABLE epbuilder.srf_pac_count
(
    episode_id varchar(6),
    mcregion varchar(100),
    Number_of_Episodes float,
    Number_of_Epis_w_PAC float,
    year numeric(18,0),
    expected_pac_count float
);


CREATE TABLE epbuilder.GCRG_DETAIL
(
    ACRG1_CODE char(6) NOT NULL,
    ACRG2_CODE char(4) NOT NULL,
    ACRG3_CODE char(2) NOT NULL,
    ACRG1_DESC varchar(140) DEFAULT NULL,
    ACRG2_DESC varchar(140) DEFAULT NULL,
    ACRG3_DESC varchar(140) DEFAULT NULL,
    CRG_CODE char(5) NOT NULL,
    CRG_DESC varchar(140) DEFAULT NULL
);


CREATE TABLE epbuilder.crg_2012_2013_2
(
    year int NOT NULL,
    quarter varchar(2) NOT NULL,
    crg_version varchar(13) NOT NULL,
    claim_begin_date date NOT NULL,
    claim_end_date date NOT NULL,
    claims_as_of_date date NOT NULL,
    recip_id char(11) NOT NULL,
    qcrg_code int NOT NULL,
    qcrg_desc varchar(140),
    qacrg1_code int NOT NULL,
    qacrg1_desc varchar(140),
    qacrg2_code int NOT NULL,
    qacrg2_desc varchar(140),
    qacrg3_code int NOT NULL,
    qacrg3_desc varchar(140),
    fincrg_q varchar(3) NOT NULL,
    q_base varchar(2) NOT NULL,
    q_severity varchar(1) NOT NULL,
    crg_code int NOT NULL,
    crg_desc varchar(140),
    acrg1_code int NOT NULL,
    acrg1_desc varchar(140),
    acrg2_code int NOT NULL,
    acrg2_desc varchar(140),
    acrg3_code int NOT NULL,
    acrg3_desc varchar(140),
    fincrg_g varchar(3) NOT NULL,
    g_base varchar(2) NOT NULL,
    g_severity varchar(1) NOT NULL,
    fincrg varchar(3) NOT NULL,
    base varchar(2) NOT NULL,
    severity varchar(1) NOT NULL,
    total_allowed_amount numeric(13,2) DEFAULT NULL::numeric(1,0),
    total_medical_allowed numeric(13,2) DEFAULT NULL::numeric(1,0),
    total_pharmacy_allowed numeric(13,2) DEFAULT NULL::numeric(1,0),
    total_episode_costs numeric(13,2) DEFAULT NULL::numeric(1,0),
    acrg_weight float DEFAULT NULL::float,
    qcrg_weight float DEFAULT NULL::float,
    acrg_average numeric(13,2) DEFAULT NULL::numeric(1,0),
    qcrg_average numeric(13,2) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.unassigned_px_dx_by_epi
(
    episode_id varchar(10),
    code_value varchar(12),
    dx_code varchar(12),
    count int,
    in_epi int,
    sum numeric(40,20)
);


CREATE TABLE epbuilder.zipcode_county
(
    County varchar(50),
    State_FIPS varchar(5),
    County_Code varchar(2),
    County_FIPS varchar(10),
    zip varchar(20),
    File_Date varchar(20)
);


CREATE TABLE epbuilder.MBR_MOTHER_BABY
(
    MOTHER_MBR_ID varchar(11) NOT NULL,
    BABY_MBR_ID varchar(11) NOT NULL,
    BIRTH_YR varchar(4) NOT NULL,
    MDW_EFF_DT date NOT NULL,
    MDW_EXP_DT date,
    INS_ROW_PRCS_DT date NOT NULL,
    MDW_INS_DT date NOT NULL,
    MDW_INS_ID numeric(38,0) NOT NULL,
    MDW_UPDT_DT date,
    MDW_UPDT_ID numeric(38,0)
);

ALTER TABLE epbuilder.MBR_MOTHER_BABY ADD CONSTRAINT XPKMEMBER_MOTHER_BABY PRIMARY KEY (MOTHER_MBR_ID, BABY_MBR_ID, BIRTH_YR, MDW_EFF_DT) DISABLED;

CREATE TABLE epbuilder.MBR_CRG_FACT
(
    YEAR varchar(4),
    QUARTER varchar(2),
    CRG_VERSION varchar(13),
    CLAIM_BEGIN_DATE date,
    CLAIM_END_DATE date,
    CLAIMS_AS_OF_DATE date,
    RECIP_ID varchar(8),
    QCRG_CODE varchar(5),
    QACRG1_CODE varchar(6),
    QACRG2_CODE varchar(4),
    QACRG3_CODE varchar(2),
    FINCRG_Q varchar(3),
    Q_BASE varchar(2),
    Q_SEVERITY varchar(1),
    CRG_CODE varchar(5),
    ACRG1_CODE varchar(6),
    ACRG2_CODE varchar(4),
    ACRG3_CODE varchar(2),
    FINCRG_G varchar(3),
    G_BASE varchar(2),
    G_SEVERITY varchar(1),
    FINCRG varchar(3),
    BASE varchar(2),
    SEVERITY varchar(1),
    MDW_INS_DT date NOT NULL,
    MDW_UPDT_DT date,
    MDW_INS_ID numeric(38,0) NOT NULL,
    MDW_UPDT_ID numeric(38,0),
    INS_ROW_PRCS_DT date NOT NULL
);


CREATE TABLE epbuilder.mbr_crg_fact_quinn
(
    YEAR int,
    QUARTER varchar(2),
    CRG_VERSION varchar(13),
    CLAIM_BEGIN_DATE date,
    CLAIM_END_DATE date,
    CLAIMS_AS_OF_DATE date,
    RECIP_ID varchar(8),
    QCRG_CODE int,
    QACRG1_CODE int,
    QACRG2_CODE int,
    QACRG3_CODE int,
    FINCRG_Q varchar(3),
    Q_BASE varchar(2),
    Q_SEVERITY varchar(1),
    CRG_CODE varchar(5),
    ACRG1_CODE int,
    ACRG2_CODE int,
    ACRG3_CODE int,
    FINCRG_G varchar(3),
    G_BASE varchar(2),
    G_SEVERITY varchar(1)
);


CREATE TABLE epbuilder.member_sub_population
(
    member_id varchar(100) DEFAULT NULL,
    sub_population varchar(20) DEFAULT NULL,
    gender varchar(2) DEFAULT NULL,
    birth_year int DEFAULT NULL::int,
    age_group varchar(20) DEFAULT NULL
);


CREATE TABLE epbuilder.member_sub_population_exclusive
(
    member_id varchar(100),
    sub_population varchar(20),
    gender varchar(2),
    birth_year int,
    age_group varchar(20)
);


CREATE TABLE epbuilder.claims_combined_with_provider_id
(
    master_claim_id varchar(100) DEFAULT NULL,
    member_id varchar(30) DEFAULT NULL,
    allowed_amt numeric(15,4) DEFAULT NULL::numeric(1,0),
    assigned_count int DEFAULT NULL::int,
    claim_line_type_code varchar(12) DEFAULT NULL,
    begin_date date DEFAULT NULL::date,
    end_date date DEFAULT NULL::date,
    provider_id varchar(30) DEFAULT NULL
);


CREATE TABLE epbuilder.vbp_contractor_provider_npi_tmp
(
    provider_npi varchar(20) DEFAULT NULL,
    vbp_contractor varchar(200) DEFAULT NULL,
    PGroup varchar(300) DEFAULT NULL
);


CREATE TABLE epbuilder.ra_expected_pac_count
(
    master_episode_id varchar(255),
    expected_pac_count float,
    raw_pac_count int
);


CREATE TABLE epbuilder.temp_m
(
    mtrnty_meid varchar(280),
    sa_exp_cost float,
    sa_exp_typical_cost float,
    sa_exp_pac_cost float
);


CREATE TABLE epbuilder.TCGP_2013_by_member
(
    member_id varchar(100),
    sub_population varchar(20),
    gender varchar(2),
    birth_year int,
    pcp varchar(100),
    total_cost numeric(40,20)
);


CREATE TABLE epbuilder.crg_cost_summary_by_member2
(
    member_id varchar(50) DEFAULT NULL,
    year varchar(20) DEFAULT NULL,
    cms_age_group varchar(255) DEFAULT NULL,
    gender varchar(20) DEFAULT NULL,
    fincrg varchar(20) DEFAULT NULL,
    actual_cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    expected_cost numeric(13,2) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.crg_cost_summary_by_member2_pps
(
    member_id varchar(50),
    year varchar(20),
    cms_age_group varchar(255),
    gender varchar(20),
    fincrg varchar(20),
    actual_cost numeric(13,2),
    expected_cost numeric(13,2),
    pps varchar(100)
);


CREATE TABLE epbuilder.expected_costs_2012
(
    gender varchar(20),
    cms_age_group varchar(255),
    fincrg varchar(20),
    member_count int,
    actual_cost_2012 numeric(31,20)
);


CREATE TABLE epbuilder.crg_cost_summary_by_member_hci3
(
    member_id varchar(50) DEFAULT NULL,
    year varchar(20) DEFAULT NULL,
    cms_age_group varchar(255) DEFAULT NULL,
    gender varchar(20) DEFAULT NULL,
    fincrg_q varchar(20) DEFAULT NULL,
    qacrg3_desc varchar(140) DEFAULT NULL,
    actual_cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    expected_cost numeric(13,2) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.crg_cost_summary_by_member_hci3_pps
(
    member_id varchar(50),
    year varchar(20),
    cms_age_group varchar(255),
    gender varchar(20),
    fincrg_q varchar(20),
    qacrg3_desc varchar(140),
    actual_cost numeric(13,2),
    expected_cost numeric(13,2),
    pps varchar(100)
);


CREATE TABLE epbuilder.TCGP_by_member_test
(
    member_id varchar(100),
    sub_population varchar(20),
    gender varchar(2),
    birth_year int,
    age_group varchar(7),
    pps varchar(100),
    pcp varchar(100),
    year numeric(18,0),
    total_cost numeric(40,20)
);


CREATE TABLE epbuilder.standard_preg_cost_info
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    PREG_master_episode_id varchar(73),
    trig_begin_date date,
    year varchar(4),
    SPLIT_TOTAL_PREGN_COST numeric(40,20),
    SPLIT_TYPICAL_PREGN_COST numeric(40,20),
    SPLIT_PAC_PREGN_COST numeric(40,20),
    UNSPLIT_TOTAL_PREGN_COST numeric(40,20),
    UNSPLIT_TYPICAL_PREGN_COST numeric(40,20),
    UNSPLIT_PAC_PREGN_COST numeric(40,20),
    date_of_firt_pregn_service date,
    months_of_pregn_services numeric(36,18),
    STANDARD_SPLIT_TOTAL numeric(112,56),
    STANDARD_SPLIT_TYPICAL numeric(112,56),
    STANDARD_SPLIT_PAC numeric(112,56),
    STANDARD_UNSPLIT_TOTAL numeric(112,56),
    STANDARD_UNSPLIT_TYPICAL numeric(112,56),
    STANDARD_UNSPLIT_PAC numeric(112,56)
);


CREATE TABLE epbuilder.ra_episode_data2
(
    epi_id varchar(73),
    member_id varchar(50),
    epi_number varchar(10),
    epi_name varchar(6),
    female int,
    age numeric(18,0),
    rec_enr int,
    eol_ind int
);


CREATE TABLE epbuilder.preg_cost_info_month
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    PREG_master_episode_id varchar(73),
    trig_begin_date date,
    year varchar(4),
    SPLIT_TOTAL_PREGN_COST numeric(40,20),
    SPLIT_TYPICAL_PREGN_COST numeric(40,20),
    SPLIT_PAC_PREGN_COST numeric(40,20),
    UNSPLIT_TOTAL_PREGN_COST numeric(40,20),
    UNSPLIT_TYPICAL_PREGN_COST numeric(40,20),
    UNSPLIT_PAC_PREGN_COST numeric(40,20),
    date_of_firt_pregn_service date,
    month_pregn_services_began int
);


CREATE TABLE epbuilder.preg_del_overlap
(
    master_claim_id varchar(100),
    allowed_amt numeric(40,20),
    count int
);


CREATE TABLE epbuilder.ra_episode_data3
(
    epi_id varchar(73),
    member_id varchar(50),
    epi_number varchar(6),
    epi_name varchar(6),
    female int,
    age int,
    rec_enr int,
    eol_ind int
);


CREATE TABLE epbuilder.ra_episode_data4js
(
    epi_id varchar(73),
    member_id varchar(50),
    epi_number varchar(6),
    epi_name varchar(6),
    female int,
    age int,
    rec_enr int,
    eol_ind int
);


CREATE TABLE epbuilder.min_enroll_date
(
    member_id varchar(50),
    min date
);


CREATE TABLE epbuilder.ra_episode_data4
(
    epi_id varchar(73),
    member_id varchar(50),
    epi_number varchar(6),
    epi_name varchar(6),
    female int,
    age int,
    rec_enr int,
    eol_ind int
);


CREATE TABLE epbuilder.PPS_TABLE_2015
(
    MEMBER_ID varchar(11),
    PPS varchar(100)
);


CREATE TABLE epbuilder.tmp_enroll_date
(
    member_id varchar(12),
    begin_date date
);


CREATE TABLE epbuilder.raw_member
(
    NATIONAL_PLAN varchar(255),
    INSURANCE_PRODUCT varchar(255),
    YEAR varchar(255),
    MONTH varchar(255),
    MEMBER_ID varchar(255),
    ENROLLMENT_START_DATE varchar(255),
    ENROLLMENT_END_DATE varchar(255),
    DUAL_ELIGIBILITY_FLAG varchar(255),
    MBR_SEX_CD varchar(255),
    BIRTH_YEAR varchar(255),
    POSTAL_CD varchar(255),
    RACE_CODE1 varchar(255),
    RACE_CODE2 varchar(255),
    HISPANIC_OR_LATINO_ETHNIC_CD varchar(255),
    PRIMARY_INSURANCE_INDICATOR varchar(255),
    PROV_ID varchar(255),
    MC_BEG_DT varchar(255),
    MEMBER_DEDUCTIBLE varchar(255),
    DISABILITY_INDICATOR_FLAG varchar(255),
    MBR_DEATH_DT varchar(255),
    PCP_NPI varchar(255),
    MDW_INS_DT varchar(255),
    MDW_INS_ID varchar(255),
    MDW_UPDT_DT varchar(255),
    MDW_UPDT_ID varchar(255)
);


CREATE TABLE epbuilder.member_mcregion_nodup
(
    member_id varchar(50),
    mcregion varchar(100)
);


CREATE TABLE epbuilder.ra_episode_data_region
(
    epi_id varchar(73),
    member_id varchar(50),
    epi_number varchar(6),
    epi_name varchar(6),
    female int,
    age int,
    rec_enr int,
    eol_ind int,
    Utica_Adirondack int,
    Central int,
    New_York_City int,
    Mid_Hudson int,
    Finger_Lakes int,
    Western int,
    Northeast int,
    Northern_Metro int,
    Long_Island int
);


CREATE TABLE epbuilder.bundle_category
(
    episode_id varchar(10),
    category varchar(80)
);


CREATE TABLE epbuilder.member_mcregion
(
    member_id varchar(50),
    zip_code varchar(16),
    mcregion varchar(100)
);


CREATE TABLE epbuilder.report_episode_detail_revised
(
    Filter_ID int DEFAULT NULL::int,
    Member_ID varchar(50) DEFAULT NULL,
    Master_Episode_ID varchar(255) NOT NULL,
    Episode_ID varchar(6) DEFAULT NULL,
    Episode_Name varchar(6) DEFAULT NULL,
    Episode_Description varchar(255) DEFAULT NULL,
    Episode_Type varchar(50) DEFAULT NULL,
    MDC varchar(2) DEFAULT NULL,
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
    Unsplit_Expected_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    IP_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    OP_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    PB_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    RX_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.exp_act
(
    epi_id varchar(73),
    epi_number varchar(255),
    master_episode_id varchar(73),
    cost numeric(40,20),
    total_exp_cost_ra_l5 float
);


CREATE TABLE epbuilder.percentiles_revised
(
    Filter_ID int DEFAULT NULL::int,
    Episode_ID varchar(6) DEFAULT NULL,
    Level int DEFAULT NULL::int,
    Split_1stPercentile_Cost float DEFAULT NULL::float,
    Split_99thPercentile_Cost float DEFAULT NULL::float,
    Split_80thPercentile_Cost float DEFAULT NULL::float,
    Annualized_Split_1stPercentile_Cost float DEFAULT NULL::float,
    Annualized_Split_99thPercentile_Cost float DEFAULT NULL::float,
    Annualized_Split_80thPercentile_Cost float DEFAULT NULL::float,
    Unsplit_1stPercentile_Cost float DEFAULT NULL::float,
    Unsplit_99thPercentile_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_1stPercentile_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_99thPercentile_Cost float DEFAULT NULL::float
);


CREATE TABLE epbuilder.pregn_with_legit_child
(
    parent_master_episode_id varchar(73)
);


CREATE TABLE epbuilder.maternity_base_episode
(
    mtrnty_meid varchar(200) DEFAULT NULL,
    member_id varchar(100) DEFAULT NULL,
    pregn_meid varchar(200) DEFAULT NULL,
    nborn_meid varchar(200) DEFAULT NULL,
    pregn_year int DEFAULT NULL::int,
    nborn_year int DEFAULT NULL::int,
    del_meid varchar(300) DEFAULT NULL
);


CREATE TABLE epbuilder.DE_status
(
    MEMBER_ID varchar(255),
    DUAL_ELIGIBILITY_FLAG varchar(255),
    AGE int
);


CREATE TABLE epbuilder.prep1_m
(
    member_id varchar(50),
    pac_count int,
    year numeric(18,0)
);


CREATE TABLE epbuilder.prep3_m
(
    member_id varchar(50),
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_count_chronic int,
    co_count_all int,
    year numeric(18,0)
);


CREATE TABLE epbuilder.prep4_m
(
    member_id varchar(50),
    assigned_cost numeric(58,38),
    pac_cost numeric(58,38),
    typical_cost numeric(58,38),
    assigned_pb_cost numeric(58,38),
    assigned_op_cost numeric(58,38),
    assigned_ip_cost numeric(58,38),
    assigned_rx_cost numeric(58,38),
    year numeric(18,0)
);


CREATE TABLE epbuilder.prep4_2_m
(
    member_id varchar(50),
    assigned_cost_unfiltered numeric(58,38),
    pac_cost numeric(58,38),
    typical_cost_unfiltered numeric(58,38),
    assigned_pb_cost_unfiltered numeric(58,38),
    assigned_op_cost_unfiltered numeric(58,38),
    assigned_ip_cost_unfiltered numeric(58,38),
    assigned_rx_cost_unfiltered numeric(58,38),
    year numeric(18,0)
);


CREATE TABLE epbuilder.prep5_m
(
    member_id varchar(50),
    episode_count int,
    episode_count_unfiltered int,
    year numeric(18,0)
);


CREATE TABLE epbuilder.prep2_m
(
    member_id varchar(50),
    pb_cost numeric(40,20),
    op_cost numeric(40,20),
    ip_cost numeric(40,20),
    rx_cost numeric(40,20),
    year numeric(18,0)
);


CREATE TABLE epbuilder.visual_analysis_table_x2
(
    analysis_type varchar(10),
    id varchar(50),
    episode_id varchar(1),
    episode_name varchar(1),
    episode_description varchar(1),
    episode_type varchar(1),
    episode_category varchar(1),
    member_id varchar(50),
    member_age float,
    cms_age_group varchar(255),
    gender varchar(20),
    member_zipcode varchar(16),
    member_county varchar(100),
    member_population varchar(20),
    total_cost numeric(13,2),
    expected_cost numeric(13,2),
    PAC_cost numeric(58,38),
    typical_cost numeric(58,38),
    expected_pac_cost numeric(1,1),
    expected_typical_cost numeric(1,1),
    ip_cost numeric(40,20),
    op_cost numeric(40,20),
    pb_cost numeric(40,20),
    rx_cost numeric(40,20),
    assigned_cost numeric(58,38),
    assigned_ip_cost numeric(58,38),
    assigned_op_cost numeric(58,38),
    assigned_pb_cost numeric(58,38),
    assigned_rx_cost numeric(58,38),
    assigned_cost_unfiltered numeric(58,38),
    assigned_ip_cost_unfiltered numeric(58,38),
    assigned_op_cost_unfiltered numeric(58,38),
    assigned_pb_cost_unfiltered numeric(58,38),
    assigned_rx_cost_unfiltered numeric(58,38),
    pps varchar(100),
    provider_id varchar(1),
    provider_name varchar(1),
    provider_zipcode varchar(1),
    provider_type varchar(1),
    mco varchar(100),
    hh varchar(100),
    pcp varchar(100),
    vbp_attrib_provider varchar(1),
    vbp_attrib_provider_zipcode varchar(1),
    vbp_contractor varchar(1),
    year numeric(37,15),
    utilization varchar(7),
    ppr varchar(1),
    ppv varchar(1),
    exclusive varchar(1),
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_count_chronic int,
    co_count_all int,
    episode_count int,
    episode_count_unfiltered int,
    episode_level int DEFAULT 0
);


CREATE TABLE epbuilder.vbp_ppe_ppr_nodup
(
    claim_id varchar(30),
    claim_trans_id numeric(38,0),
    dsch_dt date,
    claim_type_code varchar(2),
    preventable_group varchar(3),
    preventable_group_desc varchar(50),
    ppr_type_code varchar(2),
    ppr_type_code_desc varchar(100),
    mdw_insert_dt date,
    mdw_update_dt date
);


CREATE TABLE epbuilder.prep2
(
    master_episode_id varchar(260),
    pb_cost numeric(58,38),
    op_cost numeric(58,38),
    ip_cost numeric(58,38),
    rx_cost numeric(58,38)
);


CREATE TABLE epbuilder.prep3
(
    master_episode_id varchar(260),
    level int,
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_count_chronic int,
    co_count_all int,
    year numeric(18,0)
);


CREATE TABLE epbuilder.member_prep
(
    member_id varchar(50),
    birth_year int,
    gender_code varchar(2),
    zip_code varchar(16),
    county varchar(100),
    sub_population varchar(20),
    pps varchar(100),
    hh varchar(100),
    pcp varchar(100),
    mco varchar(100)
);


CREATE TABLE epbuilder.prep1
(
    master_episode_id varchar(260),
    pac_count int,
    expected_pac_count float
);


CREATE TABLE epbuilder.report_episode_detail_test
(
    Filter_ID int DEFAULT NULL::int,
    Member_ID varchar(50) DEFAULT NULL,
    Master_Episode_ID varchar(255) DEFAULT NULL,
    Episode_ID varchar(6) DEFAULT NULL,
    Episode_Name varchar(6) DEFAULT NULL,
    Episode_Description varchar(255) DEFAULT NULL,
    Episode_Type varchar(50) DEFAULT NULL,
    MDC varchar(2),
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
    Unsplit_Expected_PAC_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    IP_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    OP_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    PB_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    RX_PAC_Count numeric(13,2) DEFAULT NULL::numeric(1,0),
    year numeric(4,0),
    enrolled_month int,
    co_occurence_count_DEPANX int,
    co_occurence_count_DIVERT int,
    co_occurence_count_RHNTS int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_HTN int,
    co_occurence_count_GERD int,
    co_occurence_count_BIPLR int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_CAD int,
    co_occurence_count_COPD int,
    co_occurence_count_HF int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_ASTHMA int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_count_chronic int,
    co_count_all int,
    ip_cost float,
    op_cost float,
    pb_cost float,
    rx_cost float,
    END_OF_STUDY int,
    Facility_ID_provider_name varchar(100),
    Facility_ID_provider_zipcode varchar(100),
    Facility_ID_provider_type varchar(100),
    Physician_ID_provider_name varchar(100),
    Physician_ID_provider_zipcode varchar(1000),
    Physician_ID_provider_type varchar(100),
    state_wide_avg_split_exp_cost float,
    state_wide_avg_unsplit_exp_cost float,
    state_wide_avg_split_total_cost float,
    state_wide_avg_unsplit_total_cost float,
    split_state_wide_pac_percent float,
    unsplit_state_wide_pac_percent float
);


CREATE TABLE epbuilder.percentiles_test
(
    Filter_id int,
    Master_episode_id varchar(255),
    Episode_ID varchar(6),
    Level int,
    Split_1stPercentile_Cost float,
    Split_99thPercentile_Cost float,
    Split_80thPercentile_Cost float,
    Unsplit_1stPercentile_Cost float,
    Unsplit_99thPercentile_Cost float,
    Annualized_Split_1stPercentile_Cost float,
    Annualized_Split_99thPercentile_Cost float,
    Annualized_Split_80thPercentile_Cost float,
    Annualized_Unsplit_1stPercentile_Cost float,
    Annualized_Unsplit_99thPercentile_Cost float
);


CREATE TABLE epbuilder.non_utilizer
(
    member_id varchar(50),
    year numeric(18,0)
);


CREATE TABLE epbuilder.visual_analysis_table
(
    analysis_type varchar(20),
    id varchar(300),
    episode_id varchar(10),
    episode_name varchar(30),
    episode_description varchar(255),
    episode_type varchar(50),
    episode_category varchar(50),
    episode_level numeric(37,15),
    member_id varchar(20),
    member_age int,
    cms_age_group varchar(20),
    gender varchar(5),
    member_zipcode varchar(10),
    member_county varchar(50),
    member_population varchar(50),
    total_cost numeric(13,2),
    expected_cost numeric(13,2),
    pac_cost numeric(13,2),
    typical_cost numeric(13,2),
    expected_pac_cost numeric(13,2),
    expected_typical_cost numeric(13,2),
    ip_cost numeric(13,2),
    op_cost numeric(13,2),
    pb_cost numeric(13,2),
    rx_cost numeric(13,2),
    assigned_cost numeric(13,2),
    assigned_ip_cost numeric(13,2),
    assigned_op_cost numeric(13,2),
    assigned_pb_cost numeric(13,2),
    assigned_rx_cost numeric(13,2),
    assigned_cost_unfiltered numeric(13,2),
    assigned_ip_cost_unfiltered numeric(13,2),
    assigned_op_cost_unfiltered numeric(13,2),
    assigned_pb_cost_unfiltered numeric(13,2),
    assigned_rx_cost_unfiltered numeric(13,2),
    pps varchar(100),
    provider_id varchar(20),
    provider_name varchar(100),
    provider_zipcode varchar(20),
    provider_type varchar(100),
    mco varchar(100),
    hh varchar(100),
    pcp varchar(100),
    vbp_attrib_provider varchar(100),
    vbp_attrib_provider_zipcode varchar(100),
    vbp_contractor varchar(100),
    year numeric(37,15),
    utilization varchar(20),
    ppr int,
    ppv int,
    exclusive numeric(37,15),
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_occurence_count_chronic int,
    co_occurence_count_all int,
    episode_count int,
    episode_count_unfiltered int,
    qcrg_code int,
    qcrg_desc varchar(140),
    qacrg1_code int,
    qacrg1_desc varchar(140),
    qacrg2_code int,
    qacrg2_desc varchar(140),
    qacrg3_code int,
    qacrg3_desc varchar(140),
    fincrg_q varchar(3),
    q_base varchar(2),
    q_severity varchar(1),
    enrolled_num_month int
);


CREATE TABLE epbuilder.RFs_2012
(
    member_id varchar(100),
    factor_id varchar(25)
);


CREATE TABLE epbuilder.RFs_2012_Count
(
    member_id varchar(100),
    count int
);


CREATE TABLE epbuilder.temp_main_state_wide_values_1_1
(
    data_processing varchar(100) DEFAULT NULL,
    analysis_type varchar(100) DEFAULT NULL,
    vbp_arrangement varchar(100) DEFAULT NULL,
    member_population varchar(100) DEFAULT NULL,
    episode_name varchar(100) DEFAULT NULL,
    year int DEFAULT NULL::int,
    state_wide_total numeric(12,5) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.visual_analysis_table_va_date
(
    data_processing varchar(100),
    analysis_type varchar(100),
    id varchar(300),
    episode_id varchar(100),
    episode_name varchar(100),
    episode_description varchar(255),
    episode_type varchar(100),
    episode_category varchar(100),
    episode_level varchar(100),
    member_id varchar(100),
    member_age varchar(100),
    cms_age_group varchar(100),
    gender varchar(5),
    member_zipcode varchar(100),
    member_county varchar(100),
    member_population varchar(100),
    member_region varchar(100),
    total_cost numeric(13,2),
    expected_cost numeric(13,2),
    pac_cost numeric(13,2),
    typical_cost numeric(13,2),
    expected_pac_cost numeric(13,2),
    expected_typical_cost numeric(13,2),
    ip_cost numeric(13,2),
    op_cost numeric(13,2),
    pb_cost numeric(13,2),
    rx_cost numeric(13,2),
    assigned_cost varchar(100),
    assigned_ip_cost varchar(100),
    assigned_op_cost varchar(100),
    assigned_pb_cost varchar(100),
    assigned_rx_cost varchar(100),
    assigned_cost_unfiltered varchar(100),
    assigned_ip_cost_unfiltered varchar(100),
    assigned_op_cost_unfiltered varchar(100),
    assigned_pb_cost_unfiltered varchar(100),
    assigned_rx_cost_unfiltered varchar(100),
    pps varchar(100),
    provider_id varchar(100),
    provider_name varchar(100),
    provider_zipcode varchar(100),
    provider_type varchar(100),
    mco varchar(100),
    hh varchar(100),
    pcp varchar(100),
    vbp_attrib_provider varchar(100),
    vbp_attrib_provider_zipcode varchar(100),
    vbp_contractor varchar(100),
    subgroups varchar(300),
    year numeric(37,15),
    utilization varchar(100),
    ppr int,
    ppv int,
    exclusive numeric(37,15),
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_occurence_count_chronic int,
    co_occurence_count_all int,
    episode_count varchar(100),
    episode_count_unfiltered varchar(100),
    qcrg_code varchar(100),
    qcrg_desc varchar(140),
    qacrg1_code varchar(100),
    qacrg1_desc varchar(140),
    qacrg2_code varchar(100),
    qacrg2_desc varchar(140),
    qacrg3_code varchar(100),
    qacrg3_desc varchar(140),
    fincrg_q varchar(3),
    q_base varchar(100),
    q_severity varchar(100),
    enrolled_num_month varchar(100),
    vbp_arrangement varchar(100),
    state_wide_avg_exp_cost numeric(13,2),
    state_wide_exp_pac_rate numeric(6,5),
    state_wide_avg_total_cost numeric(13,2),
    state_wide_pac_percent numeric(6,5),
    state_wide_pac_rate numeric(6,5),
    state_wide_female_percent numeric(6,5),
    state_wide_male_percent numeric(6,5),
    state_wide_NU_percent numeric(6,5),
    state_wide_LU_percent numeric(6,5),
    state_wide_percent_co_ASTHMA numeric(6,5),
    state_wide_percent_co_ARRBLK numeric(6,5),
    state_wide_percent_co_HF numeric(6,5),
    state_wide_percent_co_COPD numeric(6,5),
    state_wide_percent_co_CAD numeric(6,5),
    state_wide_percent_co_ULCLTS numeric(6,5),
    state_wide_percent_co_BIPLR numeric(6,5),
    state_wide_percent_co_GERD numeric(6,5),
    state_wide_percent_co_HTN numeric(6,5),
    state_wide_percent_co_GLCOMA numeric(6,5),
    state_wide_percent_co_LBP numeric(6,5),
    state_wide_percent_co_CROHNS numeric(6,5),
    state_wide_percent_co_DIAB numeric(6,5),
    state_wide_percent_co_DEPRSN numeric(6,5),
    state_wide_percent_co_OSTEOA numeric(6,5),
    state_wide_percent_co_RHNTS numeric(6,5),
    state_wide_percent_co_DIVERT numeric(6,5),
    state_wide_percent_co_DEPANX numeric(6,5),
    state_wide_percent_co_PTSD numeric(6,5),
    state_wide_percent_co_SCHIZO numeric(6,5),
    state_wide_percent_co_SUDS numeric(6,5)
);


CREATE TABLE epbuilder.visual_analysis_table_va
(
    data_processing varchar(100),
    analysis_type varchar(100),
    id varchar(300),
    episode_id varchar(100),
    episode_name varchar(100),
    episode_description varchar(255),
    episode_type varchar(100),
    episode_category varchar(100),
    episode_level varchar(100),
    member_id varchar(100),
    member_age varchar(100),
    cms_age_group varchar(100),
    gender varchar(5),
    member_zipcode varchar(100),
    member_county varchar(100),
    member_population varchar(100),
    member_region varchar(100),
    total_cost numeric(13,2),
    expected_cost numeric(13,2),
    pac_cost numeric(13,2),
    typical_cost numeric(13,2),
    expected_pac_cost numeric(13,2),
    expected_typical_cost numeric(13,2),
    ip_cost numeric(13,2),
    op_cost numeric(13,2),
    pb_cost numeric(13,2),
    rx_cost numeric(13,2),
    assigned_cost varchar(100),
    assigned_ip_cost varchar(100),
    assigned_op_cost varchar(100),
    assigned_pb_cost varchar(100),
    assigned_rx_cost varchar(100),
    assigned_cost_unfiltered varchar(100),
    assigned_ip_cost_unfiltered varchar(100),
    assigned_op_cost_unfiltered varchar(100),
    assigned_pb_cost_unfiltered varchar(100),
    assigned_rx_cost_unfiltered varchar(100),
    pps varchar(100),
    provider_id varchar(100),
    provider_name varchar(100),
    provider_zipcode varchar(100),
    provider_type varchar(100),
    mco varchar(100),
    hh varchar(100),
    pcp varchar(100),
    vbp_attrib_provider varchar(100),
    vbp_attrib_provider_zipcode varchar(100),
    vbp_contractor varchar(100),
    subgroups varchar(300),
    year numeric(37,15),
    utilization varchar(100),
    ppr int,
    ppv int,
    exclusive numeric(37,15),
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_occurence_count_chronic int,
    co_occurence_count_all int,
    episode_count varchar(100),
    episode_count_unfiltered varchar(100),
    qcrg_code varchar(100),
    qcrg_desc varchar(140),
    qacrg1_code varchar(100),
    qacrg1_desc varchar(140),
    qacrg2_code varchar(100),
    qacrg2_desc varchar(140),
    qacrg3_code varchar(100),
    qacrg3_desc varchar(140),
    fincrg_q varchar(3),
    q_base varchar(100),
    q_severity varchar(100),
    enrolled_num_month varchar(100),
    vbp_arrangement varchar(100),
    state_wide_avg_exp_cost numeric(13,2),
    state_wide_exp_pac_rate numeric(6,5),
    state_wide_avg_total_cost numeric(13,2),
    state_wide_pac_percent numeric(6,5),
    state_wide_pac_rate numeric(6,5),
    state_wide_female_percent numeric(6,5),
    state_wide_male_percent numeric(6,5),
    state_wide_NU_percent numeric(6,5),
    state_wide_LU_percent numeric(6,5),
    state_wide_percent_co_ASTHMA numeric(6,5),
    state_wide_percent_co_ARRBLK numeric(6,5),
    state_wide_percent_co_HF numeric(6,5),
    state_wide_percent_co_COPD numeric(6,5),
    state_wide_percent_co_CAD numeric(6,5),
    state_wide_percent_co_ULCLTS numeric(6,5),
    state_wide_percent_co_BIPLR numeric(6,5),
    state_wide_percent_co_GERD numeric(6,5),
    state_wide_percent_co_HTN numeric(6,5),
    state_wide_percent_co_GLCOMA numeric(6,5),
    state_wide_percent_co_LBP numeric(6,5),
    state_wide_percent_co_CROHNS numeric(6,5),
    state_wide_percent_co_DIAB numeric(6,5),
    state_wide_percent_co_DEPRSN numeric(6,5),
    state_wide_percent_co_OSTEOA numeric(6,5),
    state_wide_percent_co_RHNTS numeric(6,5),
    state_wide_percent_co_DIVERT numeric(6,5),
    state_wide_percent_co_DEPANX numeric(6,5),
    state_wide_percent_co_PTSD numeric(6,5),
    state_wide_percent_co_SCHIZO numeric(6,5),
    state_wide_percent_co_SUDS numeric(6,5)
);


CREATE TABLE epbuilder.report_episode_summary_test
(
    Filter_ID int DEFAULT NULL::int,
    Episode_ID varchar(6) NOT NULL,
    Episode_Name varchar(6) DEFAULT NULL,
    Episode_Description varchar(255) DEFAULT NULL,
    Episode_Type varchar(50) DEFAULT NULL,
    MDC varchar(2) DEFAULT NULL,
    MDC_Description varchar(255) DEFAULT NULL,
    Level int NOT NULL DEFAULT 0,
    Episode_Volume int DEFAULT NULL::int,
    Split_Total_Cost float DEFAULT NULL::float,
    Split_Average_Cost float DEFAULT NULL::float,
    Split_1stPercentile_Cost float DEFAULT NULL::float,
    Split_99thPercentile_Cost float DEFAULT NULL::float,
    Split_Min_Cost float DEFAULT NULL::float,
    Split_Max_Cost float DEFAULT NULL::float,
    Split_STDEV float DEFAULT NULL::float,
    Split_CV float DEFAULT NULL::float,
    Split_Total_PAC_Cost float DEFAULT NULL::float,
    Split_Average_PAC_Cost float DEFAULT NULL::float,
    Split_PAC_Percent numeric(5,2) DEFAULT NULL::numeric(1,0),
    Split_Total_Typical_Cost float DEFAULT NULL::float,
    Split_Average_Typical_Cost float DEFAULT NULL::float,
    Split_Total_TypicalwPAC_Cost float DEFAULT NULL::float,
    Split_Average_TypicalwPAC_Cost float DEFAULT NULL::float,
    Annualized_Split_Total_Cost float DEFAULT NULL::float,
    Annualized_Split_Average_Cost float DEFAULT NULL::float,
    Annualized_Split_1stPercentile_Cost float DEFAULT NULL::float,
    Annualized_Split_99thPercentile_Cost float DEFAULT NULL::float,
    Annualized_Split_Min_Cost float DEFAULT NULL::float,
    Annualized_Split_Max_Cost float DEFAULT NULL::float,
    Annualized_Split_STDEV float DEFAULT NULL::float,
    Annualized_Split_CV float DEFAULT NULL::float,
    Annualized_Split_Total_PAC_Cost float DEFAULT NULL::float,
    Annualized_Split_Average_PAC_Cost float DEFAULT NULL::float,
    Annualized_Split_PAC_Percent numeric(5,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_Typical_Cost float DEFAULT NULL::float,
    Annualized_Split_Average_Typical_Cost float DEFAULT NULL::float,
    Annualized_Split_Total_TypicalwPAC_Cost float DEFAULT NULL::float,
    Annualized_Split_Average_TypicalwPAC_Cost float DEFAULT NULL::float,
    Unsplit_Total_Cost float DEFAULT NULL::float,
    Unsplit_Average_Cost float DEFAULT NULL::float,
    Unsplit_1stPercentile_Cost float DEFAULT NULL::float,
    Unsplit_99thPercentile_Cost float DEFAULT NULL::float,
    Unsplit_Min_Cost float DEFAULT NULL::float,
    Unsplit_Max_Cost float DEFAULT NULL::float,
    Unsplit_STDEV float DEFAULT NULL::float,
    Unsplit_CV float DEFAULT NULL::float,
    Unsplit_Total_PAC_Cost float DEFAULT NULL::float,
    Unsplit_Average_PAC_Cost float DEFAULT NULL::float,
    Unsplit_PAC_Percent numeric(5,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_Typical_Cost float DEFAULT NULL::float,
    Unsplit_Average_Typical_Cost float DEFAULT NULL::float,
    Unsplit_Total_TypicalwPAC_Cost float DEFAULT NULL::float,
    Unsplit_Average_TypicalwPAC_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Total_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Average_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_1stPercentile_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_99thPercentile_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Min_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Max_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_STDEV float DEFAULT NULL::float,
    Annualized_Unsplit_CV float DEFAULT NULL::float,
    Annualized_Unsplit_Total_PAC_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Average_PAC_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_PAC_Percent numeric(5,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_Typical_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Average_Typical_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Total_TypicalwPAC_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Average_TypicalwPAC_Cost float DEFAULT NULL::float,
    Expected_Split_Average_Cost float DEFAULT NULL::float,
    Expected_Split_Typical_IP_Average_Cost float DEFAULT NULL::float,
    Expected_Split_Typical_Other_Average_Cost float DEFAULT NULL::float,
    Expected_Split_PAC_Average_Cost float DEFAULT NULL::float,
    Expected_Unsplit_Average_Cost float DEFAULT NULL::float,
    Expected_Unsplit_Typical_IP_Average_Cost float DEFAULT NULL::float,
    Expected_Unsplit_Typical_Other_Average_Cost float DEFAULT NULL::float,
    Expected_Unsplit_PAC_Average_Cost float DEFAULT NULL::float
);

ALTER TABLE epbuilder.report_episode_summary_test ADD CONSTRAINT report_episode_summary_pk PRIMARY KEY (Episode_ID, Level) DISABLED;

CREATE TABLE epbuilder.dual_eligable
(
    member_id varchar(255),
    year varchar(16),
    DUAL_ELIGIBILITY_FLAG varchar(1)
);


CREATE TABLE epbuilder.TCGP_2012_by_member_less_maternity_with_age
(
    member_id varchar(100),
    sub_population varchar(20),
    gender varchar(2),
    birth_year int,
    age_group varchar(7),
    pps varchar(100),
    pcp varchar(100),
    total_cost numeric(40,20),
    maternity_costs_2012 numeric(58,38),
    total_costs_less_maternity_2012 numeric(58,38)
);


CREATE TABLE epbuilder.TCGP_2012_by_member_less_maternity_with_age_crg
(
    member_id varchar(100),
    sub_population varchar(20),
    gender varchar(2),
    birth_year int,
    age_group varchar(7),
    pps varchar(100),
    pcp varchar(100),
    total_cost numeric(40,20),
    maternity_costs_2012 numeric(58,38),
    total_costs_less_maternity_2012 numeric(58,38),
    fincrg varchar(3),
    year int,
    quarter varchar(2)
);


CREATE TABLE epbuilder.members_by_crg
(
    year int,
    fincrg varchar(3),
    age_group varchar(7),
    gender varchar(2),
    member_count int,
    total_cost numeric(40,20),
    average_cost numeric(58,38),
    total_cost_less_maternity numeric(58,38),
    average_cost_less_maternity numeric(76,56)
);


CREATE TABLE epbuilder.TCGP_2012_by_member_with_expected_costs
(
    member_id varchar(100),
    sub_population varchar(20),
    gender varchar(2),
    birth_year int,
    age_group varchar(7),
    pps varchar(100),
    pcp varchar(100),
    total_cost numeric(40,20),
    maternity_costs_2012 numeric(58,38),
    total_costs_less_maternity_2012 numeric(58,38),
    fincrg varchar(3),
    year int,
    quarter varchar(2),
    expected_cost_with_maternity numeric(58,38),
    expected_cost_without_maternity numeric(76,56)
);


CREATE TABLE epbuilder.TCGP_2013_by_member_with_expected_cost
(
    member_id varchar(100),
    sub_population varchar(20),
    gender varchar(2),
    birth_year int,
    age_group varchar(7),
    pps varchar(100),
    pcp varchar(100),
    total_cost numeric(40,20),
    maternity_costs_2013 numeric(58,38),
    total_costs_less_maternity_2013 numeric(58,38),
    fincrg varchar(3),
    expected_cost numeric(58,38),
    expected_less_actual_with_maternity numeric(58,38),
    expected_cost_less_maternity numeric(76,56),
    expected_less_actual_without_maternity numeric(76,56)
);


CREATE TABLE epbuilder.TCGP_2013_by_pps
(
    pps varchar(100),
    count int,
    average_actual_w_maternity float,
    average_expected_w_maternity float,
    efficiency_score_w_maternity numeric(136,78),
    average_actual_without_maternity float,
    average_expected_without_maternity float,
    efficiency_score_without_maternity numeric(190,114),
    efficiency_score_difference numeric(190,114),
    expected_less_actual_with_maternity numeric(58,38),
    expected_less_actual_without_maternity numeric(76,56),
    difference numeric(76,56),
    difference_per_member numeric(94,74)
);


CREATE TABLE epbuilder.PPR_2013_RA
(
    Claim_ID varchar(255),
    CLAIM_TRANS_ID varchar(255),
    Date varchar(255),
    Claim_Type_Code varchar(255),
    Preventable_Group varchar(255),
    Preventable_Group_Desc varchar(255),
    PPR_Type_Code varchar(255),
    PPR_Type_Code_Desc varchar(255)
);


CREATE TABLE epbuilder.NY_Grid_Output
(
    result varchar(18),
    sub_population varchar(20),
    Mem_Count int,
    IP_ST_PPA numeric(40,20),
    IP_SNF_PPA numeric(40,20),
    IP_OTHER_PPA numeric(40,20),
    OP_ED_PPA numeric(40,20),
    OP_OTHER_PPA numeric(40,20),
    PB_OTHER_PPA numeric(40,20),
    PB_NOT_OTHER_PPA numeric(40,20),
    IP_ST_PPR numeric(40,20),
    IP_SNF_PPR numeric(40,20),
    IP_OTHER_PPR numeric(40,20),
    OP_ED_PPR numeric(40,20),
    OP_OTHER_PPR numeric(40,20),
    PB_OTHER_PPR numeric(40,20),
    PB_NOT_OTHER_PPR numeric(40,20),
    IP_ST_PPV numeric(40,20),
    IP_SNF_PPV numeric(40,20),
    IP_OTHER_PPV numeric(40,20),
    OP_ED_PPV numeric(40,20),
    OP_OTHER_PPV numeric(40,20),
    PB_OTHER_PPV numeric(40,20),
    PB_NOT_OTHER_PPV numeric(40,20),
    IP_ST_PAC numeric(40,20),
    IP_SNF_PAC numeric(40,20),
    IP_OTHER_PAC numeric(40,20),
    OP_ED_PAC numeric(40,20),
    OP_OTHER_PAC numeric(40,20),
    PB_OTHER_PAC numeric(40,20),
    PB_NOT_OTHER_PAC numeric(40,20),
    IP_ST_ALL numeric(12,12),
    IP_SNF_ALL numeric(12,12),
    IP_OTHER_ALL numeric(12,12),
    OP_ED_ALL numeric(12,12),
    OP_OTHER_ALL numeric(12,12),
    PB_OTHER_ALL numeric(12,12),
    PB_NOT_OTHER_ALL numeric(12,12)
);


CREATE TABLE epbuilder.cooccurance_of_chronic_episodes
(
    master_episode_id varchar(255),
    level int,
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_count_chronic int,
    co_count_all int,
    year numeric(18,0)
);


CREATE TABLE epbuilder.crg_y1_y2
(
    YEAR int NOT NULL,
    QUARTER varchar(2) NOT NULL,
    CRG_VERSION varchar(13) NOT NULL,
    CLAIM_BEGIN_DATE date NOT NULL,
    CLAIM_END_DATE date NOT NULL,
    CLAIMS_AS_OF_DATE date NOT NULL,
    RECIP_ID char(11) NOT NULL,
    QCRG_CODE varchar(5) NOT NULL,
    QACRG1_CODE varchar(10) NOT NULL,
    QACRG2_CODE varchar(4) NOT NULL,
    QACRG3_CODE varchar(2) NOT NULL,
    FINCRG_Q varchar(3) NOT NULL,
    Q_BASE varchar(2) NOT NULL,
    Q_SEVERITY varchar(1) NOT NULL,
    CRG_CODE varchar(5) NOT NULL,
    ACRG1_CODE varchar(6) NOT NULL,
    ACRG2_CODE varchar(4) NOT NULL,
    ACRG3_CODE varchar(2) NOT NULL,
    FINCRG_G varchar(3) NOT NULL,
    G_BASE varchar(2) NOT NULL,
    G_SEVERITY varchar(1) NOT NULL,
    FINCRG varchar(3) NOT NULL,
    BASE varchar(2) NOT NULL,
    SEVERITY varchar(1) NOT NULL
);


CREATE TABLE epbuilder.all_mom
(
    member_id varchar(50)
);


CREATE TABLE epbuilder.OP_ED_Count
(
    OP_ED int,
    Count int
);


CREATE TABLE epbuilder.op_claims
(
    claim_id varchar(50),
    count int
);


CREATE TABLE epbuilder.multiple
(
    claim_id varchar(50),
    count int
);


CREATE TABLE epbuilder.V_MSR_RESULT_RY_RP1100A
(
    MSR_PUB_YR varchar(10),
    MSR_ID varchar(10),
    MSR_COMP_ID varchar(10),
    MBR_KEY int,
    MSR_YR varchar(10),
    MSR_VAL int,
    MBR_AGE int,
    MDW_INS_DT date,
    MDW_UPDT_DT date
);


CREATE TABLE epbuilder.V_NUM_EVENTS_RP1100A
(
    MSR_PUB_YR varchar(10),
    MSR_ID varchar(10),
    MSR_COMP_ID varchar(10),
    MBR_KEY int,
    MSR_YR varchar(10),
    CLAIM_TRANS_ID int,
    TCN varchar(16),
    DSCH_DT date,
    MBR_AGE int,
    MDW_INS_DT date
);


CREATE TABLE epbuilder.NY_Grid_Check
(
    Data varchar(7),
    sub_population varchar(20),
    Mem_Count int,
    IP_ST numeric(40,20),
    IP_OTHER numeric(40,20),
    OP_ED numeric(40,20),
    OP_OTHER numeric(40,20),
    PB numeric(40,20)
);


CREATE TABLE epbuilder.PQI_PDI_2013
(
    claim_id varchar(16),
    p_type varchar(8)
);


CREATE TABLE epbuilder.all_claim_pdi_pqi
(
    claim_id varchar(50),
    claim_line_type_code varchar(12),
    pdi int,
    pqi int
);


CREATE TABLE epbuilder.NY_PDI_PQI_Output_Total
(
    sub_population varchar(20),
    Mem_Count int,
    IP_ST_PDI numeric(40,20),
    IP_OTHER_PDI numeric(40,20),
    OP_ED_PDI numeric(40,20),
    OP_OTHER_PDI numeric(40,20),
    PB_PDI numeric(40,20),
    RX_PDI numeric(40,20),
    IP_ST_PQI numeric(40,20),
    IP_OTHER_PQI numeric(40,20),
    OP_ED_PQI numeric(40,20),
    OP_OTHER_PQI numeric(40,20),
    PB_PQI numeric(40,20),
    RX_PQI numeric(40,20),
    IP_ST_PAC numeric(40,20),
    IP_OTHER_PAC numeric(40,20),
    OP_ED_PAC numeric(40,20),
    OP_OTHER_PAC numeric(40,20),
    PB_PAC numeric(40,20),
    RX_PAC numeric(40,20)
);


CREATE TABLE epbuilder.ppa_example
(
    claim_key numeric(38,0),
    claim_id varchar(30),
    claim_trans_id numeric(38,0),
    dsch_dt date,
    claim_type_code varchar(2),
    preventable_group varchar(1),
    pfp_preventable_group_name varchar(30),
    pfp_preventable_group_desc varchar(50),
    preventable_status varchar(2),
    pfp_preventable_status_desc varchar(150),
    preventable_reason varchar(2),
    pfp_preventable_reason_desc varchar(150),
    mdw_insert_dt date,
    mdw_update_dt date
);


CREATE TABLE epbuilder.ppr_example
(
    Claim_ID varchar(255),
    CLAIM_TRANS_ID varchar(255),
    Date varchar(255),
    Claim_Type_Code varchar(255),
    Preventable_Group varchar(255),
    Preventable_Group_Desc varchar(255),
    PPR_Type_Code varchar(255),
    PPR_Type_Code_Desc varchar(255)
);


CREATE TABLE epbuilder.PPVs_OP_ED_by_ccs
(
    sub_population varchar(20),
    ccs_group varchar(200),
    claim_volume int,
    total_cost numeric(40,20)
);


CREATE TABLE epbuilder.NY_PDI_PQI_Output_Exclusive
(
    sub_population varchar(20),
    Mem_Count int,
    IP_ST_PDI_less_PAC numeric(40,20),
    IP_OTHER_PDI_less_PAC numeric(40,20),
    OP_ED_PDI_less_PAC numeric(40,20),
    OP_OTHER_PDI_less_PAC numeric(40,20),
    PB_PDI_less_PAC numeric(40,20),
    RX_PDI_less_PAC numeric(40,20),
    IP_ST_PQI_less_PAC numeric(40,20),
    IP_OTHER_PQI_less_PAC numeric(40,20),
    OP_ED_PQI_less_PAC numeric(40,20),
    OP_OTHER_PQI_less_PAC numeric(40,20),
    PB_PQI_less_PAC numeric(40,20),
    RX_PQI_less_PAC numeric(40,20),
    IP_ST_PAC_less_PDI_PQI numeric(40,20),
    IP_OTHER_PAC_less_PDI_PQI numeric(40,20),
    OP_ED_PAC_less_PDI_PQI numeric(40,20),
    OP_OTHER_PAC_less_PDI_PQI numeric(40,20),
    PB_PAC_less_PDI_PQI numeric(40,20),
    RX_PAC_less_PDI_PQI numeric(40,20),
    IP_ST_ALL numeric(40,20),
    IP_OTHER_ALL numeric(40,20),
    OP_ED_ALL numeric(40,20),
    OP_OTHER_ALL numeric(40,20),
    PB_ALL numeric(40,20),
    RX_ALL numeric(40,20)
);


CREATE TABLE epbuilder.PACs_IP_ST_by_ccs
(
    sub_population varchar(20),
    ccs_group varchar(200),
    claim_volume int,
    total_cost numeric(40,20)
);


CREATE TABLE epbuilder.vbp_ppe_ppa_no_ppr
(
    claim_key numeric(38,0),
    claim_id varchar(30),
    claim_trans_id numeric(38,0),
    dsch_dt date,
    claim_type_code varchar(2),
    preventable_group varchar(1),
    pfp_preventable_group_name varchar(30),
    pfp_preventable_group_desc varchar(50),
    preventable_status varchar(2),
    pfp_preventable_status_desc varchar(150),
    preventable_reason varchar(2),
    pfp_preventable_reason_desc varchar(150),
    mdw_insert_dt date,
    mdw_update_dt date
);


CREATE TABLE epbuilder.chronic_sick_prvt
(
    member_id varchar(50),
    sub_population varchar(7)
);


CREATE TABLE epbuilder.ED_Flag
(
    claim_id varchar(50),
    sub_population varchar(20),
    ED_Flag int,
    ppv int
);


CREATE TABLE epbuilder.NY_Total_Cost
(
    sub_population varchar(20),
    TOTAL_SHORT_TERM_IP numeric(40,20),
    TOTAL_OTHER_IP numeric(40,20),
    TOTAL_OP_ED numeric(40,20),
    TOTAL_OP_OTHER numeric(40,20),
    TOTAL_PB numeric(40,20),
    TOTAL_RX numeric(40,20)
);


CREATE TABLE epbuilder.all_claim_ppa_ppr_ppv
(
    claim_id varchar(50),
    claim_line_type_code varchar(12),
    ppa int,
    ppr int,
    ppv int,
    ppe int
);


CREATE TABLE epbuilder.NY_Grid_Output_Exclusive
(
    sub_population varchar(20),
    Mem_Count int,
    IP_ST_PPA numeric(40,20),
    IP_OTHER_PPA numeric(40,20),
    OP_ED_PPA numeric(40,20),
    OP_OTHER_PPA numeric(40,20),
    PB_PPA numeric(40,20),
    IP_ST_PPR numeric(40,20),
    IP_OTHER_PPR numeric(40,20),
    OP_ED_PPR numeric(40,20),
    OP_OTHER_PPR numeric(40,20),
    PB_PPR numeric(40,20),
    IP_ST_PPV numeric(40,20),
    IP_OTHER_PPV numeric(40,20),
    OP_ED_PPV numeric(40,20),
    OP_OTHER_PPV numeric(40,20),
    PB_PPV numeric(40,20),
    IP_ST_PAC numeric(40,20),
    IP_OTHER_PAC numeric(40,20),
    OP_ED_PAC numeric(40,20),
    OP_OTHER_PAC numeric(40,20),
    PB_PAC numeric(40,20),
    RX_PAC numeric(40,20),
    IP_ST_ALL numeric(40,20),
    IP_OTHER_ALL numeric(40,20),
    OP_ED_ALL numeric(40,20),
    OP_OTHER_ALL numeric(40,20),
    PB_ALL numeric(40,20),
    RX_ALL numeric(40,20)
);


CREATE TABLE epbuilder.IP_PAC_Claims
(
    member_id varchar(50),
    claim_id varchar(50),
    begin_date date,
    end_date date,
    IP_allowed_amt numeric(40,20),
    PAC int,
    ppa int,
    ppr int
);


CREATE TABLE epbuilder.PB_Bubble_Claim
(
    member_id varchar(50),
    claim_id varchar(50),
    allowed_amt numeric(40,20)
);


CREATE TABLE epbuilder.IP_PAC_w_Bubble
(
    member_id varchar(50),
    claim_id varchar(50),
    begin_date date,
    end_date date,
    IP_allowed_amt numeric(40,20),
    PAC int,
    ppa int,
    ppr int,
    PB_allowed_amt numeric(40,20)
);


CREATE TABLE epbuilder.claims_dsrp_info_CLM_LEVEL
(
    member_id varchar(50),
    sub_population varchar(20),
    master_claim_id varchar(100),
    claim_id varchar(50),
    allowed_amt numeric(40,20),
    claim_line_type_code varchar(12),
    begin_date date,
    end_date date,
    type_of_bill varchar(12),
    facility_type_code varchar(8),
    place_of_svc_code varchar(25),
    count int,
    PAC int,
    OP_ED int,
    PAS int,
    PB_OTHER int,
    Chronic_bundle int,
    IPC int,
    PREVENT int
);


CREATE TABLE epbuilder.NY_Grid_Output_Total_IPC
(
    sub_population varchar(14),
    Mem_Count int,
    IP_ST_PPA numeric(40,20),
    IP_OTHER_PPA numeric(40,20),
    OP_ED_PPA numeric(40,20),
    OP_OTHER_PPA numeric(40,20),
    PB_PPA numeric(40,20),
    IP_ST_PPR numeric(40,20),
    IP_OTHER_PPR numeric(40,20),
    OP_ED_PPR numeric(40,20),
    OP_OTHER_PPR numeric(40,20),
    PB_PPR numeric(40,20),
    IP_ST_PPV numeric(40,20),
    IP_OTHER_PPV numeric(40,20),
    OP_ED_PPV numeric(40,20),
    OP_OTHER_PPV numeric(40,20),
    PB_PPV numeric(40,20),
    IP_ST_PAC numeric(40,20),
    IP_OTHER_PAC numeric(40,20),
    OP_ED_PAC numeric(40,20),
    OP_OTHER_PAC numeric(40,20),
    PB_PAC numeric(40,20),
    RX_PAC numeric(40,20)
);


CREATE TABLE epbuilder.NY_Grid_Output_Exclusive_IPC
(
    sub_population varchar(14),
    Mem_Count int,
    IP_ST_PPA numeric(40,20),
    IP_OTHER_PPA numeric(40,20),
    OP_ED_PPA numeric(40,20),
    OP_OTHER_PPA numeric(40,20),
    PB_PPA numeric(40,20),
    IP_ST_PPR numeric(40,20),
    IP_OTHER_PPR numeric(40,20),
    OP_ED_PPR numeric(40,20),
    OP_OTHER_PPR numeric(40,20),
    PB_PPR numeric(40,20),
    IP_ST_PPV numeric(40,20),
    IP_OTHER_PPV numeric(40,20),
    OP_ED_PPV numeric(40,20),
    OP_OTHER_PPV numeric(40,20),
    PB_PPV numeric(40,20),
    IP_ST_PAC numeric(40,20),
    IP_OTHER_PAC numeric(40,20),
    OP_ED_PAC numeric(40,20),
    OP_OTHER_PAC numeric(40,20),
    PB_PAC numeric(40,20),
    RX_PAC numeric(40,20),
    IP_ST_ALL numeric(40,20),
    IP_OTHER_ALL numeric(40,20),
    OP_ED_ALL numeric(40,20),
    OP_OTHER_ALL numeric(40,20),
    PB_ALL numeric(40,20),
    RX_ALL numeric(40,20)
);


CREATE TABLE epbuilder.NY_Total_Cost_IPC
(
    sub_population varchar(20),
    Chronic_TOTAL_SHORT_TERM_IP numeric(40,20),
    Chronic_TOTAL_OTHER_IP numeric(40,20),
    Chronic_TOTAL_OP_ED numeric(40,20),
    Chronic_TOTAL_OP_OTHER numeric(40,20),
    Chronic_TOTAL_PB numeric(40,20),
    Chronic_TOTAL_RX numeric(40,20),
    IPC_TOTAL_SHORT_TERM_IP numeric(40,20),
    IPC_TOTAL_OTHER_IP numeric(40,20),
    IPC_TOTAL_OP_ED numeric(40,20),
    IPC_TOTAL_OP_OTHER numeric(40,20),
    IPC_TOTAL_PB numeric(40,20),
    IPC_TOTAL_RX numeric(40,20),
    PREVENT_TOTAL_SHORT_TERM_IP numeric(40,20),
    PREVENT_TOTAL_OTHER_IP numeric(40,20),
    PREVENT_TOTAL_OP_ED numeric(40,20),
    PREVENT_TOTAL_OP_OTHER numeric(40,20),
    PREVENT_TOTAL_PB numeric(40,20),
    PREVENT_TOTAL_RX numeric(40,20)
);


CREATE TABLE epbuilder.PCP_TABLE_20160825
(
    MEMBER_ID varchar(11) NOT NULL,
    PCP varchar(35),
    MMIS varchar(8),
    NPI varchar(10)
);


CREATE TABLE epbuilder.asthma_red
(
    member_id varchar(50),
    master_episode_id varchar(255),
    episode_type varchar(50),
    episode_id varchar(6),
    level int,
    split_total_cost numeric(13,2),
    split_total_PAC_cost numeric(13,2),
    split_total_typical_cost numeric(13,2),
    unsplit_total_cost numeric(13,2),
    unsplit_total_PAC_cost numeric(13,2),
    unsplit_total_typical_cost numeric(13,2),
    annualized_unsplit_total_cost numeric(13,2),
    annualized_unsplit_total_PAC_cost numeric(13,2),
    annualized_unsplit_total_typical_cost numeric(13,2)
);


CREATE TABLE epbuilder.asthma_associations
(
    id int,
    parent_master_episode_id varchar(73),
    child_master_episode_id varchar(73),
    association_type varchar(24),
    association_level int,
    association_count int,
    association_start_day varchar(15),
    association_end_day varchar(15)
);


CREATE TABLE epbuilder.PNE_asthma_association
(
    parent_master_episode_id varchar(73),
    "left" varchar(24),
    count int
);


CREATE TABLE epbuilder.URI_asthma_association
(
    parent_master_episode_id varchar(73),
    "left" varchar(24),
    count int
);


CREATE TABLE epbuilder.PNE_URI_association
(
    parent_master_episode_id varchar(73),
    "left" varchar(24),
    count int
);


CREATE TABLE epbuilder.assign_1_pac
(
    member_id varchar(50),
    LEVEL_1 varchar(73),
    LEVEL_2 varchar(73),
    LEVEL_2_association_type varchar(24),
    LEVEL_3 varchar(73),
    LEVEL_3_association_type varchar(24),
    LEVEL_4 varchar(73),
    LEVEL_4_association_type varchar(24),
    LEVEL_5 varchar(73),
    LEVEL_5_association_type varchar(24),
    Level_1_end date,
    Level_2_end date,
    Level_3_end date,
    Level_4_end date,
    Level_5_end date,
    assigned_type varchar(2),
    master_claim_id varchar(100),
    end_date date,
    begin_date date
);


CREATE TABLE epbuilder.epi_PACs
(
    episode varchar(73),
    level_assignment int,
    pac_proc_acute int,
    pac_chronic int
);


CREATE TABLE epbuilder.LEVEL_5_FINAL
(
    member_id varchar(50),
    LEVEL_1 varchar(73),
    LEVEL_2 varchar(73),
    LEVEL_2_association_type varchar(4),
    LEVEL_3 varchar(73),
    LEVEL_3_association_type varchar(4),
    LEVEL_4 varchar(73),
    LEVEL_4_association_type varchar(4),
    LEVEL_5 varchar(73),
    LEVEL_5_association_type varchar(4),
    Level_1_end date,
    Level_2_end date,
    Level_3_end date,
    Level_4_end date,
    Level_5_end date
);


CREATE TABLE epbuilder.ra_final_data_limit
(
    row_names varchar(255),
    epi_id varchar(255),
    member_id varchar(255),
    epi_number varchar(255),
    epi_name varchar(255),
    female int,
    age int,
    rec_enr int,
    eol_ind int,
    cost_ra_comp_l1 float,
    cost_ra_comp_l5 float,
    cost_ra_comp_other_l1 float,
    cost_ra_comp_other_l3 float,
    cost_ra_comp_other_l4 float,
    cost_ra_typ_ip_l1 float,
    cost_ra_typ_ip_l3 float,
    cost_ra_typ_ip_l4 float,
    cost_ra_typ_l1 float,
    cost_ra_typ_l5 float,
    cost_ra_typ_other_l3 float,
    cost_ra_typ_other_l4 float,
    cost_sa_comp_l1 float,
    cost_sa_comp_l5 float,
    cost_sa_comp_other_l1 float,
    cost_sa_comp_other_l3 float,
    cost_sa_comp_other_l4 float,
    cost_sa_typ_ip_l1 float,
    cost_sa_typ_ip_l3 float,
    cost_sa_typ_ip_l4 float,
    cost_sa_typ_l1 float,
    cost_sa_typ_l5 float,
    cost_sa_typ_other_l1 float,
    cost_sa_typ_other_l3 float,
    cost_sa_typ_other_l4 float,
    typ_ip_ind float,
    use_comp_l1 float,
    use_comp_l5 float,
    use_comp_other_l1 float,
    use_comp_other_l3 float,
    use_comp_other_l4 float,
    use_typ_ip_l1 float,
    use_typ_ip_l3 float,
    use_typ_ip_l4 float,
    use_typ_l1 float,
    use_typ_l5 float,
    use_typ_other_l1 float,
    use_typ_other_l3 float,
    use_typ_other_l4 float,
    RF0101 float,
    RF0102 float,
    RF0103 float,
    RF0104 float,
    RF0105 float,
    RF0106 float,
    RF0107 float,
    RF0108 float,
    RF0109 float,
    RF0110 float,
    RF0111 float,
    RF0115 float,
    RF0201 float,
    RF0301 float,
    RF0401 float,
    RF0402 float,
    RF0403 float,
    RF0404 float,
    RF0406 float,
    RF0407 float,
    RF0408 float,
    RF0501 float,
    RF0502 float,
    RF0503 float,
    RF0504 float,
    RF0505 float,
    RF0506 float,
    RF0507 float,
    RF0508 float,
    RF0509 float,
    RF0510 float,
    RF0511 float,
    RF0512 float,
    RF0513 float,
    RF0514 float,
    RF0518 float,
    RF0519 float,
    RF0520 float,
    RF0521 float,
    RF0524 float,
    RF0525 float,
    RF0601 float,
    RF0602 float,
    RF0603 float,
    RF0604 float,
    RF0606 float,
    RF0607 float,
    RF0608 float,
    RF0609 float,
    RF0610 float,
    RF0611 float,
    RF0612 float,
    RF0613 float,
    RF0614 float,
    RF0615 float,
    RF0616 float,
    RF0617 float,
    RF0618 float,
    RF0619 float,
    RF0620 float,
    RF0701 float,
    RF0702 float,
    RF0703 float,
    RF0704 float,
    RF0705 float,
    RF0801 float,
    RF0802 float,
    RF0803 float,
    RF0804 float,
    RF0805 float,
    RF0806 float,
    RF0807 float,
    RF0808 float,
    RF0809 float,
    RF0810 float,
    RF0811 float,
    RF0812 float,
    RF0813 float,
    RF0814 float,
    RF0901 float,
    RF0902 float,
    RF1001 float,
    RF1002 float,
    RF1003 float,
    RF1101 float,
    RF1102 float,
    RF1103 float,
    RF1104 float,
    RF1105 float,
    RF1301 float,
    RF1302 float,
    RF1303 float,
    RF1304 float,
    RF1305 float,
    RF1306 float,
    RF1308 float,
    RF1309 float,
    RF1310 float,
    RF1401 float,
    RF1402 float,
    RF1403 float,
    RF1406 float,
    RF1407 float,
    RF1408 float,
    RF1409 float,
    RF1410 float,
    RF1412 float,
    RF1413 float,
    RF1414 float,
    RF1415 float,
    RF1416 float,
    RF1417 float,
    RF1418 float,
    RF1419 float,
    RF1421 float,
    RF1436 float,
    RF1441 float,
    RF1450 float,
    RF1454 float,
    RF1458 float,
    RF1460 float,
    RF1461 float,
    RF1462 float,
    RF1467 float,
    RF1601 float,
    RF1602 float,
    RF1603 float,
    RF1604 float,
    RF1605 float,
    RF1701 float,
    RF1702 float,
    RF1703 float,
    RF1704 float,
    RF1705 float,
    RF1706 float,
    RF1707 float,
    RF1708 float,
    RF1709 float,
    RF1710 float,
    RF1711 float,
    RF1712 float,
    RF1801 float,
    RF1901 float,
    RF1902 float,
    RF2001 float,
    RF2002 float,
    RF2101 float,
    RF2102 float,
    RF2201 float,
    RF2301 float,
    RF2302 float,
    RF2303 float,
    RF2304 float,
    RF2305 float,
    RF2306 float,
    RF2307 float,
    RF2308 float,
    RF2309 float,
    RF2310 float,
    RF2311 float,
    RF2401 float,
    RF2402 float,
    RF2403 float,
    RF2404 float,
    RF2405 float,
    RF2501 float,
    STCT0001 float,
    STCT0002 float,
    STCT0003 float,
    STCT00100 float,
    STCT0018 float,
    STCT0020 float,
    STCT0021 float,
    STCT0022 float,
    STCT0023 float,
    STCT0027 float,
    STCT0040 float,
    STCT0045 float,
    STCT0050 float,
    STCT0053 float,
    STCT0054 float,
    STCT0062 float,
    STCT0063 float,
    STCT0064 float,
    STCT0065 float,
    STCT0066 float,
    STCT0067 float,
    STCT0068 float,
    STCT0069 float,
    STCT0070 float,
    STCT0073 float,
    STCT0074 float,
    STCT0098 float,
    STCT0099 float,
    STCT0254 float,
    STCT0255 float,
    STCT0387 float,
    STCT0388 float,
    STCT04285 float,
    STCT04286 float,
    STCT04287 float,
    STCT04288 float,
    STCT04290 float,
    STCT04291 float,
    STCT04292 float,
    STCT04294 float,
    STCT05263 float,
    STCT05264 float,
    STCT05265 float,
    STCT05266 float,
    STCT05267 float,
    STCT05268 float,
    STCT06124 float,
    STCT06125 float,
    STCT06127 float,
    STCT06128 float,
    STCT06131 float,
    STCT06132 float,
    STCT06133 float,
    STCT06134 float,
    STCT06135 float,
    STCT06136 float,
    STCT06137 float,
    STCT06138 float,
    STCT06139 float,
    STCT06140 float,
    STCT06305 float,
    STCT06306 float,
    STCT06307 float,
    STCT06312 float,
    STCT0738 float,
    STCT0739 float,
    STCT08126 float,
    STCT08127 float,
    STCT08129 float,
    STCT08130 float,
    STCT08131 float,
    STCT08132 float,
    STCT08133 float,
    STCT08134 float,
    STCT08136 float,
    STCT08138 float,
    STCT08140 float,
    STCT08141 float,
    STCT08142 float,
    STCT08143 float,
    STCT08144 float,
    STCT08145 float,
    STCT08309 float,
    STCT09100 float,
    STCT09101 float,
    STCT0996 float,
    STCT0998 float,
    STCT0999 float,
    STCT1342 float,
    STCT1343 float,
    STCT1344 float,
    STCT1345 float,
    STCT1346 float,
    STCT1347 float,
    STCT1348 float,
    STCT1349 float,
    STCT14296 float,
    STCT1455 float,
    STCT17102 float,
    STCT17104 float,
    STCT1798 float,
    STCT1799 float,
    STCT2005 float,
    STCT23308 float,
    STDX01384 float,
    STDX01385 float,
    STDX01388 float,
    STDX01398 float,
    STDX01399 float,
    STDX01400 float,
    STDX01405 float,
    STDX01409 float,
    STDX02135 float,
    STDX02157 float,
    STDX02158 float,
    STDX02159 float,
    STDX02162 float,
    STDX02163 float,
    STDX02164 float,
    STDX02165 float,
    STDX02166 float,
    STDX02167 float,
    STDX03252 float,
    STDX03254 float,
    STDX03256 float,
    STDX03257 float,
    STDX03258 float,
    STDX03259 float,
    STDX03260 float,
    STDX03261 float,
    STDX03262 float,
    STDX03264 float,
    STDX03266 float,
    STDX03267 float,
    STDX04138 float,
    STDX04163 float,
    STDX04165 float,
    STDX04167 float,
    STDX04168 float,
    STDX04172 float,
    STDX04173 float,
    STDX04174 float,
    STDX04180 float,
    STDX04181 float,
    STDX04182 float,
    STDX04183 float,
    STDX04184 float,
    STDX04185 float,
    STDX04186 float,
    STDX04188 float,
    STDX04190 float,
    STDX05281 float,
    STDX05282 float,
    STDX05283 float,
    STDX05284 float,
    STDX05285 float,
    STDX05287 float,
    STDX05288 float,
    STDX05289 float,
    STDX05291 float,
    STDX05313 float,
    STDX05314 float,
    STDX05316 float,
    STDX05318 float,
    STDX05320 float,
    STDX05323 float,
    STDX05325 float,
    STDX05326 float,
    STDX05327 float,
    STDX05328 float,
    STDX05329 float,
    STDX05330 float,
    STDX05331 float,
    STDX05332 float,
    STDX05333 float,
    STDX05334 float,
    STDX05335 float,
    STDX05336 float,
    STDX05337 float,
    STDX05339 float,
    STDX05340 float,
    STDX05343 float,
    STDX05344 float,
    STDX05345 float,
    STDX06267 float,
    STDX06268 float,
    STDX06277 float,
    STDX06281 float,
    STDX06283 float,
    STDX06285 float,
    STDX06286 float,
    STDX06287 float,
    STDX06288 float,
    STDX06290 float,
    STDX06291 float,
    STDX06295 float,
    STDX06297 float,
    STDX06298 float,
    STDX06301 float,
    STDX06306 float,
    STDX06307 float,
    STDX06308 float,
    STDX06311 float,
    STDX06312 float,
    STDX06313 float,
    STDX06314 float,
    STDX06315 float,
    STDX06316 float,
    STDX06317 float,
    STDX06318 float,
    STDX06319 float,
    STDX06320 float,
    STDX06321 float,
    STDX06322 float,
    STDX06323 float,
    STDX06324 float,
    STDX06325 float,
    STDX06326 float,
    STDX06327 float,
    STDX06328 float,
    STDX06331 float,
    STDX06332 float,
    STDX06335 float,
    STDX06336 float,
    STDX06337 float,
    STDX06340 float,
    STDX06341 float,
    STDX06342 float,
    STDX06343 float,
    STDX06344 float,
    STDX06345 float,
    STDX06346 float,
    STDX06347 float,
    STDX06348 float,
    STDX06349 float,
    STDX06350 float,
    STDX06351 float,
    STDX06353 float,
    STDX06356 float,
    STDX07101 float,
    STDX07102 float,
    STDX07103 float,
    STDX07104 float,
    STDX07105 float,
    STDX07106 float,
    STDX07107 float,
    STDX07111 float,
    STDX07112 float,
    STDX07113 float,
    STDX07116 float,
    STDX081040 float,
    STDX081041 float,
    STDX081042 float,
    STDX081043 float,
    STDX081044 float,
    STDX081045 float,
    STDX081046 float,
    STDX081047 float,
    STDX081048 float,
    STDX081050 float,
    STDX081052 float,
    STDX081053 float,
    STDX081054 float,
    STDX081055 float,
    STDX081056 float,
    STDX081057 float,
    STDX081058 float,
    STDX081059 float,
    STDX081060 float,
    STDX081061 float,
    STDX081062 float,
    STDX081063 float,
    STDX081064 float,
    STDX081065 float,
    STDX081066 float,
    STDX081067 float,
    STDX081071 float,
    STDX081073 float,
    STDX081074 float,
    STDX081075 float,
    STDX081085 float,
    STDX081086 float,
    STDX081087 float,
    STDX081089 float,
    STDX081092 float,
    STDX081093 float,
    STDX081094 float,
    STDX081095 float,
    STDX081096 float,
    STDX081097 float,
    STDX09196 float,
    STDX09197 float,
    STDX09198 float,
    STDX09199 float,
    STDX09200 float,
    STDX1007 float,
    STDX10101 float,
    STDX10102 float,
    STDX10103 float,
    STDX10104 float,
    STDX10105 float,
    STDX10106 float,
    STDX10107 float,
    STDX10108 float,
    STDX10109 float,
    STDX10111 float,
    STDX10112 float,
    STDX10113 float,
    STDX10114 float,
    STDX10115 float,
    STDX10116 float,
    STDX1019 float,
    STDX11154 float,
    STDX1194 float,
    STDX1195 float,
    STDX1234 float,
    STDX1238 float,
    STDX13100 float,
    STDX13101 float,
    STDX13102 float,
    STDX13103 float,
    STDX13104 float,
    STDX13105 float,
    STDX13106 float,
    STDX13107 float,
    STDX13111 float,
    STDX1394 float,
    STDX1395 float,
    STDX1396 float,
    STDX1397 float,
    STDX1398 float,
    STDX1399 float,
    STDX1406 float,
    STDX1407 float,
    STDX1409 float,
    STDX14100 float,
    STDX1413 float,
    STDX1422 float,
    STDX1432 float,
    STDX1436 float,
    STDX1442 float,
    STDX1445 float,
    STDX1447 float,
    STDX1448 float,
    STDX1451 float,
    STDX1453 float,
    STDX1454 float,
    STDX1455 float,
    STDX1459 float,
    STDX1460 float,
    STDX1461 float,
    STDX1462 float,
    STDX1465 float,
    STDX1466 float,
    STDX1468 float,
    STDX1470 float,
    STDX1472 float,
    STDX1474 float,
    STDX1482 float,
    STDX1483 float,
    STDX1484 float,
    STDX1487 float,
    STDX1488 float,
    STDX1490 float,
    STDX1491 float,
    STDX1492 float,
    STDX1494 float,
    STDX1495 float,
    STDX1497 float,
    STDX1498 float,
    STDX1499 float,
    STDX1583 float,
    STDX1587 float,
    STDX16293 float,
    STDX1767 float,
    STDX1768 float,
    STDX1771 float,
    STDX1772 float,
    STDX1773 float,
    STDX1846 float,
    STDX1942 float,
    STDX1953 float,
    STDX1954 float,
    STDX1955 float,
    STDX1961 float,
    STDX1963 float,
    STDX1964 float,
    STDX1965 float,
    STDX1966 float,
    STDX1969 float,
    STDX1972 float,
    STDX1975 float,
    STDX1976 float,
    STDX1977 float,
    STDX1978 float,
    STDX1979 float,
    STDX1980 float,
    STDX1981 float,
    STDX1982 float,
    STDX1983 float,
    STDX1984 float,
    STDX1985 float,
    STDX1986 float,
    STDX1987 float,
    STDX1988 float,
    STDX1990 float,
    STDX1991 float,
    STDX1992 float,
    STDX1993 float,
    STDX2008 float,
    STDX2009 float,
    STDX2012 float,
    STDX2013 float,
    STDX2014 float,
    STDX2015 float,
    STDX2016 float,
    STDX2017 float,
    STDX2018 float,
    STDX2019 float,
    STDX2020 float,
    STDX2021 float,
    STDX2023 float,
    STDX2159 float,
    STDX2361 float,
    STDX2367 float,
    STDX2369 float,
    STDX2371 float,
    STPX0002 float,
    STPX04219 float,
    STPX04220 float,
    STPX04221 float,
    STPX04222 float,
    STPX04224 float,
    STPX04225 float,
    STPX04226 float,
    STPX05228 float,
    STPX05229 float,
    STPX05230 float,
    STPX05231 float,
    STPX05236 float,
    STPX05238 float,
    STPX05239 float,
    STPX05240 float,
    STPX0544 float,
    STPX06224 float,
    STPX06226 float,
    STPX06230 float,
    STPX06231 float,
    STPX06234 float,
    STPX06240 float,
    STPX06241 float,
    STPX06243 float,
    STPX06244 float,
    STPX06245 float,
    STPX06246 float,
    STPX06247 float,
    STPX06248 float,
    STPX06249 float,
    STPX06251 float,
    STPX07231 float,
    STPX07232 float,
    STPX08217 float,
    STPX08219 float,
    STPX08220 float,
    STPX08221 float,
    STPX08226 float,
    STPX08227 float,
    STPX08228 float,
    STPX08230 float,
    STPX08231 float,
    STPX08232 float,
    STPX08233 float,
    STPX08234 float,
    STPX08235 float,
    STPX09183 float,
    STPX09184 float,
    STPX09185 float,
    STPX09186 float,
    STPX09187 float,
    STPX13133 float,
    STPX13134 float,
    STPX13135 float,
    STPX13136 float,
    STPX13137 float,
    STPX13138 float,
    STPX13139 float,
    STPX13140 float,
    STPX14146 float,
    STPX17225 float
);


CREATE TABLE epbuilder.provider_PACs_all
(
    episode varchar(73),
    level_assignment int,
    pac_proc_acute int,
    pac_chronic int,
    Attributed_Provider varchar(30)
);


CREATE TABLE epbuilder.provider_PACs
(
    episode_id varchar(24),
    level_assignment int,
    Attributed_Provider varchar(30),
    Number_of_Episodes float,
    Number_of_Epis_w_PAC float,
    PAC_Rate float
);


CREATE TABLE epbuilder.epi_pacs_lvl1
(
    epi_id varchar(73),
    pac_proc_acute int,
    pac_chronic int
);


CREATE TABLE epbuilder.ra_episode_data_rspr
(
    epi_id varchar(73),
    member_id varchar(50),
    epi_number varchar(6),
    epi_name varchar(6),
    female int,
    age int,
    rec_enr int,
    eol_ind int
);


CREATE TABLE epbuilder.ra_subtypes_rspr
(
    epi_id varchar(75),
    epi_number varchar(6),
    name varchar(25),
    value int
);


CREATE TABLE epbuilder.ra_riskfactors_rspr
(
    epi_id varchar(75),
    epi_number varchar(6),
    name varchar(25),
    value int
);


CREATE TABLE epbuilder.pac_pred_probs_lvl1
(
    epi_id varchar(255),
    pac_prob numeric(40,20)
);


CREATE TABLE epbuilder.ASTHMA_CLAIM_EXAMPLE_filtered
(
    member_id varchar(50),
    master_episode_id varchar(73),
    trigger_master_claim_id varchar(100),
    episode_id varchar(10),
    episode_type varchar(1),
    episode_begin_date date,
    episode_end_date date,
    episode_length_days int,
    trig_begin_date date,
    trig_end_date date,
    truncated int,
    master_claim_id varchar(100),
    claim_source varchar(12),
    assigned_type varchar(2),
    assigned_count int,
    rule varchar(7),
    LEVEL_1 varchar(73),
    LEVEL_2 varchar(73),
    LEVEL_3 varchar(73),
    LEVEL_4 varchar(73),
    LEVEL_5 varchar(73),
    begin_date date,
    end_date date,
    allowed_amt numeric(40,20)
);


CREATE TABLE epbuilder.ASTHMA_CLAIM_EXAMPLE
(
    member_id varchar(50),
    master_episode_id varchar(73),
    trigger_master_claim_id varchar(100),
    episode_id varchar(10),
    episode_type varchar(1),
    episode_begin_date date,
    episode_end_date date,
    episode_length_days int,
    trig_begin_date date,
    trig_end_date date,
    truncated int,
    master_claim_id varchar(100),
    claim_source varchar(12),
    assigned_type varchar(2),
    assigned_count int,
    rule varchar(7),
    LEVEL_1 varchar(73),
    LEVEL_2 varchar(73),
    LEVEL_3 varchar(73),
    LEVEL_4 varchar(73),
    LEVEL_5 varchar(73),
    begin_date date,
    end_date date,
    allowed_amt numeric(40,20)
);


CREATE TABLE epbuilder.Provider_PAC_Rates
(
    Attributed_Provider varchar(10),
    VBP_Contractor varchar(200),
    Episode_ID varchar(10),
    level_assignment int,
    Number_of_Episodes int,
    Number_of_Epis_w_PAC int,
    PAC_Rate numeric(36,18),
    N_Pred int,
    Exp_PAC numeric(40,20),
    Exp_PAC_Rate numeric(58,38),
    OE numeric(132,76)
);


CREATE TABLE epbuilder.ra_cost_use_rspr
(
    epi_id varchar(75),
    epi_number varchar(6),
    name varchar(25),
    value int
);


CREATE TABLE epbuilder.ra_exp_cost_rspr
(
    row_names varchar(255),
    epi_number varchar(255),
    epi_name varchar(255),
    epi_id varchar(255),
    eol_prob numeric(40,20) DEFAULT NULL::numeric(1,0),
    use_prob_ra_typ_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_pred_ra_typ_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    exp_cost_ra_typ_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    use_prob_sa_typ_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_pred_sa_typ_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    exp_cost_sa_typ_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    use_prob_ra_comp_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_pred_ra_comp_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    exp_cost_ra_comp_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    use_prob_sa_comp_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_pred_sa_comp_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    exp_cost_sa_comp_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    use_prob_ra_typ_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_pred_ra_typ_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    exp_cost_ra_typ_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    use_prob_sa_typ_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_pred_sa_typ_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    exp_cost_sa_typ_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    use_prob_ra_comp_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_pred_ra_comp_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    exp_cost_ra_comp_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    use_prob_sa_comp_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    cost_pred_sa_comp_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    exp_cost_sa_comp_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_exp_cost_ra_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_exp_cost_sa_l1 numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_exp_cost_ra_l5 numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_exp_cost_sa_l5 numeric(40,20) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.VBP_Costs
(
    episode_id varchar(24),
    VBP_Contractor varchar(200),
    N int,
    Act_Cost float,
    Exp_Cost float,
    RA_Cost float
);


CREATE TABLE epbuilder.VBP_PAC_Rates2
(
    VBP_Contractor varchar(200),
    Episode_ID varchar(10),
    level_assignment int,
    Number_of_Episodes int,
    Number_of_Epis_w_PAC int,
    PAC_Rate numeric(36,18),
    N_Pred int,
    Exp_PAC numeric(40,20),
    Exp_PAC_Rate numeric(58,38),
    OE numeric(132,76)
);


CREATE TABLE epbuilder.VBP_PAC_Rates
(
    VBP_Contractor varchar(200),
    Episode_ID varchar(10),
    level_assignment int,
    Number_of_Episodes int,
    Number_of_Epis_w_PAC int,
    PAC_Rate numeric(36,18),
    N_Pred int,
    Exp_PAC numeric(40,20),
    Exp_PAC_Rate numeric(58,38),
    OE numeric(132,76)
);


CREATE TABLE epbuilder.avg_cost
(
    episode_id varchar(24),
    avg_cost float,
    avg_exp float
);


CREATE TABLE epbuilder.epi_cost_rel_input_lvl1
(
    member_id varchar(50),
    episode_id varchar(24),
    episode varchar(73),
    attrib_default_physician varchar(10),
    attrib_default_facility varchar(1),
    vbp_contractor varchar(200),
    cost numeric(40,20),
    exp_cost numeric(40,20)
);


CREATE TABLE epbuilder.HIV_PATIENTS
(
    member_id varchar(50)
);


CREATE TABLE epbuilder.ALL_MAT_NBRON_FLAGs
(
    u_c_id int,
    member_id varchar(50),
    episode_id varchar(24),
    master_episode_id varchar(73),
    master_claim_id varchar(100),
    GROUP_ID char(8),
    RF_NAME varchar(80),
    csect_rf int,
    induction_rf int,
    NICU_LVL_4 int
);


CREATE TABLE epbuilder.ALL_MAT_NBRON_FLAGs2
(
    u_c_id int,
    member_id varchar(50),
    episode_id varchar(24),
    master_episode_id varchar(73),
    master_claim_id varchar(100),
    GROUP_ID char(8),
    RF_NAME varchar(80),
    csect_rf int,
    induction_rf int,
    NICU_LVL_4 int,
    level_2 varchar(73),
    level_5 varchar(73),
    DELIVERY_EPISODE varchar(73)
);


CREATE TABLE epbuilder.filters_MAT_NBORN
(
    member_id varchar(50),
    episode_id varchar(24),
    master_episode_id varchar(73),
    CSECT_WARRANTED_FILTER int,
    INDUCTION_WARRANTED_FILTER int,
    NICU_LVL_4_FILTER int
);


CREATE TABLE epbuilder.cost_compare_diab
(
    member_id varchar(50),
    episode_id varchar(24),
    episode varchar(73),
    attrib_default_physician varchar(10),
    attrib_default_facility varchar(1),
    vbp_contractor varchar(200),
    tot_cost numeric(40,20),
    t_cost numeric(40,20),
    c_cost numeric(40,20),
    exp_cost numeric(40,20)
);


CREATE TABLE epbuilder.VBP_Costs_diab
(
    episode_id varchar(24),
    VBP_Contractor varchar(200),
    N int,
    Act_Cost float,
    Exp_Cost float,
    RA_Cost float,
    t_cost float,
    c_cost float,
    pac_pct float
);


CREATE TABLE epbuilder.salient_claim_line
(
    id int,
    master_claim_id varchar(100),
    member_id varchar(50),
    claim_id varchar(50),
    claim_line_id varchar(22),
    sequence_key varchar(11),
    final_version_flag varchar(2),
    claim_encounter_flag int,
    provider_npi varchar(12),
    provider_id varchar(24),
    physician_id varchar(24),
    facility_id varchar(24),
    allowed_amt numeric(40,20),
    facility_type_code varchar(8),
    begin_date date,
    end_date date,
    place_of_svc_code varchar(25),
    claim_line_type_code varchar(12),
    assigned int,
    assigned_count int,
    quantity int,
    standard_payment_amt numeric(40,20),
    charge_amt numeric(40,20),
    paid_amt numeric(15,4),
    prepaid_amt numeric(15,4),
    copay_amt numeric(15,4),
    coinsurance_amt numeric(15,4),
    deductible_amt numeric(15,4),
    insurance_product varchar(24),
    plan_id varchar(24),
    admission_date date,
    admission_src_code varchar(12),
    admit_type_code varchar(12),
    discharge_status_code varchar(12),
    discharge_date date,
    type_of_bill varchar(12),
    rev_count int,
    drg_version varchar(12),
    ms_drg_code varchar(12),
    apr_drg_code varchar(12),
    readmission int,
    office_visit int,
    trigger int,
    ed_visit int,
    ed_visit_id varchar(12),
    core_service int,
    pas int
);


CREATE TABLE epbuilder.salient_mom_baby
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    year varchar(4)
);


CREATE TABLE epbuilder.salient_member_id_final
(
    member_id varchar(50)
);


CREATE TABLE epbuilder.deliveries_level3
(
    Filter_ID int,
    Member_ID varchar(50),
    Master_Episode_ID varchar(255),
    Episode_ID varchar(6),
    Episode_Name varchar(6),
    Episode_Description varchar(255),
    Episode_Type varchar(50),
    MDC varchar(2),
    MDC_Description varchar(255),
    Episode_Begin_Date varchar(10),
    Episode_End_Date varchar(10),
    Episode_Length int,
    Level int,
    Split_Total_Cost numeric(13,2),
    Split_1stPercentile_Cost numeric(13,2),
    Split_99thPercentile_Cost numeric(13,2),
    Split_80thPercentile_Cost numeric(13,2),
    Split_Total_PAC_Cost numeric(13,2),
    Split_Total_Typical_Cost numeric(13,2),
    Split_Total_TypicalwPAC_Cost numeric(13,2),
    Annualized_Split_Total_Cost numeric(13,2),
    Annualized_Split_1stPercentile_Cost numeric(13,2),
    Annualized_Split_99thPercentile_Cost numeric(13,2),
    Annualized_Split_80thPercentile_Cost numeric(13,2),
    Annualized_Split_Total_PAC_Cost numeric(13,2),
    Annualized_Split_Total_Typical_Cost numeric(13,2),
    Annualized_Split_Total_TypicalwPAC_Cost numeric(13,2),
    Unsplit_Total_Cost numeric(13,2),
    Unsplit_1stPercentile_Cost numeric(13,2),
    Unsplit_99thPercentile_Cost numeric(13,2),
    Unsplit_Total_PAC_Cost numeric(13,2),
    Unsplit_Total_Typical_Cost numeric(13,2),
    Unsplit_Total_TypicalwPAC_Cost numeric(13,2),
    Annualized_Unsplit_Total_Cost numeric(13,2),
    Annualized_Unsplit_1stPercentile_Cost numeric(13,2),
    Annualized_Unsplit_99thPercentile_Cost numeric(13,2),
    Annualized_Unsplit_Total_PAC_Cost numeric(13,2),
    Annualized_Unsplit_Total_Typical_Cost numeric(13,2),
    Annualized_Unsplit_Total_TypicalwPAC_Cost numeric(13,2),
    Facility_ID varchar(50),
    Physician_ID varchar(50),
    Physician_Specialty varchar(2),
    Split_Expected_Total_Cost numeric(13,2),
    Split_Expected_Typical_IP_Cost numeric(13,2),
    Split_Expected_Typical_Other_Cost numeric(13,2),
    Split_Expected_PAC_Cost numeric(13,2),
    Unsplit_Expected_Total_Cost numeric(13,2),
    Unsplit_Expected_Typical_IP_Cost numeric(13,2),
    Unsplit_Expected_Typical_Other_Cost numeric(13,2),
    Unsplit_Expected_PAC_Cost numeric(13,2),
    IP_PAC_Count numeric(13,2),
    OP_PAC_Count numeric(13,2),
    PB_PAC_Count numeric(13,2),
    RX_PAC_Count numeric(13,2),
    year numeric(4,0),
    enrolled_month int,
    co_occurence_count_DEPANX int,
    co_occurence_count_DIVERT int,
    co_occurence_count_RHNTS int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_HTN int,
    co_occurence_count_GERD int,
    co_occurence_count_BIPLR int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_CAD int,
    co_occurence_count_COPD int,
    co_occurence_count_HF int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_ASTHMA int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_count_chronic int,
    co_count_all int,
    ip_cost float,
    op_cost float,
    pb_cost float,
    rx_cost float,
    END_OF_STUDY int,
    Facility_ID_provider_name varchar(100),
    Facility_ID_provider_zipcode varchar(100),
    Facility_ID_provider_type varchar(100),
    Physician_ID_provider_name varchar(100),
    Physician_ID_provider_zipcode varchar(1000),
    Physician_ID_provider_type varchar(100),
    state_wide_avg_split_exp_cost float,
    state_wide_avg_unsplit_exp_cost float,
    state_wide_avg_split_total_cost float,
    state_wide_avg_unsplit_total_cost float,
    split_state_wide_pac_percent float,
    unsplit_state_wide_pac_percent float
);


CREATE TABLE epbuilder.salient_ra_exp_cost
(
    row_names varchar(255),
    epi_number varchar(255),
    epi_name varchar(255),
    epi_id varchar(255),
    eol_prob float,
    use_prob_ra_typ_ip_l1 float,
    cost_pred_ra_typ_ip_l1 float,
    exp_cost_ra_typ_ip_l1 float,
    use_prob_sa_typ_ip_l1 float,
    cost_pred_sa_typ_ip_l1 float,
    exp_cost_sa_typ_ip_l1 float,
    use_prob_sa_typ_other_l1 float,
    cost_pred_sa_typ_other_l1 float,
    exp_cost_sa_typ_other_l1 float,
    use_prob_ra_comp_other_l1 float,
    cost_pred_ra_comp_other_l1 float,
    exp_cost_ra_comp_other_l1 float,
    use_prob_sa_comp_other_l1 float,
    cost_pred_sa_comp_other_l1 float,
    exp_cost_sa_comp_other_l1 float,
    use_prob_ra_typ_ip_l4 float,
    cost_pred_ra_typ_ip_l4 float,
    exp_cost_ra_typ_ip_l4 float,
    use_prob_sa_typ_ip_l4 float,
    cost_pred_sa_typ_ip_l4 float,
    exp_cost_sa_typ_ip_l4 float,
    use_prob_ra_typ_other_l4 float,
    cost_pred_ra_typ_other_l4 float,
    exp_cost_ra_typ_other_l4 float,
    use_prob_sa_typ_other_l4 float,
    cost_pred_sa_typ_other_l4 float,
    exp_cost_sa_typ_other_l4 float,
    use_prob_ra_comp_other_l4 float,
    cost_pred_ra_comp_other_l4 float,
    exp_cost_ra_comp_other_l4 float,
    use_prob_sa_comp_other_l4 float,
    cost_pred_sa_comp_other_l4 float,
    exp_cost_sa_comp_other_l4 float,
    total_exp_cost_ra_l1 float,
    total_exp_cost_sa_l1 float,
    total_exp_cost_ra_l4 float,
    total_exp_cost_sa_l4 float,
    use_prob_ra_typ_l1 float,
    cost_pred_ra_typ_l1 float,
    exp_cost_ra_typ_l1 float,
    use_prob_sa_typ_l1 float,
    cost_pred_sa_typ_l1 float,
    exp_cost_sa_typ_l1 float,
    use_prob_ra_comp_l1 float,
    cost_pred_ra_comp_l1 float,
    exp_cost_ra_comp_l1 float,
    use_prob_sa_comp_l1 float,
    cost_pred_sa_comp_l1 float,
    exp_cost_sa_comp_l1 float,
    use_prob_ra_typ_l5 float,
    cost_pred_ra_typ_l5 float,
    exp_cost_ra_typ_l5 float,
    use_prob_sa_typ_l5 float,
    cost_pred_sa_typ_l5 float,
    exp_cost_sa_typ_l5 float,
    use_prob_ra_comp_l5 float,
    cost_pred_ra_comp_l5 float,
    exp_cost_ra_comp_l5 float,
    use_prob_sa_comp_l5 float,
    cost_pred_sa_comp_l5 float,
    exp_cost_sa_comp_l5 float,
    total_exp_cost_ra_l5 float,
    total_exp_cost_sa_l5 float,
    use_prob_ra_typ_ip_l3 float,
    cost_pred_ra_typ_ip_l3 float,
    exp_cost_ra_typ_ip_l3 float,
    use_prob_sa_typ_ip_l3 float,
    cost_pred_sa_typ_ip_l3 float,
    exp_cost_sa_typ_ip_l3 float,
    use_prob_ra_typ_other_l3 float,
    cost_pred_ra_typ_other_l3 float,
    exp_cost_ra_typ_other_l3 float,
    use_prob_sa_typ_other_l3 float,
    cost_pred_sa_typ_other_l3 float,
    exp_cost_sa_typ_other_l3 float,
    use_prob_ra_comp_other_l3 float,
    cost_pred_ra_comp_other_l3 float,
    exp_cost_ra_comp_other_l3 float,
    use_prob_sa_comp_other_l3 float,
    cost_pred_sa_comp_other_l3 float,
    exp_cost_sa_comp_other_l3 float,
    total_exp_cost_ra_l3 float,
    total_exp_cost_sa_l3 float
);


CREATE TABLE epbuilder.salient_episodes_all
(
    member_id varchar(50),
    master_episode_id varchar(73)
);


CREATE TABLE epbuilder.salient_association
(
    id int,
    parent_master_episode_id varchar(73),
    child_master_episode_id varchar(73),
    association_type varchar(24),
    association_level int,
    association_count int,
    association_start_day varchar(15),
    association_end_day varchar(15)
);


CREATE TABLE epbuilder.salient_episode
(
    id int,
    member_id varchar(50),
    claim_id varchar(50),
    claim_line_id varchar(22),
    master_episode_id varchar(73),
    master_claim_id varchar(100),
    episode_id varchar(10),
    episode_type varchar(1),
    episode_begin_date date,
    episode_end_date date,
    episode_length_days int,
    trig_begin_date date,
    trig_end_date date,
    associated int,
    association_count int,
    association_level int,
    truncated int,
    attrib_default_physician varchar(30),
    attrib_default_facility varchar(30)
);


CREATE TABLE epbuilder.salient_claims_combined
(
    id int,
    master_claim_id varchar(100),
    member_id varchar(50),
    allowed_amt numeric(40,20),
    assigned_count int,
    claim_line_type_code varchar(12),
    begin_date date,
    end_date date
);


CREATE TABLE epbuilder.salient_filtered_episodes
(
    filter_id int,
    master_episode_id varchar(73),
    filter_fail char(1),
    age_limit_lower char(1),
    age_limit_upper char(1),
    episode_cost_upper char(1),
    episode_cost_lower char(1),
    coverage_gap char(1),
    episode_complete char(1),
    drg char(1),
    proc_ep_orphan char(1),
    proc_ep_orph_triggered char(1)
);


CREATE TABLE epbuilder.salient_assignment
(
    id int,
    member_id varchar(50),
    master_claim_id varchar(100),
    master_episode_id varchar(73),
    claim_source varchar(12),
    assigned_type varchar(2),
    assigned_count int,
    rule varchar(7),
    pac int,
    pac_type varchar(2),
    episode_period int,
    IP_period int,
    id_new int
);


CREATE TABLE epbuilder.salient_mel
(
    id int,
    filter_id int,
    master_episode_id varchar(73),
    claim_type varchar(2),
    level int,
    split int,
    annualized int,
    cost numeric(40,20),
    cost_t numeric(40,20),
    cost_tc numeric(40,20),
    cost_c numeric(40,20),
    risk_factor_count int,
    sub_type_count int,
    probability_of_complications int,
    IP_stay_count int,
    IP_stay_facility_costs numeric(40,20),
    IP_stay_prof_costs numeric(40,20),
    IP_stay_total_costs numeric(40,20),
    IP_stay_bed_days int,
    IP_stay_avg_length int
);


CREATE TABLE epbuilder.PAC_groups
(
    episode_id varchar(6),
    episode_name varchar(6),
    episode_description varchar(37),
    code_id varchar(10),
    type_id varchar(4),
    code_name varchar(250),
    group_id char(8),
    group_name varchar(80)
);


CREATE TABLE epbuilder.build_episode_reference_old
(
    EPISODE_ID varchar(6) DEFAULT NULL,
    NAME varchar(6) DEFAULT NULL,
    TYPE varchar(45) DEFAULT NULL,
    STATUS varchar(45) DEFAULT NULL,
    DESCRIPTION varchar(37) DEFAULT NULL,
    CREATED_DATE timestamp DEFAULT NULL::timestamp,
    MODIFIED_DATE timestamp DEFAULT NULL::timestamp,
    USERS_USER_ID varchar(45) DEFAULT NULL,
    MDC_CATEGORY varchar(2) DEFAULT NULL,
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


CREATE TABLE epbuilder.run
(
    uid int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.assignment
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.association
(
    id int DEFAULT NULL::int,
    parent_master_episode_id varchar(73) DEFAULT NULL,
    child_master_episode_id varchar(73) DEFAULT NULL,
    association_type varchar(24) DEFAULT NULL,
    association_level int DEFAULT NULL::int,
    association_count int DEFAULT NULL::int,
    association_start_day varchar(15) DEFAULT NULL,
    association_end_day varchar(15) DEFAULT NULL
);


CREATE TABLE epbuilder.claim_line
(
    id int DEFAULT NULL::int,
    master_claim_id varchar(100) DEFAULT NULL,
    member_id varchar(50) DEFAULT NULL,
    claim_id varchar(50) DEFAULT NULL,
    claim_line_id varchar(22) DEFAULT NULL,
    sequence_key varchar(11) DEFAULT NULL,
    final_version_flag varchar(2) DEFAULT NULL,
    claim_encounter_flag int DEFAULT NULL::int,
    provider_npi varchar(12) DEFAULT NULL,
    provider_id varchar(24) DEFAULT NULL,
    physician_id varchar(24) DEFAULT NULL,
    facility_id varchar(24) DEFAULT NULL,
    allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    real_allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    proxy_allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
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


CREATE TABLE epbuilder.claim_line_rx
(
    id int DEFAULT NULL::int,
    master_claim_id varchar(100) DEFAULT NULL,
    member_id varchar(50) DEFAULT NULL,
    claim_id varchar(50) DEFAULT NULL,
    sequence_key varchar(11) DEFAULT NULL,
    final_version_flag varchar(5) DEFAULT NULL,
    claim_encounter_flag int DEFAULT NULL::int,
    allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    real_allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    proxy_allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
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
    plan_id varchar(24) DEFAULT NULL
);


CREATE TABLE epbuilder.claims_combined
(
    id int DEFAULT NULL::int,
    master_claim_id varchar(100) DEFAULT NULL,
    member_id varchar(50) DEFAULT NULL,
    allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    assigned_count int DEFAULT NULL::int,
    claim_line_type_code varchar(12) DEFAULT NULL,
    begin_date date DEFAULT NULL::date,
    end_date date DEFAULT NULL::date,
    real_allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0),
    proxy_allowed_amt numeric(40,20) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.code
(
    id int DEFAULT NULL::int,
    u_c_id int NOT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    function_code varchar(10) DEFAULT NULL,
    code_value varchar(12) DEFAULT NULL,
    nomen varchar(7) DEFAULT NULL,
    principal int DEFAULT 0
);


CREATE TABLE epbuilder.episode
(
    id int DEFAULT NULL::int,
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
    attrib_default_physician varchar(30) DEFAULT NULL,
    attrib_default_facility varchar(30) DEFAULT NULL
);


CREATE TABLE epbuilder.episode_aggregates
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


CREATE TABLE epbuilder.filtered_episodes
(
    filter_id int DEFAULT NULL::int,
    master_episode_id varchar(73) DEFAULT NULL,
    filter_fail int DEFAULT 0,
    age_limit_lower int NOT NULL DEFAULT 0,
    age_limit_upper int NOT NULL DEFAULT 0,
    episode_cost_upper int NOT NULL DEFAULT 0,
    episode_cost_lower int NOT NULL DEFAULT 0,
    coverage_gap int NOT NULL DEFAULT 0,
    episode_complete int NOT NULL DEFAULT 0,
    drg int NOT NULL DEFAULT 0,
    proc_ep_orphan int NOT NULL DEFAULT 0,
    proc_ep_orph_triggered int NOT NULL DEFAULT 0,
    dual_eligable numeric(1,0)
);


CREATE TABLE epbuilder.master_epid_code
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.master_epid_level
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.member
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.member_epid_level
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.member_master_epid_level
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.ndc_to_multum
(
    id int DEFAULT NULL::int,
    multum varchar(10) DEFAULT NULL,
    ndc varchar(12) DEFAULT NULL,
    drug_category varchar(14) DEFAULT NULL
);


CREATE TABLE epbuilder.pmp_su
(
    pmp_su_id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.pmp_su_metric_desc
(
    pmp_su_metric_id int DEFAULT NULL::int,
    metric_name varchar(30) DEFAULT NULL,
    metric_desc varchar(80) DEFAULT NULL
);


CREATE TABLE epbuilder.pmp_su_ranks
(
    id int DEFAULT NULL::int,
    pmp_su_id int DEFAULT NULL::int,
    pmp_su_metric_id int DEFAULT NULL::int,
    rank int DEFAULT NULL::int,
    count int DEFAULT NULL::int,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.provider
(
    id int DEFAULT NULL::int,
    provider_id varchar(24) DEFAULT NULL,
    npi varchar(12) DEFAULT NULL,
    dea_no varchar(20) DEFAULT NULL,
    group_name varchar(75) DEFAULT NULL,
    practice_name varchar(75) DEFAULT NULL,
    provider_name varchar(75) DEFAULT NULL,
    system_name varchar(75) DEFAULT NULL,
    tax_id varchar(11) DEFAULT NULL,
    medicare_id varchar(20) DEFAULT NULL,
    zipcode varchar(25) DEFAULT NULL,
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


CREATE TABLE epbuilder.provider_epid
(
    id int DEFAULT NULL::int,
    provider_id varchar(30) DEFAULT NULL,
    episode_id varchar(10) DEFAULT NULL,
    member_count int DEFAULT NULL::int,
    episode_count int DEFAULT NULL::int,
    severity_score int DEFAULT NULL::int
);


CREATE TABLE epbuilder.provider_epid_level
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.provider_master_epid_level
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.provider_specialty
(
    id int DEFAULT NULL::int,
    provider_uid int DEFAULT NULL::int,
    specialty_id varchar(24) DEFAULT NULL,
    code_source varchar(10) DEFAULT NULL
);


CREATE TABLE epbuilder.tmp_annids
(
    master_episode_id varchar(73) DEFAULT NULL,
    parent_child_id varchar(2) DEFAULT NULL
);


CREATE TABLE epbuilder.tmp_annids_c
(
    master_episode_id varchar(73) DEFAULT NULL,
    lvl int DEFAULT NULL::int
);


CREATE TABLE epbuilder.tmp_enroll_gap
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
    filter_fail int DEFAULT 0,
    data_latest_begin_date date,
    annualize_begin_date date,
    data_start_date date,
    end_of_study int DEFAULT 0,
    annualizable int DEFAULT 0
);


CREATE TABLE epbuilder.tmp_filt_proc_orp_trig
(
    master_episode_id varchar(73) DEFAULT NULL
);


CREATE TABLE epbuilder.tmp_hlow
(
    episode_id varchar(10) DEFAULT NULL,
    low numeric(40,20) DEFAULT NULL::numeric(1,0),
    high numeric(40,20) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.tmp_mel1
(
    master_episode_id varchar(73) DEFAULT NULL,
    cost numeric(40,20) DEFAULT 0.00000000000000000000,
    cost_t numeric(40,20) DEFAULT 0.00000000000000000000,
    cost_tc numeric(40,20) DEFAULT 0.00000000000000000000,
    cost_c numeric(40,20) DEFAULT 0.00000000000000000000
);


CREATE TABLE epbuilder.tmp_proc_orph
(
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    claim_line_type_code varchar(12) DEFAULT NULL,
    begin_date date DEFAULT NULL::date,
    end_date date DEFAULT NULL::date,
    confirmed int DEFAULT 0
);


CREATE TABLE epbuilder.tmp_sub_association
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


CREATE TABLE epbuilder.tmp_total_cost
(
    level int DEFAULT NULL::int,
    total_cost_unsplit numeric(40,20) DEFAULT NULL::numeric(1,0),
    total_cost_split numeric(40,20) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.tmp_uber
(
    id int DEFAULT NULL::int,
    parent_master_episode_id varchar(73) DEFAULT NULL,
    child_master_episode_id varchar(73) DEFAULT NULL,
    association_type varchar(24) DEFAULT NULL,
    association_level int DEFAULT NULL::int,
    association_count int DEFAULT NULL::int,
    association_start_day varchar(15) DEFAULT NULL,
    association_end_day varchar(15) DEFAULT NULL
);


CREATE TABLE epbuilder.triggers
(
    id int DEFAULT NULL::int,
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


CREATE TABLE epbuilder.unassigned_episodes
(
    member_id varchar(50) DEFAULT NULL,
    episode_id varchar(10) DEFAULT NULL,
    episode_begin_date date DEFAULT NULL::date,
    cost numeric(40,20) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.build_episode_reference
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


CREATE TABLE epbuilder.filter_params
(
    filter_id int NOT NULL,
    episode_id varchar(11) DEFAULT NULL,
    lower_age_limit int DEFAULT NULL::int,
    upper_age_limit int DEFAULT NULL::int
);


CREATE TABLE epbuilder.filters
(
    filter_id int NOT NULL,
    fiilter_name varchar(73) DEFAULT NULL
);


CREATE TABLE epbuilder.provider_attribution
(
    id int NOT NULL,
    master_episode_id varchar(73) DEFAULT NULL,
    master_claim_id varchar(100) DEFAULT NULL,
    episode_id varchar(10) DEFAULT NULL,
    episode_type varchar(1) DEFAULT NULL,
    claim_line_type_code varchar(12) DEFAULT NULL,
    trig_begin_date date DEFAULT NULL::date,
    trig_end_date date DEFAULT NULL::date,
    triggering_physician varchar(30) DEFAULT NULL,
    triggering_facility varchar(30) DEFAULT NULL,
    trig_claim_attr_phys varchar(30) DEFAULT NULL,
    trig_claim_attr_fac varchar(30) DEFAULT NULL,
    em_count_attr varchar(30) DEFAULT NULL,
    em_cost_attr varchar(30) DEFAULT NULL
);


CREATE TABLE epbuilder.Assign_PAC_Totals
(
    EPISODE varchar(73),
    Level_Assignment varchar(1),
    IP_PAC_Count int,
    OP_PAC_Count int,
    PB_PAC_Count int,
    RX_PAC_Count int
);


CREATE TABLE epbuilder.ra_episode_data
(
    epi_id varchar(73),
    member_id varchar(50),
    epi_number varchar(6),
    epi_name varchar(6),
    female int,
    age int,
    rec_enr int,
    eol_ind int
);


CREATE TABLE epbuilder.ra_riskfactors
(
    epi_id varchar(75),
    epi_number varchar(6),
    name varchar(25),
    value int
);


CREATE TABLE epbuilder.ra_subtypes
(
    epi_id varchar(75),
    epi_number varchar(6),
    name varchar(25),
    value int
);


CREATE TABLE epbuilder.tmp_subt_claims
(
    master_claim_id varchar(100),
    id int,
    epi varchar(10),
    trig varchar(10),
    begin_date timestamp,
    master_episode_id varchar(73),
    episode_id varchar(10)
);


CREATE TABLE epbuilder.episode_sub_types
(
    master_episode_id varchar(73),
    master_claim_id varchar(100),
    code_value varchar(12),
    subtype_group_id varchar(25),
    group_name varchar(255),
    episode_id varchar(10)
);


CREATE TABLE epbuilder.global_risk_factors
(
    member_id varchar(100),
    master_claim_id varchar(62),
    claim_date date,
    code_value char(12),
    factor_id varchar(25)
);


CREATE TABLE epbuilder.tmp_trig_claims
(
    master_episode_id varchar(73),
    episode_id varchar(10),
    episode_type varchar(1),
    assigned_type varchar(2),
    claim_line_type_code varchar(12),
    allowed_amt numeric(15,4),
    split_allowed_amt numeric(15,4)
);


CREATE TABLE epbuilder.tmp_enroll
(
    member_id varchar(12),
    begin_date date
);


CREATE TABLE epbuilder.enrollment
(
    id int DEFAULT NULL::int,
    member_id varchar(50) DEFAULT NULL,
    begin_date date DEFAULT NULL::date,
    end_date date DEFAULT NULL::date,
    age_at_enrollment int DEFAULT NULL::int,
    insurance_product varchar(24) DEFAULT NULL,
    coverage_type varchar(24) DEFAULT NULL,
    isGap int DEFAULT NULL::int
);


CREATE TABLE epbuilder.risk_factors
(
    dx varchar(25),
    codename varchar(255),
    factor_id varchar(25),
    factorname varchar(255)
);


CREATE TABLE epbuilder.sub_type_codes
(
    EPISODE_ID varchar(12),
    EPISODE_NAME varchar(50),
    CODE_VALUE varchar(25),
    TYPE_ID varchar(10),
    GROUP_ID varchar(12),
    GROUP_NAME varchar(255),
    SUBTYPE_GROUP_ID varchar(25)
);


CREATE TABLE epbuilder.mom_baby
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    year varchar(4)
);


CREATE TABLE epbuilder.vbp_ppe_ppa
(
    claim_key numeric(38,0),
    claim_id varchar(30),
    claim_trans_id numeric(38,0),
    dsch_dt date,
    claim_type_code varchar(2),
    preventable_group varchar(1),
    pfp_preventable_group_name varchar(30),
    pfp_preventable_group_desc varchar(50),
    preventable_status varchar(2),
    pfp_preventable_status_desc varchar(150),
    preventable_reason varchar(2),
    pfp_preventable_reason_desc varchar(150),
    mdw_insert_dt date,
    mdw_update_dt date
);


CREATE TABLE epbuilder.vbp_ppe_ppr
(
    claim_id varchar(30),
    claim_trans_id numeric(38,0),
    dsch_dt date,
    claim_type_code varchar(2),
    preventable_group varchar(3),
    preventable_group_desc varchar(50),
    ppr_type_code varchar(2),
    ppr_type_code_desc varchar(100),
    mdw_insert_dt date,
    mdw_update_dt date
);


CREATE TABLE epbuilder.vbp_ppe_pps
(
    claim_key numeric(38,0),
    claim_id varchar(30),
    claim_trans_id numeric(38,0),
    claim_line_num numeric(38,0),
    srv_dt date,
    claim_type_code varchar(2),
    preventbl_group_code varchar(3),
    pfp_preventbl_group_name varchar(30),
    pfp_preventbl_group_desc varchar(50),
    item_preventbl_status_code varchar(2),
    pfp_preventbl_status_item_desc varchar(150),
    item_preventbl_rsn_code varchar(2),
    pfp_preventbl_rsn_item_desc varchar(150),
    mdw_insert_dt date,
    mdw_update_dt date
);


CREATE TABLE epbuilder.vbp_ppe_ppv
(
    claim_key numeric(38,0),
    claim_id varchar(30),
    claim_trans_id numeric(38,0),
    dsch_dt date,
    claim_type_code varchar(2),
    preventable_group varchar(1),
    pfp_preventable_group_name varchar(30),
    pfp_preventable_group_desc varchar(50),
    preventable_status varchar(2),
    pfp_preventable_status_desc varchar(150),
    preventable_reason varchar(2),
    pfp_preventable_reason_desc varchar(150),
    mdw_insert_dt date,
    mdw_update_dt date
);


CREATE TABLE epbuilder.county_mcregion
(
    county varchar(100),
    mcregion varchar(100)
);


CREATE TABLE epbuilder.crg
(
    year int,
    quarter varchar(2),
    crg_version varchar(13),
    claim_begin_date date,
    claim_end_date date,
    claims_as_of_date date,
    recip_id char(11),
    qcrg_code int,
    qcrg_desc varchar(140),
    qacrg1_code int,
    qacrg1_desc varchar(140),
    qacrg2_code int,
    qacrg2_desc varchar(140),
    qacrg3_code int,
    qacrg3_desc varchar(140),
    fincrg_q varchar(3),
    q_base varchar(2),
    q_severity varchar(1),
    crg_code int,
    crg_desc varchar(140),
    acrg1_code int,
    acrg1_desc varchar(140),
    acrg2_code int,
    acrg2_desc varchar(140),
    acrg3_code int,
    acrg3_desc varchar(140),
    fincrg_g varchar(3),
    g_base varchar(2),
    g_severity varchar(1),
    fincrg varchar(3),
    base varchar(2),
    severity varchar(1),
    total_allowed_amount numeric(13,2),
    total_medical_allowed numeric(13,2),
    total_pharmacy_allowed numeric(13,2),
    total_episode_costs numeric(13,2),
    acrg_weight float,
    qcrg_weight float,
    acrg_average numeric(13,2),
    qcrg_average numeric(13,2)
);


CREATE TABLE epbuilder.crg_2012_2013
(
    YEAR int,
    QUARTER varchar(2),
    CRG_VERSION varchar(13),
    CLAIM_BEGIN_DATE date,
    CLAIM_END_DATE date,
    CLAIMS_AS_OF_DATE date,
    RECIP_ID char(11),
    QCRG_CODE varchar(5),
    QACRG1_CODE varchar(10),
    QACRG2_CODE varchar(4),
    QACRG3_CODE varchar(2),
    FINCRG_Q varchar(3),
    Q_BASE varchar(2),
    Q_SEVERITY varchar(1),
    CRG_CODE varchar(5),
    ACRG1_CODE varchar(6),
    ACRG2_CODE varchar(4),
    ACRG3_CODE varchar(2),
    FINCRG_G varchar(3),
    G_BASE varchar(2),
    G_SEVERITY varchar(1),
    FINCRG varchar(3),
    BASE varchar(2),
    SEVERITY varchar(1),
    acrg1_desc varchar(140),
    acrg2_desc varchar(140),
    acrg3_desc varchar(140),
    crg_desc varchar(140),
    qacrg1_desc varchar(140),
    qacrg2_desc varchar(140),
    qacrg3_desc varchar(140),
    qcrg_desc varchar(140)
);


CREATE TABLE epbuilder.harp_recip
(
    member_id varchar(50)
);


CREATE TABLE epbuilder.hh_table
(
    member_id varchar(11),
    hh varchar(8),
    mmis varchar(70),
    npi varchar(10)
);


CREATE TABLE epbuilder.hiv_recip
(
    member_id varchar(11)
);


CREATE TABLE epbuilder.maimo_harp_member
(
    member_id varchar(20)
);


CREATE TABLE epbuilder.mltc_recip
(
    member_id varchar(11)
);


CREATE TABLE epbuilder.mdc_desc
(
    mdc varchar(5),
    mdc_description varchar(255)
);


CREATE TABLE epbuilder.mco_mmis
(
    mco varchar(200),
    mmis varchar(20)
);


CREATE TABLE epbuilder.mco_table
(
    member_id varchar(20),
    mco varchar(200),
    mmis varchar(20),
    npi varchar(20)
);


CREATE TABLE epbuilder.MMCOR_provider_type
(
    provider_id varchar(20),
    provider_npi varchar(100),
    provider_type varchar(500)
);


CREATE TABLE epbuilder.mom_baby_2014
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50)
);


CREATE TABLE epbuilder.npi_mmcor_category
(
    npi varchar(50),
    category varchar(10),
    category_description varchar(500)
);


CREATE TABLE epbuilder.ny_zip_to_county
(
    zipcode varchar(20),
    state varchar(50),
    county varchar(100)
);


CREATE TABLE epbuilder.dd_recip
(
    member_id varchar(11)
);


CREATE TABLE epbuilder.PPS_table
(
    member_id varchar(100),
    PPS varchar(100)
);


CREATE TABLE epbuilder.PCP_table
(
    member_id varchar(20),
    pcp varchar(200),
    mmis varchar(20),
    npi varchar(20)
);


CREATE TABLE epbuilder.vbp_contractor_provider_npi
(
    provider_npi varchar(20),
    vbp_contractor varchar(200),
    PGroup varchar(300)
);


CREATE TABLE epbuilder.QC_mom_baby
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    year varchar(4)
);


CREATE TABLE epbuilder.QC_SAMPLE_PT_LIST_final
(
    member_id varchar(50)
);


CREATE TABLE epbuilder.sub_distinct
(
    child_master_episode_id varchar(73),
    association_level int
);


CREATE TABLE epbuilder.report_episode_detail
(
    Filter_ID int DEFAULT NULL::int,
    Member_ID varchar(50) DEFAULT NULL,
    Master_Episode_ID varchar(255) DEFAULT NULL,
    Episode_ID varchar(6) DEFAULT NULL,
    Episode_Name varchar(6) DEFAULT NULL,
    Episode_Description varchar(255) DEFAULT NULL,
    Episode_Type varchar(50) DEFAULT NULL,
    MDC varchar(2),
    MDC_Description varchar(255) DEFAULT NULL,
    Episode_Begin_Date varchar(10) DEFAULT NULL,
    Episode_End_Date varchar(10) DEFAULT NULL,
    Episode_Length int DEFAULT NULL::int,
    Level int DEFAULT NULL::int,
    Split_Total_Cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    Split_1stPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Split_99thPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Split_80thPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Split_Total_PAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Split_Total_Typical_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Split_Total_TypicalwPAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_1stPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_99thPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_80thPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_PAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_Typical_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_TypicalwPAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_1stPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_99thPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_PAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_Typical_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_TypicalwPAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_1stPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_99thPercentile_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_PAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_Typical_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_TypicalwPAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Facility_ID varchar(50) DEFAULT NULL,
    Physician_ID varchar(50) DEFAULT NULL,
    Physician_Specialty varchar(2) DEFAULT NULL,
    Split_Expected_Total_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Split_Expected_Typical_IP_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Split_Expected_Typical_Other_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Split_Expected_PAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Expected_Total_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Expected_Typical_IP_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Expected_Typical_Other_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Expected_PAC_Cost numeric(25,2) DEFAULT NULL::numeric(1,0),
    IP_PAC_Count numeric(25,2) DEFAULT NULL::numeric(1,0),
    OP_PAC_Count numeric(25,2) DEFAULT NULL::numeric(1,0),
    PB_PAC_Count numeric(25,2) DEFAULT NULL::numeric(1,0),
    RX_PAC_Count numeric(25,2) DEFAULT NULL::numeric(1,0),
    year numeric(4,0),
    enrolled_month int,
    co_occurence_count_DEPANX int,
    co_occurence_count_DIVERT int,
    co_occurence_count_RHNTS int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_HTN int,
    co_occurence_count_GERD int,
    co_occurence_count_BIPLR int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_CAD int,
    co_occurence_count_COPD int,
    co_occurence_count_HF int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_ASTHMA int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_count_chronic int,
    co_count_all int,
    ip_cost float,
    op_cost float,
    pb_cost float,
    rx_cost float,
    END_OF_STUDY int,
    Facility_ID_provider_name varchar(100),
    Facility_ID_provider_zipcode varchar(100),
    Facility_ID_provider_type varchar(100),
    Physician_ID_provider_name varchar(100),
    Physician_ID_provider_zipcode varchar(1000),
    Physician_ID_provider_type varchar(100)
);


CREATE TABLE epbuilder.level_5
(
    member_id varchar(50),
    LEVEL_1 varchar(73),
    LEVEL_2 varchar(73),
    LEVEL_3 varchar(73),
    LEVEL_4 varchar(73),
    LEVEL_5 varchar(73)
);


CREATE TABLE epbuilder.assign_1
(
    member_id varchar(50),
    LEVEL_1 varchar(73),
    LEVEL_2 varchar(73),
    LEVEL_3 varchar(73),
    LEVEL_4 varchar(73),
    LEVEL_5 varchar(73),
    claim_source varchar(12),
    assigned_type varchar(2),
    master_claim_id varchar(100)
);


CREATE TABLE epbuilder.tmp_ann_mel
(
    master_episode_id varchar(73),
    level int,
    parent_child_id varchar(2),
    cost numeric(15,4),
    cost_t numeric(15,4),
    cost_tc numeric(15,4),
    cost_c numeric(15,4)
);


CREATE TABLE epbuilder.percentiles
(
    Filter_id int,
    Master_episode_id varchar(255),
    Episode_ID varchar(6),
    Level int,
    Split_1stPercentile_Cost float,
    Split_99thPercentile_Cost float,
    Split_80thPercentile_Cost float,
    Unsplit_1stPercentile_Cost float,
    Unsplit_99thPercentile_Cost float,
    Annualized_Split_1stPercentile_Cost float,
    Annualized_Split_99thPercentile_Cost float,
    Annualized_Split_80thPercentile_Cost float,
    Annualized_Unsplit_1stPercentile_Cost float,
    Annualized_Unsplit_99thPercentile_Cost float
);


CREATE TABLE epbuilder.run_month_year
(
    month int,
    year numeric(18,0),
    date date
);


CREATE TABLE epbuilder.enrollment_raw
(
    year numeric(18,0),
    month int,
    date date,
    member_id varchar(50),
    insurance_product varchar(24),
    birth_year int,
    sex varchar(2),
    dod date
);


CREATE TABLE epbuilder.enrollment_raw_year_1
(
    year numeric(18,0),
    month int,
    member_id varchar(50),
    birth_year int,
    sex varchar(2),
    dod date
);


CREATE TABLE epbuilder.enrollment_raw_year_2
(
    year numeric(18,0),
    month int,
    member_id varchar(50),
    birth_year int,
    sex varchar(2),
    dod date
);


CREATE TABLE epbuilder.enrolled_month
(
    member_id varchar(50),
    year numeric(18,0),
    enrolled_month int
);


CREATE TABLE epbuilder.first_date_of_service
(
    master_episode_id varchar(73),
    min date
);


CREATE TABLE epbuilder.BABY_LEVEL_NURSERY
(
    member_id varchar(50),
    code_value varchar(12)
);


CREATE TABLE epbuilder.BABY_NURSERY
(
    member_id varchar(50),
    NURSERY_LEVEL int
);


CREATE TABLE epbuilder.del_cost_info
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    DEL_master_episode_id varchar(73),
    DELIVERY_TYPE varchar(6),
    trig_begin_date date,
    year varchar(4),
    SPLIT_TOTAL_DEL_COST numeric(40,20),
    SPLIT_TYPICAL_DEL_COST numeric(40,20),
    SPLIT_PAC_DEL_COST numeric(40,20),
    UNSPLIT_TOTAL_DEL_COST numeric(40,20),
    UNSPLIT_TYPICAL_DEL_COST numeric(40,20),
    UNSPLIT_PAC_DEL_COST numeric(40,20)
);


CREATE TABLE epbuilder.preg_cost_info
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    PREG_master_episode_id varchar(73),
    trig_begin_date date,
    year varchar(4),
    SPLIT_TOTAL_PREGN_COST numeric(40,20),
    SPLIT_TYPICAL_PREGN_COST numeric(40,20),
    SPLIT_PAC_PREGN_COST numeric(40,20),
    UNSPLIT_TOTAL_PREGN_COST numeric(40,20),
    UNSPLIT_TYPICAL_PREGN_COST numeric(40,20),
    UNSPLIT_PAC_PREGN_COST numeric(40,20)
);


CREATE TABLE epbuilder.baby_cost_info
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    NEWBORN_master_episode_id varchar(73),
    trig_begin_date date,
    year varchar(4),
    SPLIT_TOTAL_NEWBORN_COST numeric(40,20),
    SPLIT_TYPICAL_NEWBORN_COST numeric(40,20),
    SPLIT_PAC_NEWBORN_COST numeric(40,20),
    UNSPLIT_TOTAL_NEWBORN_COST numeric(40,20),
    UNSPLIT_TYPICAL_NEWBORN_COST numeric(40,20),
    UNSPLIT_PAC_NEWBORN_COST numeric(40,20),
    NURSERY_LEVEL int
);


CREATE TABLE epbuilder.report_episode_summary
(
    Filter_ID int DEFAULT NULL::int,
    Episode_ID varchar(6) NOT NULL,
    Episode_Name varchar(6) DEFAULT NULL,
    Episode_Description varchar(255) DEFAULT NULL,
    Episode_Type varchar(50) DEFAULT NULL,
    MDC varchar(2) DEFAULT NULL,
    MDC_Description varchar(255) DEFAULT NULL,
    Level int NOT NULL DEFAULT 0,
    Episode_Volume int DEFAULT NULL::int,
    Split_Total_Cost float DEFAULT NULL::float,
    Split_Average_Cost float DEFAULT NULL::float,
    Split_1stPercentile_Cost float DEFAULT NULL::float,
    Split_99thPercentile_Cost float DEFAULT NULL::float,
    Split_Min_Cost float DEFAULT NULL::float,
    Split_Max_Cost float DEFAULT NULL::float,
    Split_STDEV float DEFAULT NULL::float,
    Split_CV float DEFAULT NULL::float,
    Split_Total_PAC_Cost float DEFAULT NULL::float,
    Split_Average_PAC_Cost float DEFAULT NULL::float,
    Split_PAC_Percent numeric(5,2) DEFAULT NULL::numeric(1,0),
    Split_Total_Typical_Cost float DEFAULT NULL::float,
    Split_Average_Typical_Cost float DEFAULT NULL::float,
    Split_Total_TypicalwPAC_Cost float DEFAULT NULL::float,
    Split_Average_TypicalwPAC_Cost float DEFAULT NULL::float,
    Annualized_Split_Total_Cost float DEFAULT NULL::float,
    Annualized_Split_Average_Cost float DEFAULT NULL::float,
    Annualized_Split_1stPercentile_Cost float DEFAULT NULL::float,
    Annualized_Split_99thPercentile_Cost float DEFAULT NULL::float,
    Annualized_Split_Min_Cost float DEFAULT NULL::float,
    Annualized_Split_Max_Cost float DEFAULT NULL::float,
    Annualized_Split_STDEV float DEFAULT NULL::float,
    Annualized_Split_CV float DEFAULT NULL::float,
    Annualized_Split_Total_PAC_Cost float DEFAULT NULL::float,
    Annualized_Split_Average_PAC_Cost float DEFAULT NULL::float,
    Annualized_Split_PAC_Percent numeric(5,2) DEFAULT NULL::numeric(1,0),
    Annualized_Split_Total_Typical_Cost float DEFAULT NULL::float,
    Annualized_Split_Average_Typical_Cost float DEFAULT NULL::float,
    Annualized_Split_Total_TypicalwPAC_Cost float DEFAULT NULL::float,
    Annualized_Split_Average_TypicalwPAC_Cost float DEFAULT NULL::float,
    Unsplit_Total_Cost float DEFAULT NULL::float,
    Unsplit_Average_Cost float DEFAULT NULL::float,
    Unsplit_1stPercentile_Cost float DEFAULT NULL::float,
    Unsplit_99thPercentile_Cost float DEFAULT NULL::float,
    Unsplit_Min_Cost float DEFAULT NULL::float,
    Unsplit_Max_Cost float DEFAULT NULL::float,
    Unsplit_STDEV float DEFAULT NULL::float,
    Unsplit_CV float DEFAULT NULL::float,
    Unsplit_Total_PAC_Cost float DEFAULT NULL::float,
    Unsplit_Average_PAC_Cost float DEFAULT NULL::float,
    Unsplit_PAC_Percent numeric(5,2) DEFAULT NULL::numeric(1,0),
    Unsplit_Total_Typical_Cost float DEFAULT NULL::float,
    Unsplit_Average_Typical_Cost float DEFAULT NULL::float,
    Unsplit_Total_TypicalwPAC_Cost float DEFAULT NULL::float,
    Unsplit_Average_TypicalwPAC_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Total_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Average_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_1stPercentile_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_99thPercentile_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Min_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Max_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_STDEV float DEFAULT NULL::float,
    Annualized_Unsplit_CV float DEFAULT NULL::float,
    Annualized_Unsplit_Total_PAC_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Average_PAC_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_PAC_Percent numeric(5,2) DEFAULT NULL::numeric(1,0),
    Annualized_Unsplit_Total_Typical_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Average_Typical_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Total_TypicalwPAC_Cost float DEFAULT NULL::float,
    Annualized_Unsplit_Average_TypicalwPAC_Cost float DEFAULT NULL::float,
    Expected_Split_Average_Cost float DEFAULT NULL::float,
    Expected_Split_Typical_IP_Average_Cost float DEFAULT NULL::float,
    Expected_Split_Typical_Other_Average_Cost float DEFAULT NULL::float,
    Expected_Split_PAC_Average_Cost float DEFAULT NULL::float,
    Expected_Unsplit_Average_Cost float DEFAULT NULL::float,
    Expected_Unsplit_Typical_IP_Average_Cost float DEFAULT NULL::float,
    Expected_Unsplit_Typical_Other_Average_Cost float DEFAULT NULL::float,
    Expected_Unsplit_PAC_Average_Cost float DEFAULT NULL::float
);

ALTER TABLE epbuilder.report_episode_summary ADD CONSTRAINT report_episode_summary_pk PRIMARY KEY (Episode_ID, Level) DISABLED;

CREATE TABLE epbuilder.del_cost_exp_info
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    DEL_master_episode_id varchar(73),
    DELIVERY_TYPE varchar(6),
    trig_begin_date date,
    year varchar(4),
    SPLIT_TOTAL_DEL_COST numeric(40,20),
    SPLIT_TYPICAL_DEL_COST numeric(40,20),
    SPLIT_PAC_DEL_COST numeric(40,20),
    UNSPLIT_TOTAL_DEL_COST numeric(40,20),
    UNSPLIT_TYPICAL_DEL_COST numeric(40,20),
    UNSPLIT_PAC_DEL_COST numeric(40,20),
    split_expected_total_cost float,
    split_expected_typical_cost float,
    split_expected_pac_cost float,
    unsplit_expected_total_cost float,
    unsplit_expected_typical_cost float,
    unsplit_expected_pac_cost float
);


CREATE TABLE epbuilder.MATERNITY_BUNDLE_COSTFIELDS
(
    ENCRYPT_RECIP_ID_MOM varchar(50),
    ENCRYPT_RECIP_ID_BABY varchar(50),
    PREG_master_episode_id varchar(73),
    trig_begin_date date,
    year varchar(4),
    SPLIT_TOTAL_PREGN_COST numeric(40,20),
    SPLIT_TYPICAL_PREGN_COST numeric(40,20),
    SPLIT_PAC_PREGN_COST numeric(40,20),
    UNSPLIT_TOTAL_PREGN_COST numeric(40,20),
    UNSPLIT_TYPICAL_PREGN_COST numeric(40,20),
    UNSPLIT_PAC_PREGN_COST numeric(40,20),
    DEL_master_episode_id varchar(73),
    DELIVERY_TYPE varchar(6),
    SPLIT_TOTAL_DEL_COST numeric(40,20),
    SPLIT_TYPICAL_DEL_COST numeric(40,20),
    SPLIT_PAC_DEL_COST numeric(40,20),
    UNSPLIT_TOTAL_DEL_COST numeric(40,20),
    UNSPLIT_TYPICAL_DEL_COST numeric(40,20),
    UNSPLIT_PAC_DEL_COST numeric(40,20),
    split_expected_del_total_cost float,
    split_expected_del_typical_cost float,
    split_expected_del_cost_cost float,
    unsplit_expected_del_total_cost float,
    unsplit_expected_del_typical_cost float,
     unsplit_expected_del_pac_cost float,
    NEWBORN_master_episode_id varchar(73),
    SPLIT_TOTAL_NEWBORN_COST numeric(40,20),
    SPLIT_TYPICAL_NEWBORN_COST numeric(40,20),
    SPLIT_PAC_NEWBORN_COST numeric(40,20),
    UNSPLIT_TOTAL_NEWBORN_COST numeric(40,20),
    UNSPLIT_TYPICAL_NEWBORN_COST numeric(40,20),
    UNSPLIT_PAC_NEWBORN_COST numeric(40,20),
    NURSERY_LEVEL int,
    PREG_AVE_SPLIT_COSTS float,
    PREG_AVE_UNSPLIT_COSTS float,
    NEWBORN_AVE_SPLIT_COSTS float,
    NEWBORN_AVE_UNSPLIT_COSTS float,
    PREG_AVE_SPLIT_PAC_COSTS float,
    PREG_AVE_UNSPLIT_PAC_COSTS float,
    NEWBORN_AVE_SPLIT_PAC_COSTS float,
    NEWBORN_AVE_UNSPLIT_PAC_COSTS float,
    PREG_AVE_SPLIT_Typical_COSTS float,
    PREG_AVE_UNSPLIT_Typical_COSTS float,
    NEWBORN_AVE_SPLIT_Typical_COSTS float,
    NEWBORN_AVE_UNSPLIT_Typical_COSTS float
);


CREATE TABLE epbuilder.provider_prep
(
    provider_id varchar(24),
    provider_name varchar(75),
    provider_zipcode varchar(25),
    provider_type varchar(500),
    provider_npi varchar(12)
);


CREATE TABLE epbuilder.co_ocurrence_of_chronic_episodes
(
    master_episode_id varchar(255),
    level int,
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_count_chronic int,
    co_count_all int,
    year numeric(18,0)
);


CREATE TABLE epbuilder.member_vistualization_claim_type_temp
(
    master_claim_id varchar(100),
    member_id varchar(50),
    allowed_amt numeric(40,20),
    assigned_count int,
    claim_line_type_code varchar(12),
    begin_date date,
    end_date date,
    filter_fail_total int,
    maternity_flag int,
    episode_count int,
    ppr int,
    ppv int
);


CREATE TABLE epbuilder.ra_cost_use
(
    epi_id varchar(75),
    epi_number varchar(6),
    name varchar(25),
    value int
);


CREATE TABLE epbuilder.ra_cost_use_back_up
(
    epi_id varchar(75),
    epi_number varchar(6),
    name varchar(25),
    value int
);


CREATE TABLE epbuilder.member_vistualization_claim_type
(
    member_id varchar(50),
    year numeric(18,0),
    ip_cost numeric(40,20),
    op_cost numeric(40,20),
    pb_cost numeric(40,20),
    rx_cost numeric(40,20),
    assigned_cost numeric(40,20),
    assigned_ip_cost numeric(40,20),
    assigned_op_cost numeric(40,20),
    assigned_pb_cost numeric(40,20),
    assigned_rx_cost numeric(40,20),
    assigned_cost_unfiltered numeric(40,20),
    assigned_ip_cost_unfiltered numeric(40,20),
    assigned_op_cost_unfiltered numeric(40,20),
    assigned_pb_cost_unfiltered numeric(40,20),
    assigned_rx_cost_unfiltered numeric(40,20),
    PPR numeric(40,20),
    PPV numeric(40,20),
    pac_cost numeric(40,20),
    cms_age_group varchar(7),
    gender varchar(2),
    member_population varchar(20)
);


CREATE TABLE epbuilder.exp_cost_qacrg3_age_gender
(
    r_year int,
    r_fincrg varchar(3),
    r_cms_age_group varchar(7),
    r_gender_code varchar(2),
    sum numeric(40,20),
    r_group_exp_cost numeric(58,38),
    ppr_group_exp_cost numeric(58,38),
    ppv_group_exp_cost numeric(58,38)
);


CREATE TABLE epbuilder.crg_cost_summary_by_member
(
    member_id varchar(50) DEFAULT NULL,
    year varchar(20) DEFAULT NULL,
    cms_age_group varchar(255) DEFAULT NULL,
    gender varchar(20) DEFAULT NULL,
    fincrg varchar(20) DEFAULT NULL,
    qacrg3_desc varchar(140) DEFAULT NULL,
    actual_cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    expected_cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    ppr_expected_cost numeric(13,2) DEFAULT NULL::numeric(1,0),
    ppv_expected_cost numeric(13,2) DEFAULT NULL::numeric(1,0)
);


CREATE TABLE epbuilder.ra_coeffs
(
    row_names varchar(255) DEFAULT NULL,
    epi_number varchar(255) DEFAULT NULL,
    epi_name varchar(255) DEFAULT NULL,
    parameter varchar(255) DEFAULT NULL,
    eol varchar(255) DEFAULT NULL,
    coef_ra_typ_l1_use varchar(255) DEFAULT NULL,
    coef_ra_typ_l1_cost varchar(255) DEFAULT NULL,
    coef_sa_typ_l1_use varchar(255) DEFAULT NULL,
    coef_sa_typ_l1_cost varchar(255) DEFAULT NULL,
    coef_ra_comp_l1_use varchar(255) DEFAULT NULL,
    coef_ra_comp_l1_cost varchar(255) DEFAULT NULL,
    coef_sa_comp_l1_use varchar(255) DEFAULT NULL,
    coef_sa_comp_l1_cost varchar(255) DEFAULT NULL,
    coef_ra_typ_l5_use varchar(255) DEFAULT NULL,
    coef_ra_typ_l5_cost varchar(255) DEFAULT NULL,
    coef_sa_typ_l5_use varchar(255) DEFAULT NULL,
    coef_sa_typ_l5_cost varchar(255) DEFAULT NULL,
    coef_ra_comp_l5_use varchar(255) DEFAULT NULL,
    coef_ra_comp_l5_cost varchar(255) DEFAULT NULL,
    coef_sa_comp_l5_use varchar(255) DEFAULT NULL,
    coef_sa_comp_l5_cost varchar(255) DEFAULT NULL,
    coef_ra_typ_ip_l1_use varchar(255) DEFAULT NULL,
    coef_ra_typ_ip_l1_cost varchar(255) DEFAULT NULL,
    coef_sa_typ_ip_l1_use varchar(255) DEFAULT NULL,
    coef_sa_typ_ip_l1_cost varchar(255) DEFAULT NULL,
    coef_ra_typ_other_l1_use varchar(255) DEFAULT NULL,
    coef_ra_typ_other_l1_cost varchar(255) DEFAULT NULL,
    coef_sa_typ_other_l1_use varchar(255) DEFAULT NULL,
    coef_sa_typ_other_l1_cost varchar(255) DEFAULT NULL,
    coef_ra_comp_other_l1_use varchar(255) DEFAULT NULL,
    coef_ra_comp_other_l1_cost varchar(255) DEFAULT NULL,
    coef_sa_comp_other_l1_use varchar(255) DEFAULT NULL,
    coef_sa_comp_other_l1_cost varchar(255) DEFAULT NULL,
    coef_ra_typ_ip_l3_use varchar(255) DEFAULT NULL,
    coef_ra_typ_ip_l3_cost varchar(255) DEFAULT NULL,
    coef_sa_typ_ip_l3_use varchar(255) DEFAULT NULL,
    coef_sa_typ_ip_l3_cost varchar(255) DEFAULT NULL,
    coef_ra_typ_other_l3_use varchar(255) DEFAULT NULL,
    coef_ra_typ_other_l3_cost varchar(255) DEFAULT NULL,
    coef_sa_typ_other_l3_use varchar(255) DEFAULT NULL,
    coef_sa_typ_other_l3_cost varchar(255) DEFAULT NULL,
    coef_ra_comp_other_l3_use varchar(255) DEFAULT NULL,
    coef_ra_comp_other_l3_cost varchar(255) DEFAULT NULL,
    coef_sa_comp_other_l3_use varchar(255) DEFAULT NULL,
    coef_sa_comp_other_l3_cost varchar(255) DEFAULT NULL
);


CREATE TABLE epbuilder.ra_exp_cost
(
    row_names varchar(255) DEFAULT NULL,
    epi_number varchar(255) DEFAULT NULL,
    epi_name varchar(255) DEFAULT NULL,
    epi_id varchar(255) DEFAULT NULL,
    eol_prob float DEFAULT NULL::float,
    use_prob_ra_typ_l1 float DEFAULT NULL::float,
    cost_pred_ra_typ_l1 float DEFAULT NULL::float,
    exp_cost_ra_typ_l1 float DEFAULT NULL::float,
    use_prob_sa_typ_l1 float DEFAULT NULL::float,
    cost_pred_sa_typ_l1 float DEFAULT NULL::float,
    exp_cost_sa_typ_l1 float DEFAULT NULL::float,
    use_prob_ra_comp_l1 float DEFAULT NULL::float,
    cost_pred_ra_comp_l1 float DEFAULT NULL::float,
    exp_cost_ra_comp_l1 float DEFAULT NULL::float,
    use_prob_sa_comp_l1 float DEFAULT NULL::float,
    cost_pred_sa_comp_l1 float DEFAULT NULL::float,
    exp_cost_sa_comp_l1 float DEFAULT NULL::float,
    use_prob_ra_typ_l5 float DEFAULT NULL::float,
    cost_pred_ra_typ_l5 float DEFAULT NULL::float,
    exp_cost_ra_typ_l5 float DEFAULT NULL::float,
    use_prob_sa_typ_l5 float DEFAULT NULL::float,
    cost_pred_sa_typ_l5 float DEFAULT NULL::float,
    exp_cost_sa_typ_l5 float DEFAULT NULL::float,
    use_prob_ra_comp_l5 float DEFAULT NULL::float,
    cost_pred_ra_comp_l5 float DEFAULT NULL::float,
    exp_cost_ra_comp_l5 float DEFAULT NULL::float,
    use_prob_sa_comp_l5 float DEFAULT NULL::float,
    cost_pred_sa_comp_l5 float DEFAULT NULL::float,
    exp_cost_sa_comp_l5 float DEFAULT NULL::float,
    total_exp_cost_ra_l1 float DEFAULT NULL::float,
    total_exp_cost_sa_l1 float DEFAULT NULL::float,
    total_exp_cost_ra_l5 float DEFAULT NULL::float,
    total_exp_cost_sa_l5 float DEFAULT NULL::float,
    use_prob_ra_typ_ip_l1 float DEFAULT NULL::float,
    cost_pred_ra_typ_ip_l1 float DEFAULT NULL::float,
    exp_cost_ra_typ_ip_l1 float DEFAULT NULL::float,
    use_prob_sa_typ_ip_l1 float DEFAULT NULL::float,
    cost_pred_sa_typ_ip_l1 float DEFAULT NULL::float,
    exp_cost_sa_typ_ip_l1 float DEFAULT NULL::float,
    use_prob_ra_typ_other_l1 float DEFAULT NULL::float,
    cost_pred_ra_typ_other_l1 float DEFAULT NULL::float,
    exp_cost_ra_typ_other_l1 float DEFAULT NULL::float,
    use_prob_sa_typ_other_l1 float DEFAULT NULL::float,
    cost_pred_sa_typ_other_l1 float DEFAULT NULL::float,
    exp_cost_sa_typ_other_l1 float DEFAULT NULL::float,
    use_prob_ra_comp_other_l1 float DEFAULT NULL::float,
    cost_pred_ra_comp_other_l1 float DEFAULT NULL::float,
    exp_cost_ra_comp_other_l1 float DEFAULT NULL::float,
    use_prob_sa_comp_other_l1 float DEFAULT NULL::float,
    cost_pred_sa_comp_other_l1 float DEFAULT NULL::float,
    exp_cost_sa_comp_other_l1 float DEFAULT NULL::float,
    use_prob_ra_typ_ip_l3 float DEFAULT NULL::float,
    cost_pred_ra_typ_ip_l3 float DEFAULT NULL::float,
    exp_cost_ra_typ_ip_l3 float DEFAULT NULL::float,
    use_prob_sa_typ_ip_l3 float DEFAULT NULL::float,
    cost_pred_sa_typ_ip_l3 float DEFAULT NULL::float,
    exp_cost_sa_typ_ip_l3 float DEFAULT NULL::float,
    use_prob_ra_typ_other_l3 float DEFAULT NULL::float,
    cost_pred_ra_typ_other_l3 float DEFAULT NULL::float,
    exp_cost_ra_typ_other_l3 float DEFAULT NULL::float,
    use_prob_sa_typ_other_l3 float DEFAULT NULL::float,
    cost_pred_sa_typ_other_l3 float DEFAULT NULL::float,
    exp_cost_sa_typ_other_l3 float DEFAULT NULL::float,
    use_prob_ra_comp_other_l3 float DEFAULT NULL::float,
    cost_pred_ra_comp_other_l3 float DEFAULT NULL::float,
    exp_cost_ra_comp_other_l3 float DEFAULT NULL::float,
    use_prob_sa_comp_other_l3 float DEFAULT NULL::float,
    cost_pred_sa_comp_other_l3 float DEFAULT NULL::float,
    exp_cost_sa_comp_other_l3 float DEFAULT NULL::float,
    total_exp_cost_ra_l3 float DEFAULT NULL::float,
    total_exp_cost_sa_l3 float DEFAULT NULL::float
);


CREATE TABLE epbuilder.member_sub_population2
(
    member_id varchar(100) DEFAULT NULL,
    sub_population varchar(20) DEFAULT NULL,
    gender varchar(2) DEFAULT NULL,
    birth_year int DEFAULT NULL::int,
    age_group varchar(20) DEFAULT NULL,
    zip_code varchar(12) DEFAULT NULL,
    county varchar(12) DEFAULT NULL,
    mcregion varchar(100) DEFAULT NULL,
    PPS varchar(100),
    MCO varchar(100),
    HH varchar(100),
    PCP varchar(100),
    exclusive numeric(2,0)
);


CREATE TABLE epbuilder.member_sub_population2_exclusive
(
    member_id varchar(100),
    sub_population varchar(20),
    gender varchar(2),
    birth_year int,
    age_group varchar(20),
    zip_code varchar(12),
    county varchar(12),
    mcregion varchar(100),
    PPS varchar(100),
    MCO varchar(100),
    HH varchar(100),
    PCP varchar(100),
    exclusive numeric(2,0)
);


CREATE TABLE epbuilder.visual_analysis_table_js
(
    Analysis_type varchar(12),
    id varchar(255),
    episode_id varchar(6),
    episode_name varchar(20),
    episode_description varchar(255),
    episode_type varchar(50),
    episode_category varchar(30),
    episode_level int,
    member_id varchar(50),
    member_age numeric(18,0),
    cms_age_group varchar(7),
    gender varchar(2),
    member_zipcode varchar(12),
    member_county varchar(12),
    member_population varchar(20),
    member_region varchar(100),
    Split_Total_Cost numeric(25,2),
    Split_Total_PAC_Cost numeric(25,2),
    Split_Total_Typical_Cost numeric(25,2),
    Unsplit_Total_Cost numeric(25,2),
    Unsplit_Total_PAC_Cost numeric(25,2),
    Unsplit_Total_Typical_Cost numeric(25,2),
    Split_Expected_Total_Cost numeric(25,2),
    Split_Expected_Typical_IP_Cost numeric(25,2),
    Split_Expected_Typical_Other_Cost numeric(25,2),
    Split_Expected_PAC_Cost numeric(25,2),
    Unsplit_Expected_Total_Cost numeric(25,2),
    Unsplit_Expected_Typical_IP_Cost numeric(25,2),
    Unsplit_Expected_Typical_Other_Cost numeric(25,2),
    Unsplit_Expected_PAC_Cost numeric(25,2),
    ip_cost float,
    op_cost float,
    pb_cost float,
    rx_cost float,
    assigned_cost numeric(40,20),
    assigned_ip_cost numeric(40,20),
    assigned_op_cost numeric(40,20),
    assigned_pb_cost numeric(40,20),
    assigned_rx_cost numeric(40,20),
    assigned_cost_unfiltered numeric(40,20),
    assigned_ip_cost_unfiltered numeric(40,20),
    assigned_op_cost_unfiltered numeric(40,20),
    assigned_pb_cost_unfiltered numeric(40,20),
    assigned_rx_cost_unfiltered numeric(40,20),
    pps varchar(100),
    Facility_ID varchar(50),
    Facility_ID_provider_name varchar(100),
    Facility_ID_provider_zipcode varchar(100),
    Facility_ID_provider_type varchar(100),
    Physician_ID varchar(50),
    Physician_ID_provider_name varchar(100),
    Physician_ID_provider_zipcode varchar(1000),
    Physician_ID_provider_type varchar(100),
    mco varchar(100),
    hh varchar(100),
    pcp varchar(100),
    vbp_attrib_provider varchar(12),
    vbp_attrib_provider_zipcode varchar(12),
    vbp_contractor varchar(12),
    subgroup varchar(80),
    year numeric(4,0),
    utilization varchar(7),
    ppr varchar(80),
    ppv varchar(80),
    exclusive numeric(2,0),
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_occurence_count_chronic int,
    co_occurence_count_all int,
    episode_count float,
    episode_count_unfiltered float,
    qcrg_code varchar(12),
    qcrg_desc varchar(12),
    qacrg1_code varchar(12),
    qacrg1_desc varchar(12),
    qacrg2_code varchar(12),
    qacrg2_desc varchar(12),
    qacrg3_code varchar(12),
    qacrg3_desc varchar(12),
    fincrg varchar(12),
    q_base varchar(12),
    q_severity varchar(12),
    enrolled_num_month int,
    vbp_arrangement varchar(23),
    state_wide_female_percent float,
    state_wide_male_percent float,
    state_wide_NU_percent float,
    state_wide_LU_percent float,
    state_wide_percent_co_ASTHMA float,
    state_wide_percent_co_ARRBLK float,
    state_wide_percent_co_HF float,
    state_wide_percent_co_COPD float,
    state_wide_percent_co_CAD float,
    state_wide_percent_co_ULCLTS float,
    state_wide_percent_co_BIPLR float,
    state_wide_percent_co_GERD float,
    state_wide_percent_co_HTN float,
    state_wide_percent_co_GLCOMA float,
    state_wide_percent_co_LBP float,
    state_wide_percent_co_CROHNS float,
    state_wide_percent_co_DIAB float,
    state_wide_percent_co_DEPRSN float,
    state_wide_percent_co_OSTEOA float,
    state_wide_percent_co_RHNTS float,
    state_wide_percent_co_DIVERT float,
    state_wide_percent_co_DEPANX float,
    state_wide_percent_co_PTSD float,
    state_wide_percent_co_SCHIZO float,
    state_wide_percent_co_SUDS float,
    ppr_expected_cost numeric(40,20),
    ppv_expected_cost numeric(40,20),
    state_wide_avg_split_exp_cost float,
    state_wide_avg_unsplit_exp_cost float,
    state_wide_avg_split_total_cost float,
    state_wide_avg_unsplit_total_cost float,
    split_state_wide_pac_percent float,
    unsplit_state_wide_pac_percent float
);


CREATE TABLE epbuilder.epi_counts
(
    member_id varchar(50),
    year numeric(4,0),
    episode_count float,
    filtered_episode_count float
);


CREATE TABLE epbuilder.temp_main_state_wide_values_2
(
    analysis_type varchar(12),
    vbp_arrangement varchar(23),
    member_population varchar(20),
    episode_name varchar(20),
    year numeric(4,0),
    state_wide_female_percent float,
    state_wide_male_percent float,
    state_wide_LU_percent float,
    state_wide_NU_percent float,
    state_wide_total int,
    state_wide_avg_split_exp_cost float,
    state_wide_avg_unsplit_exp_cost float,
    state_wide_avg_split_total_cost float,
    state_wide_avg_unsplit_total_cost float,
    split_state_wide_pac_percent numeric(52,27),
    unsplit_state_wide_pac_percent numeric(52,27),
    state_wide_percent_co_ASTHMA float,
    state_wide_percent_co_ARRBLK float,
    state_wide_percent_co_HF float,
    state_wide_percent_co_COPD float,
    state_wide_percent_co_CAD float,
    state_wide_percent_co_ULCLTS float,
    state_wide_percent_co_BIPLR float,
    state_wide_percent_co_GERD float,
    state_wide_percent_co_HTN float,
    state_wide_percent_co_GLCOMA float,
    state_wide_percent_co_LBP float,
    state_wide_percent_co_CROHNS float,
    state_wide_percent_co_DIAB float,
    state_wide_percent_co_DEPRSN float,
    state_wide_percent_co_OSTEOA float,
    state_wide_percent_co_RHNTS float,
    state_wide_percent_co_DIVERT float,
    state_wide_percent_co_DEPANX float,
    state_wide_percent_co_PTSD float,
    state_wide_percent_co_SCHIZO float,
    state_wide_percent_co_SUDS float
);


CREATE TABLE epbuilder.visual_analysis_table_js_subset
(
    Analysis_type varchar(12),
    id varchar(255),
    episode_id varchar(6),
    episode_name varchar(20),
    episode_description varchar(255),
    episode_type varchar(50),
    episode_category varchar(30),
    episode_level int,
    member_id varchar(50),
    member_age numeric(18,0),
    cms_age_group varchar(7),
    gender varchar(2),
    member_zipcode varchar(12),
    member_county varchar(12),
    member_population varchar(20),
    member_region varchar(100),
    Split_Total_Cost numeric(25,2),
    Split_Total_PAC_Cost numeric(25,2),
    Split_Total_Typical_Cost numeric(25,2),
    Unsplit_Total_Cost numeric(25,2),
    Unsplit_Total_PAC_Cost numeric(25,2),
    Unsplit_Total_Typical_Cost numeric(25,2),
    Split_Expected_Total_Cost numeric(25,2),
    Split_Expected_Typical_IP_Cost numeric(25,2),
    Split_Expected_Typical_Other_Cost numeric(25,2),
    Split_Expected_PAC_Cost numeric(25,2),
    Unsplit_Expected_Total_Cost numeric(25,2),
    Unsplit_Expected_Typical_IP_Cost numeric(25,2),
    Unsplit_Expected_Typical_Other_Cost numeric(25,2),
    Unsplit_Expected_PAC_Cost numeric(25,2),
    ip_cost float,
    op_cost float,
    pb_cost float,
    rx_cost float,
    assigned_cost numeric(40,20),
    assigned_ip_cost numeric(40,20),
    assigned_op_cost numeric(40,20),
    assigned_pb_cost numeric(40,20),
    assigned_rx_cost numeric(40,20),
    assigned_cost_unfiltered numeric(40,20),
    assigned_ip_cost_unfiltered numeric(40,20),
    assigned_op_cost_unfiltered numeric(40,20),
    assigned_pb_cost_unfiltered numeric(40,20),
    assigned_rx_cost_unfiltered numeric(40,20),
    pps varchar(100),
    Facility_ID varchar(50),
    Facility_ID_provider_name varchar(100),
    Facility_ID_provider_zipcode varchar(100),
    Facility_ID_provider_type varchar(100),
    Physician_ID varchar(50),
    Physician_ID_provider_name varchar(100),
    Physician_ID_provider_zipcode varchar(1000),
    Physician_ID_provider_type varchar(100),
    mco varchar(100),
    hh varchar(100),
    pcp varchar(100),
    vbp_attrib_provider varchar(12),
    vbp_attrib_provider_zipcode varchar(12),
    vbp_contractor varchar(12),
    subgroup varchar(80),
    year numeric(4,0),
    utilization varchar(7),
    ppr varchar(80),
    ppv varchar(80),
    exclusive numeric(2,0),
    co_occurence_count_ASTHMA int,
    co_occurence_count_ARRBLK int,
    co_occurence_count_HF int,
    co_occurence_count_COPD int,
    co_occurence_count_CAD int,
    co_occurence_count_ULCLTS int,
    co_occurence_count_BIPLR int,
    co_occurence_count_GERD int,
    co_occurence_count_HTN int,
    co_occurence_count_GLCOMA int,
    co_occurence_count_LBP int,
    co_occurence_count_CROHNS int,
    co_occurence_count_DIAB int,
    co_occurence_count_DEPRSN int,
    co_occurence_count_OSTEOA int,
    co_occurence_count_RHNTS int,
    co_occurence_count_DIVERT int,
    co_occurence_count_DEPANX int,
    co_occurence_count_PTSD int,
    co_occurence_count_SCHIZO int,
    co_occurence_count_SUDS int,
    co_occurence_count_chronic int,
    co_occurence_count_all int,
    episode_count float,
    episode_count_unfiltered float,
    qcrg_code varchar(12),
    qcrg_desc varchar(12),
    qacrg1_code varchar(12),
    qacrg1_desc varchar(12),
    qacrg2_code varchar(12),
    qacrg2_desc varchar(12),
    qacrg3_code varchar(12),
    qacrg3_desc varchar(12),
    fincrg varchar(12),
    q_base varchar(12),
    q_severity varchar(12),
    enrolled_num_month int,
    vbp_arrangement varchar(23),
    state_wide_female_percent float,
    state_wide_male_percent float,
    state_wide_NU_percent float,
    state_wide_LU_percent float,
    state_wide_percent_co_ASTHMA float,
    state_wide_percent_co_ARRBLK float,
    state_wide_percent_co_HF float,
    state_wide_percent_co_COPD float,
    state_wide_percent_co_CAD float,
    state_wide_percent_co_ULCLTS float,
    state_wide_percent_co_BIPLR float,
    state_wide_percent_co_GERD float,
    state_wide_percent_co_HTN float,
    state_wide_percent_co_GLCOMA float,
    state_wide_percent_co_LBP float,
    state_wide_percent_co_CROHNS float,
    state_wide_percent_co_DIAB float,
    state_wide_percent_co_DEPRSN float,
    state_wide_percent_co_OSTEOA float,
    state_wide_percent_co_RHNTS float,
    state_wide_percent_co_DIVERT float,
    state_wide_percent_co_DEPANX float,
    state_wide_percent_co_PTSD float,
    state_wide_percent_co_SCHIZO float,
    state_wide_percent_co_SUDS float,
    ppr_expected_cost numeric(40,20),
    ppv_expected_cost numeric(40,20),
    state_wide_avg_split_exp_cost float,
    state_wide_avg_unsplit_exp_cost float,
    state_wide_avg_split_total_cost float,
    state_wide_avg_unsplit_total_cost float,
    split_state_wide_pac_percent float,
    unsplit_state_wide_pac_percent float
);



CREATE PROJECTION epbuilder.tmp_mel1_gtzero_t /*+createtype(L)*/ 
(
 master_episode_id,
 cost_gtzero_t,
 cost_t_gtzero_t,
 cost_c_gtzero_t
)
AS
 SELECT tmp_mel1_gtzero_t.master_episode_id,
        tmp_mel1_gtzero_t.cost_gtzero_t,
        tmp_mel1_gtzero_t.cost_t_gtzero_t,
        tmp_mel1_gtzero_t.cost_c_gtzero_t
 FROM epbuilder.tmp_mel1_gtzero_t
 ORDER BY tmp_mel1_gtzero_t.master_episode_id
SEGMENTED BY hash(tmp_mel1_gtzero_t.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_mel1_gtzero_tnot /*+createtype(L)*/ 
(
 master_episode_id,
 cost_gtzero_tnot,
 cost_t_gtzero_tnot,
 cost_c_gtzero_tnot
)
AS
 SELECT tmp_mel1_gtzero_tnot.master_episode_id,
        tmp_mel1_gtzero_tnot.cost_gtzero_tnot,
        tmp_mel1_gtzero_tnot.cost_t_gtzero_tnot,
        tmp_mel1_gtzero_tnot.cost_c_gtzero_tnot
 FROM epbuilder.tmp_mel1_gtzero_tnot
 ORDER BY tmp_mel1_gtzero_tnot.master_episode_id
SEGMENTED BY hash(tmp_mel1_gtzero_tnot.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_mel1_zero_t /*+createtype(L)*/ 
(
 master_episode_id,
 cost_zero_t,
 cost_t_zero_t,
 cost_c_zero_t
)
AS
 SELECT tmp_mel1_zero_t.master_episode_id,
        tmp_mel1_zero_t.cost_zero_t,
        tmp_mel1_zero_t.cost_t_zero_t,
        tmp_mel1_zero_t.cost_c_zero_t
 FROM epbuilder.tmp_mel1_zero_t
 ORDER BY tmp_mel1_zero_t.master_episode_id
SEGMENTED BY hash(tmp_mel1_zero_t.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_mel1_zero_tnot /*+createtype(L)*/ 
(
 master_episode_id,
 cost_zero_tnot,
 cost_t_zero_tnot,
 cost_c_zero_tnot
)
AS
 SELECT tmp_mel1_zero_tnot.master_episode_id,
        tmp_mel1_zero_tnot.cost_zero_tnot,
        tmp_mel1_zero_tnot.cost_t_zero_tnot,
        tmp_mel1_zero_tnot.cost_c_zero_tnot
 FROM epbuilder.tmp_mel1_zero_tnot
 ORDER BY tmp_mel1_zero_tnot.master_episode_id
SEGMENTED BY hash(tmp_mel1_zero_tnot.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.omh_harp /*+createtype(A)*/ 
(
 ENCRYPTED_ID,
 OMH_HARP_IND,
 OASAS_HARP_IND,
 PREV_SENT_IND,
 RECIP_COUNTY_CODE,
 RECIP_COUNTY_NAME,
 NYC_ROS_IND,
 LESS2M_MC_PLAN_ID,
 LESS2M_MC_PLAN_NAME,
 LESS2M_MC_PLAN_TYPE,
 LESS2M_MCAID_ELIG_IND,
 LESS2M_MCARE_ELIG_IND,
 LESS2M_HH_OUTREACH_IND,
 LESS2M_HH_ENGAGE_IND,
 LESS1M_MC_PLAN_ID,
 LESS1M_MC_PLAN_NAME,
 LESS1M_MC_PLAN_TYPE,
 LESS1M_MCAID_ELIG_IND,
 LESS1M_MCARE_ELIG_IND,
 LESS1M_HH_OUTREACH_IND,
 LESS1M_HH_ENGAGE_IND,
 CURR_MC_PLAN_ID,
 CURR_MC_PLAN_NAME,
 CURR_MC_PLAN_TYPE,
 CURR_MCAID_ELIG_IND,
 CURR_MCARE_ELIG_IND,
 CURR_HH_OUTREACH_IND,
 CURR_HH_ENGAGE_IND,
 LESS2M_ENROLL_MC_DOH_OMC_IND,
 LESS2M_ENROLL_MC_PLAN_ID,
 LESS2M_ENROLL_MC_PLAN_NAME,
 LESS2M_ENROLL_MC_PLAN_TYPE,
 LESS1M_ENROLL_MC_DOH_OMC_IND,
 LESS1M_ENROLL_MC_PLAN_ID,
 LESS1M_ENROLL_MC_PLAN_NAME,
 LESS1M_ENROLL_MC_PLAN_TYPE,
 CURR_ENROLL_MC_DOH_OMC_ID,
 CURR_ENROLL_MC_PLAN_ID,
 CURR_ENROLL_MC_PLAN_NAME,
 CURR_ENROLL_MC_PLAN_TYPE,
 CURR_RECIP_AGE,
 RECIP_DOB,
 RECIP_MONTH_OF_BIRTH,
 OMC_OR_PLAN_ID,
 MMC_PLAN_NAME_AGG,
 TRANS_DIST_78_IND,
 CURR_COMB_PLAN_ID,
 CURR_COMB_PLAN_NAME,
 CURR_COMB_PLAN_TYPE,
 HARP_LIST_ELIGIBLE_IND,
 ENROLLED_IND,
 WAS_ASSESSED_IND,
 FROM_ENROLL_H_CODE,
 FROM_ENROLL_BEG_DT,
 FROM_ENROLL_END_DT,
 EXCEPTION_CHANGE_DATE,
 ASSESSMENT_DATE,
 ASSESSMENT_TIER,
 LAST_SENT_BEG_DT,
 LAST_SENT_END_DT,
 LAST_SENT_HAS_ERROR,
 LAST_SENT_CODE,
 LAST_SENT_SUBMITTED_DT,
 CURR_ACTUAL_H_CODE,
 CURR_CALCULATED_H_CODE,
 Sent_or_elegible
)
AS
 SELECT omh_harp.ENCRYPTED_ID,
        omh_harp.OMH_HARP_IND,
        omh_harp.OASAS_HARP_IND,
        omh_harp.PREV_SENT_IND,
        omh_harp.RECIP_COUNTY_CODE,
        omh_harp.RECIP_COUNTY_NAME,
        omh_harp.NYC_ROS_IND,
        omh_harp.LESS2M_MC_PLAN_ID,
        omh_harp.LESS2M_MC_PLAN_NAME,
        omh_harp.LESS2M_MC_PLAN_TYPE,
        omh_harp.LESS2M_MCAID_ELIG_IND,
        omh_harp.LESS2M_MCARE_ELIG_IND,
        omh_harp.LESS2M_HH_OUTREACH_IND,
        omh_harp.LESS2M_HH_ENGAGE_IND,
        omh_harp.LESS1M_MC_PLAN_ID,
        omh_harp.LESS1M_MC_PLAN_NAME,
        omh_harp.LESS1M_MC_PLAN_TYPE,
        omh_harp.LESS1M_MCAID_ELIG_IND,
        omh_harp.LESS1M_MCARE_ELIG_IND,
        omh_harp.LESS1M_HH_OUTREACH_IND,
        omh_harp.LESS1M_HH_ENGAGE_IND,
        omh_harp.CURR_MC_PLAN_ID,
        omh_harp.CURR_MC_PLAN_NAME,
        omh_harp.CURR_MC_PLAN_TYPE,
        omh_harp.CURR_MCAID_ELIG_IND,
        omh_harp.CURR_MCARE_ELIG_IND,
        omh_harp.CURR_HH_OUTREACH_IND,
        omh_harp.CURR_HH_ENGAGE_IND,
        omh_harp.LESS2M_ENROLL_MC_DOH_OMC_IND,
        omh_harp.LESS2M_ENROLL_MC_PLAN_ID,
        omh_harp.LESS2M_ENROLL_MC_PLAN_NAME,
        omh_harp.LESS2M_ENROLL_MC_PLAN_TYPE,
        omh_harp.LESS1M_ENROLL_MC_DOH_OMC_IND,
        omh_harp.LESS1M_ENROLL_MC_PLAN_ID,
        omh_harp.LESS1M_ENROLL_MC_PLAN_NAME,
        omh_harp.LESS1M_ENROLL_MC_PLAN_TYPE,
        omh_harp.CURR_ENROLL_MC_DOH_OMC_ID,
        omh_harp.CURR_ENROLL_MC_PLAN_ID,
        omh_harp.CURR_ENROLL_MC_PLAN_NAME,
        omh_harp.CURR_ENROLL_MC_PLAN_TYPE,
        omh_harp.CURR_RECIP_AGE,
        omh_harp.RECIP_DOB,
        omh_harp.RECIP_MONTH_OF_BIRTH,
        omh_harp.OMC_OR_PLAN_ID,
        omh_harp.MMC_PLAN_NAME_AGG,
        omh_harp.TRANS_DIST_78_IND,
        omh_harp.CURR_COMB_PLAN_ID,
        omh_harp.CURR_COMB_PLAN_NAME,
        omh_harp.CURR_COMB_PLAN_TYPE,
        omh_harp.HARP_LIST_ELIGIBLE_IND,
        omh_harp.ENROLLED_IND,
        omh_harp.WAS_ASSESSED_IND,
        omh_harp.FROM_ENROLL_H_CODE,
        omh_harp.FROM_ENROLL_BEG_DT,
        omh_harp.FROM_ENROLL_END_DT,
        omh_harp.EXCEPTION_CHANGE_DATE,
        omh_harp.ASSESSMENT_DATE,
        omh_harp.ASSESSMENT_TIER,
        omh_harp.LAST_SENT_BEG_DT,
        omh_harp.LAST_SENT_END_DT,
        omh_harp.LAST_SENT_HAS_ERROR,
        omh_harp.LAST_SENT_CODE,
        omh_harp.LAST_SENT_SUBMITTED_DT,
        omh_harp.CURR_ACTUAL_H_CODE,
        omh_harp.CURR_CALCULATED_H_CODE,
        omh_harp.Sent_or_elegible
 FROM epbuilder.omh_harp
 ORDER BY omh_harp.ENCRYPTED_ID,
          omh_harp.OMH_HARP_IND,
          omh_harp.OASAS_HARP_IND,
          omh_harp.PREV_SENT_IND,
          omh_harp.RECIP_COUNTY_CODE,
          omh_harp.RECIP_COUNTY_NAME,
          omh_harp.NYC_ROS_IND,
          omh_harp.LESS2M_MC_PLAN_ID,
          omh_harp.LESS2M_MC_PLAN_NAME,
          omh_harp.LESS2M_MC_PLAN_TYPE,
          omh_harp.LESS2M_MCAID_ELIG_IND,
          omh_harp.LESS2M_MCARE_ELIG_IND,
          omh_harp.LESS2M_HH_OUTREACH_IND,
          omh_harp.LESS2M_HH_ENGAGE_IND,
          omh_harp.LESS1M_MC_PLAN_ID,
          omh_harp.LESS1M_MC_PLAN_NAME,
          omh_harp.LESS1M_MC_PLAN_TYPE,
          omh_harp.LESS1M_MCAID_ELIG_IND,
          omh_harp.LESS1M_MCARE_ELIG_IND,
          omh_harp.LESS1M_HH_OUTREACH_IND,
          omh_harp.LESS1M_HH_ENGAGE_IND,
          omh_harp.CURR_MC_PLAN_ID,
          omh_harp.CURR_MC_PLAN_NAME,
          omh_harp.CURR_MC_PLAN_TYPE,
          omh_harp.CURR_MCAID_ELIG_IND,
          omh_harp.CURR_MCARE_ELIG_IND,
          omh_harp.CURR_HH_OUTREACH_IND,
          omh_harp.CURR_HH_ENGAGE_IND,
          omh_harp.LESS2M_ENROLL_MC_DOH_OMC_IND,
          omh_harp.LESS2M_ENROLL_MC_PLAN_ID,
          omh_harp.LESS2M_ENROLL_MC_PLAN_NAME,
          omh_harp.LESS2M_ENROLL_MC_PLAN_TYPE,
          omh_harp.LESS1M_ENROLL_MC_DOH_OMC_IND,
          omh_harp.LESS1M_ENROLL_MC_PLAN_ID,
          omh_harp.LESS1M_ENROLL_MC_PLAN_NAME,
          omh_harp.LESS1M_ENROLL_MC_PLAN_TYPE,
          omh_harp.CURR_ENROLL_MC_DOH_OMC_ID,
          omh_harp.CURR_ENROLL_MC_PLAN_ID,
          omh_harp.CURR_ENROLL_MC_PLAN_NAME,
          omh_harp.CURR_ENROLL_MC_PLAN_TYPE,
          omh_harp.CURR_RECIP_AGE,
          omh_harp.RECIP_DOB,
          omh_harp.RECIP_MONTH_OF_BIRTH,
          omh_harp.OMC_OR_PLAN_ID,
          omh_harp.MMC_PLAN_NAME_AGG,
          omh_harp.TRANS_DIST_78_IND,
          omh_harp.CURR_COMB_PLAN_ID,
          omh_harp.CURR_COMB_PLAN_NAME,
          omh_harp.CURR_COMB_PLAN_TYPE,
          omh_harp.HARP_LIST_ELIGIBLE_IND,
          omh_harp.ENROLLED_IND,
          omh_harp.WAS_ASSESSED_IND,
          omh_harp.FROM_ENROLL_H_CODE,
          omh_harp.FROM_ENROLL_BEG_DT,
          omh_harp.FROM_ENROLL_END_DT,
          omh_harp.EXCEPTION_CHANGE_DATE,
          omh_harp.ASSESSMENT_DATE,
          omh_harp.ASSESSMENT_TIER,
          omh_harp.LAST_SENT_BEG_DT,
          omh_harp.LAST_SENT_END_DT,
          omh_harp.LAST_SENT_HAS_ERROR,
          omh_harp.LAST_SENT_CODE,
          omh_harp.LAST_SENT_SUBMITTED_DT,
          omh_harp.CURR_ACTUAL_H_CODE,
          omh_harp.CURR_CALCULATED_H_CODE,
          omh_harp.Sent_or_elegible
SEGMENTED BY hash(omh_harp.OMH_HARP_IND, omh_harp.OASAS_HARP_IND, omh_harp.PREV_SENT_IND, omh_harp.RECIP_COUNTY_CODE, omh_harp.NYC_ROS_IND, omh_harp.LESS2M_MC_PLAN_ID, omh_harp.LESS2M_MC_PLAN_TYPE, omh_harp.LESS2M_MCAID_ELIG_IND, omh_harp.LESS2M_MCARE_ELIG_IND, omh_harp.LESS2M_HH_OUTREACH_IND, omh_harp.LESS2M_HH_ENGAGE_IND, omh_harp.LESS1M_MC_PLAN_ID, omh_harp.LESS1M_MC_PLAN_TYPE, omh_harp.LESS1M_MCAID_ELIG_IND, omh_harp.LESS1M_MCARE_ELIG_IND, omh_harp.LESS1M_HH_OUTREACH_IND, omh_harp.LESS1M_HH_ENGAGE_IND, omh_harp.CURR_MC_PLAN_ID, omh_harp.CURR_MC_PLAN_TYPE, omh_harp.CURR_MCAID_ELIG_IND, omh_harp.CURR_MCARE_ELIG_IND, omh_harp.CURR_HH_OUTREACH_IND, omh_harp.CURR_HH_ENGAGE_IND, omh_harp.LESS2M_ENROLL_MC_DOH_OMC_IND, omh_harp.LESS2M_ENROLL_MC_PLAN_ID, omh_harp.LESS1M_ENROLL_MC_DOH_OMC_IND, omh_harp.LESS1M_ENROLL_MC_PLAN_ID, omh_harp.CURR_ENROLL_MC_DOH_OMC_ID, omh_harp.CURR_ENROLL_MC_PLAN_ID, omh_harp.CURR_RECIP_AGE, omh_harp.RECIP_DOB, omh_harp.RECIP_MONTH_OF_BIRTH) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ccs_grouping /*+createtype(A)*/ 
(
 epi_id,
 mdc,
 ccs_group,
 code,
 code_desc,
 subgroups
)
AS
 SELECT ccs_grouping.epi_id,
        ccs_grouping.mdc,
        ccs_grouping.ccs_group,
        ccs_grouping.code,
        ccs_grouping.code_desc,
        ccs_grouping.subgroups
 FROM epbuilder.ccs_grouping
 ORDER BY ccs_grouping.epi_id,
          ccs_grouping.mdc,
          ccs_grouping.ccs_group,
          ccs_grouping.code,
          ccs_grouping.code_desc,
          ccs_grouping.subgroups
SEGMENTED BY hash(ccs_grouping.epi_id, ccs_grouping.mdc, ccs_grouping.code, ccs_grouping.subgroups, ccs_grouping.ccs_group, ccs_grouping.code_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrollment_raw_2012 /*+createtype(A)*/ 
(
 year,
 month,
 member_id,
 dob,
 sex,
 dod
)
AS
 SELECT enrollment_raw_2012.year,
        enrollment_raw_2012.month,
        enrollment_raw_2012.member_id,
        enrollment_raw_2012.dob,
        enrollment_raw_2012.sex,
        enrollment_raw_2012.dod
 FROM epbuilder.enrollment_raw_2012
 ORDER BY enrollment_raw_2012.year,
          enrollment_raw_2012.month,
          enrollment_raw_2012.member_id,
          enrollment_raw_2012.dob,
          enrollment_raw_2012.sex,
          enrollment_raw_2012.dod
SEGMENTED BY hash(enrollment_raw_2012.year, enrollment_raw_2012.month, enrollment_raw_2012.sex, enrollment_raw_2012.dob, enrollment_raw_2012.member_id, enrollment_raw_2012.dod) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrollment_raw_2013 /*+createtype(A)*/ 
(
 year,
 month,
 member_id,
 dob,
 sex,
 dod
)
AS
 SELECT enrollment_raw_2013.year,
        enrollment_raw_2013.month,
        enrollment_raw_2013.member_id,
        enrollment_raw_2013.dob,
        enrollment_raw_2013.sex,
        enrollment_raw_2013.dod
 FROM epbuilder.enrollment_raw_2013
 ORDER BY enrollment_raw_2013.year,
          enrollment_raw_2013.month,
          enrollment_raw_2013.member_id,
          enrollment_raw_2013.dob,
          enrollment_raw_2013.sex,
          enrollment_raw_2013.dod
SEGMENTED BY hash(enrollment_raw_2013.year, enrollment_raw_2013.month, enrollment_raw_2013.sex, enrollment_raw_2013.dob, enrollment_raw_2013.member_id, enrollment_raw_2013.dod) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_prov_attr /*+createtype(L)*/ 
(
 master_episode_id,
 physician_id,
 max_count,
 max_cost
)
AS
 SELECT tmp_prov_attr.master_episode_id,
        tmp_prov_attr.physician_id,
        tmp_prov_attr.max_count,
        tmp_prov_attr.max_cost
 FROM epbuilder.tmp_prov_attr
 ORDER BY tmp_prov_attr.master_episode_id,
          tmp_prov_attr.physician_id,
          tmp_prov_attr.max_count,
          tmp_prov_attr.max_cost
SEGMENTED BY hash(tmp_prov_attr.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.Descriptive_Stats /*+createtype(L)*/ 
(
 Field,
 Data
)
AS
 SELECT Descriptive_Stats.Field,
        Descriptive_Stats.Data
 FROM epbuilder.Descriptive_Stats
 ORDER BY Descriptive_Stats.Field,
          Descriptive_Stats.Data
SEGMENTED BY hash(Descriptive_Stats.Field, Descriptive_Stats.Data) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrollment_raw_new /*+createtype(A)*/ 
(
 year,
 month,
 date,
 member_id,
 insurance_product,
 birth_year,
 sex,
 dod
)
AS
 SELECT enrollment_raw_new.year,
        enrollment_raw_new.month,
        enrollment_raw_new.date,
        enrollment_raw_new.member_id,
        enrollment_raw_new.insurance_product,
        enrollment_raw_new.birth_year,
        enrollment_raw_new.sex,
        enrollment_raw_new.dod
 FROM epbuilder.enrollment_raw_new
 ORDER BY enrollment_raw_new.month,
          enrollment_raw_new.year,
          enrollment_raw_new.date
SEGMENTED BY hash(enrollment_raw_new.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.claims_dsrp_info /*+createtype(A)*/ 
(
 id,
 master_claim_id,
 member_id,
 allowed_amt,
 assigned_count,
 claim_line_type_code,
 begin_date,
 end_date,
 claim_id,
 type_of_bill,
 facility_type_code,
 place_of_svc_code,
 function_code,
 code_value,
 nomen,
 principal,
 master_episode_id,
 assigned_type,
 LEVEL_5,
 LEVEL_5_association_type,
 BETOS_CATEGORY,
 PB_OTHER,
 OP_ED,
 PAS,
 Chronic_bundle,
 IPC,
 PREVENT
)
AS
 SELECT claims_dsrp_info.id,
        claims_dsrp_info.master_claim_id,
        claims_dsrp_info.member_id,
        claims_dsrp_info.allowed_amt,
        claims_dsrp_info.assigned_count,
        claims_dsrp_info.claim_line_type_code,
        claims_dsrp_info.begin_date,
        claims_dsrp_info.end_date,
        claims_dsrp_info.claim_id,
        claims_dsrp_info.type_of_bill,
        claims_dsrp_info.facility_type_code,
        claims_dsrp_info.place_of_svc_code,
        claims_dsrp_info.function_code,
        claims_dsrp_info.code_value,
        claims_dsrp_info.nomen,
        claims_dsrp_info.principal,
        claims_dsrp_info.master_episode_id,
        claims_dsrp_info.assigned_type,
        claims_dsrp_info.LEVEL_5,
        claims_dsrp_info.LEVEL_5_association_type,
        claims_dsrp_info.BETOS_CATEGORY,
        claims_dsrp_info.PB_OTHER,
        claims_dsrp_info.OP_ED,
        claims_dsrp_info.PAS,
        claims_dsrp_info.Chronic_bundle,
        claims_dsrp_info.IPC,
        claims_dsrp_info.PREVENT
 FROM epbuilder.claims_dsrp_info
 ORDER BY claims_dsrp_info.id
SEGMENTED BY hash(claims_dsrp_info.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.preg_assign_analysis /*+createtype(A)*/ 
(
 master_episode_id,
 allowed_amt,
 ASSIGNED_TO_PREGN,
 ASSIGNED_TO_VAGDEL,
 ASSIGNED_TO_CSECT,
 ASSIGNED_TO_PREVENT
)
AS
 SELECT preg_assign_analysis.master_episode_id,
        preg_assign_analysis.allowed_amt,
        preg_assign_analysis.ASSIGNED_TO_PREGN,
        preg_assign_analysis.ASSIGNED_TO_VAGDEL,
        preg_assign_analysis.ASSIGNED_TO_CSECT,
        preg_assign_analysis.ASSIGNED_TO_PREVENT
 FROM epbuilder.preg_assign_analysis
 ORDER BY preg_assign_analysis.master_episode_id,
          preg_assign_analysis.allowed_amt
SEGMENTED BY hash(preg_assign_analysis.master_episode_id, preg_assign_analysis.allowed_amt) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.QCRG_DETAIL /*+createtype(L)*/ 
(
 QACRG1_CODE,
 QACRG2_CODE,
 QACRG3_CODE,
 QACRG1_DESC,
 QACRG2_DESC,
 QACRG3_DESC,
 QCRG_CODE,
 QCRG_DESC
)
AS
 SELECT QCRG_DETAIL.QACRG1_CODE,
        QCRG_DETAIL.QACRG2_CODE,
        QCRG_DETAIL.QACRG3_CODE,
        QCRG_DETAIL.QACRG1_DESC,
        QCRG_DETAIL.QACRG2_DESC,
        QCRG_DETAIL.QACRG3_DESC,
        QCRG_DETAIL.QCRG_CODE,
        QCRG_DETAIL.QCRG_DESC
 FROM epbuilder.QCRG_DETAIL
 ORDER BY QCRG_DETAIL.QACRG1_CODE,
          QCRG_DETAIL.QACRG2_CODE,
          QCRG_DETAIL.QACRG3_CODE,
          QCRG_DETAIL.QACRG1_DESC,
          QCRG_DETAIL.QACRG2_DESC,
          QCRG_DETAIL.QACRG3_DESC,
          QCRG_DETAIL.QCRG_CODE,
          QCRG_DETAIL.QCRG_DESC
SEGMENTED BY hash(QCRG_DETAIL.QACRG1_CODE, QCRG_DETAIL.QACRG2_CODE, QCRG_DETAIL.QACRG3_CODE, QCRG_DETAIL.QCRG_CODE, QCRG_DETAIL.QACRG3_DESC, QCRG_DETAIL.QACRG1_DESC, QCRG_DETAIL.QCRG_DESC, QCRG_DETAIL.QACRG2_DESC) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_enroll_gap1 /*+createtype(A)*/ 
(
 master_episode_id,
 member_id,
 episode_length,
 gap_length,
 filter_fail
)
AS
 SELECT tmp_enroll_gap1.master_episode_id,
        tmp_enroll_gap1.member_id,
        tmp_enroll_gap1.episode_length,
        tmp_enroll_gap1.gap_length,
        tmp_enroll_gap1.filter_fail
 FROM epbuilder.tmp_enroll_gap1
 ORDER BY tmp_enroll_gap1.master_episode_id,
          tmp_enroll_gap1.member_id,
          tmp_enroll_gap1.episode_length,
          tmp_enroll_gap1.gap_length,
          tmp_enroll_gap1.filter_fail
SEGMENTED BY hash(tmp_enroll_gap1.episode_length, tmp_enroll_gap1.gap_length, tmp_enroll_gap1.filter_fail, tmp_enroll_gap1.member_id, tmp_enroll_gap1.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_proc_orph_conf /*+createtype(A)*/ 
(
 master_episode_id,
 master_claim_id,
 claim_line_type_code,
 begin_date,
 end_date,
 confirmed
)
AS
 SELECT tmp_proc_orph_conf.master_episode_id,
        tmp_proc_orph_conf.master_claim_id,
        tmp_proc_orph_conf.claim_line_type_code,
        tmp_proc_orph_conf.begin_date,
        tmp_proc_orph_conf.end_date,
        tmp_proc_orph_conf.confirmed
 FROM epbuilder.tmp_proc_orph_conf
 ORDER BY tmp_proc_orph_conf.master_episode_id,
          tmp_proc_orph_conf.master_claim_id,
          tmp_proc_orph_conf.claim_line_type_code,
          tmp_proc_orph_conf.begin_date,
          tmp_proc_orph_conf.end_date
SEGMENTED BY hash(tmp_proc_orph_conf.begin_date, tmp_proc_orph_conf.end_date, tmp_proc_orph_conf.confirmed, tmp_proc_orph_conf.claim_line_type_code, tmp_proc_orph_conf.master_episode_id, tmp_proc_orph_conf.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_proc_orph_conf2 /*+createtype(A)*/ 
(
 master_episode_id,
 master_claim_id,
 claim_line_type_code,
 begin_date,
 end_date,
 confirmed
)
AS
 SELECT tmp_proc_orph_conf2.master_episode_id,
        tmp_proc_orph_conf2.master_claim_id,
        tmp_proc_orph_conf2.claim_line_type_code,
        tmp_proc_orph_conf2.begin_date,
        tmp_proc_orph_conf2.end_date,
        tmp_proc_orph_conf2.confirmed
 FROM epbuilder.tmp_proc_orph_conf2
 ORDER BY tmp_proc_orph_conf2.master_episode_id,
          tmp_proc_orph_conf2.master_claim_id,
          tmp_proc_orph_conf2.claim_line_type_code,
          tmp_proc_orph_conf2.begin_date,
          tmp_proc_orph_conf2.end_date
SEGMENTED BY hash(tmp_proc_orph_conf2.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.filtered_episodes_TESTING /*+createtype(A)*/ 
(
 filter_id,
 master_episode_id,
 filter_fail,
 age_limit_lower,
 age_limit_upper,
 coverage_gap,
 proc_ep_orphan,
 proc_ep_orph_triggered,
 episode_complete,
 drg,
 episode_cost_lower,
 episode_cost_upper
)
AS
 SELECT filtered_episodes_TESTING.filter_id,
        filtered_episodes_TESTING.master_episode_id,
        filtered_episodes_TESTING.filter_fail,
        filtered_episodes_TESTING.age_limit_lower,
        filtered_episodes_TESTING.age_limit_upper,
        filtered_episodes_TESTING.coverage_gap,
        filtered_episodes_TESTING.proc_ep_orphan,
        filtered_episodes_TESTING.proc_ep_orph_triggered,
        filtered_episodes_TESTING.episode_complete,
        filtered_episodes_TESTING.drg,
        filtered_episodes_TESTING.episode_cost_lower,
        filtered_episodes_TESTING.episode_cost_upper
 FROM epbuilder.filtered_episodes_TESTING
 ORDER BY filtered_episodes_TESTING.filter_id,
          filtered_episodes_TESTING.master_episode_id
SEGMENTED BY hash(filtered_episodes_TESTING.filter_id, filtered_episodes_TESTING.filter_fail, filtered_episodes_TESTING.age_limit_lower, filtered_episodes_TESTING.age_limit_upper, filtered_episodes_TESTING.coverage_gap, filtered_episodes_TESTING.proc_ep_orphan, filtered_episodes_TESTING.proc_ep_orph_triggered, filtered_episodes_TESTING.episode_complete, filtered_episodes_TESTING.drg, filtered_episodes_TESTING.episode_cost_lower, filtered_episodes_TESTING.episode_cost_upper, filtered_episodes_TESTING.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrollment_raw_old /*+createtype(A)*/ 
(
 year,
 month,
 member_id,
 dob,
 sex,
 dod
)
AS
 SELECT enrollment_raw_old.year,
        enrollment_raw_old.month,
        enrollment_raw_old.member_id,
        enrollment_raw_old.dob,
        enrollment_raw_old.sex,
        enrollment_raw_old.dod
 FROM epbuilder.enrollment_raw_old
 ORDER BY enrollment_raw_old.year,
          enrollment_raw_old.month,
          enrollment_raw_old.member_id,
          enrollment_raw_old.dob,
          enrollment_raw_old.sex,
          enrollment_raw_old.dod
SEGMENTED BY hash(enrollment_raw_old.year, enrollment_raw_old.month, enrollment_raw_old.sex, enrollment_raw_old.dob, enrollment_raw_old.member_id, enrollment_raw_old.dod) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.MTRNTY_nborn_delivery /*+createtype(A)*/ 
(
 del_meid,
 nborn_meid
)
AS
 SELECT MTRNTY_nborn_delivery.del_meid,
        MTRNTY_nborn_delivery.nborn_meid
 FROM epbuilder.MTRNTY_nborn_delivery
 ORDER BY MTRNTY_nborn_delivery.del_meid,
          MTRNTY_nborn_delivery.nborn_meid
SEGMENTED BY hash(MTRNTY_nborn_delivery.del_meid, MTRNTY_nborn_delivery.nborn_meid) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.report_episode_detail_revised_justincase /*+createtype(A)*/ 
(
 Filter_ID,
 Member_ID,
 Master_Episode_ID,
 Episode_ID,
 Episode_Name,
 Episode_Description,
 Episode_Type,
 MDC,
 MDC_Description,
 Episode_Begin_Date,
 Episode_End_Date,
 Episode_Length,
 Level,
 Split_Total_Cost,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_80thPercentile_Cost,
 Split_Total_PAC_Cost,
 Split_Total_Typical_Cost,
 Split_Total_TypicalwPAC_Cost,
 Annualized_Split_Total_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_80thPercentile_Cost,
 Annualized_Split_Total_PAC_Cost,
 Annualized_Split_Total_Typical_Cost,
 Annualized_Split_Total_TypicalwPAC_Cost,
 Unsplit_Total_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Unsplit_Total_PAC_Cost,
 Unsplit_Total_Typical_Cost,
 Unsplit_Total_TypicalwPAC_Cost,
 Annualized_Unsplit_Total_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_Total_PAC_Cost,
 Annualized_Unsplit_Total_Typical_Cost,
 Annualized_Unsplit_Total_TypicalwPAC_Cost,
 Facility_ID,
 Physician_ID,
 Physician_Specialty,
 Split_Expected_Total_Cost,
 Split_Expected_Typical_IP_Cost,
 Split_Expected_Typical_Other_Cost,
 Split_Expected_PAC_Cost,
 Unsplit_Expected_Total_Cost,
 Unsplit_Expected_Typical_IP_Cost,
 Unsplit_Expected_Typical_Other_Cost,
 Unsplit_Expected_PAC_Cost,
 IP_PAC_Count,
 OP_PAC_Count,
 PB_PAC_Count,
 RX_PAC_Count
)
AS
 SELECT report_episode_detail_revised_justincase.Filter_ID,
        report_episode_detail_revised_justincase.Member_ID,
        report_episode_detail_revised_justincase.Master_Episode_ID,
        report_episode_detail_revised_justincase.Episode_ID,
        report_episode_detail_revised_justincase.Episode_Name,
        report_episode_detail_revised_justincase.Episode_Description,
        report_episode_detail_revised_justincase.Episode_Type,
        report_episode_detail_revised_justincase.MDC,
        report_episode_detail_revised_justincase.MDC_Description,
        report_episode_detail_revised_justincase.Episode_Begin_Date,
        report_episode_detail_revised_justincase.Episode_End_Date,
        report_episode_detail_revised_justincase.Episode_Length,
        report_episode_detail_revised_justincase.Level,
        report_episode_detail_revised_justincase.Split_Total_Cost,
        report_episode_detail_revised_justincase.Split_1stPercentile_Cost,
        report_episode_detail_revised_justincase.Split_99thPercentile_Cost,
        report_episode_detail_revised_justincase.Split_80thPercentile_Cost,
        report_episode_detail_revised_justincase.Split_Total_PAC_Cost,
        report_episode_detail_revised_justincase.Split_Total_Typical_Cost,
        report_episode_detail_revised_justincase.Split_Total_TypicalwPAC_Cost,
        report_episode_detail_revised_justincase.Annualized_Split_Total_Cost,
        report_episode_detail_revised_justincase.Annualized_Split_1stPercentile_Cost,
        report_episode_detail_revised_justincase.Annualized_Split_99thPercentile_Cost,
        report_episode_detail_revised_justincase.Annualized_Split_80thPercentile_Cost,
        report_episode_detail_revised_justincase.Annualized_Split_Total_PAC_Cost,
        report_episode_detail_revised_justincase.Annualized_Split_Total_Typical_Cost,
        report_episode_detail_revised_justincase.Annualized_Split_Total_TypicalwPAC_Cost,
        report_episode_detail_revised_justincase.Unsplit_Total_Cost,
        report_episode_detail_revised_justincase.Unsplit_1stPercentile_Cost,
        report_episode_detail_revised_justincase.Unsplit_99thPercentile_Cost,
        report_episode_detail_revised_justincase.Unsplit_Total_PAC_Cost,
        report_episode_detail_revised_justincase.Unsplit_Total_Typical_Cost,
        report_episode_detail_revised_justincase.Unsplit_Total_TypicalwPAC_Cost,
        report_episode_detail_revised_justincase.Annualized_Unsplit_Total_Cost,
        report_episode_detail_revised_justincase.Annualized_Unsplit_1stPercentile_Cost,
        report_episode_detail_revised_justincase.Annualized_Unsplit_99thPercentile_Cost,
        report_episode_detail_revised_justincase.Annualized_Unsplit_Total_PAC_Cost,
        report_episode_detail_revised_justincase.Annualized_Unsplit_Total_Typical_Cost,
        report_episode_detail_revised_justincase.Annualized_Unsplit_Total_TypicalwPAC_Cost,
        report_episode_detail_revised_justincase.Facility_ID,
        report_episode_detail_revised_justincase.Physician_ID,
        report_episode_detail_revised_justincase.Physician_Specialty,
        report_episode_detail_revised_justincase.Split_Expected_Total_Cost,
        report_episode_detail_revised_justincase.Split_Expected_Typical_IP_Cost,
        report_episode_detail_revised_justincase.Split_Expected_Typical_Other_Cost,
        report_episode_detail_revised_justincase.Split_Expected_PAC_Cost,
        report_episode_detail_revised_justincase.Unsplit_Expected_Total_Cost,
        report_episode_detail_revised_justincase.Unsplit_Expected_Typical_IP_Cost,
        report_episode_detail_revised_justincase.Unsplit_Expected_Typical_Other_Cost,
        report_episode_detail_revised_justincase.Unsplit_Expected_PAC_Cost,
        report_episode_detail_revised_justincase.IP_PAC_Count,
        report_episode_detail_revised_justincase.OP_PAC_Count,
        report_episode_detail_revised_justincase.PB_PAC_Count,
        report_episode_detail_revised_justincase.RX_PAC_Count
 FROM epbuilder.report_episode_detail_revised_justincase
 ORDER BY report_episode_detail_revised_justincase.Filter_ID,
          report_episode_detail_revised_justincase.Member_ID,
          report_episode_detail_revised_justincase.Master_Episode_ID,
          report_episode_detail_revised_justincase.Episode_ID,
          report_episode_detail_revised_justincase.Episode_Name,
          report_episode_detail_revised_justincase.Episode_Description,
          report_episode_detail_revised_justincase.Episode_Type,
          report_episode_detail_revised_justincase.MDC,
          report_episode_detail_revised_justincase.MDC_Description,
          report_episode_detail_revised_justincase.Episode_Begin_Date,
          report_episode_detail_revised_justincase.Episode_End_Date,
          report_episode_detail_revised_justincase.Episode_Length,
          report_episode_detail_revised_justincase.Level,
          report_episode_detail_revised_justincase.Split_Total_Cost,
          report_episode_detail_revised_justincase.Split_1stPercentile_Cost,
          report_episode_detail_revised_justincase.Split_99thPercentile_Cost,
          report_episode_detail_revised_justincase.Split_80thPercentile_Cost,
          report_episode_detail_revised_justincase.Split_Total_PAC_Cost,
          report_episode_detail_revised_justincase.Split_Total_Typical_Cost,
          report_episode_detail_revised_justincase.Split_Total_TypicalwPAC_Cost,
          report_episode_detail_revised_justincase.Annualized_Split_Total_Cost,
          report_episode_detail_revised_justincase.Annualized_Split_1stPercentile_Cost,
          report_episode_detail_revised_justincase.Annualized_Split_99thPercentile_Cost,
          report_episode_detail_revised_justincase.Annualized_Split_80thPercentile_Cost,
          report_episode_detail_revised_justincase.Annualized_Split_Total_PAC_Cost,
          report_episode_detail_revised_justincase.Annualized_Split_Total_Typical_Cost,
          report_episode_detail_revised_justincase.Annualized_Split_Total_TypicalwPAC_Cost,
          report_episode_detail_revised_justincase.Unsplit_Total_Cost,
          report_episode_detail_revised_justincase.Unsplit_1stPercentile_Cost,
          report_episode_detail_revised_justincase.Unsplit_99thPercentile_Cost,
          report_episode_detail_revised_justincase.Unsplit_Total_PAC_Cost,
          report_episode_detail_revised_justincase.Unsplit_Total_Typical_Cost,
          report_episode_detail_revised_justincase.Unsplit_Total_TypicalwPAC_Cost,
          report_episode_detail_revised_justincase.Annualized_Unsplit_Total_Cost,
          report_episode_detail_revised_justincase.Annualized_Unsplit_1stPercentile_Cost,
          report_episode_detail_revised_justincase.Annualized_Unsplit_99thPercentile_Cost,
          report_episode_detail_revised_justincase.Annualized_Unsplit_Total_PAC_Cost,
          report_episode_detail_revised_justincase.Annualized_Unsplit_Total_Typical_Cost,
          report_episode_detail_revised_justincase.Annualized_Unsplit_Total_TypicalwPAC_Cost,
          report_episode_detail_revised_justincase.Facility_ID,
          report_episode_detail_revised_justincase.Physician_ID,
          report_episode_detail_revised_justincase.Physician_Specialty,
          report_episode_detail_revised_justincase.Split_Expected_Total_Cost,
          report_episode_detail_revised_justincase.Split_Expected_Typical_IP_Cost,
          report_episode_detail_revised_justincase.Split_Expected_Typical_Other_Cost,
          report_episode_detail_revised_justincase.Split_Expected_PAC_Cost,
          report_episode_detail_revised_justincase.Unsplit_Expected_Total_Cost,
          report_episode_detail_revised_justincase.Unsplit_Expected_Typical_IP_Cost,
          report_episode_detail_revised_justincase.Unsplit_Expected_Typical_Other_Cost,
          report_episode_detail_revised_justincase.Unsplit_Expected_PAC_Cost,
          report_episode_detail_revised_justincase.IP_PAC_Count,
          report_episode_detail_revised_justincase.OP_PAC_Count,
          report_episode_detail_revised_justincase.PB_PAC_Count,
          report_episode_detail_revised_justincase.RX_PAC_Count
SEGMENTED BY hash(report_episode_detail_revised_justincase.Filter_ID, report_episode_detail_revised_justincase.Episode_ID, report_episode_detail_revised_justincase.Episode_Name, report_episode_detail_revised_justincase.MDC, report_episode_detail_revised_justincase.Episode_Length, report_episode_detail_revised_justincase.Level, report_episode_detail_revised_justincase.Split_Total_Cost, report_episode_detail_revised_justincase.Split_1stPercentile_Cost, report_episode_detail_revised_justincase.Split_99thPercentile_Cost, report_episode_detail_revised_justincase.Split_80thPercentile_Cost, report_episode_detail_revised_justincase.Split_Total_PAC_Cost, report_episode_detail_revised_justincase.Split_Total_Typical_Cost, report_episode_detail_revised_justincase.Split_Total_TypicalwPAC_Cost, report_episode_detail_revised_justincase.Annualized_Split_Total_Cost, report_episode_detail_revised_justincase.Annualized_Split_1stPercentile_Cost, report_episode_detail_revised_justincase.Annualized_Split_99thPercentile_Cost, report_episode_detail_revised_justincase.Annualized_Split_80thPercentile_Cost, report_episode_detail_revised_justincase.Annualized_Split_Total_PAC_Cost, report_episode_detail_revised_justincase.Annualized_Split_Total_Typical_Cost, report_episode_detail_revised_justincase.Annualized_Split_Total_TypicalwPAC_Cost, report_episode_detail_revised_justincase.Unsplit_Total_Cost, report_episode_detail_revised_justincase.Unsplit_1stPercentile_Cost, report_episode_detail_revised_justincase.Unsplit_99thPercentile_Cost, report_episode_detail_revised_justincase.Unsplit_Total_PAC_Cost, report_episode_detail_revised_justincase.Unsplit_Total_Typical_Cost, report_episode_detail_revised_justincase.Unsplit_Total_TypicalwPAC_Cost, report_episode_detail_revised_justincase.Annualized_Unsplit_Total_Cost, report_episode_detail_revised_justincase.Annualized_Unsplit_1stPercentile_Cost, report_episode_detail_revised_justincase.Annualized_Unsplit_99thPercentile_Cost, report_episode_detail_revised_justincase.Annualized_Unsplit_Total_PAC_Cost, report_episode_detail_revised_justincase.Annualized_Unsplit_Total_Typical_Cost, report_episode_detail_revised_justincase.Annualized_Unsplit_Total_TypicalwPAC_Cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.srf_pac_count /*+createtype(A)*/ 
(
 episode_id,
 mcregion,
 Number_of_Episodes,
 Number_of_Epis_w_PAC,
 year,
 expected_pac_count
)
AS
 SELECT srf_pac_count.episode_id,
        srf_pac_count.mcregion,
        srf_pac_count.Number_of_Episodes,
        srf_pac_count.Number_of_Epis_w_PAC,
        srf_pac_count.year,
        srf_pac_count.expected_pac_count
 FROM epbuilder.srf_pac_count
 ORDER BY srf_pac_count.mcregion,
          srf_pac_count.episode_id,
          srf_pac_count.year
SEGMENTED BY hash(srf_pac_count.mcregion, srf_pac_count.episode_id, srf_pac_count.year) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.GCRG_DETAIL /*+createtype(L)*/ 
(
 ACRG1_CODE,
 ACRG2_CODE,
 ACRG3_CODE,
 ACRG1_DESC,
 ACRG2_DESC,
 ACRG3_DESC,
 CRG_CODE,
 CRG_DESC
)
AS
 SELECT GCRG_DETAIL.ACRG1_CODE,
        GCRG_DETAIL.ACRG2_CODE,
        GCRG_DETAIL.ACRG3_CODE,
        GCRG_DETAIL.ACRG1_DESC,
        GCRG_DETAIL.ACRG2_DESC,
        GCRG_DETAIL.ACRG3_DESC,
        GCRG_DETAIL.CRG_CODE,
        GCRG_DETAIL.CRG_DESC
 FROM epbuilder.GCRG_DETAIL
 ORDER BY GCRG_DETAIL.ACRG1_CODE,
          GCRG_DETAIL.ACRG2_CODE,
          GCRG_DETAIL.ACRG3_CODE,
          GCRG_DETAIL.ACRG1_DESC,
          GCRG_DETAIL.ACRG2_DESC,
          GCRG_DETAIL.ACRG3_DESC,
          GCRG_DETAIL.CRG_CODE,
          GCRG_DETAIL.CRG_DESC
SEGMENTED BY hash(GCRG_DETAIL.ACRG1_CODE, GCRG_DETAIL.ACRG2_CODE, GCRG_DETAIL.ACRG3_CODE, GCRG_DETAIL.CRG_CODE, GCRG_DETAIL.ACRG1_DESC, GCRG_DETAIL.ACRG2_DESC, GCRG_DETAIL.ACRG3_DESC, GCRG_DETAIL.CRG_DESC) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.crg_2012_2013_2 /*+createtype(L)*/ 
(
 year,
 quarter,
 crg_version,
 claim_begin_date,
 claim_end_date,
 claims_as_of_date,
 recip_id,
 qcrg_code,
 qcrg_desc,
 qacrg1_code,
 qacrg1_desc,
 qacrg2_code,
 qacrg2_desc,
 qacrg3_code,
 qacrg3_desc,
 fincrg_q,
 q_base,
 q_severity,
 crg_code,
 crg_desc,
 acrg1_code,
 acrg1_desc,
 acrg2_code,
 acrg2_desc,
 acrg3_code,
 acrg3_desc,
 fincrg_g,
 g_base,
 g_severity,
 fincrg,
 base,
 severity,
 total_allowed_amount,
 total_medical_allowed,
 total_pharmacy_allowed,
 total_episode_costs,
 acrg_weight,
 qcrg_weight,
 acrg_average,
 qcrg_average
)
AS
 SELECT crg_2012_2013_2.year,
        crg_2012_2013_2.quarter,
        crg_2012_2013_2.crg_version,
        crg_2012_2013_2.claim_begin_date,
        crg_2012_2013_2.claim_end_date,
        crg_2012_2013_2.claims_as_of_date,
        crg_2012_2013_2.recip_id,
        crg_2012_2013_2.qcrg_code,
        crg_2012_2013_2.qcrg_desc,
        crg_2012_2013_2.qacrg1_code,
        crg_2012_2013_2.qacrg1_desc,
        crg_2012_2013_2.qacrg2_code,
        crg_2012_2013_2.qacrg2_desc,
        crg_2012_2013_2.qacrg3_code,
        crg_2012_2013_2.qacrg3_desc,
        crg_2012_2013_2.fincrg_q,
        crg_2012_2013_2.q_base,
        crg_2012_2013_2.q_severity,
        crg_2012_2013_2.crg_code,
        crg_2012_2013_2.crg_desc,
        crg_2012_2013_2.acrg1_code,
        crg_2012_2013_2.acrg1_desc,
        crg_2012_2013_2.acrg2_code,
        crg_2012_2013_2.acrg2_desc,
        crg_2012_2013_2.acrg3_code,
        crg_2012_2013_2.acrg3_desc,
        crg_2012_2013_2.fincrg_g,
        crg_2012_2013_2.g_base,
        crg_2012_2013_2.g_severity,
        crg_2012_2013_2.fincrg,
        crg_2012_2013_2.base,
        crg_2012_2013_2.severity,
        crg_2012_2013_2.total_allowed_amount,
        crg_2012_2013_2.total_medical_allowed,
        crg_2012_2013_2.total_pharmacy_allowed,
        crg_2012_2013_2.total_episode_costs,
        crg_2012_2013_2.acrg_weight,
        crg_2012_2013_2.qcrg_weight,
        crg_2012_2013_2.acrg_average,
        crg_2012_2013_2.qcrg_average
 FROM epbuilder.crg_2012_2013_2
 ORDER BY crg_2012_2013_2.year,
          crg_2012_2013_2.quarter,
          crg_2012_2013_2.crg_version,
          crg_2012_2013_2.claim_begin_date,
          crg_2012_2013_2.claim_end_date,
          crg_2012_2013_2.claims_as_of_date,
          crg_2012_2013_2.recip_id
SEGMENTED BY hash(crg_2012_2013_2.year, crg_2012_2013_2.quarter, crg_2012_2013_2.claim_begin_date, crg_2012_2013_2.claim_end_date, crg_2012_2013_2.claims_as_of_date, crg_2012_2013_2.qcrg_code, crg_2012_2013_2.qacrg1_code, crg_2012_2013_2.qacrg2_code, crg_2012_2013_2.qacrg3_code, crg_2012_2013_2.fincrg_q, crg_2012_2013_2.q_base, crg_2012_2013_2.q_severity, crg_2012_2013_2.crg_code, crg_2012_2013_2.acrg1_code, crg_2012_2013_2.acrg2_code, crg_2012_2013_2.acrg3_code, crg_2012_2013_2.fincrg_g, crg_2012_2013_2.g_base, crg_2012_2013_2.g_severity, crg_2012_2013_2.fincrg, crg_2012_2013_2.base, crg_2012_2013_2.severity, crg_2012_2013_2.total_allowed_amount, crg_2012_2013_2.total_medical_allowed, crg_2012_2013_2.total_pharmacy_allowed, crg_2012_2013_2.total_episode_costs, crg_2012_2013_2.acrg_weight, crg_2012_2013_2.qcrg_weight, crg_2012_2013_2.acrg_average, crg_2012_2013_2.qcrg_average, crg_2012_2013_2.recip_id, crg_2012_2013_2.crg_version) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.unassigned_px_dx_by_epi /*+createtype(A)*/ 
(
 episode_id,
 code_value,
 dx_code,
 count,
 in_epi,
 sum
)
AS
 SELECT unassigned_px_dx_by_epi.episode_id,
        unassigned_px_dx_by_epi.code_value,
        unassigned_px_dx_by_epi.dx_code,
        unassigned_px_dx_by_epi.count,
        unassigned_px_dx_by_epi.in_epi,
        unassigned_px_dx_by_epi.sum
 FROM epbuilder.unassigned_px_dx_by_epi
 ORDER BY unassigned_px_dx_by_epi.episode_id,
          unassigned_px_dx_by_epi.code_value,
          unassigned_px_dx_by_epi.dx_code
SEGMENTED BY hash(unassigned_px_dx_by_epi.episode_id, unassigned_px_dx_by_epi.code_value, unassigned_px_dx_by_epi.dx_code) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.zipcode_county /*+createtype(L)*/ 
(
 County,
 State_FIPS,
 County_Code,
 County_FIPS,
 zip,
 File_Date
)
AS
 SELECT zipcode_county.County,
        zipcode_county.State_FIPS,
        zipcode_county.County_Code,
        zipcode_county.County_FIPS,
        zipcode_county.zip,
        zipcode_county.File_Date
 FROM epbuilder.zipcode_county
 ORDER BY zipcode_county.County,
          zipcode_county.State_FIPS,
          zipcode_county.County_Code,
          zipcode_county.County_FIPS,
          zipcode_county.zip,
          zipcode_county.File_Date
SEGMENTED BY hash(zipcode_county.State_FIPS, zipcode_county.County_Code, zipcode_county.County_FIPS, zipcode_county.zip, zipcode_county.File_Date, zipcode_county.County) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.MBR_CRG_FACT /*+createtype(L)*/ 
(
 YEAR,
 QUARTER,
 CRG_VERSION,
 CLAIM_BEGIN_DATE,
 CLAIM_END_DATE,
 CLAIMS_AS_OF_DATE,
 RECIP_ID,
 QCRG_CODE,
 QACRG1_CODE,
 QACRG2_CODE,
 QACRG3_CODE,
 FINCRG_Q,
 Q_BASE,
 Q_SEVERITY,
 CRG_CODE,
 ACRG1_CODE,
 ACRG2_CODE,
 ACRG3_CODE,
 FINCRG_G,
 G_BASE,
 G_SEVERITY,
 FINCRG,
 BASE,
 SEVERITY,
 MDW_INS_DT,
 MDW_UPDT_DT,
 MDW_INS_ID,
 MDW_UPDT_ID,
 INS_ROW_PRCS_DT
)
AS
 SELECT MBR_CRG_FACT.YEAR,
        MBR_CRG_FACT.QUARTER,
        MBR_CRG_FACT.CRG_VERSION,
        MBR_CRG_FACT.CLAIM_BEGIN_DATE,
        MBR_CRG_FACT.CLAIM_END_DATE,
        MBR_CRG_FACT.CLAIMS_AS_OF_DATE,
        MBR_CRG_FACT.RECIP_ID,
        MBR_CRG_FACT.QCRG_CODE,
        MBR_CRG_FACT.QACRG1_CODE,
        MBR_CRG_FACT.QACRG2_CODE,
        MBR_CRG_FACT.QACRG3_CODE,
        MBR_CRG_FACT.FINCRG_Q,
        MBR_CRG_FACT.Q_BASE,
        MBR_CRG_FACT.Q_SEVERITY,
        MBR_CRG_FACT.CRG_CODE,
        MBR_CRG_FACT.ACRG1_CODE,
        MBR_CRG_FACT.ACRG2_CODE,
        MBR_CRG_FACT.ACRG3_CODE,
        MBR_CRG_FACT.FINCRG_G,
        MBR_CRG_FACT.G_BASE,
        MBR_CRG_FACT.G_SEVERITY,
        MBR_CRG_FACT.FINCRG,
        MBR_CRG_FACT.BASE,
        MBR_CRG_FACT.SEVERITY,
        MBR_CRG_FACT.MDW_INS_DT,
        MBR_CRG_FACT.MDW_UPDT_DT,
        MBR_CRG_FACT.MDW_INS_ID,
        MBR_CRG_FACT.MDW_UPDT_ID,
        MBR_CRG_FACT.INS_ROW_PRCS_DT
 FROM epbuilder.MBR_CRG_FACT
 ORDER BY MBR_CRG_FACT.YEAR,
          MBR_CRG_FACT.QUARTER,
          MBR_CRG_FACT.CRG_VERSION,
          MBR_CRG_FACT.CLAIM_BEGIN_DATE,
          MBR_CRG_FACT.CLAIM_END_DATE,
          MBR_CRG_FACT.CLAIMS_AS_OF_DATE,
          MBR_CRG_FACT.RECIP_ID,
          MBR_CRG_FACT.QCRG_CODE,
          MBR_CRG_FACT.QACRG1_CODE,
          MBR_CRG_FACT.QACRG2_CODE,
          MBR_CRG_FACT.QACRG3_CODE,
          MBR_CRG_FACT.FINCRG_Q,
          MBR_CRG_FACT.Q_BASE,
          MBR_CRG_FACT.Q_SEVERITY,
          MBR_CRG_FACT.CRG_CODE,
          MBR_CRG_FACT.ACRG1_CODE,
          MBR_CRG_FACT.ACRG2_CODE,
          MBR_CRG_FACT.ACRG3_CODE,
          MBR_CRG_FACT.FINCRG_G,
          MBR_CRG_FACT.G_BASE,
          MBR_CRG_FACT.G_SEVERITY,
          MBR_CRG_FACT.FINCRG,
          MBR_CRG_FACT.BASE,
          MBR_CRG_FACT.SEVERITY,
          MBR_CRG_FACT.MDW_INS_DT,
          MBR_CRG_FACT.MDW_UPDT_DT,
          MBR_CRG_FACT.MDW_INS_ID,
          MBR_CRG_FACT.MDW_UPDT_ID,
          MBR_CRG_FACT.INS_ROW_PRCS_DT
SEGMENTED BY hash(MBR_CRG_FACT.YEAR, MBR_CRG_FACT.QUARTER, MBR_CRG_FACT.CLAIM_BEGIN_DATE, MBR_CRG_FACT.CLAIM_END_DATE, MBR_CRG_FACT.CLAIMS_AS_OF_DATE, MBR_CRG_FACT.RECIP_ID, MBR_CRG_FACT.QCRG_CODE, MBR_CRG_FACT.QACRG1_CODE, MBR_CRG_FACT.QACRG2_CODE, MBR_CRG_FACT.QACRG3_CODE, MBR_CRG_FACT.FINCRG_Q, MBR_CRG_FACT.Q_BASE, MBR_CRG_FACT.Q_SEVERITY, MBR_CRG_FACT.CRG_CODE, MBR_CRG_FACT.ACRG1_CODE, MBR_CRG_FACT.ACRG2_CODE, MBR_CRG_FACT.ACRG3_CODE, MBR_CRG_FACT.FINCRG_G, MBR_CRG_FACT.G_BASE, MBR_CRG_FACT.G_SEVERITY, MBR_CRG_FACT.FINCRG, MBR_CRG_FACT.BASE, MBR_CRG_FACT.SEVERITY, MBR_CRG_FACT.MDW_INS_DT, MBR_CRG_FACT.MDW_UPDT_DT, MBR_CRG_FACT.INS_ROW_PRCS_DT, MBR_CRG_FACT.CRG_VERSION, MBR_CRG_FACT.MDW_INS_ID, MBR_CRG_FACT.MDW_UPDT_ID) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.mbr_crg_fact_quinn /*+createtype(A)*/ 
(
 YEAR,
 QUARTER,
 CRG_VERSION,
 CLAIM_BEGIN_DATE,
 CLAIM_END_DATE,
 CLAIMS_AS_OF_DATE,
 RECIP_ID,
 QCRG_CODE,
 QACRG1_CODE,
 QACRG2_CODE,
 QACRG3_CODE,
 FINCRG_Q,
 Q_BASE,
 Q_SEVERITY,
 CRG_CODE,
 ACRG1_CODE,
 ACRG2_CODE,
 ACRG3_CODE,
 FINCRG_G,
 G_BASE,
 G_SEVERITY
)
AS
 SELECT mbr_crg_fact_quinn.YEAR,
        mbr_crg_fact_quinn.QUARTER,
        mbr_crg_fact_quinn.CRG_VERSION,
        mbr_crg_fact_quinn.CLAIM_BEGIN_DATE,
        mbr_crg_fact_quinn.CLAIM_END_DATE,
        mbr_crg_fact_quinn.CLAIMS_AS_OF_DATE,
        mbr_crg_fact_quinn.RECIP_ID,
        mbr_crg_fact_quinn.QCRG_CODE,
        mbr_crg_fact_quinn.QACRG1_CODE,
        mbr_crg_fact_quinn.QACRG2_CODE,
        mbr_crg_fact_quinn.QACRG3_CODE,
        mbr_crg_fact_quinn.FINCRG_Q,
        mbr_crg_fact_quinn.Q_BASE,
        mbr_crg_fact_quinn.Q_SEVERITY,
        mbr_crg_fact_quinn.CRG_CODE,
        mbr_crg_fact_quinn.ACRG1_CODE,
        mbr_crg_fact_quinn.ACRG2_CODE,
        mbr_crg_fact_quinn.ACRG3_CODE,
        mbr_crg_fact_quinn.FINCRG_G,
        mbr_crg_fact_quinn.G_BASE,
        mbr_crg_fact_quinn.G_SEVERITY
 FROM epbuilder.mbr_crg_fact_quinn
 ORDER BY mbr_crg_fact_quinn.YEAR,
          mbr_crg_fact_quinn.QUARTER,
          mbr_crg_fact_quinn.CRG_VERSION,
          mbr_crg_fact_quinn.CLAIM_BEGIN_DATE,
          mbr_crg_fact_quinn.CLAIM_END_DATE,
          mbr_crg_fact_quinn.CLAIMS_AS_OF_DATE,
          mbr_crg_fact_quinn.RECIP_ID,
          mbr_crg_fact_quinn.QCRG_CODE,
          mbr_crg_fact_quinn.QACRG1_CODE,
          mbr_crg_fact_quinn.QACRG2_CODE,
          mbr_crg_fact_quinn.QACRG3_CODE,
          mbr_crg_fact_quinn.FINCRG_Q,
          mbr_crg_fact_quinn.Q_BASE,
          mbr_crg_fact_quinn.Q_SEVERITY,
          mbr_crg_fact_quinn.CRG_CODE,
          mbr_crg_fact_quinn.ACRG1_CODE,
          mbr_crg_fact_quinn.ACRG2_CODE,
          mbr_crg_fact_quinn.ACRG3_CODE,
          mbr_crg_fact_quinn.FINCRG_G,
          mbr_crg_fact_quinn.G_BASE,
          mbr_crg_fact_quinn.G_SEVERITY
SEGMENTED BY hash(mbr_crg_fact_quinn.YEAR, mbr_crg_fact_quinn.QUARTER, mbr_crg_fact_quinn.CLAIM_BEGIN_DATE, mbr_crg_fact_quinn.CLAIM_END_DATE, mbr_crg_fact_quinn.CLAIMS_AS_OF_DATE, mbr_crg_fact_quinn.RECIP_ID, mbr_crg_fact_quinn.QCRG_CODE, mbr_crg_fact_quinn.QACRG1_CODE, mbr_crg_fact_quinn.QACRG2_CODE, mbr_crg_fact_quinn.QACRG3_CODE, mbr_crg_fact_quinn.FINCRG_Q, mbr_crg_fact_quinn.Q_BASE, mbr_crg_fact_quinn.Q_SEVERITY, mbr_crg_fact_quinn.CRG_CODE, mbr_crg_fact_quinn.ACRG1_CODE, mbr_crg_fact_quinn.ACRG2_CODE, mbr_crg_fact_quinn.ACRG3_CODE, mbr_crg_fact_quinn.FINCRG_G, mbr_crg_fact_quinn.G_BASE, mbr_crg_fact_quinn.G_SEVERITY, mbr_crg_fact_quinn.CRG_VERSION) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_sub_population /*+createtype(L)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group
)
AS
 SELECT member_sub_population.member_id,
        member_sub_population.sub_population,
        member_sub_population.gender,
        member_sub_population.birth_year,
        member_sub_population.age_group
 FROM epbuilder.member_sub_population
 ORDER BY member_sub_population.member_id,
          member_sub_population.sub_population,
          member_sub_population.gender,
          member_sub_population.birth_year,
          member_sub_population.age_group
SEGMENTED BY hash(member_sub_population.gender, member_sub_population.birth_year, member_sub_population.sub_population, member_sub_population.age_group, member_sub_population.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_sub_population_exclusive /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group
)
AS
 SELECT member_sub_population_exclusive.member_id,
        member_sub_population_exclusive.sub_population,
        member_sub_population_exclusive.gender,
        member_sub_population_exclusive.birth_year,
        member_sub_population_exclusive.age_group
 FROM epbuilder.member_sub_population_exclusive
 ORDER BY member_sub_population_exclusive.member_id,
          member_sub_population_exclusive.sub_population,
          member_sub_population_exclusive.gender,
          member_sub_population_exclusive.birth_year,
          member_sub_population_exclusive.age_group
SEGMENTED BY hash(member_sub_population_exclusive.gender, member_sub_population_exclusive.birth_year, member_sub_population_exclusive.sub_population, member_sub_population_exclusive.age_group, member_sub_population_exclusive.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.claims_combined_with_provider_id /*+createtype(L)*/ 
(
 master_claim_id,
 member_id,
 allowed_amt,
 assigned_count,
 claim_line_type_code,
 begin_date,
 end_date,
 provider_id
)
AS
 SELECT claims_combined_with_provider_id.master_claim_id,
        claims_combined_with_provider_id.member_id,
        claims_combined_with_provider_id.allowed_amt,
        claims_combined_with_provider_id.assigned_count,
        claims_combined_with_provider_id.claim_line_type_code,
        claims_combined_with_provider_id.begin_date,
        claims_combined_with_provider_id.end_date,
        claims_combined_with_provider_id.provider_id
 FROM epbuilder.claims_combined_with_provider_id
 ORDER BY claims_combined_with_provider_id.master_claim_id
SEGMENTED BY hash(claims_combined_with_provider_id.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.vbp_contractor_provider_npi_tmp /*+createtype(L)*/ 
(
 provider_npi,
 vbp_contractor,
 PGroup
)
AS
 SELECT vbp_contractor_provider_npi_tmp.provider_npi,
        vbp_contractor_provider_npi_tmp.vbp_contractor,
        vbp_contractor_provider_npi_tmp.PGroup
 FROM epbuilder.vbp_contractor_provider_npi_tmp
 ORDER BY vbp_contractor_provider_npi_tmp.provider_npi,
          vbp_contractor_provider_npi_tmp.vbp_contractor,
          vbp_contractor_provider_npi_tmp.PGroup
SEGMENTED BY hash(vbp_contractor_provider_npi_tmp.provider_npi, vbp_contractor_provider_npi_tmp.vbp_contractor, vbp_contractor_provider_npi_tmp.PGroup) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_expected_pac_count /*+createtype(A)*/ 
(
 master_episode_id,
 expected_pac_count,
 raw_pac_count
)
AS
 SELECT ra_expected_pac_count.master_episode_id,
        ra_expected_pac_count.expected_pac_count,
        ra_expected_pac_count.raw_pac_count
 FROM epbuilder.ra_expected_pac_count
 ORDER BY ra_expected_pac_count.master_episode_id
SEGMENTED BY hash(ra_expected_pac_count.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.temp_m /*+createtype(A)*/ 
(
 mtrnty_meid,
 sa_exp_cost,
 sa_exp_typical_cost,
 sa_exp_pac_cost
)
AS
 SELECT temp_m.mtrnty_meid,
        temp_m.sa_exp_cost,
        temp_m.sa_exp_typical_cost,
        temp_m.sa_exp_pac_cost
 FROM epbuilder.temp_m
 ORDER BY temp_m.mtrnty_meid,
          temp_m.sa_exp_cost,
          temp_m.sa_exp_typical_cost,
          temp_m.sa_exp_pac_cost
SEGMENTED BY hash(temp_m.sa_exp_cost, temp_m.sa_exp_typical_cost, temp_m.sa_exp_pac_cost, temp_m.mtrnty_meid) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.TCGP_2013_by_member /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 pcp,
 total_cost
)
AS
 SELECT TCGP_2013_by_member.member_id,
        TCGP_2013_by_member.sub_population,
        TCGP_2013_by_member.gender,
        TCGP_2013_by_member.birth_year,
        TCGP_2013_by_member.pcp,
        TCGP_2013_by_member.total_cost
 FROM epbuilder.TCGP_2013_by_member
 ORDER BY TCGP_2013_by_member.member_id,
          TCGP_2013_by_member.sub_population,
          TCGP_2013_by_member.gender,
          TCGP_2013_by_member.birth_year,
          TCGP_2013_by_member.pcp
SEGMENTED BY hash(TCGP_2013_by_member.member_id, TCGP_2013_by_member.sub_population, TCGP_2013_by_member.gender, TCGP_2013_by_member.birth_year, TCGP_2013_by_member.pcp) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.crg_cost_summary_by_member2 /*+createtype(L)*/ 
(
 member_id,
 year,
 cms_age_group,
 gender,
 fincrg,
 actual_cost,
 expected_cost
)
AS
 SELECT crg_cost_summary_by_member2.member_id,
        crg_cost_summary_by_member2.year,
        crg_cost_summary_by_member2.cms_age_group,
        crg_cost_summary_by_member2.gender,
        crg_cost_summary_by_member2.fincrg,
        crg_cost_summary_by_member2.actual_cost,
        crg_cost_summary_by_member2.expected_cost
 FROM epbuilder.crg_cost_summary_by_member2
 ORDER BY crg_cost_summary_by_member2.member_id,
          crg_cost_summary_by_member2.year,
          crg_cost_summary_by_member2.cms_age_group,
          crg_cost_summary_by_member2.gender,
          crg_cost_summary_by_member2.fincrg,
          crg_cost_summary_by_member2.actual_cost,
          crg_cost_summary_by_member2.expected_cost
SEGMENTED BY hash(crg_cost_summary_by_member2.actual_cost, crg_cost_summary_by_member2.expected_cost, crg_cost_summary_by_member2.year, crg_cost_summary_by_member2.gender, crg_cost_summary_by_member2.fincrg, crg_cost_summary_by_member2.member_id, crg_cost_summary_by_member2.cms_age_group) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.crg_cost_summary_by_member2_pps /*+createtype(A)*/ 
(
 member_id,
 year,
 cms_age_group,
 gender,
 fincrg,
 actual_cost,
 expected_cost,
 pps
)
AS
 SELECT crg_cost_summary_by_member2_pps.member_id,
        crg_cost_summary_by_member2_pps.year,
        crg_cost_summary_by_member2_pps.cms_age_group,
        crg_cost_summary_by_member2_pps.gender,
        crg_cost_summary_by_member2_pps.fincrg,
        crg_cost_summary_by_member2_pps.actual_cost,
        crg_cost_summary_by_member2_pps.expected_cost,
        crg_cost_summary_by_member2_pps.pps
 FROM epbuilder.crg_cost_summary_by_member2_pps
 ORDER BY crg_cost_summary_by_member2_pps.member_id,
          crg_cost_summary_by_member2_pps.year,
          crg_cost_summary_by_member2_pps.cms_age_group,
          crg_cost_summary_by_member2_pps.gender,
          crg_cost_summary_by_member2_pps.fincrg,
          crg_cost_summary_by_member2_pps.actual_cost,
          crg_cost_summary_by_member2_pps.expected_cost,
          crg_cost_summary_by_member2_pps.pps
SEGMENTED BY hash(crg_cost_summary_by_member2_pps.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.expected_costs_2012 /*+createtype(A)*/ 
(
 gender,
 cms_age_group,
 fincrg,
 member_count,
 actual_cost_2012
)
AS
 SELECT expected_costs_2012.gender,
        expected_costs_2012.cms_age_group,
        expected_costs_2012.fincrg,
        expected_costs_2012.member_count,
        expected_costs_2012.actual_cost_2012
 FROM epbuilder.expected_costs_2012
 ORDER BY expected_costs_2012.gender,
          expected_costs_2012.cms_age_group,
          expected_costs_2012.fincrg
SEGMENTED BY hash(expected_costs_2012.gender, expected_costs_2012.cms_age_group, expected_costs_2012.fincrg) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.crg_cost_summary_by_member_hci3 /*+createtype(L)*/ 
(
 member_id,
 year,
 cms_age_group,
 gender,
 fincrg_q,
 qacrg3_desc,
 actual_cost,
 expected_cost
)
AS
 SELECT crg_cost_summary_by_member_hci3.member_id,
        crg_cost_summary_by_member_hci3.year,
        crg_cost_summary_by_member_hci3.cms_age_group,
        crg_cost_summary_by_member_hci3.gender,
        crg_cost_summary_by_member_hci3.fincrg_q,
        crg_cost_summary_by_member_hci3.qacrg3_desc,
        crg_cost_summary_by_member_hci3.actual_cost,
        crg_cost_summary_by_member_hci3.expected_cost
 FROM epbuilder.crg_cost_summary_by_member_hci3
 ORDER BY crg_cost_summary_by_member_hci3.member_id,
          crg_cost_summary_by_member_hci3.year,
          crg_cost_summary_by_member_hci3.cms_age_group,
          crg_cost_summary_by_member_hci3.gender,
          crg_cost_summary_by_member_hci3.fincrg_q,
          crg_cost_summary_by_member_hci3.qacrg3_desc,
          crg_cost_summary_by_member_hci3.actual_cost,
          crg_cost_summary_by_member_hci3.expected_cost
SEGMENTED BY hash(crg_cost_summary_by_member_hci3.actual_cost, crg_cost_summary_by_member_hci3.expected_cost, crg_cost_summary_by_member_hci3.year, crg_cost_summary_by_member_hci3.gender, crg_cost_summary_by_member_hci3.fincrg_q, crg_cost_summary_by_member_hci3.member_id, crg_cost_summary_by_member_hci3.qacrg3_desc, crg_cost_summary_by_member_hci3.cms_age_group) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.crg_cost_summary_by_member_hci3_pps /*+createtype(A)*/ 
(
 member_id,
 year,
 cms_age_group,
 gender,
 fincrg_q,
 qacrg3_desc,
 actual_cost,
 expected_cost,
 pps
)
AS
 SELECT crg_cost_summary_by_member_hci3_pps.member_id,
        crg_cost_summary_by_member_hci3_pps.year,
        crg_cost_summary_by_member_hci3_pps.cms_age_group,
        crg_cost_summary_by_member_hci3_pps.gender,
        crg_cost_summary_by_member_hci3_pps.fincrg_q,
        crg_cost_summary_by_member_hci3_pps.qacrg3_desc,
        crg_cost_summary_by_member_hci3_pps.actual_cost,
        crg_cost_summary_by_member_hci3_pps.expected_cost,
        crg_cost_summary_by_member_hci3_pps.pps
 FROM epbuilder.crg_cost_summary_by_member_hci3_pps
 ORDER BY crg_cost_summary_by_member_hci3_pps.member_id,
          crg_cost_summary_by_member_hci3_pps.year,
          crg_cost_summary_by_member_hci3_pps.cms_age_group,
          crg_cost_summary_by_member_hci3_pps.gender,
          crg_cost_summary_by_member_hci3_pps.fincrg_q,
          crg_cost_summary_by_member_hci3_pps.qacrg3_desc,
          crg_cost_summary_by_member_hci3_pps.actual_cost,
          crg_cost_summary_by_member_hci3_pps.expected_cost,
          crg_cost_summary_by_member_hci3_pps.pps
SEGMENTED BY hash(crg_cost_summary_by_member_hci3_pps.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.TCGP_by_member_test /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group,
 pps,
 pcp,
 year,
 total_cost
)
AS
 SELECT TCGP_by_member_test.member_id,
        TCGP_by_member_test.sub_population,
        TCGP_by_member_test.gender,
        TCGP_by_member_test.birth_year,
        TCGP_by_member_test.age_group,
        TCGP_by_member_test.pps,
        TCGP_by_member_test.pcp,
        TCGP_by_member_test.year,
        TCGP_by_member_test.total_cost
 FROM epbuilder.TCGP_by_member_test
 ORDER BY TCGP_by_member_test.member_id,
          TCGP_by_member_test.sub_population,
          TCGP_by_member_test.gender,
          TCGP_by_member_test.birth_year,
          TCGP_by_member_test.pps,
          TCGP_by_member_test.pcp,
          TCGP_by_member_test.year
SEGMENTED BY hash(TCGP_by_member_test.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.standard_preg_cost_info /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 PREG_master_episode_id,
 trig_begin_date,
 year,
 SPLIT_TOTAL_PREGN_COST,
 SPLIT_TYPICAL_PREGN_COST,
 SPLIT_PAC_PREGN_COST,
 UNSPLIT_TOTAL_PREGN_COST,
 UNSPLIT_TYPICAL_PREGN_COST,
 UNSPLIT_PAC_PREGN_COST,
 date_of_firt_pregn_service,
 months_of_pregn_services,
 STANDARD_SPLIT_TOTAL,
 STANDARD_SPLIT_TYPICAL,
 STANDARD_SPLIT_PAC,
 STANDARD_UNSPLIT_TOTAL,
 STANDARD_UNSPLIT_TYPICAL,
 STANDARD_UNSPLIT_PAC
)
AS
 SELECT standard_preg_cost_info.ENCRYPT_RECIP_ID_MOM,
        standard_preg_cost_info.ENCRYPT_RECIP_ID_BABY,
        standard_preg_cost_info.PREG_master_episode_id,
        standard_preg_cost_info.trig_begin_date,
        standard_preg_cost_info.year,
        standard_preg_cost_info.SPLIT_TOTAL_PREGN_COST,
        standard_preg_cost_info.SPLIT_TYPICAL_PREGN_COST,
        standard_preg_cost_info.SPLIT_PAC_PREGN_COST,
        standard_preg_cost_info.UNSPLIT_TOTAL_PREGN_COST,
        standard_preg_cost_info.UNSPLIT_TYPICAL_PREGN_COST,
        standard_preg_cost_info.UNSPLIT_PAC_PREGN_COST,
        standard_preg_cost_info.date_of_firt_pregn_service,
        standard_preg_cost_info.months_of_pregn_services,
        standard_preg_cost_info.STANDARD_SPLIT_TOTAL,
        standard_preg_cost_info.STANDARD_SPLIT_TYPICAL,
        standard_preg_cost_info.STANDARD_SPLIT_PAC,
        standard_preg_cost_info.STANDARD_UNSPLIT_TOTAL,
        standard_preg_cost_info.STANDARD_UNSPLIT_TYPICAL,
        standard_preg_cost_info.STANDARD_UNSPLIT_PAC
 FROM epbuilder.standard_preg_cost_info
 ORDER BY standard_preg_cost_info.ENCRYPT_RECIP_ID_MOM,
          standard_preg_cost_info.ENCRYPT_RECIP_ID_BABY,
          standard_preg_cost_info.PREG_master_episode_id,
          standard_preg_cost_info.trig_begin_date,
          standard_preg_cost_info.year,
          standard_preg_cost_info.SPLIT_TOTAL_PREGN_COST,
          standard_preg_cost_info.SPLIT_TYPICAL_PREGN_COST,
          standard_preg_cost_info.SPLIT_PAC_PREGN_COST,
          standard_preg_cost_info.UNSPLIT_TOTAL_PREGN_COST,
          standard_preg_cost_info.UNSPLIT_TYPICAL_PREGN_COST,
          standard_preg_cost_info.UNSPLIT_PAC_PREGN_COST
SEGMENTED BY hash(standard_preg_cost_info.PREG_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_episode_data2 /*+createtype(A)*/ 
(
 epi_id,
 member_id,
 epi_number,
 epi_name,
 female,
 age,
 rec_enr,
 eol_ind
)
AS
 SELECT ra_episode_data2.epi_id,
        ra_episode_data2.member_id,
        ra_episode_data2.epi_number,
        ra_episode_data2.epi_name,
        ra_episode_data2.female,
        ra_episode_data2.age,
        ra_episode_data2.rec_enr,
        ra_episode_data2.eol_ind
 FROM epbuilder.ra_episode_data2
 ORDER BY ra_episode_data2.epi_id,
          ra_episode_data2.member_id,
          ra_episode_data2.epi_number,
          ra_episode_data2.epi_name,
          ra_episode_data2.female,
          ra_episode_data2.age,
          ra_episode_data2.rec_enr,
          ra_episode_data2.eol_ind
SEGMENTED BY hash(ra_episode_data2.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.preg_cost_info_month /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 PREG_master_episode_id,
 trig_begin_date,
 year,
 SPLIT_TOTAL_PREGN_COST,
 SPLIT_TYPICAL_PREGN_COST,
 SPLIT_PAC_PREGN_COST,
 UNSPLIT_TOTAL_PREGN_COST,
 UNSPLIT_TYPICAL_PREGN_COST,
 UNSPLIT_PAC_PREGN_COST,
 date_of_firt_pregn_service,
 month_pregn_services_began
)
AS
 SELECT preg_cost_info_month.ENCRYPT_RECIP_ID_MOM,
        preg_cost_info_month.ENCRYPT_RECIP_ID_BABY,
        preg_cost_info_month.PREG_master_episode_id,
        preg_cost_info_month.trig_begin_date,
        preg_cost_info_month.year,
        preg_cost_info_month.SPLIT_TOTAL_PREGN_COST,
        preg_cost_info_month.SPLIT_TYPICAL_PREGN_COST,
        preg_cost_info_month.SPLIT_PAC_PREGN_COST,
        preg_cost_info_month.UNSPLIT_TOTAL_PREGN_COST,
        preg_cost_info_month.UNSPLIT_TYPICAL_PREGN_COST,
        preg_cost_info_month.UNSPLIT_PAC_PREGN_COST,
        preg_cost_info_month.date_of_firt_pregn_service,
        preg_cost_info_month.month_pregn_services_began
 FROM epbuilder.preg_cost_info_month
 ORDER BY preg_cost_info_month.ENCRYPT_RECIP_ID_MOM,
          preg_cost_info_month.ENCRYPT_RECIP_ID_BABY,
          preg_cost_info_month.PREG_master_episode_id,
          preg_cost_info_month.trig_begin_date,
          preg_cost_info_month.year,
          preg_cost_info_month.SPLIT_TOTAL_PREGN_COST,
          preg_cost_info_month.SPLIT_TYPICAL_PREGN_COST,
          preg_cost_info_month.SPLIT_PAC_PREGN_COST,
          preg_cost_info_month.UNSPLIT_TOTAL_PREGN_COST,
          preg_cost_info_month.UNSPLIT_TYPICAL_PREGN_COST,
          preg_cost_info_month.UNSPLIT_PAC_PREGN_COST
SEGMENTED BY hash(preg_cost_info_month.PREG_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.preg_del_overlap /*+createtype(A)*/ 
(
 master_claim_id,
 allowed_amt,
 count
)
AS
 SELECT preg_del_overlap.master_claim_id,
        preg_del_overlap.allowed_amt,
        preg_del_overlap.count
 FROM epbuilder.preg_del_overlap
 ORDER BY preg_del_overlap.master_claim_id,
          preg_del_overlap.allowed_amt
SEGMENTED BY hash(preg_del_overlap.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_episode_data3 /*+createtype(L)*/ 
(
 epi_id,
 member_id,
 epi_number,
 epi_name,
 female,
 age,
 rec_enr,
 eol_ind
)
AS
 SELECT ra_episode_data3.epi_id,
        ra_episode_data3.member_id,
        ra_episode_data3.epi_number,
        ra_episode_data3.epi_name,
        ra_episode_data3.female,
        ra_episode_data3.age,
        ra_episode_data3.rec_enr,
        ra_episode_data3.eol_ind
 FROM epbuilder.ra_episode_data3
 ORDER BY ra_episode_data3.epi_id,
          ra_episode_data3.member_id
SEGMENTED BY hash(ra_episode_data3.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.min_enroll_date /*+createtype(A)*/ 
(
 member_id,
 min
)
AS
 SELECT min_enroll_date.member_id,
        min_enroll_date.min
 FROM epbuilder.min_enroll_date
 ORDER BY min_enroll_date.member_id
SEGMENTED BY hash(min_enroll_date.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_episode_data4 /*+createtype(L)*/ 
(
 epi_id,
 member_id,
 epi_number,
 epi_name,
 female,
 age,
 rec_enr,
 eol_ind
)
AS
 SELECT ra_episode_data4.epi_id,
        ra_episode_data4.member_id,
        ra_episode_data4.epi_number,
        ra_episode_data4.epi_name,
        ra_episode_data4.female,
        ra_episode_data4.age,
        ra_episode_data4.rec_enr,
        ra_episode_data4.eol_ind
 FROM epbuilder.ra_episode_data4
 ORDER BY ra_episode_data4.epi_id,
          ra_episode_data4.member_id
SEGMENTED BY hash(ra_episode_data4.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PPS_TABLE_2015 /*+createtype(L)*/ 
(
 MEMBER_ID,
 PPS
)
AS
 SELECT PPS_TABLE_2015.MEMBER_ID,
        PPS_TABLE_2015.PPS
 FROM epbuilder.PPS_TABLE_2015
 ORDER BY PPS_TABLE_2015.MEMBER_ID,
          PPS_TABLE_2015.PPS
SEGMENTED BY hash(PPS_TABLE_2015.MEMBER_ID, PPS_TABLE_2015.PPS) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_enroll_date /*+createtype(L)*/ 
(
 member_id,
 begin_date
)
AS
 SELECT tmp_enroll_date.member_id,
        tmp_enroll_date.begin_date
 FROM epbuilder.tmp_enroll_date
 ORDER BY tmp_enroll_date.member_id,
          tmp_enroll_date.begin_date
SEGMENTED BY hash(tmp_enroll_date.begin_date, tmp_enroll_date.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.raw_member /*+createtype(L)*/ 
(
 NATIONAL_PLAN,
 INSURANCE_PRODUCT,
 YEAR,
 MONTH,
 MEMBER_ID,
 ENROLLMENT_START_DATE,
 ENROLLMENT_END_DATE,
 DUAL_ELIGIBILITY_FLAG,
 MBR_SEX_CD,
 BIRTH_YEAR,
 POSTAL_CD,
 RACE_CODE1,
 RACE_CODE2,
 HISPANIC_OR_LATINO_ETHNIC_CD,
 PRIMARY_INSURANCE_INDICATOR,
 PROV_ID,
 MC_BEG_DT,
 MEMBER_DEDUCTIBLE,
 DISABILITY_INDICATOR_FLAG,
 MBR_DEATH_DT,
 PCP_NPI,
 MDW_INS_DT,
 MDW_INS_ID,
 MDW_UPDT_DT,
 MDW_UPDT_ID
)
AS
 SELECT raw_member.NATIONAL_PLAN,
        raw_member.INSURANCE_PRODUCT,
        raw_member.YEAR,
        raw_member.MONTH,
        raw_member.MEMBER_ID,
        raw_member.ENROLLMENT_START_DATE,
        raw_member.ENROLLMENT_END_DATE,
        raw_member.DUAL_ELIGIBILITY_FLAG,
        raw_member.MBR_SEX_CD,
        raw_member.BIRTH_YEAR,
        raw_member.POSTAL_CD,
        raw_member.RACE_CODE1,
        raw_member.RACE_CODE2,
        raw_member.HISPANIC_OR_LATINO_ETHNIC_CD,
        raw_member.PRIMARY_INSURANCE_INDICATOR,
        raw_member.PROV_ID,
        raw_member.MC_BEG_DT,
        raw_member.MEMBER_DEDUCTIBLE,
        raw_member.DISABILITY_INDICATOR_FLAG,
        raw_member.MBR_DEATH_DT,
        raw_member.PCP_NPI,
        raw_member.MDW_INS_DT,
        raw_member.MDW_INS_ID,
        raw_member.MDW_UPDT_DT,
        raw_member.MDW_UPDT_ID
 FROM epbuilder.raw_member
 ORDER BY raw_member.NATIONAL_PLAN,
          raw_member.INSURANCE_PRODUCT,
          raw_member.YEAR,
          raw_member.MONTH,
          raw_member.MEMBER_ID,
          raw_member.ENROLLMENT_START_DATE,
          raw_member.ENROLLMENT_END_DATE,
          raw_member.DUAL_ELIGIBILITY_FLAG,
          raw_member.MBR_SEX_CD,
          raw_member.BIRTH_YEAR,
          raw_member.POSTAL_CD,
          raw_member.RACE_CODE1,
          raw_member.RACE_CODE2,
          raw_member.HISPANIC_OR_LATINO_ETHNIC_CD,
          raw_member.PRIMARY_INSURANCE_INDICATOR,
          raw_member.PROV_ID,
          raw_member.MC_BEG_DT,
          raw_member.MEMBER_DEDUCTIBLE,
          raw_member.DISABILITY_INDICATOR_FLAG,
          raw_member.MBR_DEATH_DT,
          raw_member.PCP_NPI,
          raw_member.MDW_INS_DT,
          raw_member.MDW_INS_ID,
          raw_member.MDW_UPDT_DT,
          raw_member.MDW_UPDT_ID
SEGMENTED BY hash(raw_member.NATIONAL_PLAN, raw_member.INSURANCE_PRODUCT, raw_member.YEAR, raw_member.MONTH, raw_member.MEMBER_ID, raw_member.ENROLLMENT_START_DATE, raw_member.ENROLLMENT_END_DATE, raw_member.DUAL_ELIGIBILITY_FLAG, raw_member.MBR_SEX_CD, raw_member.BIRTH_YEAR, raw_member.POSTAL_CD, raw_member.RACE_CODE1, raw_member.RACE_CODE2, raw_member.HISPANIC_OR_LATINO_ETHNIC_CD, raw_member.PRIMARY_INSURANCE_INDICATOR, raw_member.PROV_ID, raw_member.MC_BEG_DT, raw_member.MEMBER_DEDUCTIBLE, raw_member.DISABILITY_INDICATOR_FLAG, raw_member.MBR_DEATH_DT, raw_member.PCP_NPI, raw_member.MDW_INS_DT, raw_member.MDW_INS_ID, raw_member.MDW_UPDT_DT, raw_member.MDW_UPDT_ID) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_mcregion_nodup /*+createtype(A)*/ 
(
 member_id,
 mcregion
)
AS
 SELECT member_mcregion_nodup.member_id,
        member_mcregion_nodup.mcregion
 FROM epbuilder.member_mcregion_nodup
 ORDER BY member_mcregion_nodup.member_id,
          member_mcregion_nodup.mcregion
SEGMENTED BY hash(member_mcregion_nodup.member_id, member_mcregion_nodup.mcregion) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_episode_data_region /*+createtype(L)*/ 
(
 epi_id,
 member_id,
 epi_number,
 epi_name,
 female,
 age,
 rec_enr,
 eol_ind,
 Utica_Adirondack,
 Central,
 New_York_City,
 Mid_Hudson,
 Finger_Lakes,
 Western,
 Northeast,
 Northern_Metro,
 Long_Island
)
AS
 SELECT ra_episode_data_region.epi_id,
        ra_episode_data_region.member_id,
        ra_episode_data_region.epi_number,
        ra_episode_data_region.epi_name,
        ra_episode_data_region.female,
        ra_episode_data_region.age,
        ra_episode_data_region.rec_enr,
        ra_episode_data_region.eol_ind,
        ra_episode_data_region.Utica_Adirondack,
        ra_episode_data_region.Central,
        ra_episode_data_region.New_York_City,
        ra_episode_data_region.Mid_Hudson,
        ra_episode_data_region.Finger_Lakes,
        ra_episode_data_region.Western,
        ra_episode_data_region.Northeast,
        ra_episode_data_region.Northern_Metro,
        ra_episode_data_region.Long_Island
 FROM epbuilder.ra_episode_data_region
 ORDER BY ra_episode_data_region.epi_id,
          ra_episode_data_region.member_id
SEGMENTED BY hash(ra_episode_data_region.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.bundle_category /*+createtype(A)*/ 
(
 episode_id,
 category
)
AS
 SELECT bundle_category.episode_id,
        bundle_category.category
 FROM epbuilder.bundle_category
 ORDER BY bundle_category.episode_id
SEGMENTED BY hash(bundle_category.episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_mcregion /*+createtype(A)*/ 
(
 member_id,
 zip_code,
 mcregion
)
AS
 SELECT member_mcregion.member_id,
        member_mcregion.zip_code,
        member_mcregion.mcregion
 FROM epbuilder.member_mcregion
 ORDER BY member_mcregion.member_id,
          member_mcregion.zip_code,
          member_mcregion.mcregion
SEGMENTED BY hash(member_mcregion.zip_code, member_mcregion.member_id, member_mcregion.mcregion) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.report_episode_detail_revised /*+createtype(L)*/ 
(
 Filter_ID,
 Member_ID,
 Master_Episode_ID,
 Episode_ID,
 Episode_Name,
 Episode_Description,
 Episode_Type,
 MDC,
 MDC_Description,
 Episode_Begin_Date,
 Episode_End_Date,
 Episode_Length,
 Level,
 Split_Total_Cost,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_80thPercentile_Cost,
 Split_Total_PAC_Cost,
 Split_Total_Typical_Cost,
 Split_Total_TypicalwPAC_Cost,
 Annualized_Split_Total_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_80thPercentile_Cost,
 Annualized_Split_Total_PAC_Cost,
 Annualized_Split_Total_Typical_Cost,
 Annualized_Split_Total_TypicalwPAC_Cost,
 Unsplit_Total_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Unsplit_Total_PAC_Cost,
 Unsplit_Total_Typical_Cost,
 Unsplit_Total_TypicalwPAC_Cost,
 Annualized_Unsplit_Total_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_Total_PAC_Cost,
 Annualized_Unsplit_Total_Typical_Cost,
 Annualized_Unsplit_Total_TypicalwPAC_Cost,
 Facility_ID,
 Physician_ID,
 Physician_Specialty,
 Split_Expected_Total_Cost,
 Split_Expected_Typical_IP_Cost,
 Split_Expected_Typical_Other_Cost,
 Split_Expected_PAC_Cost,
 Unsplit_Expected_Total_Cost,
 Unsplit_Expected_Typical_IP_Cost,
 Unsplit_Expected_Typical_Other_Cost,
 Unsplit_Expected_PAC_Cost,
 IP_PAC_Count,
 OP_PAC_Count,
 PB_PAC_Count,
 RX_PAC_Count
)
AS
 SELECT report_episode_detail_revised.Filter_ID,
        report_episode_detail_revised.Member_ID,
        report_episode_detail_revised.Master_Episode_ID,
        report_episode_detail_revised.Episode_ID,
        report_episode_detail_revised.Episode_Name,
        report_episode_detail_revised.Episode_Description,
        report_episode_detail_revised.Episode_Type,
        report_episode_detail_revised.MDC,
        report_episode_detail_revised.MDC_Description,
        report_episode_detail_revised.Episode_Begin_Date,
        report_episode_detail_revised.Episode_End_Date,
        report_episode_detail_revised.Episode_Length,
        report_episode_detail_revised.Level,
        report_episode_detail_revised.Split_Total_Cost,
        report_episode_detail_revised.Split_1stPercentile_Cost,
        report_episode_detail_revised.Split_99thPercentile_Cost,
        report_episode_detail_revised.Split_80thPercentile_Cost,
        report_episode_detail_revised.Split_Total_PAC_Cost,
        report_episode_detail_revised.Split_Total_Typical_Cost,
        report_episode_detail_revised.Split_Total_TypicalwPAC_Cost,
        report_episode_detail_revised.Annualized_Split_Total_Cost,
        report_episode_detail_revised.Annualized_Split_1stPercentile_Cost,
        report_episode_detail_revised.Annualized_Split_99thPercentile_Cost,
        report_episode_detail_revised.Annualized_Split_80thPercentile_Cost,
        report_episode_detail_revised.Annualized_Split_Total_PAC_Cost,
        report_episode_detail_revised.Annualized_Split_Total_Typical_Cost,
        report_episode_detail_revised.Annualized_Split_Total_TypicalwPAC_Cost,
        report_episode_detail_revised.Unsplit_Total_Cost,
        report_episode_detail_revised.Unsplit_1stPercentile_Cost,
        report_episode_detail_revised.Unsplit_99thPercentile_Cost,
        report_episode_detail_revised.Unsplit_Total_PAC_Cost,
        report_episode_detail_revised.Unsplit_Total_Typical_Cost,
        report_episode_detail_revised.Unsplit_Total_TypicalwPAC_Cost,
        report_episode_detail_revised.Annualized_Unsplit_Total_Cost,
        report_episode_detail_revised.Annualized_Unsplit_1stPercentile_Cost,
        report_episode_detail_revised.Annualized_Unsplit_99thPercentile_Cost,
        report_episode_detail_revised.Annualized_Unsplit_Total_PAC_Cost,
        report_episode_detail_revised.Annualized_Unsplit_Total_Typical_Cost,
        report_episode_detail_revised.Annualized_Unsplit_Total_TypicalwPAC_Cost,
        report_episode_detail_revised.Facility_ID,
        report_episode_detail_revised.Physician_ID,
        report_episode_detail_revised.Physician_Specialty,
        report_episode_detail_revised.Split_Expected_Total_Cost,
        report_episode_detail_revised.Split_Expected_Typical_IP_Cost,
        report_episode_detail_revised.Split_Expected_Typical_Other_Cost,
        report_episode_detail_revised.Split_Expected_PAC_Cost,
        report_episode_detail_revised.Unsplit_Expected_Total_Cost,
        report_episode_detail_revised.Unsplit_Expected_Typical_IP_Cost,
        report_episode_detail_revised.Unsplit_Expected_Typical_Other_Cost,
        report_episode_detail_revised.Unsplit_Expected_PAC_Cost,
        report_episode_detail_revised.IP_PAC_Count,
        report_episode_detail_revised.OP_PAC_Count,
        report_episode_detail_revised.PB_PAC_Count,
        report_episode_detail_revised.RX_PAC_Count
 FROM epbuilder.report_episode_detail_revised
 ORDER BY report_episode_detail_revised.Filter_ID,
          report_episode_detail_revised.Member_ID,
          report_episode_detail_revised.Master_Episode_ID,
          report_episode_detail_revised.Episode_ID,
          report_episode_detail_revised.Episode_Name,
          report_episode_detail_revised.Episode_Description,
          report_episode_detail_revised.Episode_Type,
          report_episode_detail_revised.MDC,
          report_episode_detail_revised.MDC_Description,
          report_episode_detail_revised.Episode_Begin_Date,
          report_episode_detail_revised.Episode_End_Date,
          report_episode_detail_revised.Episode_Length,
          report_episode_detail_revised.Level,
          report_episode_detail_revised.Split_Total_Cost,
          report_episode_detail_revised.Split_1stPercentile_Cost,
          report_episode_detail_revised.Split_99thPercentile_Cost,
          report_episode_detail_revised.Split_80thPercentile_Cost,
          report_episode_detail_revised.Split_Total_PAC_Cost,
          report_episode_detail_revised.Split_Total_Typical_Cost,
          report_episode_detail_revised.Split_Total_TypicalwPAC_Cost,
          report_episode_detail_revised.Annualized_Split_Total_Cost,
          report_episode_detail_revised.Annualized_Split_1stPercentile_Cost,
          report_episode_detail_revised.Annualized_Split_99thPercentile_Cost,
          report_episode_detail_revised.Annualized_Split_80thPercentile_Cost,
          report_episode_detail_revised.Annualized_Split_Total_PAC_Cost,
          report_episode_detail_revised.Annualized_Split_Total_Typical_Cost,
          report_episode_detail_revised.Annualized_Split_Total_TypicalwPAC_Cost,
          report_episode_detail_revised.Unsplit_Total_Cost,
          report_episode_detail_revised.Unsplit_1stPercentile_Cost,
          report_episode_detail_revised.Unsplit_99thPercentile_Cost,
          report_episode_detail_revised.Unsplit_Total_PAC_Cost,
          report_episode_detail_revised.Unsplit_Total_Typical_Cost,
          report_episode_detail_revised.Unsplit_Total_TypicalwPAC_Cost,
          report_episode_detail_revised.Annualized_Unsplit_Total_Cost,
          report_episode_detail_revised.Annualized_Unsplit_1stPercentile_Cost,
          report_episode_detail_revised.Annualized_Unsplit_99thPercentile_Cost,
          report_episode_detail_revised.Annualized_Unsplit_Total_PAC_Cost,
          report_episode_detail_revised.Annualized_Unsplit_Total_Typical_Cost,
          report_episode_detail_revised.Annualized_Unsplit_Total_TypicalwPAC_Cost,
          report_episode_detail_revised.Facility_ID,
          report_episode_detail_revised.Physician_ID,
          report_episode_detail_revised.Physician_Specialty,
          report_episode_detail_revised.Split_Expected_Total_Cost,
          report_episode_detail_revised.Split_Expected_Typical_IP_Cost,
          report_episode_detail_revised.Split_Expected_Typical_Other_Cost,
          report_episode_detail_revised.Split_Expected_PAC_Cost,
          report_episode_detail_revised.Unsplit_Expected_Total_Cost,
          report_episode_detail_revised.Unsplit_Expected_Typical_IP_Cost,
          report_episode_detail_revised.Unsplit_Expected_Typical_Other_Cost,
          report_episode_detail_revised.Unsplit_Expected_PAC_Cost,
          report_episode_detail_revised.IP_PAC_Count,
          report_episode_detail_revised.OP_PAC_Count,
          report_episode_detail_revised.PB_PAC_Count,
          report_episode_detail_revised.RX_PAC_Count
SEGMENTED BY hash(report_episode_detail_revised.Filter_ID, report_episode_detail_revised.Episode_ID, report_episode_detail_revised.Episode_Name, report_episode_detail_revised.MDC, report_episode_detail_revised.Episode_Length, report_episode_detail_revised.Level, report_episode_detail_revised.Split_Total_Cost, report_episode_detail_revised.Split_1stPercentile_Cost, report_episode_detail_revised.Split_99thPercentile_Cost, report_episode_detail_revised.Split_80thPercentile_Cost, report_episode_detail_revised.Split_Total_PAC_Cost, report_episode_detail_revised.Split_Total_Typical_Cost, report_episode_detail_revised.Split_Total_TypicalwPAC_Cost, report_episode_detail_revised.Annualized_Split_Total_Cost, report_episode_detail_revised.Annualized_Split_1stPercentile_Cost, report_episode_detail_revised.Annualized_Split_99thPercentile_Cost, report_episode_detail_revised.Annualized_Split_80thPercentile_Cost, report_episode_detail_revised.Annualized_Split_Total_PAC_Cost, report_episode_detail_revised.Annualized_Split_Total_Typical_Cost, report_episode_detail_revised.Annualized_Split_Total_TypicalwPAC_Cost, report_episode_detail_revised.Unsplit_Total_Cost, report_episode_detail_revised.Unsplit_1stPercentile_Cost, report_episode_detail_revised.Unsplit_99thPercentile_Cost, report_episode_detail_revised.Unsplit_Total_PAC_Cost, report_episode_detail_revised.Unsplit_Total_Typical_Cost, report_episode_detail_revised.Unsplit_Total_TypicalwPAC_Cost, report_episode_detail_revised.Annualized_Unsplit_Total_Cost, report_episode_detail_revised.Annualized_Unsplit_1stPercentile_Cost, report_episode_detail_revised.Annualized_Unsplit_99thPercentile_Cost, report_episode_detail_revised.Annualized_Unsplit_Total_PAC_Cost, report_episode_detail_revised.Annualized_Unsplit_Total_Typical_Cost, report_episode_detail_revised.Annualized_Unsplit_Total_TypicalwPAC_Cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.exp_act /*+createtype(A)*/ 
(
 epi_id,
 epi_number,
 master_episode_id,
 cost,
 total_exp_cost_ra_l5
)
AS
 SELECT exp_act.epi_id,
        exp_act.epi_number,
        exp_act.master_episode_id,
        exp_act.cost,
        exp_act.total_exp_cost_ra_l5
 FROM epbuilder.exp_act
 ORDER BY exp_act.epi_id,
          exp_act.epi_number,
          exp_act.master_episode_id,
          exp_act.cost,
          exp_act.total_exp_cost_ra_l5
SEGMENTED BY hash(exp_act.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.percentiles_revised /*+createtype(L)*/ 
(
 Filter_ID,
 Episode_ID,
 Level,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_80thPercentile_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_80thPercentile_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost
)
AS
 SELECT percentiles_revised.Filter_ID,
        percentiles_revised.Episode_ID,
        percentiles_revised.Level,
        percentiles_revised.Split_1stPercentile_Cost,
        percentiles_revised.Split_99thPercentile_Cost,
        percentiles_revised.Split_80thPercentile_Cost,
        percentiles_revised.Annualized_Split_1stPercentile_Cost,
        percentiles_revised.Annualized_Split_99thPercentile_Cost,
        percentiles_revised.Annualized_Split_80thPercentile_Cost,
        percentiles_revised.Unsplit_1stPercentile_Cost,
        percentiles_revised.Unsplit_99thPercentile_Cost,
        percentiles_revised.Annualized_Unsplit_1stPercentile_Cost,
        percentiles_revised.Annualized_Unsplit_99thPercentile_Cost
 FROM epbuilder.percentiles_revised
 ORDER BY percentiles_revised.Filter_ID,
          percentiles_revised.Episode_ID,
          percentiles_revised.Level,
          percentiles_revised.Split_1stPercentile_Cost,
          percentiles_revised.Split_99thPercentile_Cost,
          percentiles_revised.Split_80thPercentile_Cost
SEGMENTED BY hash(percentiles_revised.Filter_ID, percentiles_revised.Episode_ID, percentiles_revised.Level, percentiles_revised.Split_1stPercentile_Cost, percentiles_revised.Split_99thPercentile_Cost, percentiles_revised.Split_80thPercentile_Cost, percentiles_revised.Annualized_Split_1stPercentile_Cost, percentiles_revised.Annualized_Split_99thPercentile_Cost, percentiles_revised.Annualized_Split_80thPercentile_Cost, percentiles_revised.Unsplit_1stPercentile_Cost, percentiles_revised.Unsplit_99thPercentile_Cost, percentiles_revised.Annualized_Unsplit_1stPercentile_Cost, percentiles_revised.Annualized_Unsplit_99thPercentile_Cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.pregn_with_legit_child /*+createtype(A)*/ 
(
 parent_master_episode_id
)
AS
 SELECT pregn_with_legit_child.parent_master_episode_id
 FROM epbuilder.pregn_with_legit_child
 ORDER BY pregn_with_legit_child.parent_master_episode_id
SEGMENTED BY hash(pregn_with_legit_child.parent_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.maternity_base_episode /*+createtype(L)*/ 
(
 mtrnty_meid,
 member_id,
 pregn_meid,
 nborn_meid,
 pregn_year,
 nborn_year,
 del_meid
)
AS
 SELECT maternity_base_episode.mtrnty_meid,
        maternity_base_episode.member_id,
        maternity_base_episode.pregn_meid,
        maternity_base_episode.nborn_meid,
        maternity_base_episode.pregn_year,
        maternity_base_episode.nborn_year,
        maternity_base_episode.del_meid
 FROM epbuilder.maternity_base_episode
 ORDER BY maternity_base_episode.mtrnty_meid,
          maternity_base_episode.member_id,
          maternity_base_episode.pregn_meid,
          maternity_base_episode.nborn_meid,
          maternity_base_episode.pregn_year,
          maternity_base_episode.nborn_year
SEGMENTED BY hash(maternity_base_episode.pregn_year, maternity_base_episode.nborn_year, maternity_base_episode.member_id, maternity_base_episode.mtrnty_meid, maternity_base_episode.pregn_meid, maternity_base_episode.nborn_meid) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.DE_status /*+createtype(A)*/ 
(
 MEMBER_ID,
 DUAL_ELIGIBILITY_FLAG,
 AGE
)
AS
 SELECT DE_status.MEMBER_ID,
        DE_status.DUAL_ELIGIBILITY_FLAG,
        DE_status.AGE
 FROM epbuilder.DE_status
 ORDER BY DE_status.MEMBER_ID,
          DE_status.DUAL_ELIGIBILITY_FLAG,
          DE_status.AGE
SEGMENTED BY hash(DE_status.MEMBER_ID, DE_status.DUAL_ELIGIBILITY_FLAG, DE_status.AGE) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep1_m /*+createtype(A)*/ 
(
 member_id,
 pac_count,
 year
)
AS
 SELECT prep1_m.member_id,
        prep1_m.pac_count,
        prep1_m.year
 FROM epbuilder.prep1_m
 ORDER BY prep1_m.member_id
SEGMENTED BY hash(prep1_m.pac_count, prep1_m.year, prep1_m.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep3_m /*+createtype(A)*/ 
(
 member_id,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_count_chronic,
 co_count_all,
 year
)
AS
 SELECT prep3_m.member_id,
        prep3_m.co_occurence_count_ASTHMA,
        prep3_m.co_occurence_count_ARRBLK,
        prep3_m.co_occurence_count_HF,
        prep3_m.co_occurence_count_COPD,
        prep3_m.co_occurence_count_CAD,
        prep3_m.co_occurence_count_ULCLTS,
        prep3_m.co_occurence_count_BIPLR,
        prep3_m.co_occurence_count_GERD,
        prep3_m.co_occurence_count_HTN,
        prep3_m.co_occurence_count_GLCOMA,
        prep3_m.co_occurence_count_LBP,
        prep3_m.co_occurence_count_CROHNS,
        prep3_m.co_occurence_count_DIAB,
        prep3_m.co_occurence_count_DEPRSN,
        prep3_m.co_occurence_count_OSTEOA,
        prep3_m.co_occurence_count_RHNTS,
        prep3_m.co_occurence_count_DIVERT,
        prep3_m.co_occurence_count_DEPANX,
        prep3_m.co_occurence_count_PTSD,
        prep3_m.co_occurence_count_SCHIZO,
        prep3_m.co_occurence_count_SUDS,
        prep3_m.co_count_chronic,
        prep3_m.co_count_all,
        prep3_m.year
 FROM epbuilder.prep3_m
 ORDER BY prep3_m.member_id,
          prep3_m.year
SEGMENTED BY hash(prep3_m.member_id, prep3_m.year) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep4_m /*+createtype(A)*/ 
(
 member_id,
 assigned_cost,
 pac_cost,
 typical_cost,
 assigned_pb_cost,
 assigned_op_cost,
 assigned_ip_cost,
 assigned_rx_cost,
 year
)
AS
 SELECT prep4_m.member_id,
        prep4_m.assigned_cost,
        prep4_m.pac_cost,
        prep4_m.typical_cost,
        prep4_m.assigned_pb_cost,
        prep4_m.assigned_op_cost,
        prep4_m.assigned_ip_cost,
        prep4_m.assigned_rx_cost,
        prep4_m.year
 FROM epbuilder.prep4_m
 ORDER BY prep4_m.member_id,
          prep4_m.year
SEGMENTED BY hash(prep4_m.member_id, prep4_m.year) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep4_2_m /*+createtype(A)*/ 
(
 member_id,
 assigned_cost_unfiltered,
 pac_cost,
 typical_cost_unfiltered,
 assigned_pb_cost_unfiltered,
 assigned_op_cost_unfiltered,
 assigned_ip_cost_unfiltered,
 assigned_rx_cost_unfiltered,
 year
)
AS
 SELECT prep4_2_m.member_id,
        prep4_2_m.assigned_cost_unfiltered,
        prep4_2_m.pac_cost,
        prep4_2_m.typical_cost_unfiltered,
        prep4_2_m.assigned_pb_cost_unfiltered,
        prep4_2_m.assigned_op_cost_unfiltered,
        prep4_2_m.assigned_ip_cost_unfiltered,
        prep4_2_m.assigned_rx_cost_unfiltered,
        prep4_2_m.year
 FROM epbuilder.prep4_2_m
 ORDER BY prep4_2_m.member_id,
          prep4_2_m.year
SEGMENTED BY hash(prep4_2_m.member_id, prep4_2_m.year) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep5_m /*+createtype(A)*/ 
(
 member_id,
 episode_count,
 episode_count_unfiltered,
 year
)
AS
 SELECT prep5_m.member_id,
        prep5_m.episode_count,
        prep5_m.episode_count_unfiltered,
        prep5_m.year
 FROM epbuilder.prep5_m
 ORDER BY prep5_m.member_id,
          prep5_m.year
SEGMENTED BY hash(prep5_m.member_id, prep5_m.year) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep2_m /*+createtype(A)*/ 
(
 member_id,
 pb_cost,
 op_cost,
 ip_cost,
 rx_cost,
 year
)
AS
 SELECT prep2_m.member_id,
        prep2_m.pb_cost,
        prep2_m.op_cost,
        prep2_m.ip_cost,
        prep2_m.rx_cost,
        prep2_m.year
 FROM epbuilder.prep2_m
 ORDER BY prep2_m.member_id,
          prep2_m.year
SEGMENTED BY hash(prep2_m.member_id, prep2_m.year) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.visual_analysis_table_x2 /*+createtype(A)*/ 
(
 analysis_type,
 id,
 episode_id,
 episode_name,
 episode_description,
 episode_type,
 episode_category,
 member_id,
 member_age,
 cms_age_group,
 gender,
 member_zipcode,
 member_county,
 member_population,
 total_cost,
 expected_cost,
 PAC_cost,
 typical_cost,
 expected_pac_cost,
 expected_typical_cost,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 assigned_cost,
 assigned_ip_cost,
 assigned_op_cost,
 assigned_pb_cost,
 assigned_rx_cost,
 assigned_cost_unfiltered,
 assigned_ip_cost_unfiltered,
 assigned_op_cost_unfiltered,
 assigned_pb_cost_unfiltered,
 assigned_rx_cost_unfiltered,
 pps,
 provider_id,
 provider_name,
 provider_zipcode,
 provider_type,
 mco,
 hh,
 pcp,
 vbp_attrib_provider,
 vbp_attrib_provider_zipcode,
 vbp_contractor,
 year,
 utilization,
 ppr,
 ppv,
 exclusive,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_count_chronic,
 co_count_all,
 episode_count,
 episode_count_unfiltered,
 episode_level
)
AS
 SELECT visual_analysis_table_x2.analysis_type,
        visual_analysis_table_x2.id,
        visual_analysis_table_x2.episode_id,
        visual_analysis_table_x2.episode_name,
        visual_analysis_table_x2.episode_description,
        visual_analysis_table_x2.episode_type,
        visual_analysis_table_x2.episode_category,
        visual_analysis_table_x2.member_id,
        visual_analysis_table_x2.member_age,
        visual_analysis_table_x2.cms_age_group,
        visual_analysis_table_x2.gender,
        visual_analysis_table_x2.member_zipcode,
        visual_analysis_table_x2.member_county,
        visual_analysis_table_x2.member_population,
        visual_analysis_table_x2.total_cost,
        visual_analysis_table_x2.expected_cost,
        visual_analysis_table_x2.PAC_cost,
        visual_analysis_table_x2.typical_cost,
        visual_analysis_table_x2.expected_pac_cost,
        visual_analysis_table_x2.expected_typical_cost,
        visual_analysis_table_x2.ip_cost,
        visual_analysis_table_x2.op_cost,
        visual_analysis_table_x2.pb_cost,
        visual_analysis_table_x2.rx_cost,
        visual_analysis_table_x2.assigned_cost,
        visual_analysis_table_x2.assigned_ip_cost,
        visual_analysis_table_x2.assigned_op_cost,
        visual_analysis_table_x2.assigned_pb_cost,
        visual_analysis_table_x2.assigned_rx_cost,
        visual_analysis_table_x2.assigned_cost_unfiltered,
        visual_analysis_table_x2.assigned_ip_cost_unfiltered,
        visual_analysis_table_x2.assigned_op_cost_unfiltered,
        visual_analysis_table_x2.assigned_pb_cost_unfiltered,
        visual_analysis_table_x2.assigned_rx_cost_unfiltered,
        visual_analysis_table_x2.pps,
        visual_analysis_table_x2.provider_id,
        visual_analysis_table_x2.provider_name,
        visual_analysis_table_x2.provider_zipcode,
        visual_analysis_table_x2.provider_type,
        visual_analysis_table_x2.mco,
        visual_analysis_table_x2.hh,
        visual_analysis_table_x2.pcp,
        visual_analysis_table_x2.vbp_attrib_provider,
        visual_analysis_table_x2.vbp_attrib_provider_zipcode,
        visual_analysis_table_x2.vbp_contractor,
        visual_analysis_table_x2.year,
        visual_analysis_table_x2.utilization,
        visual_analysis_table_x2.ppr,
        visual_analysis_table_x2.ppv,
        visual_analysis_table_x2.exclusive,
        visual_analysis_table_x2.co_occurence_count_ASTHMA,
        visual_analysis_table_x2.co_occurence_count_ARRBLK,
        visual_analysis_table_x2.co_occurence_count_HF,
        visual_analysis_table_x2.co_occurence_count_COPD,
        visual_analysis_table_x2.co_occurence_count_CAD,
        visual_analysis_table_x2.co_occurence_count_ULCLTS,
        visual_analysis_table_x2.co_occurence_count_BIPLR,
        visual_analysis_table_x2.co_occurence_count_GERD,
        visual_analysis_table_x2.co_occurence_count_HTN,
        visual_analysis_table_x2.co_occurence_count_GLCOMA,
        visual_analysis_table_x2.co_occurence_count_LBP,
        visual_analysis_table_x2.co_occurence_count_CROHNS,
        visual_analysis_table_x2.co_occurence_count_DIAB,
        visual_analysis_table_x2.co_occurence_count_DEPRSN,
        visual_analysis_table_x2.co_occurence_count_OSTEOA,
        visual_analysis_table_x2.co_occurence_count_RHNTS,
        visual_analysis_table_x2.co_occurence_count_DIVERT,
        visual_analysis_table_x2.co_occurence_count_DEPANX,
        visual_analysis_table_x2.co_occurence_count_PTSD,
        visual_analysis_table_x2.co_occurence_count_SCHIZO,
        visual_analysis_table_x2.co_occurence_count_SUDS,
        visual_analysis_table_x2.co_count_chronic,
        visual_analysis_table_x2.co_count_all,
        visual_analysis_table_x2.episode_count,
        visual_analysis_table_x2.episode_count_unfiltered,
        visual_analysis_table_x2.episode_level
 FROM epbuilder.visual_analysis_table_x2
 ORDER BY visual_analysis_table_x2.id
SEGMENTED BY hash(visual_analysis_table_x2.episode_id, visual_analysis_table_x2.episode_name, visual_analysis_table_x2.episode_description, visual_analysis_table_x2.episode_type, visual_analysis_table_x2.episode_category, visual_analysis_table_x2.member_age, visual_analysis_table_x2.total_cost, visual_analysis_table_x2.expected_cost, visual_analysis_table_x2.expected_pac_cost, visual_analysis_table_x2.expected_typical_cost, visual_analysis_table_x2.provider_id, visual_analysis_table_x2.provider_name, visual_analysis_table_x2.provider_zipcode, visual_analysis_table_x2.provider_type, visual_analysis_table_x2.vbp_attrib_provider, visual_analysis_table_x2.vbp_attrib_provider_zipcode, visual_analysis_table_x2.vbp_contractor, visual_analysis_table_x2.utilization, visual_analysis_table_x2.ppr, visual_analysis_table_x2.ppv, visual_analysis_table_x2.exclusive, visual_analysis_table_x2.co_occurence_count_ASTHMA, visual_analysis_table_x2.co_occurence_count_ARRBLK, visual_analysis_table_x2.co_occurence_count_HF, visual_analysis_table_x2.co_occurence_count_COPD, visual_analysis_table_x2.co_occurence_count_CAD, visual_analysis_table_x2.co_occurence_count_ULCLTS, visual_analysis_table_x2.co_occurence_count_BIPLR, visual_analysis_table_x2.co_occurence_count_GERD, visual_analysis_table_x2.co_occurence_count_HTN, visual_analysis_table_x2.co_occurence_count_GLCOMA, visual_analysis_table_x2.co_occurence_count_LBP) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.vbp_ppe_ppr_nodup /*+createtype(A)*/ 
(
 claim_id,
 claim_trans_id,
 dsch_dt,
 claim_type_code,
 preventable_group,
 preventable_group_desc,
 ppr_type_code,
 ppr_type_code_desc,
 mdw_insert_dt,
 mdw_update_dt
)
AS
 SELECT vbp_ppe_ppr_nodup.claim_id,
        vbp_ppe_ppr_nodup.claim_trans_id,
        vbp_ppe_ppr_nodup.dsch_dt,
        vbp_ppe_ppr_nodup.claim_type_code,
        vbp_ppe_ppr_nodup.preventable_group,
        vbp_ppe_ppr_nodup.preventable_group_desc,
        vbp_ppe_ppr_nodup.ppr_type_code,
        vbp_ppe_ppr_nodup.ppr_type_code_desc,
        vbp_ppe_ppr_nodup.mdw_insert_dt,
        vbp_ppe_ppr_nodup.mdw_update_dt
 FROM epbuilder.vbp_ppe_ppr_nodup
 ORDER BY vbp_ppe_ppr_nodup.claim_id,
          vbp_ppe_ppr_nodup.claim_trans_id,
          vbp_ppe_ppr_nodup.dsch_dt,
          vbp_ppe_ppr_nodup.claim_type_code,
          vbp_ppe_ppr_nodup.preventable_group,
          vbp_ppe_ppr_nodup.preventable_group_desc,
          vbp_ppe_ppr_nodup.ppr_type_code,
          vbp_ppe_ppr_nodup.ppr_type_code_desc,
          vbp_ppe_ppr_nodup.mdw_insert_dt,
          vbp_ppe_ppr_nodup.mdw_update_dt
SEGMENTED BY hash(vbp_ppe_ppr_nodup.dsch_dt, vbp_ppe_ppr_nodup.claim_type_code, vbp_ppe_ppr_nodup.preventable_group, vbp_ppe_ppr_nodup.ppr_type_code, vbp_ppe_ppr_nodup.mdw_insert_dt, vbp_ppe_ppr_nodup.mdw_update_dt, vbp_ppe_ppr_nodup.claim_trans_id, vbp_ppe_ppr_nodup.claim_id, vbp_ppe_ppr_nodup.preventable_group_desc, vbp_ppe_ppr_nodup.ppr_type_code_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep2 /*+createtype(A)*/ 
(
 master_episode_id,
 pb_cost,
 op_cost,
 ip_cost,
 rx_cost
)
AS
 SELECT prep2.master_episode_id,
        prep2.pb_cost,
        prep2.op_cost,
        prep2.ip_cost,
        prep2.rx_cost
 FROM epbuilder.prep2
 ORDER BY prep2.master_episode_id
SEGMENTED BY hash(prep2.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep3 /*+createtype(A)*/ 
(
 master_episode_id,
 level,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_count_chronic,
 co_count_all,
 year
)
AS
 SELECT prep3.master_episode_id,
        prep3.level,
        prep3.co_occurence_count_ASTHMA,
        prep3.co_occurence_count_ARRBLK,
        prep3.co_occurence_count_HF,
        prep3.co_occurence_count_COPD,
        prep3.co_occurence_count_CAD,
        prep3.co_occurence_count_ULCLTS,
        prep3.co_occurence_count_BIPLR,
        prep3.co_occurence_count_GERD,
        prep3.co_occurence_count_HTN,
        prep3.co_occurence_count_GLCOMA,
        prep3.co_occurence_count_LBP,
        prep3.co_occurence_count_CROHNS,
        prep3.co_occurence_count_DIAB,
        prep3.co_occurence_count_DEPRSN,
        prep3.co_occurence_count_OSTEOA,
        prep3.co_occurence_count_RHNTS,
        prep3.co_occurence_count_DIVERT,
        prep3.co_occurence_count_DEPANX,
        prep3.co_occurence_count_PTSD,
        prep3.co_occurence_count_SCHIZO,
        prep3.co_occurence_count_SUDS,
        prep3.co_count_chronic,
        prep3.co_count_all,
        prep3.year
 FROM epbuilder.prep3
 ORDER BY prep3.master_episode_id,
          prep3.level
SEGMENTED BY hash(prep3.master_episode_id, prep3.level) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_prep /*+createtype(A)*/ 
(
 member_id,
 birth_year,
 gender_code,
 zip_code,
 county,
 sub_population,
 pps,
 hh,
 pcp,
 mco
)
AS
 SELECT member_prep.member_id,
        member_prep.birth_year,
        member_prep.gender_code,
        member_prep.zip_code,
        member_prep.county,
        member_prep.sub_population,
        member_prep.pps,
        member_prep.hh,
        member_prep.pcp,
        member_prep.mco
 FROM epbuilder.member_prep
 ORDER BY member_prep.member_id,
          member_prep.birth_year,
          member_prep.gender_code,
          member_prep.zip_code,
          member_prep.county,
          member_prep.sub_population,
          member_prep.pps,
          member_prep.hh,
          member_prep.pcp,
          member_prep.mco
SEGMENTED BY hash(member_prep.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.prep1 /*+createtype(A)*/ 
(
 master_episode_id,
 pac_count,
 expected_pac_count
)
AS
 SELECT prep1.master_episode_id,
        prep1.pac_count,
        prep1.expected_pac_count
 FROM epbuilder.prep1
 ORDER BY prep1.master_episode_id,
          prep1.pac_count,
          prep1.expected_pac_count
SEGMENTED BY hash(prep1.pac_count, prep1.expected_pac_count, prep1.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.report_episode_detail_test /*+createtype(L)*/ 
(
 Filter_ID,
 Member_ID,
 Master_Episode_ID,
 Episode_ID,
 Episode_Name,
 Episode_Description,
 Episode_Type,
 MDC,
 MDC_Description,
 Episode_Begin_Date,
 Episode_End_Date,
 Episode_Length,
 Level,
 Split_Total_Cost,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_80thPercentile_Cost,
 Split_Total_PAC_Cost,
 Split_Total_Typical_Cost,
 Split_Total_TypicalwPAC_Cost,
 Annualized_Split_Total_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_80thPercentile_Cost,
 Annualized_Split_Total_PAC_Cost,
 Annualized_Split_Total_Typical_Cost,
 Annualized_Split_Total_TypicalwPAC_Cost,
 Unsplit_Total_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Unsplit_Total_PAC_Cost,
 Unsplit_Total_Typical_Cost,
 Unsplit_Total_TypicalwPAC_Cost,
 Annualized_Unsplit_Total_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_Total_PAC_Cost,
 Annualized_Unsplit_Total_Typical_Cost,
 Annualized_Unsplit_Total_TypicalwPAC_Cost,
 Facility_ID,
 Physician_ID,
 Physician_Specialty,
 Split_Expected_Total_Cost,
 Split_Expected_Typical_IP_Cost,
 Split_Expected_Typical_Other_Cost,
 Split_Expected_PAC_Cost,
 Unsplit_Expected_Total_Cost,
 Unsplit_Expected_Typical_IP_Cost,
 Unsplit_Expected_Typical_Other_Cost,
 Unsplit_Expected_PAC_Cost,
 IP_PAC_Count,
 OP_PAC_Count,
 PB_PAC_Count,
 RX_PAC_Count,
 year,
 enrolled_month,
 co_occurence_count_DEPANX,
 co_occurence_count_DIVERT,
 co_occurence_count_RHNTS,
 co_occurence_count_OSTEOA,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_GLCOMA,
 co_occurence_count_HTN,
 co_occurence_count_GERD,
 co_occurence_count_BIPLR,
 co_occurence_count_ULCLTS,
 co_occurence_count_CAD,
 co_occurence_count_COPD,
 co_occurence_count_HF,
 co_occurence_count_ARRBLK,
 co_occurence_count_ASTHMA,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_count_chronic,
 co_count_all,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 END_OF_STUDY,
 Facility_ID_provider_name,
 Facility_ID_provider_zipcode,
 Facility_ID_provider_type,
 Physician_ID_provider_name,
 Physician_ID_provider_zipcode,
 Physician_ID_provider_type,
 state_wide_avg_split_exp_cost,
 state_wide_avg_unsplit_exp_cost,
 state_wide_avg_split_total_cost,
 state_wide_avg_unsplit_total_cost,
 split_state_wide_pac_percent,
 unsplit_state_wide_pac_percent
)
AS
 SELECT report_episode_detail_test.Filter_ID,
        report_episode_detail_test.Member_ID,
        report_episode_detail_test.Master_Episode_ID,
        report_episode_detail_test.Episode_ID,
        report_episode_detail_test.Episode_Name,
        report_episode_detail_test.Episode_Description,
        report_episode_detail_test.Episode_Type,
        report_episode_detail_test.MDC,
        report_episode_detail_test.MDC_Description,
        report_episode_detail_test.Episode_Begin_Date,
        report_episode_detail_test.Episode_End_Date,
        report_episode_detail_test.Episode_Length,
        report_episode_detail_test.Level,
        report_episode_detail_test.Split_Total_Cost,
        report_episode_detail_test.Split_1stPercentile_Cost,
        report_episode_detail_test.Split_99thPercentile_Cost,
        report_episode_detail_test.Split_80thPercentile_Cost,
        report_episode_detail_test.Split_Total_PAC_Cost,
        report_episode_detail_test.Split_Total_Typical_Cost,
        report_episode_detail_test.Split_Total_TypicalwPAC_Cost,
        report_episode_detail_test.Annualized_Split_Total_Cost,
        report_episode_detail_test.Annualized_Split_1stPercentile_Cost,
        report_episode_detail_test.Annualized_Split_99thPercentile_Cost,
        report_episode_detail_test.Annualized_Split_80thPercentile_Cost,
        report_episode_detail_test.Annualized_Split_Total_PAC_Cost,
        report_episode_detail_test.Annualized_Split_Total_Typical_Cost,
        report_episode_detail_test.Annualized_Split_Total_TypicalwPAC_Cost,
        report_episode_detail_test.Unsplit_Total_Cost,
        report_episode_detail_test.Unsplit_1stPercentile_Cost,
        report_episode_detail_test.Unsplit_99thPercentile_Cost,
        report_episode_detail_test.Unsplit_Total_PAC_Cost,
        report_episode_detail_test.Unsplit_Total_Typical_Cost,
        report_episode_detail_test.Unsplit_Total_TypicalwPAC_Cost,
        report_episode_detail_test.Annualized_Unsplit_Total_Cost,
        report_episode_detail_test.Annualized_Unsplit_1stPercentile_Cost,
        report_episode_detail_test.Annualized_Unsplit_99thPercentile_Cost,
        report_episode_detail_test.Annualized_Unsplit_Total_PAC_Cost,
        report_episode_detail_test.Annualized_Unsplit_Total_Typical_Cost,
        report_episode_detail_test.Annualized_Unsplit_Total_TypicalwPAC_Cost,
        report_episode_detail_test.Facility_ID,
        report_episode_detail_test.Physician_ID,
        report_episode_detail_test.Physician_Specialty,
        report_episode_detail_test.Split_Expected_Total_Cost,
        report_episode_detail_test.Split_Expected_Typical_IP_Cost,
        report_episode_detail_test.Split_Expected_Typical_Other_Cost,
        report_episode_detail_test.Split_Expected_PAC_Cost,
        report_episode_detail_test.Unsplit_Expected_Total_Cost,
        report_episode_detail_test.Unsplit_Expected_Typical_IP_Cost,
        report_episode_detail_test.Unsplit_Expected_Typical_Other_Cost,
        report_episode_detail_test.Unsplit_Expected_PAC_Cost,
        report_episode_detail_test.IP_PAC_Count,
        report_episode_detail_test.OP_PAC_Count,
        report_episode_detail_test.PB_PAC_Count,
        report_episode_detail_test.RX_PAC_Count,
        report_episode_detail_test.year,
        report_episode_detail_test.enrolled_month,
        report_episode_detail_test.co_occurence_count_DEPANX,
        report_episode_detail_test.co_occurence_count_DIVERT,
        report_episode_detail_test.co_occurence_count_RHNTS,
        report_episode_detail_test.co_occurence_count_OSTEOA,
        report_episode_detail_test.co_occurence_count_DIAB,
        report_episode_detail_test.co_occurence_count_DEPRSN,
        report_episode_detail_test.co_occurence_count_LBP,
        report_episode_detail_test.co_occurence_count_CROHNS,
        report_episode_detail_test.co_occurence_count_GLCOMA,
        report_episode_detail_test.co_occurence_count_HTN,
        report_episode_detail_test.co_occurence_count_GERD,
        report_episode_detail_test.co_occurence_count_BIPLR,
        report_episode_detail_test.co_occurence_count_ULCLTS,
        report_episode_detail_test.co_occurence_count_CAD,
        report_episode_detail_test.co_occurence_count_COPD,
        report_episode_detail_test.co_occurence_count_HF,
        report_episode_detail_test.co_occurence_count_ARRBLK,
        report_episode_detail_test.co_occurence_count_ASTHMA,
        report_episode_detail_test.co_occurence_count_PTSD,
        report_episode_detail_test.co_occurence_count_SCHIZO,
        report_episode_detail_test.co_occurence_count_SUDS,
        report_episode_detail_test.co_count_chronic,
        report_episode_detail_test.co_count_all,
        report_episode_detail_test.ip_cost,
        report_episode_detail_test.op_cost,
        report_episode_detail_test.pb_cost,
        report_episode_detail_test.rx_cost,
        report_episode_detail_test.END_OF_STUDY,
        report_episode_detail_test.Facility_ID_provider_name,
        report_episode_detail_test.Facility_ID_provider_zipcode,
        report_episode_detail_test.Facility_ID_provider_type,
        report_episode_detail_test.Physician_ID_provider_name,
        report_episode_detail_test.Physician_ID_provider_zipcode,
        report_episode_detail_test.Physician_ID_provider_type,
        report_episode_detail_test.state_wide_avg_split_exp_cost,
        report_episode_detail_test.state_wide_avg_unsplit_exp_cost,
        report_episode_detail_test.state_wide_avg_split_total_cost,
        report_episode_detail_test.state_wide_avg_unsplit_total_cost,
        report_episode_detail_test.split_state_wide_pac_percent,
        report_episode_detail_test.unsplit_state_wide_pac_percent
 FROM epbuilder.report_episode_detail_test
 ORDER BY report_episode_detail_test.Filter_ID,
          report_episode_detail_test.Member_ID,
          report_episode_detail_test.Master_Episode_ID,
          report_episode_detail_test.Episode_ID,
          report_episode_detail_test.Episode_Name,
          report_episode_detail_test.Episode_Description,
          report_episode_detail_test.Episode_Type,
          report_episode_detail_test.MDC,
          report_episode_detail_test.MDC_Description,
          report_episode_detail_test.Episode_Begin_Date,
          report_episode_detail_test.Episode_End_Date,
          report_episode_detail_test.Episode_Length,
          report_episode_detail_test.Level,
          report_episode_detail_test.Split_Total_Cost,
          report_episode_detail_test.Split_1stPercentile_Cost,
          report_episode_detail_test.Split_99thPercentile_Cost,
          report_episode_detail_test.Split_80thPercentile_Cost,
          report_episode_detail_test.Split_Total_PAC_Cost,
          report_episode_detail_test.Split_Total_Typical_Cost,
          report_episode_detail_test.Split_Total_TypicalwPAC_Cost,
          report_episode_detail_test.Annualized_Split_Total_Cost,
          report_episode_detail_test.Annualized_Split_1stPercentile_Cost,
          report_episode_detail_test.Annualized_Split_99thPercentile_Cost,
          report_episode_detail_test.Annualized_Split_80thPercentile_Cost,
          report_episode_detail_test.Annualized_Split_Total_PAC_Cost,
          report_episode_detail_test.Annualized_Split_Total_Typical_Cost,
          report_episode_detail_test.Annualized_Split_Total_TypicalwPAC_Cost,
          report_episode_detail_test.Unsplit_Total_Cost,
          report_episode_detail_test.Unsplit_1stPercentile_Cost,
          report_episode_detail_test.Unsplit_99thPercentile_Cost,
          report_episode_detail_test.Unsplit_Total_PAC_Cost,
          report_episode_detail_test.Unsplit_Total_Typical_Cost,
          report_episode_detail_test.Unsplit_Total_TypicalwPAC_Cost,
          report_episode_detail_test.Annualized_Unsplit_Total_Cost,
          report_episode_detail_test.Annualized_Unsplit_1stPercentile_Cost,
          report_episode_detail_test.Annualized_Unsplit_99thPercentile_Cost,
          report_episode_detail_test.Annualized_Unsplit_Total_PAC_Cost,
          report_episode_detail_test.Annualized_Unsplit_Total_Typical_Cost,
          report_episode_detail_test.Annualized_Unsplit_Total_TypicalwPAC_Cost,
          report_episode_detail_test.Facility_ID,
          report_episode_detail_test.Physician_ID,
          report_episode_detail_test.Physician_Specialty,
          report_episode_detail_test.Split_Expected_Total_Cost,
          report_episode_detail_test.Split_Expected_Typical_IP_Cost,
          report_episode_detail_test.Split_Expected_Typical_Other_Cost,
          report_episode_detail_test.Split_Expected_PAC_Cost,
          report_episode_detail_test.Unsplit_Expected_Total_Cost,
          report_episode_detail_test.Unsplit_Expected_Typical_IP_Cost,
          report_episode_detail_test.Unsplit_Expected_Typical_Other_Cost,
          report_episode_detail_test.Unsplit_Expected_PAC_Cost,
          report_episode_detail_test.IP_PAC_Count,
          report_episode_detail_test.OP_PAC_Count,
          report_episode_detail_test.PB_PAC_Count,
          report_episode_detail_test.RX_PAC_Count
SEGMENTED BY hash(report_episode_detail_test.Filter_ID, report_episode_detail_test.Episode_ID, report_episode_detail_test.Episode_Name, report_episode_detail_test.MDC, report_episode_detail_test.Episode_Length, report_episode_detail_test.Level, report_episode_detail_test.Split_Total_Cost, report_episode_detail_test.Split_1stPercentile_Cost, report_episode_detail_test.Split_99thPercentile_Cost, report_episode_detail_test.Split_80thPercentile_Cost, report_episode_detail_test.Split_Total_PAC_Cost, report_episode_detail_test.Split_Total_Typical_Cost, report_episode_detail_test.Split_Total_TypicalwPAC_Cost, report_episode_detail_test.Annualized_Split_Total_Cost, report_episode_detail_test.Annualized_Split_1stPercentile_Cost, report_episode_detail_test.Annualized_Split_99thPercentile_Cost, report_episode_detail_test.Annualized_Split_80thPercentile_Cost, report_episode_detail_test.Annualized_Split_Total_PAC_Cost, report_episode_detail_test.Annualized_Split_Total_Typical_Cost, report_episode_detail_test.Annualized_Split_Total_TypicalwPAC_Cost, report_episode_detail_test.Unsplit_Total_Cost, report_episode_detail_test.Unsplit_1stPercentile_Cost, report_episode_detail_test.Unsplit_99thPercentile_Cost, report_episode_detail_test.Unsplit_Total_PAC_Cost, report_episode_detail_test.Unsplit_Total_Typical_Cost, report_episode_detail_test.Unsplit_Total_TypicalwPAC_Cost, report_episode_detail_test.Annualized_Unsplit_Total_Cost, report_episode_detail_test.Annualized_Unsplit_1stPercentile_Cost, report_episode_detail_test.Annualized_Unsplit_99thPercentile_Cost, report_episode_detail_test.Annualized_Unsplit_Total_PAC_Cost, report_episode_detail_test.Annualized_Unsplit_Total_Typical_Cost, report_episode_detail_test.Annualized_Unsplit_Total_TypicalwPAC_Cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.percentiles_test /*+createtype(A)*/ 
(
 Filter_id,
 Master_episode_id,
 Episode_ID,
 Level,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_80thPercentile_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_80thPercentile_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost
)
AS
 SELECT percentiles_test.Filter_id,
        percentiles_test.Master_episode_id,
        percentiles_test.Episode_ID,
        percentiles_test.Level,
        percentiles_test.Split_1stPercentile_Cost,
        percentiles_test.Split_99thPercentile_Cost,
        percentiles_test.Split_80thPercentile_Cost,
        percentiles_test.Unsplit_1stPercentile_Cost,
        percentiles_test.Unsplit_99thPercentile_Cost,
        percentiles_test.Annualized_Split_1stPercentile_Cost,
        percentiles_test.Annualized_Split_99thPercentile_Cost,
        percentiles_test.Annualized_Split_80thPercentile_Cost,
        percentiles_test.Annualized_Unsplit_1stPercentile_Cost,
        percentiles_test.Annualized_Unsplit_99thPercentile_Cost
 FROM epbuilder.percentiles_test
 ORDER BY percentiles_test.Filter_id,
          percentiles_test.Episode_ID,
          percentiles_test.Level
SEGMENTED BY hash(percentiles_test.Filter_id, percentiles_test.Episode_ID, percentiles_test.Level) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.non_utilizer /*+createtype(A)*/ 
(
 member_id,
 year
)
AS
 SELECT non_utilizer.member_id,
        non_utilizer.year
 FROM epbuilder.non_utilizer
 ORDER BY non_utilizer.member_id,
          non_utilizer.year
SEGMENTED BY hash(non_utilizer.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.visual_analysis_table_tmp /*+createtype(A)*/ 
(
 analysis_type,
 id,
 episode_id,
 episode_name,
 episode_description,
 episode_type,
 episode_category,
 episode_level,
 member_id,
 member_age,
 cms_age_group,
 gender,
 member_zipcode,
 member_county,
 member_population,
 total_cost,
 expected_cost,
 pac_cost,
 typical_cost,
 expected_pac_cost,
 expected_typical_cost,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 assigned_cost,
 assigned_ip_cost,
 assigned_op_cost,
 assigned_pb_cost,
 assigned_rx_cost,
 assigned_cost_unfiltered,
 assigned_ip_cost_unfiltered,
 assigned_op_cost_unfiltered,
 assigned_pb_cost_unfiltered,
 assigned_rx_cost_unfiltered,
 pps,
 provider_id,
 provider_name,
 provider_zipcode,
 provider_type,
 mco,
 hh,
 pcp,
 vbp_attrib_provider,
 vbp_attrib_provider_zipcode,
 vbp_contractor,
 year,
 utilization,
 ppr,
 ppv,
 exclusive,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_occurence_count_chronic,
 co_occurence_count_all,
 episode_count,
 episode_count_unfiltered,
 qcrg_code,
 qcrg_desc,
 qacrg1_code,
 qacrg1_desc,
 qacrg2_code,
 qacrg2_desc,
 qacrg3_code,
 qacrg3_desc,
 fincrg_q,
 q_base,
 q_severity,
 enrolled_num_month
)
AS
 SELECT visual_analysis_table.analysis_type,
        visual_analysis_table.id,
        visual_analysis_table.episode_id,
        visual_analysis_table.episode_name,
        visual_analysis_table.episode_description,
        visual_analysis_table.episode_type,
        visual_analysis_table.episode_category,
        visual_analysis_table.episode_level,
        visual_analysis_table.member_id,
        visual_analysis_table.member_age,
        visual_analysis_table.cms_age_group,
        visual_analysis_table.gender,
        visual_analysis_table.member_zipcode,
        visual_analysis_table.member_county,
        visual_analysis_table.member_population,
        visual_analysis_table.total_cost,
        visual_analysis_table.expected_cost,
        visual_analysis_table.pac_cost,
        visual_analysis_table.typical_cost,
        visual_analysis_table.expected_pac_cost,
        visual_analysis_table.expected_typical_cost,
        visual_analysis_table.ip_cost,
        visual_analysis_table.op_cost,
        visual_analysis_table.pb_cost,
        visual_analysis_table.rx_cost,
        visual_analysis_table.assigned_cost,
        visual_analysis_table.assigned_ip_cost,
        visual_analysis_table.assigned_op_cost,
        visual_analysis_table.assigned_pb_cost,
        visual_analysis_table.assigned_rx_cost,
        visual_analysis_table.assigned_cost_unfiltered,
        visual_analysis_table.assigned_ip_cost_unfiltered,
        visual_analysis_table.assigned_op_cost_unfiltered,
        visual_analysis_table.assigned_pb_cost_unfiltered,
        visual_analysis_table.assigned_rx_cost_unfiltered,
        visual_analysis_table.pps,
        visual_analysis_table.provider_id,
        visual_analysis_table.provider_name,
        visual_analysis_table.provider_zipcode,
        visual_analysis_table.provider_type,
        visual_analysis_table.mco,
        visual_analysis_table.hh,
        visual_analysis_table.pcp,
        visual_analysis_table.vbp_attrib_provider,
        visual_analysis_table.vbp_attrib_provider_zipcode,
        visual_analysis_table.vbp_contractor,
        visual_analysis_table.year,
        visual_analysis_table.utilization,
        visual_analysis_table.ppr,
        visual_analysis_table.ppv,
        visual_analysis_table.exclusive,
        visual_analysis_table.co_occurence_count_ASTHMA,
        visual_analysis_table.co_occurence_count_ARRBLK,
        visual_analysis_table.co_occurence_count_HF,
        visual_analysis_table.co_occurence_count_COPD,
        visual_analysis_table.co_occurence_count_CAD,
        visual_analysis_table.co_occurence_count_ULCLTS,
        visual_analysis_table.co_occurence_count_BIPLR,
        visual_analysis_table.co_occurence_count_GERD,
        visual_analysis_table.co_occurence_count_HTN,
        visual_analysis_table.co_occurence_count_GLCOMA,
        visual_analysis_table.co_occurence_count_LBP,
        visual_analysis_table.co_occurence_count_CROHNS,
        visual_analysis_table.co_occurence_count_DIAB,
        visual_analysis_table.co_occurence_count_DEPRSN,
        visual_analysis_table.co_occurence_count_OSTEOA,
        visual_analysis_table.co_occurence_count_RHNTS,
        visual_analysis_table.co_occurence_count_DIVERT,
        visual_analysis_table.co_occurence_count_DEPANX,
        visual_analysis_table.co_occurence_count_PTSD,
        visual_analysis_table.co_occurence_count_SCHIZO,
        visual_analysis_table.co_occurence_count_SUDS,
        visual_analysis_table.co_occurence_count_chronic,
        visual_analysis_table.co_occurence_count_all,
        visual_analysis_table.episode_count,
        visual_analysis_table.episode_count_unfiltered,
        visual_analysis_table.qcrg_code,
        visual_analysis_table.qcrg_desc,
        visual_analysis_table.qacrg1_code,
        visual_analysis_table.qacrg1_desc,
        visual_analysis_table.qacrg2_code,
        visual_analysis_table.qacrg2_desc,
        visual_analysis_table.qacrg3_code,
        visual_analysis_table.qacrg3_desc,
        visual_analysis_table.fincrg_q,
        visual_analysis_table.q_base,
        visual_analysis_table.q_severity,
        visual_analysis_table.enrolled_num_month
 FROM epbuilder.visual_analysis_table
 ORDER BY visual_analysis_table.analysis_type,
          visual_analysis_table.id,
          visual_analysis_table.episode_id,
          visual_analysis_table.episode_name,
          visual_analysis_table.episode_description,
          visual_analysis_table.episode_type,
          visual_analysis_table.episode_category,
          visual_analysis_table.episode_level,
          visual_analysis_table.member_id,
          visual_analysis_table.member_age,
          visual_analysis_table.cms_age_group,
          visual_analysis_table.gender,
          visual_analysis_table.member_zipcode,
          visual_analysis_table.member_county,
          visual_analysis_table.member_population,
          visual_analysis_table.total_cost,
          visual_analysis_table.expected_cost,
          visual_analysis_table.pac_cost,
          visual_analysis_table.typical_cost,
          visual_analysis_table.expected_pac_cost,
          visual_analysis_table.expected_typical_cost,
          visual_analysis_table.ip_cost,
          visual_analysis_table.op_cost,
          visual_analysis_table.pb_cost,
          visual_analysis_table.rx_cost,
          visual_analysis_table.assigned_cost,
          visual_analysis_table.assigned_ip_cost,
          visual_analysis_table.assigned_op_cost,
          visual_analysis_table.assigned_pb_cost,
          visual_analysis_table.assigned_rx_cost,
          visual_analysis_table.assigned_cost_unfiltered,
          visual_analysis_table.assigned_ip_cost_unfiltered,
          visual_analysis_table.assigned_op_cost_unfiltered,
          visual_analysis_table.assigned_pb_cost_unfiltered,
          visual_analysis_table.assigned_rx_cost_unfiltered,
          visual_analysis_table.pps,
          visual_analysis_table.provider_id,
          visual_analysis_table.provider_name,
          visual_analysis_table.provider_zipcode,
          visual_analysis_table.provider_type,
          visual_analysis_table.mco,
          visual_analysis_table.hh,
          visual_analysis_table.pcp,
          visual_analysis_table.vbp_attrib_provider,
          visual_analysis_table.vbp_attrib_provider_zipcode,
          visual_analysis_table.vbp_contractor,
          visual_analysis_table.year,
          visual_analysis_table.utilization,
          visual_analysis_table.ppr,
          visual_analysis_table.ppv,
          visual_analysis_table.exclusive,
          visual_analysis_table.co_occurence_count_ASTHMA,
          visual_analysis_table.co_occurence_count_ARRBLK,
          visual_analysis_table.co_occurence_count_HF,
          visual_analysis_table.co_occurence_count_COPD,
          visual_analysis_table.co_occurence_count_CAD,
          visual_analysis_table.co_occurence_count_ULCLTS,
          visual_analysis_table.co_occurence_count_BIPLR,
          visual_analysis_table.co_occurence_count_GERD,
          visual_analysis_table.co_occurence_count_HTN,
          visual_analysis_table.co_occurence_count_GLCOMA,
          visual_analysis_table.co_occurence_count_LBP,
          visual_analysis_table.co_occurence_count_CROHNS,
          visual_analysis_table.co_occurence_count_DIAB,
          visual_analysis_table.co_occurence_count_DEPRSN,
          visual_analysis_table.co_occurence_count_OSTEOA,
          visual_analysis_table.co_occurence_count_RHNTS,
          visual_analysis_table.co_occurence_count_DIVERT,
          visual_analysis_table.co_occurence_count_DEPANX,
          visual_analysis_table.co_occurence_count_PTSD,
          visual_analysis_table.co_occurence_count_SCHIZO,
          visual_analysis_table.co_occurence_count_SUDS,
          visual_analysis_table.co_occurence_count_chronic,
          visual_analysis_table.co_occurence_count_all,
          visual_analysis_table.episode_count,
          visual_analysis_table.episode_count_unfiltered,
          visual_analysis_table.qcrg_code,
          visual_analysis_table.qcrg_desc,
          visual_analysis_table.qacrg1_code,
          visual_analysis_table.qacrg1_desc,
          visual_analysis_table.qacrg2_code,
          visual_analysis_table.qacrg2_desc,
          visual_analysis_table.qacrg3_code,
          visual_analysis_table.qacrg3_desc,
          visual_analysis_table.fincrg_q,
          visual_analysis_table.q_base,
          visual_analysis_table.q_severity,
          visual_analysis_table.enrolled_num_month
SEGMENTED BY hash(visual_analysis_table.member_age, visual_analysis_table.gender, visual_analysis_table.total_cost, visual_analysis_table.expected_cost, visual_analysis_table.pac_cost, visual_analysis_table.typical_cost, visual_analysis_table.expected_pac_cost, visual_analysis_table.expected_typical_cost, visual_analysis_table.ip_cost, visual_analysis_table.op_cost, visual_analysis_table.pb_cost, visual_analysis_table.rx_cost, visual_analysis_table.assigned_cost, visual_analysis_table.assigned_ip_cost, visual_analysis_table.assigned_op_cost, visual_analysis_table.assigned_pb_cost, visual_analysis_table.assigned_rx_cost, visual_analysis_table.assigned_cost_unfiltered, visual_analysis_table.assigned_ip_cost_unfiltered, visual_analysis_table.assigned_op_cost_unfiltered, visual_analysis_table.assigned_pb_cost_unfiltered, visual_analysis_table.assigned_rx_cost_unfiltered, visual_analysis_table.ppr, visual_analysis_table.ppv, visual_analysis_table.co_occurence_count_ASTHMA, visual_analysis_table.co_occurence_count_ARRBLK, visual_analysis_table.co_occurence_count_HF, visual_analysis_table.co_occurence_count_COPD, visual_analysis_table.co_occurence_count_CAD, visual_analysis_table.co_occurence_count_ULCLTS, visual_analysis_table.co_occurence_count_BIPLR, visual_analysis_table.co_occurence_count_GERD) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.RFs_2012 /*+createtype(A)*/ 
(
 member_id,
 factor_id
)
AS
 SELECT RFs_2012.member_id,
        RFs_2012.factor_id
 FROM epbuilder.RFs_2012
 ORDER BY RFs_2012.member_id,
          RFs_2012.factor_id
SEGMENTED BY hash(RFs_2012.member_id, RFs_2012.factor_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.RFs_2012_Count /*+createtype(A)*/ 
(
 member_id,
 count
)
AS
 SELECT RFs_2012_Count.member_id,
        RFs_2012_Count.count
 FROM epbuilder.RFs_2012_Count
 ORDER BY RFs_2012_Count.member_id
SEGMENTED BY hash(RFs_2012_Count.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.temp_main_state_wide_values_1_1 /*+createtype(L)*/ 
(
 data_processing,
 analysis_type,
 vbp_arrangement,
 member_population,
 episode_name,
 year,
 state_wide_total
)
AS
 SELECT temp_main_state_wide_values_1_1.data_processing,
        temp_main_state_wide_values_1_1.analysis_type,
        temp_main_state_wide_values_1_1.vbp_arrangement,
        temp_main_state_wide_values_1_1.member_population,
        temp_main_state_wide_values_1_1.episode_name,
        temp_main_state_wide_values_1_1.year,
        temp_main_state_wide_values_1_1.state_wide_total
 FROM epbuilder.temp_main_state_wide_values_1_1
 ORDER BY temp_main_state_wide_values_1_1.data_processing,
          temp_main_state_wide_values_1_1.analysis_type,
          temp_main_state_wide_values_1_1.vbp_arrangement,
          temp_main_state_wide_values_1_1.member_population,
          temp_main_state_wide_values_1_1.episode_name
SEGMENTED BY hash(temp_main_state_wide_values_1_1.year, temp_main_state_wide_values_1_1.state_wide_total, temp_main_state_wide_values_1_1.data_processing, temp_main_state_wide_values_1_1.analysis_type, temp_main_state_wide_values_1_1.vbp_arrangement, temp_main_state_wide_values_1_1.member_population, temp_main_state_wide_values_1_1.episode_name) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.visual_analysis_table_va_date /*+createtype(A)*/ 
(
 data_processing,
 analysis_type,
 id,
 episode_id,
 episode_name,
 episode_description,
 episode_type,
 episode_category,
 episode_level,
 member_id,
 member_age,
 cms_age_group,
 gender,
 member_zipcode,
 member_county,
 member_population,
 member_region,
 total_cost,
 expected_cost,
 pac_cost,
 typical_cost,
 expected_pac_cost,
 expected_typical_cost,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 assigned_cost,
 assigned_ip_cost,
 assigned_op_cost,
 assigned_pb_cost,
 assigned_rx_cost,
 assigned_cost_unfiltered,
 assigned_ip_cost_unfiltered,
 assigned_op_cost_unfiltered,
 assigned_pb_cost_unfiltered,
 assigned_rx_cost_unfiltered,
 pps,
 provider_id,
 provider_name,
 provider_zipcode,
 provider_type,
 mco,
 hh,
 pcp,
 vbp_attrib_provider,
 vbp_attrib_provider_zipcode,
 vbp_contractor,
 subgroups,
 year,
 utilization,
 ppr,
 ppv,
 exclusive,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_occurence_count_chronic,
 co_occurence_count_all,
 episode_count,
 episode_count_unfiltered,
 qcrg_code,
 qcrg_desc,
 qacrg1_code,
 qacrg1_desc,
 qacrg2_code,
 qacrg2_desc,
 qacrg3_code,
 qacrg3_desc,
 fincrg_q,
 q_base,
 q_severity,
 enrolled_num_month,
 vbp_arrangement,
 state_wide_avg_exp_cost,
 state_wide_exp_pac_rate,
 state_wide_avg_total_cost,
 state_wide_pac_percent,
 state_wide_pac_rate,
 state_wide_female_percent,
 state_wide_male_percent,
 state_wide_NU_percent,
 state_wide_LU_percent,
 state_wide_percent_co_ASTHMA,
 state_wide_percent_co_ARRBLK,
 state_wide_percent_co_HF,
 state_wide_percent_co_COPD,
 state_wide_percent_co_CAD,
 state_wide_percent_co_ULCLTS,
 state_wide_percent_co_BIPLR,
 state_wide_percent_co_GERD,
 state_wide_percent_co_HTN,
 state_wide_percent_co_GLCOMA,
 state_wide_percent_co_LBP,
 state_wide_percent_co_CROHNS,
 state_wide_percent_co_DIAB,
 state_wide_percent_co_DEPRSN,
 state_wide_percent_co_OSTEOA,
 state_wide_percent_co_RHNTS,
 state_wide_percent_co_DIVERT,
 state_wide_percent_co_DEPANX,
 state_wide_percent_co_PTSD,
 state_wide_percent_co_SCHIZO,
 state_wide_percent_co_SUDS
)
AS
 SELECT visual_analysis_table_va_date.data_processing,
        visual_analysis_table_va_date.analysis_type,
        visual_analysis_table_va_date.id,
        visual_analysis_table_va_date.episode_id,
        visual_analysis_table_va_date.episode_name,
        visual_analysis_table_va_date.episode_description,
        visual_analysis_table_va_date.episode_type,
        visual_analysis_table_va_date.episode_category,
        visual_analysis_table_va_date.episode_level,
        visual_analysis_table_va_date.member_id,
        visual_analysis_table_va_date.member_age,
        visual_analysis_table_va_date.cms_age_group,
        visual_analysis_table_va_date.gender,
        visual_analysis_table_va_date.member_zipcode,
        visual_analysis_table_va_date.member_county,
        visual_analysis_table_va_date.member_population,
        visual_analysis_table_va_date.member_region,
        visual_analysis_table_va_date.total_cost,
        visual_analysis_table_va_date.expected_cost,
        visual_analysis_table_va_date.pac_cost,
        visual_analysis_table_va_date.typical_cost,
        visual_analysis_table_va_date.expected_pac_cost,
        visual_analysis_table_va_date.expected_typical_cost,
        visual_analysis_table_va_date.ip_cost,
        visual_analysis_table_va_date.op_cost,
        visual_analysis_table_va_date.pb_cost,
        visual_analysis_table_va_date.rx_cost,
        visual_analysis_table_va_date.assigned_cost,
        visual_analysis_table_va_date.assigned_ip_cost,
        visual_analysis_table_va_date.assigned_op_cost,
        visual_analysis_table_va_date.assigned_pb_cost,
        visual_analysis_table_va_date.assigned_rx_cost,
        visual_analysis_table_va_date.assigned_cost_unfiltered,
        visual_analysis_table_va_date.assigned_ip_cost_unfiltered,
        visual_analysis_table_va_date.assigned_op_cost_unfiltered,
        visual_analysis_table_va_date.assigned_pb_cost_unfiltered,
        visual_analysis_table_va_date.assigned_rx_cost_unfiltered,
        visual_analysis_table_va_date.pps,
        visual_analysis_table_va_date.provider_id,
        visual_analysis_table_va_date.provider_name,
        visual_analysis_table_va_date.provider_zipcode,
        visual_analysis_table_va_date.provider_type,
        visual_analysis_table_va_date.mco,
        visual_analysis_table_va_date.hh,
        visual_analysis_table_va_date.pcp,
        visual_analysis_table_va_date.vbp_attrib_provider,
        visual_analysis_table_va_date.vbp_attrib_provider_zipcode,
        visual_analysis_table_va_date.vbp_contractor,
        visual_analysis_table_va_date.subgroups,
        visual_analysis_table_va_date.year,
        visual_analysis_table_va_date.utilization,
        visual_analysis_table_va_date.ppr,
        visual_analysis_table_va_date.ppv,
        visual_analysis_table_va_date.exclusive,
        visual_analysis_table_va_date.co_occurence_count_ASTHMA,
        visual_analysis_table_va_date.co_occurence_count_ARRBLK,
        visual_analysis_table_va_date.co_occurence_count_HF,
        visual_analysis_table_va_date.co_occurence_count_COPD,
        visual_analysis_table_va_date.co_occurence_count_CAD,
        visual_analysis_table_va_date.co_occurence_count_ULCLTS,
        visual_analysis_table_va_date.co_occurence_count_BIPLR,
        visual_analysis_table_va_date.co_occurence_count_GERD,
        visual_analysis_table_va_date.co_occurence_count_HTN,
        visual_analysis_table_va_date.co_occurence_count_GLCOMA,
        visual_analysis_table_va_date.co_occurence_count_LBP,
        visual_analysis_table_va_date.co_occurence_count_CROHNS,
        visual_analysis_table_va_date.co_occurence_count_DIAB,
        visual_analysis_table_va_date.co_occurence_count_DEPRSN,
        visual_analysis_table_va_date.co_occurence_count_OSTEOA,
        visual_analysis_table_va_date.co_occurence_count_RHNTS,
        visual_analysis_table_va_date.co_occurence_count_DIVERT,
        visual_analysis_table_va_date.co_occurence_count_DEPANX,
        visual_analysis_table_va_date.co_occurence_count_PTSD,
        visual_analysis_table_va_date.co_occurence_count_SCHIZO,
        visual_analysis_table_va_date.co_occurence_count_SUDS,
        visual_analysis_table_va_date.co_occurence_count_chronic,
        visual_analysis_table_va_date.co_occurence_count_all,
        visual_analysis_table_va_date.episode_count,
        visual_analysis_table_va_date.episode_count_unfiltered,
        visual_analysis_table_va_date.qcrg_code,
        visual_analysis_table_va_date.qcrg_desc,
        visual_analysis_table_va_date.qacrg1_code,
        visual_analysis_table_va_date.qacrg1_desc,
        visual_analysis_table_va_date.qacrg2_code,
        visual_analysis_table_va_date.qacrg2_desc,
        visual_analysis_table_va_date.qacrg3_code,
        visual_analysis_table_va_date.qacrg3_desc,
        visual_analysis_table_va_date.fincrg_q,
        visual_analysis_table_va_date.q_base,
        visual_analysis_table_va_date.q_severity,
        visual_analysis_table_va_date.enrolled_num_month,
        visual_analysis_table_va_date.vbp_arrangement,
        visual_analysis_table_va_date.state_wide_avg_exp_cost,
        visual_analysis_table_va_date.state_wide_exp_pac_rate,
        visual_analysis_table_va_date.state_wide_avg_total_cost,
        visual_analysis_table_va_date.state_wide_pac_percent,
        visual_analysis_table_va_date.state_wide_pac_rate,
        visual_analysis_table_va_date.state_wide_female_percent,
        visual_analysis_table_va_date.state_wide_male_percent,
        visual_analysis_table_va_date.state_wide_NU_percent,
        visual_analysis_table_va_date.state_wide_LU_percent,
        visual_analysis_table_va_date.state_wide_percent_co_ASTHMA,
        visual_analysis_table_va_date.state_wide_percent_co_ARRBLK,
        visual_analysis_table_va_date.state_wide_percent_co_HF,
        visual_analysis_table_va_date.state_wide_percent_co_COPD,
        visual_analysis_table_va_date.state_wide_percent_co_CAD,
        visual_analysis_table_va_date.state_wide_percent_co_ULCLTS,
        visual_analysis_table_va_date.state_wide_percent_co_BIPLR,
        visual_analysis_table_va_date.state_wide_percent_co_GERD,
        visual_analysis_table_va_date.state_wide_percent_co_HTN,
        visual_analysis_table_va_date.state_wide_percent_co_GLCOMA,
        visual_analysis_table_va_date.state_wide_percent_co_LBP,
        visual_analysis_table_va_date.state_wide_percent_co_CROHNS,
        visual_analysis_table_va_date.state_wide_percent_co_DIAB,
        visual_analysis_table_va_date.state_wide_percent_co_DEPRSN,
        visual_analysis_table_va_date.state_wide_percent_co_OSTEOA,
        visual_analysis_table_va_date.state_wide_percent_co_RHNTS,
        visual_analysis_table_va_date.state_wide_percent_co_DIVERT,
        visual_analysis_table_va_date.state_wide_percent_co_DEPANX,
        visual_analysis_table_va_date.state_wide_percent_co_PTSD,
        visual_analysis_table_va_date.state_wide_percent_co_SCHIZO,
        visual_analysis_table_va_date.state_wide_percent_co_SUDS
 FROM epbuilder.visual_analysis_table_va_date
 ORDER BY visual_analysis_table_va_date.id
SEGMENTED BY hash(visual_analysis_table_va_date.gender, visual_analysis_table_va_date.total_cost, visual_analysis_table_va_date.expected_cost, visual_analysis_table_va_date.pac_cost, visual_analysis_table_va_date.typical_cost, visual_analysis_table_va_date.expected_pac_cost, visual_analysis_table_va_date.expected_typical_cost, visual_analysis_table_va_date.ip_cost, visual_analysis_table_va_date.op_cost, visual_analysis_table_va_date.pb_cost, visual_analysis_table_va_date.rx_cost, visual_analysis_table_va_date.ppr, visual_analysis_table_va_date.ppv, visual_analysis_table_va_date.co_occurence_count_ASTHMA, visual_analysis_table_va_date.co_occurence_count_ARRBLK, visual_analysis_table_va_date.co_occurence_count_HF, visual_analysis_table_va_date.co_occurence_count_COPD, visual_analysis_table_va_date.co_occurence_count_CAD, visual_analysis_table_va_date.co_occurence_count_ULCLTS, visual_analysis_table_va_date.co_occurence_count_BIPLR, visual_analysis_table_va_date.co_occurence_count_GERD, visual_analysis_table_va_date.co_occurence_count_HTN, visual_analysis_table_va_date.co_occurence_count_GLCOMA, visual_analysis_table_va_date.co_occurence_count_LBP, visual_analysis_table_va_date.co_occurence_count_CROHNS, visual_analysis_table_va_date.co_occurence_count_DIAB, visual_analysis_table_va_date.co_occurence_count_DEPRSN, visual_analysis_table_va_date.co_occurence_count_OSTEOA, visual_analysis_table_va_date.co_occurence_count_RHNTS, visual_analysis_table_va_date.co_occurence_count_DIVERT, visual_analysis_table_va_date.co_occurence_count_DEPANX, visual_analysis_table_va_date.co_occurence_count_PTSD) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.visual_analysis_table_va /*+createtype(A)*/ 
(
 data_processing,
 analysis_type,
 id,
 episode_id,
 episode_name,
 episode_description,
 episode_type,
 episode_category,
 episode_level,
 member_id,
 member_age,
 cms_age_group,
 gender,
 member_zipcode,
 member_county,
 member_population,
 member_region,
 total_cost,
 expected_cost,
 pac_cost,
 typical_cost,
 expected_pac_cost,
 expected_typical_cost,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 assigned_cost,
 assigned_ip_cost,
 assigned_op_cost,
 assigned_pb_cost,
 assigned_rx_cost,
 assigned_cost_unfiltered,
 assigned_ip_cost_unfiltered,
 assigned_op_cost_unfiltered,
 assigned_pb_cost_unfiltered,
 assigned_rx_cost_unfiltered,
 pps,
 provider_id,
 provider_name,
 provider_zipcode,
 provider_type,
 mco,
 hh,
 pcp,
 vbp_attrib_provider,
 vbp_attrib_provider_zipcode,
 vbp_contractor,
 subgroups,
 year,
 utilization,
 ppr,
 ppv,
 exclusive,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_occurence_count_chronic,
 co_occurence_count_all,
 episode_count,
 episode_count_unfiltered,
 qcrg_code,
 qcrg_desc,
 qacrg1_code,
 qacrg1_desc,
 qacrg2_code,
 qacrg2_desc,
 qacrg3_code,
 qacrg3_desc,
 fincrg_q,
 q_base,
 q_severity,
 enrolled_num_month,
 vbp_arrangement,
 state_wide_avg_exp_cost,
 state_wide_exp_pac_rate,
 state_wide_avg_total_cost,
 state_wide_pac_percent,
 state_wide_pac_rate,
 state_wide_female_percent,
 state_wide_male_percent,
 state_wide_NU_percent,
 state_wide_LU_percent,
 state_wide_percent_co_ASTHMA,
 state_wide_percent_co_ARRBLK,
 state_wide_percent_co_HF,
 state_wide_percent_co_COPD,
 state_wide_percent_co_CAD,
 state_wide_percent_co_ULCLTS,
 state_wide_percent_co_BIPLR,
 state_wide_percent_co_GERD,
 state_wide_percent_co_HTN,
 state_wide_percent_co_GLCOMA,
 state_wide_percent_co_LBP,
 state_wide_percent_co_CROHNS,
 state_wide_percent_co_DIAB,
 state_wide_percent_co_DEPRSN,
 state_wide_percent_co_OSTEOA,
 state_wide_percent_co_RHNTS,
 state_wide_percent_co_DIVERT,
 state_wide_percent_co_DEPANX,
 state_wide_percent_co_PTSD,
 state_wide_percent_co_SCHIZO,
 state_wide_percent_co_SUDS
)
AS
 SELECT visual_analysis_table_va.data_processing,
        visual_analysis_table_va.analysis_type,
        visual_analysis_table_va.id,
        visual_analysis_table_va.episode_id,
        visual_analysis_table_va.episode_name,
        visual_analysis_table_va.episode_description,
        visual_analysis_table_va.episode_type,
        visual_analysis_table_va.episode_category,
        visual_analysis_table_va.episode_level,
        visual_analysis_table_va.member_id,
        visual_analysis_table_va.member_age,
        visual_analysis_table_va.cms_age_group,
        visual_analysis_table_va.gender,
        visual_analysis_table_va.member_zipcode,
        visual_analysis_table_va.member_county,
        visual_analysis_table_va.member_population,
        visual_analysis_table_va.member_region,
        visual_analysis_table_va.total_cost,
        visual_analysis_table_va.expected_cost,
        visual_analysis_table_va.pac_cost,
        visual_analysis_table_va.typical_cost,
        visual_analysis_table_va.expected_pac_cost,
        visual_analysis_table_va.expected_typical_cost,
        visual_analysis_table_va.ip_cost,
        visual_analysis_table_va.op_cost,
        visual_analysis_table_va.pb_cost,
        visual_analysis_table_va.rx_cost,
        visual_analysis_table_va.assigned_cost,
        visual_analysis_table_va.assigned_ip_cost,
        visual_analysis_table_va.assigned_op_cost,
        visual_analysis_table_va.assigned_pb_cost,
        visual_analysis_table_va.assigned_rx_cost,
        visual_analysis_table_va.assigned_cost_unfiltered,
        visual_analysis_table_va.assigned_ip_cost_unfiltered,
        visual_analysis_table_va.assigned_op_cost_unfiltered,
        visual_analysis_table_va.assigned_pb_cost_unfiltered,
        visual_analysis_table_va.assigned_rx_cost_unfiltered,
        visual_analysis_table_va.pps,
        visual_analysis_table_va.provider_id,
        visual_analysis_table_va.provider_name,
        visual_analysis_table_va.provider_zipcode,
        visual_analysis_table_va.provider_type,
        visual_analysis_table_va.mco,
        visual_analysis_table_va.hh,
        visual_analysis_table_va.pcp,
        visual_analysis_table_va.vbp_attrib_provider,
        visual_analysis_table_va.vbp_attrib_provider_zipcode,
        visual_analysis_table_va.vbp_contractor,
        visual_analysis_table_va.subgroups,
        visual_analysis_table_va.year,
        visual_analysis_table_va.utilization,
        visual_analysis_table_va.ppr,
        visual_analysis_table_va.ppv,
        visual_analysis_table_va.exclusive,
        visual_analysis_table_va.co_occurence_count_ASTHMA,
        visual_analysis_table_va.co_occurence_count_ARRBLK,
        visual_analysis_table_va.co_occurence_count_HF,
        visual_analysis_table_va.co_occurence_count_COPD,
        visual_analysis_table_va.co_occurence_count_CAD,
        visual_analysis_table_va.co_occurence_count_ULCLTS,
        visual_analysis_table_va.co_occurence_count_BIPLR,
        visual_analysis_table_va.co_occurence_count_GERD,
        visual_analysis_table_va.co_occurence_count_HTN,
        visual_analysis_table_va.co_occurence_count_GLCOMA,
        visual_analysis_table_va.co_occurence_count_LBP,
        visual_analysis_table_va.co_occurence_count_CROHNS,
        visual_analysis_table_va.co_occurence_count_DIAB,
        visual_analysis_table_va.co_occurence_count_DEPRSN,
        visual_analysis_table_va.co_occurence_count_OSTEOA,
        visual_analysis_table_va.co_occurence_count_RHNTS,
        visual_analysis_table_va.co_occurence_count_DIVERT,
        visual_analysis_table_va.co_occurence_count_DEPANX,
        visual_analysis_table_va.co_occurence_count_PTSD,
        visual_analysis_table_va.co_occurence_count_SCHIZO,
        visual_analysis_table_va.co_occurence_count_SUDS,
        visual_analysis_table_va.co_occurence_count_chronic,
        visual_analysis_table_va.co_occurence_count_all,
        visual_analysis_table_va.episode_count,
        visual_analysis_table_va.episode_count_unfiltered,
        visual_analysis_table_va.qcrg_code,
        visual_analysis_table_va.qcrg_desc,
        visual_analysis_table_va.qacrg1_code,
        visual_analysis_table_va.qacrg1_desc,
        visual_analysis_table_va.qacrg2_code,
        visual_analysis_table_va.qacrg2_desc,
        visual_analysis_table_va.qacrg3_code,
        visual_analysis_table_va.qacrg3_desc,
        visual_analysis_table_va.fincrg_q,
        visual_analysis_table_va.q_base,
        visual_analysis_table_va.q_severity,
        visual_analysis_table_va.enrolled_num_month,
        visual_analysis_table_va.vbp_arrangement,
        visual_analysis_table_va.state_wide_avg_exp_cost,
        visual_analysis_table_va.state_wide_exp_pac_rate,
        visual_analysis_table_va.state_wide_avg_total_cost,
        visual_analysis_table_va.state_wide_pac_percent,
        visual_analysis_table_va.state_wide_pac_rate,
        visual_analysis_table_va.state_wide_female_percent,
        visual_analysis_table_va.state_wide_male_percent,
        visual_analysis_table_va.state_wide_NU_percent,
        visual_analysis_table_va.state_wide_LU_percent,
        visual_analysis_table_va.state_wide_percent_co_ASTHMA,
        visual_analysis_table_va.state_wide_percent_co_ARRBLK,
        visual_analysis_table_va.state_wide_percent_co_HF,
        visual_analysis_table_va.state_wide_percent_co_COPD,
        visual_analysis_table_va.state_wide_percent_co_CAD,
        visual_analysis_table_va.state_wide_percent_co_ULCLTS,
        visual_analysis_table_va.state_wide_percent_co_BIPLR,
        visual_analysis_table_va.state_wide_percent_co_GERD,
        visual_analysis_table_va.state_wide_percent_co_HTN,
        visual_analysis_table_va.state_wide_percent_co_GLCOMA,
        visual_analysis_table_va.state_wide_percent_co_LBP,
        visual_analysis_table_va.state_wide_percent_co_CROHNS,
        visual_analysis_table_va.state_wide_percent_co_DIAB,
        visual_analysis_table_va.state_wide_percent_co_DEPRSN,
        visual_analysis_table_va.state_wide_percent_co_OSTEOA,
        visual_analysis_table_va.state_wide_percent_co_RHNTS,
        visual_analysis_table_va.state_wide_percent_co_DIVERT,
        visual_analysis_table_va.state_wide_percent_co_DEPANX,
        visual_analysis_table_va.state_wide_percent_co_PTSD,
        visual_analysis_table_va.state_wide_percent_co_SCHIZO,
        visual_analysis_table_va.state_wide_percent_co_SUDS
 FROM epbuilder.visual_analysis_table_va
 ORDER BY visual_analysis_table_va.id
SEGMENTED BY hash(visual_analysis_table_va.gender, visual_analysis_table_va.total_cost, visual_analysis_table_va.expected_cost, visual_analysis_table_va.pac_cost, visual_analysis_table_va.typical_cost, visual_analysis_table_va.expected_pac_cost, visual_analysis_table_va.expected_typical_cost, visual_analysis_table_va.ip_cost, visual_analysis_table_va.op_cost, visual_analysis_table_va.pb_cost, visual_analysis_table_va.rx_cost, visual_analysis_table_va.ppr, visual_analysis_table_va.ppv, visual_analysis_table_va.co_occurence_count_ASTHMA, visual_analysis_table_va.co_occurence_count_ARRBLK, visual_analysis_table_va.co_occurence_count_HF, visual_analysis_table_va.co_occurence_count_COPD, visual_analysis_table_va.co_occurence_count_CAD, visual_analysis_table_va.co_occurence_count_ULCLTS, visual_analysis_table_va.co_occurence_count_BIPLR, visual_analysis_table_va.co_occurence_count_GERD, visual_analysis_table_va.co_occurence_count_HTN, visual_analysis_table_va.co_occurence_count_GLCOMA, visual_analysis_table_va.co_occurence_count_LBP, visual_analysis_table_va.co_occurence_count_CROHNS, visual_analysis_table_va.co_occurence_count_DIAB, visual_analysis_table_va.co_occurence_count_DEPRSN, visual_analysis_table_va.co_occurence_count_OSTEOA, visual_analysis_table_va.co_occurence_count_RHNTS, visual_analysis_table_va.co_occurence_count_DIVERT, visual_analysis_table_va.co_occurence_count_DEPANX, visual_analysis_table_va.co_occurence_count_PTSD) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.report_episode_summary_test /*+createtype(L)*/ 
(
 Filter_ID,
 Episode_ID,
 Episode_Name,
 Episode_Description,
 Episode_Type,
 MDC,
 MDC_Description,
 Level,
 Episode_Volume,
 Split_Total_Cost,
 Split_Average_Cost,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_Min_Cost,
 Split_Max_Cost,
 Split_STDEV,
 Split_CV,
 Split_Total_PAC_Cost,
 Split_Average_PAC_Cost,
 Split_PAC_Percent,
 Split_Total_Typical_Cost,
 Split_Average_Typical_Cost,
 Split_Total_TypicalwPAC_Cost,
 Split_Average_TypicalwPAC_Cost,
 Annualized_Split_Total_Cost,
 Annualized_Split_Average_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_Min_Cost,
 Annualized_Split_Max_Cost,
 Annualized_Split_STDEV,
 Annualized_Split_CV,
 Annualized_Split_Total_PAC_Cost,
 Annualized_Split_Average_PAC_Cost,
 Annualized_Split_PAC_Percent,
 Annualized_Split_Total_Typical_Cost,
 Annualized_Split_Average_Typical_Cost,
 Annualized_Split_Total_TypicalwPAC_Cost,
 Annualized_Split_Average_TypicalwPAC_Cost,
 Unsplit_Total_Cost,
 Unsplit_Average_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Unsplit_Min_Cost,
 Unsplit_Max_Cost,
 Unsplit_STDEV,
 Unsplit_CV,
 Unsplit_Total_PAC_Cost,
 Unsplit_Average_PAC_Cost,
 Unsplit_PAC_Percent,
 Unsplit_Total_Typical_Cost,
 Unsplit_Average_Typical_Cost,
 Unsplit_Total_TypicalwPAC_Cost,
 Unsplit_Average_TypicalwPAC_Cost,
 Annualized_Unsplit_Total_Cost,
 Annualized_Unsplit_Average_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_Min_Cost,
 Annualized_Unsplit_Max_Cost,
 Annualized_Unsplit_STDEV,
 Annualized_Unsplit_CV,
 Annualized_Unsplit_Total_PAC_Cost,
 Annualized_Unsplit_Average_PAC_Cost,
 Annualized_Unsplit_PAC_Percent,
 Annualized_Unsplit_Total_Typical_Cost,
 Annualized_Unsplit_Average_Typical_Cost,
 Annualized_Unsplit_Total_TypicalwPAC_Cost,
 Annualized_Unsplit_Average_TypicalwPAC_Cost,
 Expected_Split_Average_Cost,
 Expected_Split_Typical_IP_Average_Cost,
 Expected_Split_Typical_Other_Average_Cost,
 Expected_Split_PAC_Average_Cost,
 Expected_Unsplit_Average_Cost,
 Expected_Unsplit_Typical_IP_Average_Cost,
 Expected_Unsplit_Typical_Other_Average_Cost,
 Expected_Unsplit_PAC_Average_Cost
)
AS
 SELECT report_episode_summary_test.Filter_ID,
        report_episode_summary_test.Episode_ID,
        report_episode_summary_test.Episode_Name,
        report_episode_summary_test.Episode_Description,
        report_episode_summary_test.Episode_Type,
        report_episode_summary_test.MDC,
        report_episode_summary_test.MDC_Description,
        report_episode_summary_test.Level,
        report_episode_summary_test.Episode_Volume,
        report_episode_summary_test.Split_Total_Cost,
        report_episode_summary_test.Split_Average_Cost,
        report_episode_summary_test.Split_1stPercentile_Cost,
        report_episode_summary_test.Split_99thPercentile_Cost,
        report_episode_summary_test.Split_Min_Cost,
        report_episode_summary_test.Split_Max_Cost,
        report_episode_summary_test.Split_STDEV,
        report_episode_summary_test.Split_CV,
        report_episode_summary_test.Split_Total_PAC_Cost,
        report_episode_summary_test.Split_Average_PAC_Cost,
        report_episode_summary_test.Split_PAC_Percent,
        report_episode_summary_test.Split_Total_Typical_Cost,
        report_episode_summary_test.Split_Average_Typical_Cost,
        report_episode_summary_test.Split_Total_TypicalwPAC_Cost,
        report_episode_summary_test.Split_Average_TypicalwPAC_Cost,
        report_episode_summary_test.Annualized_Split_Total_Cost,
        report_episode_summary_test.Annualized_Split_Average_Cost,
        report_episode_summary_test.Annualized_Split_1stPercentile_Cost,
        report_episode_summary_test.Annualized_Split_99thPercentile_Cost,
        report_episode_summary_test.Annualized_Split_Min_Cost,
        report_episode_summary_test.Annualized_Split_Max_Cost,
        report_episode_summary_test.Annualized_Split_STDEV,
        report_episode_summary_test.Annualized_Split_CV,
        report_episode_summary_test.Annualized_Split_Total_PAC_Cost,
        report_episode_summary_test.Annualized_Split_Average_PAC_Cost,
        report_episode_summary_test.Annualized_Split_PAC_Percent,
        report_episode_summary_test.Annualized_Split_Total_Typical_Cost,
        report_episode_summary_test.Annualized_Split_Average_Typical_Cost,
        report_episode_summary_test.Annualized_Split_Total_TypicalwPAC_Cost,
        report_episode_summary_test.Annualized_Split_Average_TypicalwPAC_Cost,
        report_episode_summary_test.Unsplit_Total_Cost,
        report_episode_summary_test.Unsplit_Average_Cost,
        report_episode_summary_test.Unsplit_1stPercentile_Cost,
        report_episode_summary_test.Unsplit_99thPercentile_Cost,
        report_episode_summary_test.Unsplit_Min_Cost,
        report_episode_summary_test.Unsplit_Max_Cost,
        report_episode_summary_test.Unsplit_STDEV,
        report_episode_summary_test.Unsplit_CV,
        report_episode_summary_test.Unsplit_Total_PAC_Cost,
        report_episode_summary_test.Unsplit_Average_PAC_Cost,
        report_episode_summary_test.Unsplit_PAC_Percent,
        report_episode_summary_test.Unsplit_Total_Typical_Cost,
        report_episode_summary_test.Unsplit_Average_Typical_Cost,
        report_episode_summary_test.Unsplit_Total_TypicalwPAC_Cost,
        report_episode_summary_test.Unsplit_Average_TypicalwPAC_Cost,
        report_episode_summary_test.Annualized_Unsplit_Total_Cost,
        report_episode_summary_test.Annualized_Unsplit_Average_Cost,
        report_episode_summary_test.Annualized_Unsplit_1stPercentile_Cost,
        report_episode_summary_test.Annualized_Unsplit_99thPercentile_Cost,
        report_episode_summary_test.Annualized_Unsplit_Min_Cost,
        report_episode_summary_test.Annualized_Unsplit_Max_Cost,
        report_episode_summary_test.Annualized_Unsplit_STDEV,
        report_episode_summary_test.Annualized_Unsplit_CV,
        report_episode_summary_test.Annualized_Unsplit_Total_PAC_Cost,
        report_episode_summary_test.Annualized_Unsplit_Average_PAC_Cost,
        report_episode_summary_test.Annualized_Unsplit_PAC_Percent,
        report_episode_summary_test.Annualized_Unsplit_Total_Typical_Cost,
        report_episode_summary_test.Annualized_Unsplit_Average_Typical_Cost,
        report_episode_summary_test.Annualized_Unsplit_Total_TypicalwPAC_Cost,
        report_episode_summary_test.Annualized_Unsplit_Average_TypicalwPAC_Cost,
        report_episode_summary_test.Expected_Split_Average_Cost,
        report_episode_summary_test.Expected_Split_Typical_IP_Average_Cost,
        report_episode_summary_test.Expected_Split_Typical_Other_Average_Cost,
        report_episode_summary_test.Expected_Split_PAC_Average_Cost,
        report_episode_summary_test.Expected_Unsplit_Average_Cost,
        report_episode_summary_test.Expected_Unsplit_Typical_IP_Average_Cost,
        report_episode_summary_test.Expected_Unsplit_Typical_Other_Average_Cost,
        report_episode_summary_test.Expected_Unsplit_PAC_Average_Cost
 FROM epbuilder.report_episode_summary_test
 ORDER BY report_episode_summary_test.Episode_ID,
          report_episode_summary_test.Level
SEGMENTED BY hash(report_episode_summary_test.Episode_ID, report_episode_summary_test.Level) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.dual_eligable /*+createtype(A)*/ 
(
 member_id,
 year,
 DUAL_ELIGIBILITY_FLAG
)
AS
 SELECT dual_eligable.member_id,
        dual_eligable.year,
        dual_eligable.DUAL_ELIGIBILITY_FLAG
 FROM epbuilder.dual_eligable
 ORDER BY dual_eligable.member_id,
          dual_eligable.year
SEGMENTED BY hash(dual_eligable.member_id, dual_eligable.year) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.TCGP_2012_by_member_less_maternity_with_age /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group,
 pps,
 pcp,
 total_cost,
 maternity_costs_2012,
 total_costs_less_maternity_2012
)
AS
 SELECT TCGP_2012_by_member_less_maternity_with_age.member_id,
        TCGP_2012_by_member_less_maternity_with_age.sub_population,
        TCGP_2012_by_member_less_maternity_with_age.gender,
        TCGP_2012_by_member_less_maternity_with_age.birth_year,
        TCGP_2012_by_member_less_maternity_with_age.age_group,
        TCGP_2012_by_member_less_maternity_with_age.pps,
        TCGP_2012_by_member_less_maternity_with_age.pcp,
        TCGP_2012_by_member_less_maternity_with_age.total_cost,
        TCGP_2012_by_member_less_maternity_with_age.maternity_costs_2012,
        TCGP_2012_by_member_less_maternity_with_age.total_costs_less_maternity_2012
 FROM epbuilder.TCGP_2012_by_member_less_maternity_with_age
 ORDER BY TCGP_2012_by_member_less_maternity_with_age.member_id,
          TCGP_2012_by_member_less_maternity_with_age.sub_population,
          TCGP_2012_by_member_less_maternity_with_age.gender,
          TCGP_2012_by_member_less_maternity_with_age.birth_year,
          TCGP_2012_by_member_less_maternity_with_age.pps,
          TCGP_2012_by_member_less_maternity_with_age.pcp
SEGMENTED BY hash(TCGP_2012_by_member_less_maternity_with_age.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.TCGP_2012_by_member_less_maternity_with_age_crg /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group,
 pps,
 pcp,
 total_cost,
 maternity_costs_2012,
 total_costs_less_maternity_2012,
 fincrg,
 year,
 quarter
)
AS
 SELECT TCGP_2012_by_member_less_maternity_with_age_crg.member_id,
        TCGP_2012_by_member_less_maternity_with_age_crg.sub_population,
        TCGP_2012_by_member_less_maternity_with_age_crg.gender,
        TCGP_2012_by_member_less_maternity_with_age_crg.birth_year,
        TCGP_2012_by_member_less_maternity_with_age_crg.age_group,
        TCGP_2012_by_member_less_maternity_with_age_crg.pps,
        TCGP_2012_by_member_less_maternity_with_age_crg.pcp,
        TCGP_2012_by_member_less_maternity_with_age_crg.total_cost,
        TCGP_2012_by_member_less_maternity_with_age_crg.maternity_costs_2012,
        TCGP_2012_by_member_less_maternity_with_age_crg.total_costs_less_maternity_2012,
        TCGP_2012_by_member_less_maternity_with_age_crg.fincrg,
        TCGP_2012_by_member_less_maternity_with_age_crg.year,
        TCGP_2012_by_member_less_maternity_with_age_crg.quarter
 FROM epbuilder.TCGP_2012_by_member_less_maternity_with_age_crg
 ORDER BY TCGP_2012_by_member_less_maternity_with_age_crg.member_id,
          TCGP_2012_by_member_less_maternity_with_age_crg.sub_population,
          TCGP_2012_by_member_less_maternity_with_age_crg.gender,
          TCGP_2012_by_member_less_maternity_with_age_crg.birth_year,
          TCGP_2012_by_member_less_maternity_with_age_crg.age_group,
          TCGP_2012_by_member_less_maternity_with_age_crg.pps,
          TCGP_2012_by_member_less_maternity_with_age_crg.pcp,
          TCGP_2012_by_member_less_maternity_with_age_crg.total_cost,
          TCGP_2012_by_member_less_maternity_with_age_crg.maternity_costs_2012,
          TCGP_2012_by_member_less_maternity_with_age_crg.total_costs_less_maternity_2012,
          TCGP_2012_by_member_less_maternity_with_age_crg.fincrg,
          TCGP_2012_by_member_less_maternity_with_age_crg.year,
          TCGP_2012_by_member_less_maternity_with_age_crg.quarter
SEGMENTED BY hash(TCGP_2012_by_member_less_maternity_with_age_crg.gender, TCGP_2012_by_member_less_maternity_with_age_crg.birth_year, TCGP_2012_by_member_less_maternity_with_age_crg.age_group, TCGP_2012_by_member_less_maternity_with_age_crg.fincrg, TCGP_2012_by_member_less_maternity_with_age_crg.year, TCGP_2012_by_member_less_maternity_with_age_crg.quarter, TCGP_2012_by_member_less_maternity_with_age_crg.sub_population, TCGP_2012_by_member_less_maternity_with_age_crg.total_cost, TCGP_2012_by_member_less_maternity_with_age_crg.maternity_costs_2012, TCGP_2012_by_member_less_maternity_with_age_crg.total_costs_less_maternity_2012, TCGP_2012_by_member_less_maternity_with_age_crg.member_id, TCGP_2012_by_member_less_maternity_with_age_crg.pps, TCGP_2012_by_member_less_maternity_with_age_crg.pcp) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.members_by_crg /*+createtype(A)*/ 
(
 year,
 fincrg,
 age_group,
 gender,
 member_count,
 total_cost,
 average_cost,
 total_cost_less_maternity,
 average_cost_less_maternity
)
AS
 SELECT members_by_crg.year,
        members_by_crg.fincrg,
        members_by_crg.age_group,
        members_by_crg.gender,
        members_by_crg.member_count,
        members_by_crg.total_cost,
        members_by_crg.average_cost,
        members_by_crg.total_cost_less_maternity,
        members_by_crg.average_cost_less_maternity
 FROM epbuilder.members_by_crg
 ORDER BY members_by_crg.year,
          members_by_crg.fincrg,
          members_by_crg.age_group,
          members_by_crg.gender
SEGMENTED BY hash(members_by_crg.year, members_by_crg.fincrg, members_by_crg.age_group, members_by_crg.gender) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.TCGP_2012_by_member_with_expected_costs /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group,
 pps,
 pcp,
 total_cost,
 maternity_costs_2012,
 total_costs_less_maternity_2012,
 fincrg,
 year,
 quarter,
 expected_cost_with_maternity,
 expected_cost_without_maternity
)
AS
 SELECT TCGP_2012_by_member_with_expected_costs.member_id,
        TCGP_2012_by_member_with_expected_costs.sub_population,
        TCGP_2012_by_member_with_expected_costs.gender,
        TCGP_2012_by_member_with_expected_costs.birth_year,
        TCGP_2012_by_member_with_expected_costs.age_group,
        TCGP_2012_by_member_with_expected_costs.pps,
        TCGP_2012_by_member_with_expected_costs.pcp,
        TCGP_2012_by_member_with_expected_costs.total_cost,
        TCGP_2012_by_member_with_expected_costs.maternity_costs_2012,
        TCGP_2012_by_member_with_expected_costs.total_costs_less_maternity_2012,
        TCGP_2012_by_member_with_expected_costs.fincrg,
        TCGP_2012_by_member_with_expected_costs.year,
        TCGP_2012_by_member_with_expected_costs.quarter,
        TCGP_2012_by_member_with_expected_costs.expected_cost_with_maternity,
        TCGP_2012_by_member_with_expected_costs.expected_cost_without_maternity
 FROM epbuilder.TCGP_2012_by_member_with_expected_costs
 ORDER BY TCGP_2012_by_member_with_expected_costs.member_id,
          TCGP_2012_by_member_with_expected_costs.sub_population,
          TCGP_2012_by_member_with_expected_costs.gender,
          TCGP_2012_by_member_with_expected_costs.birth_year,
          TCGP_2012_by_member_with_expected_costs.age_group,
          TCGP_2012_by_member_with_expected_costs.pps,
          TCGP_2012_by_member_with_expected_costs.pcp,
          TCGP_2012_by_member_with_expected_costs.total_cost,
          TCGP_2012_by_member_with_expected_costs.maternity_costs_2012,
          TCGP_2012_by_member_with_expected_costs.total_costs_less_maternity_2012,
          TCGP_2012_by_member_with_expected_costs.fincrg,
          TCGP_2012_by_member_with_expected_costs.year,
          TCGP_2012_by_member_with_expected_costs.quarter
SEGMENTED BY hash(TCGP_2012_by_member_with_expected_costs.fincrg, TCGP_2012_by_member_with_expected_costs.age_group, TCGP_2012_by_member_with_expected_costs.gender) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.TCGP_2013_by_member_with_expected_cost /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group,
 pps,
 pcp,
 total_cost,
 maternity_costs_2013,
 total_costs_less_maternity_2013,
 fincrg,
 expected_cost,
 expected_less_actual_with_maternity,
 expected_cost_less_maternity,
 expected_less_actual_without_maternity
)
AS
 SELECT TCGP_2013_by_member_with_expected_cost.member_id,
        TCGP_2013_by_member_with_expected_cost.sub_population,
        TCGP_2013_by_member_with_expected_cost.gender,
        TCGP_2013_by_member_with_expected_cost.birth_year,
        TCGP_2013_by_member_with_expected_cost.age_group,
        TCGP_2013_by_member_with_expected_cost.pps,
        TCGP_2013_by_member_with_expected_cost.pcp,
        TCGP_2013_by_member_with_expected_cost.total_cost,
        TCGP_2013_by_member_with_expected_cost.maternity_costs_2013,
        TCGP_2013_by_member_with_expected_cost.total_costs_less_maternity_2013,
        TCGP_2013_by_member_with_expected_cost.fincrg,
        TCGP_2013_by_member_with_expected_cost.expected_cost,
        TCGP_2013_by_member_with_expected_cost.expected_less_actual_with_maternity,
        TCGP_2013_by_member_with_expected_cost.expected_cost_less_maternity,
        TCGP_2013_by_member_with_expected_cost.expected_less_actual_without_maternity
 FROM epbuilder.TCGP_2013_by_member_with_expected_cost
 ORDER BY TCGP_2013_by_member_with_expected_cost.member_id,
          TCGP_2013_by_member_with_expected_cost.sub_population,
          TCGP_2013_by_member_with_expected_cost.gender,
          TCGP_2013_by_member_with_expected_cost.birth_year,
          TCGP_2013_by_member_with_expected_cost.age_group,
          TCGP_2013_by_member_with_expected_cost.pps,
          TCGP_2013_by_member_with_expected_cost.pcp,
          TCGP_2013_by_member_with_expected_cost.total_cost,
          TCGP_2013_by_member_with_expected_cost.maternity_costs_2013,
          TCGP_2013_by_member_with_expected_cost.total_costs_less_maternity_2013,
          TCGP_2013_by_member_with_expected_cost.fincrg
SEGMENTED BY hash(TCGP_2013_by_member_with_expected_cost.fincrg, TCGP_2013_by_member_with_expected_cost.age_group, TCGP_2013_by_member_with_expected_cost.gender) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.TCGP_2013_by_pps /*+createtype(A)*/ 
(
 pps,
 count,
 average_actual_w_maternity,
 average_expected_w_maternity,
 efficiency_score_w_maternity,
 average_actual_without_maternity,
 average_expected_without_maternity,
 efficiency_score_without_maternity,
 efficiency_score_difference,
 expected_less_actual_with_maternity,
 expected_less_actual_without_maternity,
 difference,
 difference_per_member
)
AS
 SELECT TCGP_2013_by_pps.pps,
        TCGP_2013_by_pps.count,
        TCGP_2013_by_pps.average_actual_w_maternity,
        TCGP_2013_by_pps.average_expected_w_maternity,
        TCGP_2013_by_pps.efficiency_score_w_maternity,
        TCGP_2013_by_pps.average_actual_without_maternity,
        TCGP_2013_by_pps.average_expected_without_maternity,
        TCGP_2013_by_pps.efficiency_score_without_maternity,
        TCGP_2013_by_pps.efficiency_score_difference,
        TCGP_2013_by_pps.expected_less_actual_with_maternity,
        TCGP_2013_by_pps.expected_less_actual_without_maternity,
        TCGP_2013_by_pps.difference,
        TCGP_2013_by_pps.difference_per_member
 FROM epbuilder.TCGP_2013_by_pps
 ORDER BY TCGP_2013_by_pps.pps
SEGMENTED BY hash(TCGP_2013_by_pps.pps) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PPR_2013_RA /*+createtype(L)*/ 
(
 Claim_ID,
 CLAIM_TRANS_ID,
 Date,
 Claim_Type_Code,
 Preventable_Group,
 Preventable_Group_Desc,
 PPR_Type_Code,
 PPR_Type_Code_Desc
)
AS
 SELECT PPR_2013_RA.Claim_ID,
        PPR_2013_RA.CLAIM_TRANS_ID,
        PPR_2013_RA.Date,
        PPR_2013_RA.Claim_Type_Code,
        PPR_2013_RA.Preventable_Group,
        PPR_2013_RA.Preventable_Group_Desc,
        PPR_2013_RA.PPR_Type_Code,
        PPR_2013_RA.PPR_Type_Code_Desc
 FROM epbuilder.PPR_2013_RA
 ORDER BY PPR_2013_RA.Claim_ID,
          PPR_2013_RA.CLAIM_TRANS_ID,
          PPR_2013_RA.Date,
          PPR_2013_RA.Claim_Type_Code,
          PPR_2013_RA.Preventable_Group,
          PPR_2013_RA.Preventable_Group_Desc,
          PPR_2013_RA.PPR_Type_Code,
          PPR_2013_RA.PPR_Type_Code_Desc
SEGMENTED BY hash(PPR_2013_RA.Claim_ID, PPR_2013_RA.CLAIM_TRANS_ID, PPR_2013_RA.Date, PPR_2013_RA.Claim_Type_Code, PPR_2013_RA.Preventable_Group, PPR_2013_RA.Preventable_Group_Desc, PPR_2013_RA.PPR_Type_Code, PPR_2013_RA.PPR_Type_Code_Desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_Grid_Output /*+createtype(A)*/ 
(
 result,
 sub_population,
 Mem_Count,
 IP_ST_PPA,
 IP_SNF_PPA,
 IP_OTHER_PPA,
 OP_ED_PPA,
 OP_OTHER_PPA,
 PB_OTHER_PPA,
 PB_NOT_OTHER_PPA,
 IP_ST_PPR,
 IP_SNF_PPR,
 IP_OTHER_PPR,
 OP_ED_PPR,
 OP_OTHER_PPR,
 PB_OTHER_PPR,
 PB_NOT_OTHER_PPR,
 IP_ST_PPV,
 IP_SNF_PPV,
 IP_OTHER_PPV,
 OP_ED_PPV,
 OP_OTHER_PPV,
 PB_OTHER_PPV,
 PB_NOT_OTHER_PPV,
 IP_ST_PAC,
 IP_SNF_PAC,
 IP_OTHER_PAC,
 OP_ED_PAC,
 OP_OTHER_PAC,
 PB_OTHER_PAC,
 PB_NOT_OTHER_PAC,
 IP_ST_ALL,
 IP_SNF_ALL,
 IP_OTHER_ALL,
 OP_ED_ALL,
 OP_OTHER_ALL,
 PB_OTHER_ALL,
 PB_NOT_OTHER_ALL
)
AS
 SELECT NY_Grid_Output.result,
        NY_Grid_Output.sub_population,
        NY_Grid_Output.Mem_Count,
        NY_Grid_Output.IP_ST_PPA,
        NY_Grid_Output.IP_SNF_PPA,
        NY_Grid_Output.IP_OTHER_PPA,
        NY_Grid_Output.OP_ED_PPA,
        NY_Grid_Output.OP_OTHER_PPA,
        NY_Grid_Output.PB_OTHER_PPA,
        NY_Grid_Output.PB_NOT_OTHER_PPA,
        NY_Grid_Output.IP_ST_PPR,
        NY_Grid_Output.IP_SNF_PPR,
        NY_Grid_Output.IP_OTHER_PPR,
        NY_Grid_Output.OP_ED_PPR,
        NY_Grid_Output.OP_OTHER_PPR,
        NY_Grid_Output.PB_OTHER_PPR,
        NY_Grid_Output.PB_NOT_OTHER_PPR,
        NY_Grid_Output.IP_ST_PPV,
        NY_Grid_Output.IP_SNF_PPV,
        NY_Grid_Output.IP_OTHER_PPV,
        NY_Grid_Output.OP_ED_PPV,
        NY_Grid_Output.OP_OTHER_PPV,
        NY_Grid_Output.PB_OTHER_PPV,
        NY_Grid_Output.PB_NOT_OTHER_PPV,
        NY_Grid_Output.IP_ST_PAC,
        NY_Grid_Output.IP_SNF_PAC,
        NY_Grid_Output.IP_OTHER_PAC,
        NY_Grid_Output.OP_ED_PAC,
        NY_Grid_Output.OP_OTHER_PAC,
        NY_Grid_Output.PB_OTHER_PAC,
        NY_Grid_Output.PB_NOT_OTHER_PAC,
        NY_Grid_Output.IP_ST_ALL,
        NY_Grid_Output.IP_SNF_ALL,
        NY_Grid_Output.IP_OTHER_ALL,
        NY_Grid_Output.OP_ED_ALL,
        NY_Grid_Output.OP_OTHER_ALL,
        NY_Grid_Output.PB_OTHER_ALL,
        NY_Grid_Output.PB_NOT_OTHER_ALL
 FROM epbuilder.NY_Grid_Output
 ORDER BY NY_Grid_Output.sub_population
SEGMENTED BY hash(NY_Grid_Output.sub_population) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.cooccurance_of_chronic_episodes /*+createtype(A)*/ 
(
 master_episode_id,
 level,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_count_chronic,
 co_count_all,
 year
)
AS
 SELECT cooccurance_of_chronic_episodes.master_episode_id,
        cooccurance_of_chronic_episodes.level,
        cooccurance_of_chronic_episodes.co_occurence_count_ASTHMA,
        cooccurance_of_chronic_episodes.co_occurence_count_ARRBLK,
        cooccurance_of_chronic_episodes.co_occurence_count_HF,
        cooccurance_of_chronic_episodes.co_occurence_count_COPD,
        cooccurance_of_chronic_episodes.co_occurence_count_CAD,
        cooccurance_of_chronic_episodes.co_occurence_count_ULCLTS,
        cooccurance_of_chronic_episodes.co_occurence_count_BIPLR,
        cooccurance_of_chronic_episodes.co_occurence_count_GERD,
        cooccurance_of_chronic_episodes.co_occurence_count_HTN,
        cooccurance_of_chronic_episodes.co_occurence_count_GLCOMA,
        cooccurance_of_chronic_episodes.co_occurence_count_LBP,
        cooccurance_of_chronic_episodes.co_occurence_count_CROHNS,
        cooccurance_of_chronic_episodes.co_occurence_count_DIAB,
        cooccurance_of_chronic_episodes.co_occurence_count_DEPRSN,
        cooccurance_of_chronic_episodes.co_occurence_count_OSTEOA,
        cooccurance_of_chronic_episodes.co_occurence_count_RHNTS,
        cooccurance_of_chronic_episodes.co_occurence_count_DIVERT,
        cooccurance_of_chronic_episodes.co_occurence_count_DEPANX,
        cooccurance_of_chronic_episodes.co_occurence_count_PTSD,
        cooccurance_of_chronic_episodes.co_occurence_count_SCHIZO,
        cooccurance_of_chronic_episodes.co_occurence_count_SUDS,
        cooccurance_of_chronic_episodes.co_count_chronic,
        cooccurance_of_chronic_episodes.co_count_all,
        cooccurance_of_chronic_episodes.year
 FROM epbuilder.cooccurance_of_chronic_episodes
 ORDER BY cooccurance_of_chronic_episodes.master_episode_id,
          cooccurance_of_chronic_episodes.level
SEGMENTED BY hash(cooccurance_of_chronic_episodes.master_episode_id, cooccurance_of_chronic_episodes.level) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.all_mom /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT all_mom.member_id
 FROM epbuilder.all_mom
 ORDER BY all_mom.member_id
SEGMENTED BY hash(all_mom.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.OP_ED_Count /*+createtype(A)*/ 
(
 OP_ED,
 Count
)
AS
 SELECT OP_ED_Count.OP_ED,
        OP_ED_Count.Count
 FROM epbuilder.OP_ED_Count
 ORDER BY OP_ED_Count.OP_ED
SEGMENTED BY hash(OP_ED_Count.OP_ED) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.op_claims /*+createtype(A)*/ 
(
 claim_id,
 count
)
AS
 SELECT op_claims.claim_id,
        op_claims.count
 FROM epbuilder.op_claims
 ORDER BY op_claims.claim_id
SEGMENTED BY hash(op_claims.claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.multiple /*+createtype(A)*/ 
(
 claim_id,
 count
)
AS
 SELECT multiple.claim_id,
        multiple.count
 FROM epbuilder.multiple
 ORDER BY multiple.claim_id
SEGMENTED BY hash(multiple.claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_Grid_Check /*+createtype(A)*/ 
(
 Data,
 sub_population,
 Mem_Count,
 IP_ST,
 IP_OTHER,
 OP_ED,
 OP_OTHER,
 PB
)
AS
 SELECT NY_Grid_Check.Data,
        NY_Grid_Check.sub_population,
        NY_Grid_Check.Mem_Count,
        NY_Grid_Check.IP_ST,
        NY_Grid_Check.IP_OTHER,
        NY_Grid_Check.OP_ED,
        NY_Grid_Check.OP_OTHER,
        NY_Grid_Check.PB
 FROM epbuilder.NY_Grid_Check
 ORDER BY NY_Grid_Check.sub_population
SEGMENTED BY hash(NY_Grid_Check.sub_population) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PQI_PDI_2013 /*+createtype(A)*/ 
(
 claim_id,
 p_type
)
AS
 SELECT PQI_PDI_2013.claim_id,
        PQI_PDI_2013.p_type
 FROM epbuilder.PQI_PDI_2013
 ORDER BY PQI_PDI_2013.claim_id,
          PQI_PDI_2013.p_type
SEGMENTED BY hash(PQI_PDI_2013.claim_id, PQI_PDI_2013.p_type) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.all_claim_pdi_pqi /*+createtype(A)*/ 
(
 claim_id,
 claim_line_type_code,
 pdi,
 pqi
)
AS
 SELECT all_claim_pdi_pqi.claim_id,
        all_claim_pdi_pqi.claim_line_type_code,
        all_claim_pdi_pqi.pdi,
        all_claim_pdi_pqi.pqi
 FROM epbuilder.all_claim_pdi_pqi
 ORDER BY all_claim_pdi_pqi.claim_id,
          all_claim_pdi_pqi.claim_line_type_code,
          all_claim_pdi_pqi.pdi,
          all_claim_pdi_pqi.pqi
SEGMENTED BY hash(all_claim_pdi_pqi.claim_id, all_claim_pdi_pqi.claim_line_type_code, all_claim_pdi_pqi.pdi, all_claim_pdi_pqi.pqi) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_PDI_PQI_Output_Total /*+createtype(A)*/ 
(
 sub_population,
 Mem_Count,
 IP_ST_PDI,
 IP_OTHER_PDI,
 OP_ED_PDI,
 OP_OTHER_PDI,
 PB_PDI,
 RX_PDI,
 IP_ST_PQI,
 IP_OTHER_PQI,
 OP_ED_PQI,
 OP_OTHER_PQI,
 PB_PQI,
 RX_PQI,
 IP_ST_PAC,
 IP_OTHER_PAC,
 OP_ED_PAC,
 OP_OTHER_PAC,
 PB_PAC,
 RX_PAC
)
AS
 SELECT NY_PDI_PQI_Output_Total.sub_population,
        NY_PDI_PQI_Output_Total.Mem_Count,
        NY_PDI_PQI_Output_Total.IP_ST_PDI,
        NY_PDI_PQI_Output_Total.IP_OTHER_PDI,
        NY_PDI_PQI_Output_Total.OP_ED_PDI,
        NY_PDI_PQI_Output_Total.OP_OTHER_PDI,
        NY_PDI_PQI_Output_Total.PB_PDI,
        NY_PDI_PQI_Output_Total.RX_PDI,
        NY_PDI_PQI_Output_Total.IP_ST_PQI,
        NY_PDI_PQI_Output_Total.IP_OTHER_PQI,
        NY_PDI_PQI_Output_Total.OP_ED_PQI,
        NY_PDI_PQI_Output_Total.OP_OTHER_PQI,
        NY_PDI_PQI_Output_Total.PB_PQI,
        NY_PDI_PQI_Output_Total.RX_PQI,
        NY_PDI_PQI_Output_Total.IP_ST_PAC,
        NY_PDI_PQI_Output_Total.IP_OTHER_PAC,
        NY_PDI_PQI_Output_Total.OP_ED_PAC,
        NY_PDI_PQI_Output_Total.OP_OTHER_PAC,
        NY_PDI_PQI_Output_Total.PB_PAC,
        NY_PDI_PQI_Output_Total.RX_PAC
 FROM epbuilder.NY_PDI_PQI_Output_Total
 ORDER BY NY_PDI_PQI_Output_Total.sub_population
SEGMENTED BY hash(NY_PDI_PQI_Output_Total.sub_population) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ppa_example /*+createtype(A)*/ 
(
 claim_key,
 claim_id,
 claim_trans_id,
 dsch_dt,
 claim_type_code,
 preventable_group,
 pfp_preventable_group_name,
 pfp_preventable_group_desc,
 preventable_status,
 pfp_preventable_status_desc,
 preventable_reason,
 pfp_preventable_reason_desc,
 mdw_insert_dt,
 mdw_update_dt
)
AS
 SELECT ppa_example.claim_key,
        ppa_example.claim_id,
        ppa_example.claim_trans_id,
        ppa_example.dsch_dt,
        ppa_example.claim_type_code,
        ppa_example.preventable_group,
        ppa_example.pfp_preventable_group_name,
        ppa_example.pfp_preventable_group_desc,
        ppa_example.preventable_status,
        ppa_example.pfp_preventable_status_desc,
        ppa_example.preventable_reason,
        ppa_example.pfp_preventable_reason_desc,
        ppa_example.mdw_insert_dt,
        ppa_example.mdw_update_dt
 FROM epbuilder.ppa_example
 ORDER BY ppa_example.claim_key,
          ppa_example.claim_id,
          ppa_example.claim_trans_id,
          ppa_example.dsch_dt,
          ppa_example.claim_type_code,
          ppa_example.preventable_group,
          ppa_example.pfp_preventable_group_name,
          ppa_example.pfp_preventable_group_desc,
          ppa_example.preventable_status,
          ppa_example.pfp_preventable_status_desc,
          ppa_example.preventable_reason,
          ppa_example.pfp_preventable_reason_desc,
          ppa_example.mdw_insert_dt,
          ppa_example.mdw_update_dt
SEGMENTED BY hash(ppa_example.dsch_dt, ppa_example.claim_type_code, ppa_example.preventable_group, ppa_example.preventable_status, ppa_example.preventable_reason, ppa_example.mdw_insert_dt, ppa_example.mdw_update_dt, ppa_example.claim_key, ppa_example.claim_trans_id, ppa_example.claim_id, ppa_example.pfp_preventable_group_name, ppa_example.pfp_preventable_group_desc, ppa_example.pfp_preventable_status_desc, ppa_example.pfp_preventable_reason_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ppr_example /*+createtype(A)*/ 
(
 Claim_ID,
 CLAIM_TRANS_ID,
 Date,
 Claim_Type_Code,
 Preventable_Group,
 Preventable_Group_Desc,
 PPR_Type_Code,
 PPR_Type_Code_Desc
)
AS
 SELECT ppr_example.Claim_ID,
        ppr_example.CLAIM_TRANS_ID,
        ppr_example.Date,
        ppr_example.Claim_Type_Code,
        ppr_example.Preventable_Group,
        ppr_example.Preventable_Group_Desc,
        ppr_example.PPR_Type_Code,
        ppr_example.PPR_Type_Code_Desc
 FROM epbuilder.ppr_example
 ORDER BY ppr_example.Claim_ID,
          ppr_example.CLAIM_TRANS_ID,
          ppr_example.Date,
          ppr_example.Claim_Type_Code,
          ppr_example.Preventable_Group,
          ppr_example.Preventable_Group_Desc,
          ppr_example.PPR_Type_Code,
          ppr_example.PPR_Type_Code_Desc
SEGMENTED BY hash(ppr_example.Claim_ID, ppr_example.CLAIM_TRANS_ID, ppr_example.Date, ppr_example.Claim_Type_Code, ppr_example.Preventable_Group, ppr_example.Preventable_Group_Desc, ppr_example.PPR_Type_Code, ppr_example.PPR_Type_Code_Desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PPVs_OP_ED_by_ccs /*+createtype(A)*/ 
(
 sub_population,
 ccs_group,
 claim_volume,
 total_cost
)
AS
 SELECT PPVs_OP_ED_by_ccs.sub_population,
        PPVs_OP_ED_by_ccs.ccs_group,
        PPVs_OP_ED_by_ccs.claim_volume,
        PPVs_OP_ED_by_ccs.total_cost
 FROM epbuilder.PPVs_OP_ED_by_ccs
 ORDER BY PPVs_OP_ED_by_ccs.sub_population,
          PPVs_OP_ED_by_ccs.ccs_group
SEGMENTED BY hash(PPVs_OP_ED_by_ccs.sub_population, PPVs_OP_ED_by_ccs.ccs_group) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_PDI_PQI_Output_Exclusive /*+createtype(A)*/ 
(
 sub_population,
 Mem_Count,
 IP_ST_PDI_less_PAC,
 IP_OTHER_PDI_less_PAC,
 OP_ED_PDI_less_PAC,
 OP_OTHER_PDI_less_PAC,
 PB_PDI_less_PAC,
 RX_PDI_less_PAC,
 IP_ST_PQI_less_PAC,
 IP_OTHER_PQI_less_PAC,
 OP_ED_PQI_less_PAC,
 OP_OTHER_PQI_less_PAC,
 PB_PQI_less_PAC,
 RX_PQI_less_PAC,
 IP_ST_PAC_less_PDI_PQI,
 IP_OTHER_PAC_less_PDI_PQI,
 OP_ED_PAC_less_PDI_PQI,
 OP_OTHER_PAC_less_PDI_PQI,
 PB_PAC_less_PDI_PQI,
 RX_PAC_less_PDI_PQI,
 IP_ST_ALL,
 IP_OTHER_ALL,
 OP_ED_ALL,
 OP_OTHER_ALL,
 PB_ALL,
 RX_ALL
)
AS
 SELECT NY_PDI_PQI_Output_Exclusive.sub_population,
        NY_PDI_PQI_Output_Exclusive.Mem_Count,
        NY_PDI_PQI_Output_Exclusive.IP_ST_PDI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.IP_OTHER_PDI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.OP_ED_PDI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.OP_OTHER_PDI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.PB_PDI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.RX_PDI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.IP_ST_PQI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.IP_OTHER_PQI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.OP_ED_PQI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.OP_OTHER_PQI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.PB_PQI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.RX_PQI_less_PAC,
        NY_PDI_PQI_Output_Exclusive.IP_ST_PAC_less_PDI_PQI,
        NY_PDI_PQI_Output_Exclusive.IP_OTHER_PAC_less_PDI_PQI,
        NY_PDI_PQI_Output_Exclusive.OP_ED_PAC_less_PDI_PQI,
        NY_PDI_PQI_Output_Exclusive.OP_OTHER_PAC_less_PDI_PQI,
        NY_PDI_PQI_Output_Exclusive.PB_PAC_less_PDI_PQI,
        NY_PDI_PQI_Output_Exclusive.RX_PAC_less_PDI_PQI,
        NY_PDI_PQI_Output_Exclusive.IP_ST_ALL,
        NY_PDI_PQI_Output_Exclusive.IP_OTHER_ALL,
        NY_PDI_PQI_Output_Exclusive.OP_ED_ALL,
        NY_PDI_PQI_Output_Exclusive.OP_OTHER_ALL,
        NY_PDI_PQI_Output_Exclusive.PB_ALL,
        NY_PDI_PQI_Output_Exclusive.RX_ALL
 FROM epbuilder.NY_PDI_PQI_Output_Exclusive
 ORDER BY NY_PDI_PQI_Output_Exclusive.sub_population
SEGMENTED BY hash(NY_PDI_PQI_Output_Exclusive.sub_population) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PACs_IP_ST_by_ccs /*+createtype(A)*/ 
(
 sub_population,
 ccs_group,
 claim_volume,
 total_cost
)
AS
 SELECT PACs_IP_ST_by_ccs.sub_population,
        PACs_IP_ST_by_ccs.ccs_group,
        PACs_IP_ST_by_ccs.claim_volume,
        PACs_IP_ST_by_ccs.total_cost
 FROM epbuilder.PACs_IP_ST_by_ccs
 ORDER BY PACs_IP_ST_by_ccs.sub_population,
          PACs_IP_ST_by_ccs.ccs_group
SEGMENTED BY hash(PACs_IP_ST_by_ccs.sub_population, PACs_IP_ST_by_ccs.ccs_group) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.vbp_ppe_ppa_no_ppr /*+createtype(A)*/ 
(
 claim_key,
 claim_id,
 claim_trans_id,
 dsch_dt,
 claim_type_code,
 preventable_group,
 pfp_preventable_group_name,
 pfp_preventable_group_desc,
 preventable_status,
 pfp_preventable_status_desc,
 preventable_reason,
 pfp_preventable_reason_desc,
 mdw_insert_dt,
 mdw_update_dt
)
AS
 SELECT vbp_ppe_ppa_no_ppr.claim_key,
        vbp_ppe_ppa_no_ppr.claim_id,
        vbp_ppe_ppa_no_ppr.claim_trans_id,
        vbp_ppe_ppa_no_ppr.dsch_dt,
        vbp_ppe_ppa_no_ppr.claim_type_code,
        vbp_ppe_ppa_no_ppr.preventable_group,
        vbp_ppe_ppa_no_ppr.pfp_preventable_group_name,
        vbp_ppe_ppa_no_ppr.pfp_preventable_group_desc,
        vbp_ppe_ppa_no_ppr.preventable_status,
        vbp_ppe_ppa_no_ppr.pfp_preventable_status_desc,
        vbp_ppe_ppa_no_ppr.preventable_reason,
        vbp_ppe_ppa_no_ppr.pfp_preventable_reason_desc,
        vbp_ppe_ppa_no_ppr.mdw_insert_dt,
        vbp_ppe_ppa_no_ppr.mdw_update_dt
 FROM epbuilder.vbp_ppe_ppa_no_ppr
 ORDER BY vbp_ppe_ppa_no_ppr.claim_key,
          vbp_ppe_ppa_no_ppr.claim_id,
          vbp_ppe_ppa_no_ppr.claim_trans_id,
          vbp_ppe_ppa_no_ppr.dsch_dt,
          vbp_ppe_ppa_no_ppr.claim_type_code,
          vbp_ppe_ppa_no_ppr.preventable_group,
          vbp_ppe_ppa_no_ppr.pfp_preventable_group_name,
          vbp_ppe_ppa_no_ppr.pfp_preventable_group_desc,
          vbp_ppe_ppa_no_ppr.preventable_status,
          vbp_ppe_ppa_no_ppr.pfp_preventable_status_desc,
          vbp_ppe_ppa_no_ppr.preventable_reason,
          vbp_ppe_ppa_no_ppr.pfp_preventable_reason_desc,
          vbp_ppe_ppa_no_ppr.mdw_insert_dt,
          vbp_ppe_ppa_no_ppr.mdw_update_dt
SEGMENTED BY hash(vbp_ppe_ppa_no_ppr.dsch_dt, vbp_ppe_ppa_no_ppr.claim_type_code, vbp_ppe_ppa_no_ppr.preventable_group, vbp_ppe_ppa_no_ppr.preventable_status, vbp_ppe_ppa_no_ppr.preventable_reason, vbp_ppe_ppa_no_ppr.mdw_insert_dt, vbp_ppe_ppa_no_ppr.mdw_update_dt, vbp_ppe_ppa_no_ppr.claim_key, vbp_ppe_ppa_no_ppr.claim_trans_id, vbp_ppe_ppa_no_ppr.claim_id, vbp_ppe_ppa_no_ppr.pfp_preventable_group_name, vbp_ppe_ppa_no_ppr.pfp_preventable_group_desc, vbp_ppe_ppa_no_ppr.pfp_preventable_status_desc, vbp_ppe_ppa_no_ppr.pfp_preventable_reason_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.chronic_sick_prvt /*+createtype(A)*/ 
(
 member_id,
 sub_population
)
AS
 SELECT chronic_sick_prvt.member_id,
        chronic_sick_prvt.sub_population
 FROM epbuilder.chronic_sick_prvt
 ORDER BY chronic_sick_prvt.member_id
SEGMENTED BY hash(chronic_sick_prvt.sub_population, chronic_sick_prvt.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ED_Flag /*+createtype(A)*/ 
(
 claim_id,
 sub_population,
 ED_Flag,
 ppv
)
AS
 SELECT ED_Flag.claim_id,
        ED_Flag.sub_population,
        ED_Flag.ED_Flag,
        ED_Flag.ppv
 FROM epbuilder.ED_Flag
 ORDER BY ED_Flag.claim_id,
          ED_Flag.sub_population
SEGMENTED BY hash(ED_Flag.claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_Total_Cost /*+createtype(A)*/ 
(
 sub_population,
 TOTAL_SHORT_TERM_IP,
 TOTAL_OTHER_IP,
 TOTAL_OP_ED,
 TOTAL_OP_OTHER,
 TOTAL_PB,
 TOTAL_RX
)
AS
 SELECT NY_Total_Cost.sub_population,
        NY_Total_Cost.TOTAL_SHORT_TERM_IP,
        NY_Total_Cost.TOTAL_OTHER_IP,
        NY_Total_Cost.TOTAL_OP_ED,
        NY_Total_Cost.TOTAL_OP_OTHER,
        NY_Total_Cost.TOTAL_PB,
        NY_Total_Cost.TOTAL_RX
 FROM epbuilder.NY_Total_Cost
 ORDER BY NY_Total_Cost.sub_population
SEGMENTED BY hash(NY_Total_Cost.sub_population) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.all_claim_ppa_ppr_ppv /*+createtype(A)*/ 
(
 claim_id,
 claim_line_type_code,
 ppa,
 ppr,
 ppv,
 ppe
)
AS
 SELECT all_claim_ppa_ppr_ppv.claim_id,
        all_claim_ppa_ppr_ppv.claim_line_type_code,
        all_claim_ppa_ppr_ppv.ppa,
        all_claim_ppa_ppr_ppv.ppr,
        all_claim_ppa_ppr_ppv.ppv,
        all_claim_ppa_ppr_ppv.ppe
 FROM epbuilder.all_claim_ppa_ppr_ppv
 ORDER BY all_claim_ppa_ppr_ppv.claim_id,
          all_claim_ppa_ppr_ppv.claim_line_type_code,
          all_claim_ppa_ppr_ppv.ppa,
          all_claim_ppa_ppr_ppv.ppr,
          all_claim_ppa_ppr_ppv.ppv,
          all_claim_ppa_ppr_ppv.ppe
SEGMENTED BY hash(all_claim_ppa_ppr_ppv.claim_id, all_claim_ppa_ppr_ppv.claim_line_type_code, all_claim_ppa_ppr_ppv.ppa, all_claim_ppa_ppr_ppv.ppr, all_claim_ppa_ppr_ppv.ppv, all_claim_ppa_ppr_ppv.ppe) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_Grid_Output_Exclusive /*+createtype(A)*/ 
(
 sub_population,
 Mem_Count,
 IP_ST_PPA,
 IP_OTHER_PPA,
 OP_ED_PPA,
 OP_OTHER_PPA,
 PB_PPA,
 IP_ST_PPR,
 IP_OTHER_PPR,
 OP_ED_PPR,
 OP_OTHER_PPR,
 PB_PPR,
 IP_ST_PPV,
 IP_OTHER_PPV,
 OP_ED_PPV,
 OP_OTHER_PPV,
 PB_PPV,
 IP_ST_PAC,
 IP_OTHER_PAC,
 OP_ED_PAC,
 OP_OTHER_PAC,
 PB_PAC,
 RX_PAC,
 IP_ST_ALL,
 IP_OTHER_ALL,
 OP_ED_ALL,
 OP_OTHER_ALL,
 PB_ALL,
 RX_ALL
)
AS
 SELECT NY_Grid_Output_Exclusive.sub_population,
        NY_Grid_Output_Exclusive.Mem_Count,
        NY_Grid_Output_Exclusive.IP_ST_PPA,
        NY_Grid_Output_Exclusive.IP_OTHER_PPA,
        NY_Grid_Output_Exclusive.OP_ED_PPA,
        NY_Grid_Output_Exclusive.OP_OTHER_PPA,
        NY_Grid_Output_Exclusive.PB_PPA,
        NY_Grid_Output_Exclusive.IP_ST_PPR,
        NY_Grid_Output_Exclusive.IP_OTHER_PPR,
        NY_Grid_Output_Exclusive.OP_ED_PPR,
        NY_Grid_Output_Exclusive.OP_OTHER_PPR,
        NY_Grid_Output_Exclusive.PB_PPR,
        NY_Grid_Output_Exclusive.IP_ST_PPV,
        NY_Grid_Output_Exclusive.IP_OTHER_PPV,
        NY_Grid_Output_Exclusive.OP_ED_PPV,
        NY_Grid_Output_Exclusive.OP_OTHER_PPV,
        NY_Grid_Output_Exclusive.PB_PPV,
        NY_Grid_Output_Exclusive.IP_ST_PAC,
        NY_Grid_Output_Exclusive.IP_OTHER_PAC,
        NY_Grid_Output_Exclusive.OP_ED_PAC,
        NY_Grid_Output_Exclusive.OP_OTHER_PAC,
        NY_Grid_Output_Exclusive.PB_PAC,
        NY_Grid_Output_Exclusive.RX_PAC,
        NY_Grid_Output_Exclusive.IP_ST_ALL,
        NY_Grid_Output_Exclusive.IP_OTHER_ALL,
        NY_Grid_Output_Exclusive.OP_ED_ALL,
        NY_Grid_Output_Exclusive.OP_OTHER_ALL,
        NY_Grid_Output_Exclusive.PB_ALL,
        NY_Grid_Output_Exclusive.RX_ALL
 FROM epbuilder.NY_Grid_Output_Exclusive
 ORDER BY NY_Grid_Output_Exclusive.sub_population
SEGMENTED BY hash(NY_Grid_Output_Exclusive.sub_population) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.IP_PAC_Claims /*+createtype(A)*/ 
(
 member_id,
 claim_id,
 begin_date,
 end_date,
 IP_allowed_amt,
 PAC,
 ppa,
 ppr
)
AS
 SELECT IP_PAC_Claims.member_id,
        IP_PAC_Claims.claim_id,
        IP_PAC_Claims.begin_date,
        IP_PAC_Claims.end_date,
        IP_PAC_Claims.IP_allowed_amt,
        IP_PAC_Claims.PAC,
        IP_PAC_Claims.ppa,
        IP_PAC_Claims.ppr
 FROM epbuilder.IP_PAC_Claims
 ORDER BY IP_PAC_Claims.member_id
SEGMENTED BY hash(IP_PAC_Claims.claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PB_Bubble_Claim /*+createtype(A)*/ 
(
 member_id,
 claim_id,
 allowed_amt
)
AS
 SELECT PB_Bubble_Claim.member_id,
        PB_Bubble_Claim.claim_id,
        PB_Bubble_Claim.allowed_amt
 FROM epbuilder.PB_Bubble_Claim
 ORDER BY PB_Bubble_Claim.member_id,
          PB_Bubble_Claim.claim_id
SEGMENTED BY hash(PB_Bubble_Claim.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.IP_PAC_w_Bubble /*+createtype(A)*/ 
(
 member_id,
 claim_id,
 begin_date,
 end_date,
 IP_allowed_amt,
 PAC,
 ppa,
 ppr,
 PB_allowed_amt
)
AS
 SELECT IP_PAC_w_Bubble.member_id,
        IP_PAC_w_Bubble.claim_id,
        IP_PAC_w_Bubble.begin_date,
        IP_PAC_w_Bubble.end_date,
        IP_PAC_w_Bubble.IP_allowed_amt,
        IP_PAC_w_Bubble.PAC,
        IP_PAC_w_Bubble.ppa,
        IP_PAC_w_Bubble.ppr,
        IP_PAC_w_Bubble.PB_allowed_amt
 FROM epbuilder.IP_PAC_w_Bubble
 ORDER BY IP_PAC_w_Bubble.member_id
SEGMENTED BY hash(IP_PAC_w_Bubble.claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.claims_dsrp_info_CLM_LEVEL /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 master_claim_id,
 claim_id,
 allowed_amt,
 claim_line_type_code,
 begin_date,
 end_date,
 type_of_bill,
 facility_type_code,
 place_of_svc_code,
 count,
 PAC,
 OP_ED,
 PAS,
 PB_OTHER,
 Chronic_bundle,
 IPC,
 PREVENT
)
AS
 SELECT claims_dsrp_info_CLM_LEVEL.member_id,
        claims_dsrp_info_CLM_LEVEL.sub_population,
        claims_dsrp_info_CLM_LEVEL.master_claim_id,
        claims_dsrp_info_CLM_LEVEL.claim_id,
        claims_dsrp_info_CLM_LEVEL.allowed_amt,
        claims_dsrp_info_CLM_LEVEL.claim_line_type_code,
        claims_dsrp_info_CLM_LEVEL.begin_date,
        claims_dsrp_info_CLM_LEVEL.end_date,
        claims_dsrp_info_CLM_LEVEL.type_of_bill,
        claims_dsrp_info_CLM_LEVEL.facility_type_code,
        claims_dsrp_info_CLM_LEVEL.place_of_svc_code,
        claims_dsrp_info_CLM_LEVEL.count,
        claims_dsrp_info_CLM_LEVEL.PAC,
        claims_dsrp_info_CLM_LEVEL.OP_ED,
        claims_dsrp_info_CLM_LEVEL.PAS,
        claims_dsrp_info_CLM_LEVEL.PB_OTHER,
        claims_dsrp_info_CLM_LEVEL.Chronic_bundle,
        claims_dsrp_info_CLM_LEVEL.IPC,
        claims_dsrp_info_CLM_LEVEL.PREVENT
 FROM epbuilder.claims_dsrp_info_CLM_LEVEL
 ORDER BY claims_dsrp_info_CLM_LEVEL.member_id,
          claims_dsrp_info_CLM_LEVEL.sub_population,
          claims_dsrp_info_CLM_LEVEL.master_claim_id,
          claims_dsrp_info_CLM_LEVEL.claim_id,
          claims_dsrp_info_CLM_LEVEL.allowed_amt,
          claims_dsrp_info_CLM_LEVEL.claim_line_type_code,
          claims_dsrp_info_CLM_LEVEL.begin_date,
          claims_dsrp_info_CLM_LEVEL.end_date,
          claims_dsrp_info_CLM_LEVEL.type_of_bill,
          claims_dsrp_info_CLM_LEVEL.facility_type_code,
          claims_dsrp_info_CLM_LEVEL.place_of_svc_code
SEGMENTED BY hash(claims_dsrp_info_CLM_LEVEL.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_Grid_Output_Total_IPC /*+createtype(A)*/ 
(
 sub_population,
 Mem_Count,
 IP_ST_PPA,
 IP_OTHER_PPA,
 OP_ED_PPA,
 OP_OTHER_PPA,
 PB_PPA,
 IP_ST_PPR,
 IP_OTHER_PPR,
 OP_ED_PPR,
 OP_OTHER_PPR,
 PB_PPR,
 IP_ST_PPV,
 IP_OTHER_PPV,
 OP_ED_PPV,
 OP_OTHER_PPV,
 PB_PPV,
 IP_ST_PAC,
 IP_OTHER_PAC,
 OP_ED_PAC,
 OP_OTHER_PAC,
 PB_PAC,
 RX_PAC
)
AS
 SELECT NY_Grid_Output_Total_IPC.sub_population,
        NY_Grid_Output_Total_IPC.Mem_Count,
        NY_Grid_Output_Total_IPC.IP_ST_PPA,
        NY_Grid_Output_Total_IPC.IP_OTHER_PPA,
        NY_Grid_Output_Total_IPC.OP_ED_PPA,
        NY_Grid_Output_Total_IPC.OP_OTHER_PPA,
        NY_Grid_Output_Total_IPC.PB_PPA,
        NY_Grid_Output_Total_IPC.IP_ST_PPR,
        NY_Grid_Output_Total_IPC.IP_OTHER_PPR,
        NY_Grid_Output_Total_IPC.OP_ED_PPR,
        NY_Grid_Output_Total_IPC.OP_OTHER_PPR,
        NY_Grid_Output_Total_IPC.PB_PPR,
        NY_Grid_Output_Total_IPC.IP_ST_PPV,
        NY_Grid_Output_Total_IPC.IP_OTHER_PPV,
        NY_Grid_Output_Total_IPC.OP_ED_PPV,
        NY_Grid_Output_Total_IPC.OP_OTHER_PPV,
        NY_Grid_Output_Total_IPC.PB_PPV,
        NY_Grid_Output_Total_IPC.IP_ST_PAC,
        NY_Grid_Output_Total_IPC.IP_OTHER_PAC,
        NY_Grid_Output_Total_IPC.OP_ED_PAC,
        NY_Grid_Output_Total_IPC.OP_OTHER_PAC,
        NY_Grid_Output_Total_IPC.PB_PAC,
        NY_Grid_Output_Total_IPC.RX_PAC
 FROM epbuilder.NY_Grid_Output_Total_IPC
 ORDER BY NY_Grid_Output_Total_IPC.sub_population,
          NY_Grid_Output_Total_IPC.Mem_Count,
          NY_Grid_Output_Total_IPC.IP_ST_PPA,
          NY_Grid_Output_Total_IPC.IP_OTHER_PPA,
          NY_Grid_Output_Total_IPC.OP_ED_PPA,
          NY_Grid_Output_Total_IPC.OP_OTHER_PPA,
          NY_Grid_Output_Total_IPC.PB_PPA,
          NY_Grid_Output_Total_IPC.IP_ST_PPR,
          NY_Grid_Output_Total_IPC.IP_OTHER_PPR,
          NY_Grid_Output_Total_IPC.OP_ED_PPR,
          NY_Grid_Output_Total_IPC.OP_OTHER_PPR,
          NY_Grid_Output_Total_IPC.PB_PPR,
          NY_Grid_Output_Total_IPC.IP_ST_PPV,
          NY_Grid_Output_Total_IPC.IP_OTHER_PPV,
          NY_Grid_Output_Total_IPC.OP_ED_PPV,
          NY_Grid_Output_Total_IPC.OP_OTHER_PPV,
          NY_Grid_Output_Total_IPC.PB_PPV,
          NY_Grid_Output_Total_IPC.IP_ST_PAC,
          NY_Grid_Output_Total_IPC.IP_OTHER_PAC,
          NY_Grid_Output_Total_IPC.OP_ED_PAC,
          NY_Grid_Output_Total_IPC.OP_OTHER_PAC,
          NY_Grid_Output_Total_IPC.PB_PAC,
          NY_Grid_Output_Total_IPC.RX_PAC
SEGMENTED BY hash(NY_Grid_Output_Total_IPC.Mem_Count, NY_Grid_Output_Total_IPC.sub_population, NY_Grid_Output_Total_IPC.IP_ST_PPA, NY_Grid_Output_Total_IPC.IP_OTHER_PPA, NY_Grid_Output_Total_IPC.OP_ED_PPA, NY_Grid_Output_Total_IPC.OP_OTHER_PPA, NY_Grid_Output_Total_IPC.PB_PPA, NY_Grid_Output_Total_IPC.IP_ST_PPR, NY_Grid_Output_Total_IPC.IP_OTHER_PPR, NY_Grid_Output_Total_IPC.OP_ED_PPR, NY_Grid_Output_Total_IPC.OP_OTHER_PPR, NY_Grid_Output_Total_IPC.PB_PPR, NY_Grid_Output_Total_IPC.IP_ST_PPV, NY_Grid_Output_Total_IPC.IP_OTHER_PPV, NY_Grid_Output_Total_IPC.OP_ED_PPV, NY_Grid_Output_Total_IPC.OP_OTHER_PPV, NY_Grid_Output_Total_IPC.PB_PPV, NY_Grid_Output_Total_IPC.IP_ST_PAC, NY_Grid_Output_Total_IPC.IP_OTHER_PAC, NY_Grid_Output_Total_IPC.OP_ED_PAC, NY_Grid_Output_Total_IPC.OP_OTHER_PAC, NY_Grid_Output_Total_IPC.PB_PAC, NY_Grid_Output_Total_IPC.RX_PAC) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_Grid_Output_Exclusive_IPC /*+createtype(A)*/ 
(
 sub_population,
 Mem_Count,
 IP_ST_PPA,
 IP_OTHER_PPA,
 OP_ED_PPA,
 OP_OTHER_PPA,
 PB_PPA,
 IP_ST_PPR,
 IP_OTHER_PPR,
 OP_ED_PPR,
 OP_OTHER_PPR,
 PB_PPR,
 IP_ST_PPV,
 IP_OTHER_PPV,
 OP_ED_PPV,
 OP_OTHER_PPV,
 PB_PPV,
 IP_ST_PAC,
 IP_OTHER_PAC,
 OP_ED_PAC,
 OP_OTHER_PAC,
 PB_PAC,
 RX_PAC,
 IP_ST_ALL,
 IP_OTHER_ALL,
 OP_ED_ALL,
 OP_OTHER_ALL,
 PB_ALL,
 RX_ALL
)
AS
 SELECT NY_Grid_Output_Exclusive_IPC.sub_population,
        NY_Grid_Output_Exclusive_IPC.Mem_Count,
        NY_Grid_Output_Exclusive_IPC.IP_ST_PPA,
        NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPA,
        NY_Grid_Output_Exclusive_IPC.OP_ED_PPA,
        NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPA,
        NY_Grid_Output_Exclusive_IPC.PB_PPA,
        NY_Grid_Output_Exclusive_IPC.IP_ST_PPR,
        NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPR,
        NY_Grid_Output_Exclusive_IPC.OP_ED_PPR,
        NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPR,
        NY_Grid_Output_Exclusive_IPC.PB_PPR,
        NY_Grid_Output_Exclusive_IPC.IP_ST_PPV,
        NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPV,
        NY_Grid_Output_Exclusive_IPC.OP_ED_PPV,
        NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPV,
        NY_Grid_Output_Exclusive_IPC.PB_PPV,
        NY_Grid_Output_Exclusive_IPC.IP_ST_PAC,
        NY_Grid_Output_Exclusive_IPC.IP_OTHER_PAC,
        NY_Grid_Output_Exclusive_IPC.OP_ED_PAC,
        NY_Grid_Output_Exclusive_IPC.OP_OTHER_PAC,
        NY_Grid_Output_Exclusive_IPC.PB_PAC,
        NY_Grid_Output_Exclusive_IPC.RX_PAC,
        NY_Grid_Output_Exclusive_IPC.IP_ST_ALL,
        NY_Grid_Output_Exclusive_IPC.IP_OTHER_ALL,
        NY_Grid_Output_Exclusive_IPC.OP_ED_ALL,
        NY_Grid_Output_Exclusive_IPC.OP_OTHER_ALL,
        NY_Grid_Output_Exclusive_IPC.PB_ALL,
        NY_Grid_Output_Exclusive_IPC.RX_ALL
 FROM epbuilder.NY_Grid_Output_Exclusive_IPC
 ORDER BY NY_Grid_Output_Exclusive_IPC.sub_population,
          NY_Grid_Output_Exclusive_IPC.Mem_Count,
          NY_Grid_Output_Exclusive_IPC.IP_ST_PPA,
          NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPA,
          NY_Grid_Output_Exclusive_IPC.OP_ED_PPA,
          NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPA,
          NY_Grid_Output_Exclusive_IPC.PB_PPA,
          NY_Grid_Output_Exclusive_IPC.IP_ST_PPR,
          NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPR,
          NY_Grid_Output_Exclusive_IPC.OP_ED_PPR,
          NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPR,
          NY_Grid_Output_Exclusive_IPC.PB_PPR,
          NY_Grid_Output_Exclusive_IPC.IP_ST_PPV,
          NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPV,
          NY_Grid_Output_Exclusive_IPC.OP_ED_PPV,
          NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPV,
          NY_Grid_Output_Exclusive_IPC.PB_PPV,
          NY_Grid_Output_Exclusive_IPC.IP_ST_PAC,
          NY_Grid_Output_Exclusive_IPC.IP_OTHER_PAC,
          NY_Grid_Output_Exclusive_IPC.OP_ED_PAC,
          NY_Grid_Output_Exclusive_IPC.OP_OTHER_PAC,
          NY_Grid_Output_Exclusive_IPC.PB_PAC,
          NY_Grid_Output_Exclusive_IPC.RX_PAC,
          NY_Grid_Output_Exclusive_IPC.IP_ST_ALL,
          NY_Grid_Output_Exclusive_IPC.IP_OTHER_ALL,
          NY_Grid_Output_Exclusive_IPC.OP_ED_ALL,
          NY_Grid_Output_Exclusive_IPC.OP_OTHER_ALL,
          NY_Grid_Output_Exclusive_IPC.PB_ALL,
          NY_Grid_Output_Exclusive_IPC.RX_ALL
SEGMENTED BY hash(NY_Grid_Output_Exclusive_IPC.Mem_Count, NY_Grid_Output_Exclusive_IPC.sub_population, NY_Grid_Output_Exclusive_IPC.IP_ST_PPA, NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPA, NY_Grid_Output_Exclusive_IPC.OP_ED_PPA, NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPA, NY_Grid_Output_Exclusive_IPC.PB_PPA, NY_Grid_Output_Exclusive_IPC.IP_ST_PPR, NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPR, NY_Grid_Output_Exclusive_IPC.OP_ED_PPR, NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPR, NY_Grid_Output_Exclusive_IPC.PB_PPR, NY_Grid_Output_Exclusive_IPC.IP_ST_PPV, NY_Grid_Output_Exclusive_IPC.IP_OTHER_PPV, NY_Grid_Output_Exclusive_IPC.OP_ED_PPV, NY_Grid_Output_Exclusive_IPC.OP_OTHER_PPV, NY_Grid_Output_Exclusive_IPC.PB_PPV, NY_Grid_Output_Exclusive_IPC.IP_ST_PAC, NY_Grid_Output_Exclusive_IPC.IP_OTHER_PAC, NY_Grid_Output_Exclusive_IPC.OP_ED_PAC, NY_Grid_Output_Exclusive_IPC.OP_OTHER_PAC, NY_Grid_Output_Exclusive_IPC.PB_PAC, NY_Grid_Output_Exclusive_IPC.RX_PAC, NY_Grid_Output_Exclusive_IPC.IP_ST_ALL, NY_Grid_Output_Exclusive_IPC.IP_OTHER_ALL, NY_Grid_Output_Exclusive_IPC.OP_ED_ALL, NY_Grid_Output_Exclusive_IPC.OP_OTHER_ALL, NY_Grid_Output_Exclusive_IPC.PB_ALL, NY_Grid_Output_Exclusive_IPC.RX_ALL) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.NY_Total_Cost_IPC /*+createtype(A)*/ 
(
 sub_population,
 Chronic_TOTAL_SHORT_TERM_IP,
 Chronic_TOTAL_OTHER_IP,
 Chronic_TOTAL_OP_ED,
 Chronic_TOTAL_OP_OTHER,
 Chronic_TOTAL_PB,
 Chronic_TOTAL_RX,
 IPC_TOTAL_SHORT_TERM_IP,
 IPC_TOTAL_OTHER_IP,
 IPC_TOTAL_OP_ED,
 IPC_TOTAL_OP_OTHER,
 IPC_TOTAL_PB,
 IPC_TOTAL_RX,
 PREVENT_TOTAL_SHORT_TERM_IP,
 PREVENT_TOTAL_OTHER_IP,
 PREVENT_TOTAL_OP_ED,
 PREVENT_TOTAL_OP_OTHER,
 PREVENT_TOTAL_PB,
 PREVENT_TOTAL_RX
)
AS
 SELECT NY_Total_Cost_IPC.sub_population,
        NY_Total_Cost_IPC.Chronic_TOTAL_SHORT_TERM_IP,
        NY_Total_Cost_IPC.Chronic_TOTAL_OTHER_IP,
        NY_Total_Cost_IPC.Chronic_TOTAL_OP_ED,
        NY_Total_Cost_IPC.Chronic_TOTAL_OP_OTHER,
        NY_Total_Cost_IPC.Chronic_TOTAL_PB,
        NY_Total_Cost_IPC.Chronic_TOTAL_RX,
        NY_Total_Cost_IPC.IPC_TOTAL_SHORT_TERM_IP,
        NY_Total_Cost_IPC.IPC_TOTAL_OTHER_IP,
        NY_Total_Cost_IPC.IPC_TOTAL_OP_ED,
        NY_Total_Cost_IPC.IPC_TOTAL_OP_OTHER,
        NY_Total_Cost_IPC.IPC_TOTAL_PB,
        NY_Total_Cost_IPC.IPC_TOTAL_RX,
        NY_Total_Cost_IPC.PREVENT_TOTAL_SHORT_TERM_IP,
        NY_Total_Cost_IPC.PREVENT_TOTAL_OTHER_IP,
        NY_Total_Cost_IPC.PREVENT_TOTAL_OP_ED,
        NY_Total_Cost_IPC.PREVENT_TOTAL_OP_OTHER,
        NY_Total_Cost_IPC.PREVENT_TOTAL_PB,
        NY_Total_Cost_IPC.PREVENT_TOTAL_RX
 FROM epbuilder.NY_Total_Cost_IPC
 ORDER BY NY_Total_Cost_IPC.sub_population
SEGMENTED BY hash(NY_Total_Cost_IPC.sub_population) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.asthma_red /*+createtype(A)*/ 
(
 member_id,
 master_episode_id,
 episode_type,
 episode_id,
 level,
 split_total_cost,
 split_total_PAC_cost,
 split_total_typical_cost,
 unsplit_total_cost,
 unsplit_total_PAC_cost,
 unsplit_total_typical_cost,
 annualized_unsplit_total_cost,
 annualized_unsplit_total_PAC_cost,
 annualized_unsplit_total_typical_cost
)
AS
 SELECT asthma_red.member_id,
        asthma_red.master_episode_id,
        asthma_red.episode_type,
        asthma_red.episode_id,
        asthma_red.level,
        asthma_red.split_total_cost,
        asthma_red.split_total_PAC_cost,
        asthma_red.split_total_typical_cost,
        asthma_red.unsplit_total_cost,
        asthma_red.unsplit_total_PAC_cost,
        asthma_red.unsplit_total_typical_cost,
        asthma_red.annualized_unsplit_total_cost,
        asthma_red.annualized_unsplit_total_PAC_cost,
        asthma_red.annualized_unsplit_total_typical_cost
 FROM epbuilder.asthma_red
 ORDER BY asthma_red.master_episode_id
SEGMENTED BY hash(asthma_red.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.asthma_associations /*+createtype(A)*/ 
(
 id,
 parent_master_episode_id,
 child_master_episode_id,
 association_type,
 association_level,
 association_count,
 association_start_day,
 association_end_day
)
AS
 SELECT asthma_associations.id,
        asthma_associations.parent_master_episode_id,
        asthma_associations.child_master_episode_id,
        asthma_associations.association_type,
        asthma_associations.association_level,
        asthma_associations.association_count,
        asthma_associations.association_start_day,
        asthma_associations.association_end_day
 FROM epbuilder.asthma_associations
 ORDER BY asthma_associations.id
SEGMENTED BY hash(asthma_associations.id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PNE_asthma_association /*+createtype(A)*/ 
(
 parent_master_episode_id,
 "left",
 count
)
AS
 SELECT PNE_asthma_association.parent_master_episode_id,
        PNE_asthma_association."left",
        PNE_asthma_association.count
 FROM epbuilder.PNE_asthma_association
 ORDER BY PNE_asthma_association.parent_master_episode_id,
          PNE_asthma_association."left"
SEGMENTED BY hash(PNE_asthma_association.parent_master_episode_id, PNE_asthma_association."left") ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.URI_asthma_association /*+createtype(A)*/ 
(
 parent_master_episode_id,
 "left",
 count
)
AS
 SELECT URI_asthma_association.parent_master_episode_id,
        URI_asthma_association."left",
        URI_asthma_association.count
 FROM epbuilder.URI_asthma_association
 ORDER BY URI_asthma_association.parent_master_episode_id,
          URI_asthma_association."left"
SEGMENTED BY hash(URI_asthma_association.parent_master_episode_id, URI_asthma_association."left") ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PNE_URI_association /*+createtype(A)*/ 
(
 parent_master_episode_id,
 "left",
 count
)
AS
 SELECT PNE_URI_association.parent_master_episode_id,
        PNE_URI_association."left",
        PNE_URI_association.count
 FROM epbuilder.PNE_URI_association
 ORDER BY PNE_URI_association.parent_master_episode_id,
          PNE_URI_association."left",
          PNE_URI_association.count
SEGMENTED BY hash(PNE_URI_association.parent_master_episode_id, PNE_URI_association."left") ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.assign_1_pac /*+createtype(A)*/ 
(
 member_id,
 LEVEL_1,
 LEVEL_2,
 LEVEL_2_association_type,
 LEVEL_3,
 LEVEL_3_association_type,
 LEVEL_4,
 LEVEL_4_association_type,
 LEVEL_5,
 LEVEL_5_association_type,
 Level_1_end,
 Level_2_end,
 Level_3_end,
 Level_4_end,
 Level_5_end,
 assigned_type,
 master_claim_id,
 end_date,
 begin_date
)
AS
 SELECT assign_1_pac.member_id,
        assign_1_pac.LEVEL_1,
        assign_1_pac.LEVEL_2,
        assign_1_pac.LEVEL_2_association_type,
        assign_1_pac.LEVEL_3,
        assign_1_pac.LEVEL_3_association_type,
        assign_1_pac.LEVEL_4,
        assign_1_pac.LEVEL_4_association_type,
        assign_1_pac.LEVEL_5,
        assign_1_pac.LEVEL_5_association_type,
        assign_1_pac.Level_1_end,
        assign_1_pac.Level_2_end,
        assign_1_pac.Level_3_end,
        assign_1_pac.Level_4_end,
        assign_1_pac.Level_5_end,
        assign_1_pac.assigned_type,
        assign_1_pac.master_claim_id,
        assign_1_pac.end_date,
        assign_1_pac.begin_date
 FROM epbuilder.assign_1_pac
 ORDER BY assign_1_pac.member_id,
          assign_1_pac.LEVEL_1,
          assign_1_pac.LEVEL_2,
          assign_1_pac.LEVEL_2_association_type,
          assign_1_pac.LEVEL_3,
          assign_1_pac.LEVEL_3_association_type,
          assign_1_pac.LEVEL_4,
          assign_1_pac.LEVEL_4_association_type,
          assign_1_pac.LEVEL_5,
          assign_1_pac.LEVEL_5_association_type,
          assign_1_pac.Level_1_end,
          assign_1_pac.Level_2_end,
          assign_1_pac.Level_3_end,
          assign_1_pac.Level_4_end,
          assign_1_pac.Level_5_end,
          assign_1_pac.assigned_type,
          assign_1_pac.master_claim_id,
          assign_1_pac.end_date,
          assign_1_pac.begin_date
SEGMENTED BY hash(assign_1_pac.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.epi_PACs /*+createtype(A)*/ 
(
 episode,
 level_assignment,
 pac_proc_acute,
 pac_chronic
)
AS
 SELECT epi_PACs.episode,
        epi_PACs.level_assignment,
        epi_PACs.pac_proc_acute,
        epi_PACs.pac_chronic
 FROM epbuilder.epi_PACs
 ORDER BY epi_PACs.episode,
          epi_PACs.level_assignment,
          epi_PACs.pac_proc_acute,
          epi_PACs.pac_chronic
SEGMENTED BY hash(epi_PACs.episode) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.LEVEL_5_FINAL /*+createtype(A)*/ 
(
 member_id,
 LEVEL_1,
 LEVEL_2,
 LEVEL_2_association_type,
 LEVEL_3,
 LEVEL_3_association_type,
 LEVEL_4,
 LEVEL_4_association_type,
 LEVEL_5,
 LEVEL_5_association_type,
 Level_1_end,
 Level_2_end,
 Level_3_end,
 Level_4_end,
 Level_5_end
)
AS
 SELECT LEVEL_5_FINAL.member_id,
        LEVEL_5_FINAL.LEVEL_1,
        LEVEL_5_FINAL.LEVEL_2,
        LEVEL_5_FINAL.LEVEL_2_association_type,
        LEVEL_5_FINAL.LEVEL_3,
        LEVEL_5_FINAL.LEVEL_3_association_type,
        LEVEL_5_FINAL.LEVEL_4,
        LEVEL_5_FINAL.LEVEL_4_association_type,
        LEVEL_5_FINAL.LEVEL_5,
        LEVEL_5_FINAL.LEVEL_5_association_type,
        LEVEL_5_FINAL.Level_1_end,
        LEVEL_5_FINAL.Level_2_end,
        LEVEL_5_FINAL.Level_3_end,
        LEVEL_5_FINAL.Level_4_end,
        LEVEL_5_FINAL.Level_5_end
 FROM epbuilder.LEVEL_5_FINAL
 ORDER BY LEVEL_5_FINAL.member_id,
          LEVEL_5_FINAL.LEVEL_1,
          LEVEL_5_FINAL.LEVEL_2,
          LEVEL_5_FINAL.LEVEL_2_association_type,
          LEVEL_5_FINAL.LEVEL_3,
          LEVEL_5_FINAL.LEVEL_3_association_type,
          LEVEL_5_FINAL.LEVEL_4,
          LEVEL_5_FINAL.LEVEL_4_association_type,
          LEVEL_5_FINAL.LEVEL_5,
          LEVEL_5_FINAL.LEVEL_5_association_type,
          LEVEL_5_FINAL.Level_1_end,
          LEVEL_5_FINAL.Level_2_end,
          LEVEL_5_FINAL.Level_3_end,
          LEVEL_5_FINAL.Level_4_end,
          LEVEL_5_FINAL.Level_5_end
SEGMENTED BY hash(LEVEL_5_FINAL.LEVEL_5) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_final_data_limit /*+createtype(A)*/ 
(
 row_names,
 epi_id,
 member_id,
 epi_number,
 epi_name,
 female,
 age,
 rec_enr,
 eol_ind,
 cost_ra_comp_l1,
 cost_ra_comp_l5,
 cost_ra_comp_other_l1,
 cost_ra_comp_other_l3,
 cost_ra_comp_other_l4,
 cost_ra_typ_ip_l1,
 cost_ra_typ_ip_l3,
 cost_ra_typ_ip_l4,
 cost_ra_typ_l1,
 cost_ra_typ_l5,
 cost_ra_typ_other_l3,
 cost_ra_typ_other_l4,
 cost_sa_comp_l1,
 cost_sa_comp_l5,
 cost_sa_comp_other_l1,
 cost_sa_comp_other_l3,
 cost_sa_comp_other_l4,
 cost_sa_typ_ip_l1,
 cost_sa_typ_ip_l3,
 cost_sa_typ_ip_l4,
 cost_sa_typ_l1,
 cost_sa_typ_l5,
 cost_sa_typ_other_l1,
 cost_sa_typ_other_l3,
 cost_sa_typ_other_l4,
 typ_ip_ind,
 use_comp_l1,
 use_comp_l5,
 use_comp_other_l1,
 use_comp_other_l3,
 use_comp_other_l4,
 use_typ_ip_l1,
 use_typ_ip_l3,
 use_typ_ip_l4,
 use_typ_l1,
 use_typ_l5,
 use_typ_other_l1,
 use_typ_other_l3,
 use_typ_other_l4,
 RF0101,
 RF0102,
 RF0103,
 RF0104,
 RF0105,
 RF0106,
 RF0107,
 RF0108,
 RF0109,
 RF0110,
 RF0111,
 RF0115,
 RF0201,
 RF0301,
 RF0401,
 RF0402,
 RF0403,
 RF0404,
 RF0406,
 RF0407,
 RF0408,
 RF0501,
 RF0502,
 RF0503,
 RF0504,
 RF0505,
 RF0506,
 RF0507,
 RF0508,
 RF0509,
 RF0510,
 RF0511,
 RF0512,
 RF0513,
 RF0514,
 RF0518,
 RF0519,
 RF0520,
 RF0521,
 RF0524,
 RF0525,
 RF0601,
 RF0602,
 RF0603,
 RF0604,
 RF0606,
 RF0607,
 RF0608,
 RF0609,
 RF0610,
 RF0611,
 RF0612,
 RF0613,
 RF0614,
 RF0615,
 RF0616,
 RF0617,
 RF0618,
 RF0619,
 RF0620,
 RF0701,
 RF0702,
 RF0703,
 RF0704,
 RF0705,
 RF0801,
 RF0802,
 RF0803,
 RF0804,
 RF0805,
 RF0806,
 RF0807,
 RF0808,
 RF0809,
 RF0810,
 RF0811,
 RF0812,
 RF0813,
 RF0814,
 RF0901,
 RF0902,
 RF1001,
 RF1002,
 RF1003,
 RF1101,
 RF1102,
 RF1103,
 RF1104,
 RF1105,
 RF1301,
 RF1302,
 RF1303,
 RF1304,
 RF1305,
 RF1306,
 RF1308,
 RF1309,
 RF1310,
 RF1401,
 RF1402,
 RF1403,
 RF1406,
 RF1407,
 RF1408,
 RF1409,
 RF1410,
 RF1412,
 RF1413,
 RF1414,
 RF1415,
 RF1416,
 RF1417,
 RF1418,
 RF1419,
 RF1421,
 RF1436,
 RF1441,
 RF1450,
 RF1454,
 RF1458,
 RF1460,
 RF1461,
 RF1462,
 RF1467,
 RF1601,
 RF1602,
 RF1603,
 RF1604,
 RF1605,
 RF1701,
 RF1702,
 RF1703,
 RF1704,
 RF1705,
 RF1706,
 RF1707,
 RF1708,
 RF1709,
 RF1710,
 RF1711,
 RF1712,
 RF1801,
 RF1901,
 RF1902,
 RF2001,
 RF2002,
 RF2101,
 RF2102,
 RF2201,
 RF2301,
 RF2302,
 RF2303,
 RF2304,
 RF2305,
 RF2306,
 RF2307,
 RF2308,
 RF2309,
 RF2310,
 RF2311,
 RF2401,
 RF2402,
 RF2403,
 RF2404,
 RF2405,
 RF2501,
 STCT0001,
 STCT0002,
 STCT0003,
 STCT00100,
 STCT0018,
 STCT0020,
 STCT0021,
 STCT0022,
 STCT0023,
 STCT0027,
 STCT0040,
 STCT0045,
 STCT0050,
 STCT0053,
 STCT0054,
 STCT0062,
 STCT0063,
 STCT0064,
 STCT0065,
 STCT0066,
 STCT0067,
 STCT0068,
 STCT0069,
 STCT0070,
 STCT0073,
 STCT0074,
 STCT0098,
 STCT0099,
 STCT0254,
 STCT0255,
 STCT0387,
 STCT0388,
 STCT04285,
 STCT04286,
 STCT04287,
 STCT04288,
 STCT04290,
 STCT04291,
 STCT04292,
 STCT04294,
 STCT05263,
 STCT05264,
 STCT05265,
 STCT05266,
 STCT05267,
 STCT05268,
 STCT06124,
 STCT06125,
 STCT06127,
 STCT06128,
 STCT06131,
 STCT06132,
 STCT06133,
 STCT06134,
 STCT06135,
 STCT06136,
 STCT06137,
 STCT06138,
 STCT06139,
 STCT06140,
 STCT06305,
 STCT06306,
 STCT06307,
 STCT06312,
 STCT0738,
 STCT0739,
 STCT08126,
 STCT08127,
 STCT08129,
 STCT08130,
 STCT08131,
 STCT08132,
 STCT08133,
 STCT08134,
 STCT08136,
 STCT08138,
 STCT08140,
 STCT08141,
 STCT08142,
 STCT08143,
 STCT08144,
 STCT08145,
 STCT08309,
 STCT09100,
 STCT09101,
 STCT0996,
 STCT0998,
 STCT0999,
 STCT1342,
 STCT1343,
 STCT1344,
 STCT1345,
 STCT1346,
 STCT1347,
 STCT1348,
 STCT1349,
 STCT14296,
 STCT1455,
 STCT17102,
 STCT17104,
 STCT1798,
 STCT1799,
 STCT2005,
 STCT23308,
 STDX01384,
 STDX01385,
 STDX01388,
 STDX01398,
 STDX01399,
 STDX01400,
 STDX01405,
 STDX01409,
 STDX02135,
 STDX02157,
 STDX02158,
 STDX02159,
 STDX02162,
 STDX02163,
 STDX02164,
 STDX02165,
 STDX02166,
 STDX02167,
 STDX03252,
 STDX03254,
 STDX03256,
 STDX03257,
 STDX03258,
 STDX03259,
 STDX03260,
 STDX03261,
 STDX03262,
 STDX03264,
 STDX03266,
 STDX03267,
 STDX04138,
 STDX04163,
 STDX04165,
 STDX04167,
 STDX04168,
 STDX04172,
 STDX04173,
 STDX04174,
 STDX04180,
 STDX04181,
 STDX04182,
 STDX04183,
 STDX04184,
 STDX04185,
 STDX04186,
 STDX04188,
 STDX04190,
 STDX05281,
 STDX05282,
 STDX05283,
 STDX05284,
 STDX05285,
 STDX05287,
 STDX05288,
 STDX05289,
 STDX05291,
 STDX05313,
 STDX05314,
 STDX05316,
 STDX05318,
 STDX05320,
 STDX05323,
 STDX05325,
 STDX05326,
 STDX05327,
 STDX05328,
 STDX05329,
 STDX05330,
 STDX05331,
 STDX05332,
 STDX05333,
 STDX05334,
 STDX05335,
 STDX05336,
 STDX05337,
 STDX05339,
 STDX05340,
 STDX05343,
 STDX05344,
 STDX05345,
 STDX06267,
 STDX06268,
 STDX06277,
 STDX06281,
 STDX06283,
 STDX06285,
 STDX06286,
 STDX06287,
 STDX06288,
 STDX06290,
 STDX06291,
 STDX06295,
 STDX06297,
 STDX06298,
 STDX06301,
 STDX06306,
 STDX06307,
 STDX06308,
 STDX06311,
 STDX06312,
 STDX06313,
 STDX06314,
 STDX06315,
 STDX06316,
 STDX06317,
 STDX06318,
 STDX06319,
 STDX06320,
 STDX06321,
 STDX06322,
 STDX06323,
 STDX06324,
 STDX06325,
 STDX06326,
 STDX06327,
 STDX06328,
 STDX06331,
 STDX06332,
 STDX06335,
 STDX06336,
 STDX06337,
 STDX06340,
 STDX06341,
 STDX06342,
 STDX06343,
 STDX06344,
 STDX06345,
 STDX06346,
 STDX06347,
 STDX06348,
 STDX06349,
 STDX06350,
 STDX06351,
 STDX06353,
 STDX06356,
 STDX07101,
 STDX07102,
 STDX07103,
 STDX07104,
 STDX07105,
 STDX07106,
 STDX07107,
 STDX07111,
 STDX07112,
 STDX07113,
 STDX07116,
 STDX081040,
 STDX081041,
 STDX081042,
 STDX081043,
 STDX081044,
 STDX081045,
 STDX081046,
 STDX081047,
 STDX081048,
 STDX081050,
 STDX081052,
 STDX081053,
 STDX081054,
 STDX081055,
 STDX081056,
 STDX081057,
 STDX081058,
 STDX081059,
 STDX081060,
 STDX081061,
 STDX081062,
 STDX081063,
 STDX081064,
 STDX081065,
 STDX081066,
 STDX081067,
 STDX081071,
 STDX081073,
 STDX081074,
 STDX081075,
 STDX081085,
 STDX081086,
 STDX081087,
 STDX081089,
 STDX081092,
 STDX081093,
 STDX081094,
 STDX081095,
 STDX081096,
 STDX081097,
 STDX09196,
 STDX09197,
 STDX09198,
 STDX09199,
 STDX09200,
 STDX1007,
 STDX10101,
 STDX10102,
 STDX10103,
 STDX10104,
 STDX10105,
 STDX10106,
 STDX10107,
 STDX10108,
 STDX10109,
 STDX10111,
 STDX10112,
 STDX10113,
 STDX10114,
 STDX10115,
 STDX10116,
 STDX1019,
 STDX11154,
 STDX1194,
 STDX1195,
 STDX1234,
 STDX1238,
 STDX13100,
 STDX13101,
 STDX13102,
 STDX13103,
 STDX13104,
 STDX13105,
 STDX13106,
 STDX13107,
 STDX13111,
 STDX1394,
 STDX1395,
 STDX1396,
 STDX1397,
 STDX1398,
 STDX1399,
 STDX1406,
 STDX1407,
 STDX1409,
 STDX14100,
 STDX1413,
 STDX1422,
 STDX1432,
 STDX1436,
 STDX1442,
 STDX1445,
 STDX1447,
 STDX1448,
 STDX1451,
 STDX1453,
 STDX1454,
 STDX1455,
 STDX1459,
 STDX1460,
 STDX1461,
 STDX1462,
 STDX1465,
 STDX1466,
 STDX1468,
 STDX1470,
 STDX1472,
 STDX1474,
 STDX1482,
 STDX1483,
 STDX1484,
 STDX1487,
 STDX1488,
 STDX1490,
 STDX1491,
 STDX1492,
 STDX1494,
 STDX1495,
 STDX1497,
 STDX1498,
 STDX1499,
 STDX1583,
 STDX1587,
 STDX16293,
 STDX1767,
 STDX1768,
 STDX1771,
 STDX1772,
 STDX1773,
 STDX1846,
 STDX1942,
 STDX1953,
 STDX1954,
 STDX1955,
 STDX1961,
 STDX1963,
 STDX1964,
 STDX1965,
 STDX1966,
 STDX1969,
 STDX1972,
 STDX1975,
 STDX1976,
 STDX1977,
 STDX1978,
 STDX1979,
 STDX1980,
 STDX1981,
 STDX1982,
 STDX1983,
 STDX1984,
 STDX1985,
 STDX1986,
 STDX1987,
 STDX1988,
 STDX1990,
 STDX1991,
 STDX1992,
 STDX1993,
 STDX2008,
 STDX2009,
 STDX2012,
 STDX2013,
 STDX2014,
 STDX2015,
 STDX2016,
 STDX2017,
 STDX2018,
 STDX2019,
 STDX2020,
 STDX2021,
 STDX2023,
 STDX2159,
 STDX2361,
 STDX2367,
 STDX2369,
 STDX2371,
 STPX0002,
 STPX04219,
 STPX04220,
 STPX04221,
 STPX04222,
 STPX04224,
 STPX04225,
 STPX04226,
 STPX05228,
 STPX05229,
 STPX05230,
 STPX05231,
 STPX05236,
 STPX05238,
 STPX05239,
 STPX05240,
 STPX0544,
 STPX06224,
 STPX06226,
 STPX06230,
 STPX06231,
 STPX06234,
 STPX06240,
 STPX06241,
 STPX06243,
 STPX06244,
 STPX06245,
 STPX06246,
 STPX06247,
 STPX06248,
 STPX06249,
 STPX06251,
 STPX07231,
 STPX07232,
 STPX08217,
 STPX08219,
 STPX08220,
 STPX08221,
 STPX08226,
 STPX08227,
 STPX08228,
 STPX08230,
 STPX08231,
 STPX08232,
 STPX08233,
 STPX08234,
 STPX08235,
 STPX09183,
 STPX09184,
 STPX09185,
 STPX09186,
 STPX09187,
 STPX13133,
 STPX13134,
 STPX13135,
 STPX13136,
 STPX13137,
 STPX13138,
 STPX13139,
 STPX13140,
 STPX14146,
 STPX17225
)
AS
 SELECT ra_final_data_limit.row_names,
        ra_final_data_limit.epi_id,
        ra_final_data_limit.member_id,
        ra_final_data_limit.epi_number,
        ra_final_data_limit.epi_name,
        ra_final_data_limit.female,
        ra_final_data_limit.age,
        ra_final_data_limit.rec_enr,
        ra_final_data_limit.eol_ind,
        ra_final_data_limit.cost_ra_comp_l1,
        ra_final_data_limit.cost_ra_comp_l5,
        ra_final_data_limit.cost_ra_comp_other_l1,
        ra_final_data_limit.cost_ra_comp_other_l3,
        ra_final_data_limit.cost_ra_comp_other_l4,
        ra_final_data_limit.cost_ra_typ_ip_l1,
        ra_final_data_limit.cost_ra_typ_ip_l3,
        ra_final_data_limit.cost_ra_typ_ip_l4,
        ra_final_data_limit.cost_ra_typ_l1,
        ra_final_data_limit.cost_ra_typ_l5,
        ra_final_data_limit.cost_ra_typ_other_l3,
        ra_final_data_limit.cost_ra_typ_other_l4,
        ra_final_data_limit.cost_sa_comp_l1,
        ra_final_data_limit.cost_sa_comp_l5,
        ra_final_data_limit.cost_sa_comp_other_l1,
        ra_final_data_limit.cost_sa_comp_other_l3,
        ra_final_data_limit.cost_sa_comp_other_l4,
        ra_final_data_limit.cost_sa_typ_ip_l1,
        ra_final_data_limit.cost_sa_typ_ip_l3,
        ra_final_data_limit.cost_sa_typ_ip_l4,
        ra_final_data_limit.cost_sa_typ_l1,
        ra_final_data_limit.cost_sa_typ_l5,
        ra_final_data_limit.cost_sa_typ_other_l1,
        ra_final_data_limit.cost_sa_typ_other_l3,
        ra_final_data_limit.cost_sa_typ_other_l4,
        ra_final_data_limit.typ_ip_ind,
        ra_final_data_limit.use_comp_l1,
        ra_final_data_limit.use_comp_l5,
        ra_final_data_limit.use_comp_other_l1,
        ra_final_data_limit.use_comp_other_l3,
        ra_final_data_limit.use_comp_other_l4,
        ra_final_data_limit.use_typ_ip_l1,
        ra_final_data_limit.use_typ_ip_l3,
        ra_final_data_limit.use_typ_ip_l4,
        ra_final_data_limit.use_typ_l1,
        ra_final_data_limit.use_typ_l5,
        ra_final_data_limit.use_typ_other_l1,
        ra_final_data_limit.use_typ_other_l3,
        ra_final_data_limit.use_typ_other_l4,
        ra_final_data_limit.RF0101,
        ra_final_data_limit.RF0102,
        ra_final_data_limit.RF0103,
        ra_final_data_limit.RF0104,
        ra_final_data_limit.RF0105,
        ra_final_data_limit.RF0106,
        ra_final_data_limit.RF0107,
        ra_final_data_limit.RF0108,
        ra_final_data_limit.RF0109,
        ra_final_data_limit.RF0110,
        ra_final_data_limit.RF0111,
        ra_final_data_limit.RF0115,
        ra_final_data_limit.RF0201,
        ra_final_data_limit.RF0301,
        ra_final_data_limit.RF0401,
        ra_final_data_limit.RF0402,
        ra_final_data_limit.RF0403,
        ra_final_data_limit.RF0404,
        ra_final_data_limit.RF0406,
        ra_final_data_limit.RF0407,
        ra_final_data_limit.RF0408,
        ra_final_data_limit.RF0501,
        ra_final_data_limit.RF0502,
        ra_final_data_limit.RF0503,
        ra_final_data_limit.RF0504,
        ra_final_data_limit.RF0505,
        ra_final_data_limit.RF0506,
        ra_final_data_limit.RF0507,
        ra_final_data_limit.RF0508,
        ra_final_data_limit.RF0509,
        ra_final_data_limit.RF0510,
        ra_final_data_limit.RF0511,
        ra_final_data_limit.RF0512,
        ra_final_data_limit.RF0513,
        ra_final_data_limit.RF0514,
        ra_final_data_limit.RF0518,
        ra_final_data_limit.RF0519,
        ra_final_data_limit.RF0520,
        ra_final_data_limit.RF0521,
        ra_final_data_limit.RF0524,
        ra_final_data_limit.RF0525,
        ra_final_data_limit.RF0601,
        ra_final_data_limit.RF0602,
        ra_final_data_limit.RF0603,
        ra_final_data_limit.RF0604,
        ra_final_data_limit.RF0606,
        ra_final_data_limit.RF0607,
        ra_final_data_limit.RF0608,
        ra_final_data_limit.RF0609,
        ra_final_data_limit.RF0610,
        ra_final_data_limit.RF0611,
        ra_final_data_limit.RF0612,
        ra_final_data_limit.RF0613,
        ra_final_data_limit.RF0614,
        ra_final_data_limit.RF0615,
        ra_final_data_limit.RF0616,
        ra_final_data_limit.RF0617,
        ra_final_data_limit.RF0618,
        ra_final_data_limit.RF0619,
        ra_final_data_limit.RF0620,
        ra_final_data_limit.RF0701,
        ra_final_data_limit.RF0702,
        ra_final_data_limit.RF0703,
        ra_final_data_limit.RF0704,
        ra_final_data_limit.RF0705,
        ra_final_data_limit.RF0801,
        ra_final_data_limit.RF0802,
        ra_final_data_limit.RF0803,
        ra_final_data_limit.RF0804,
        ra_final_data_limit.RF0805,
        ra_final_data_limit.RF0806,
        ra_final_data_limit.RF0807,
        ra_final_data_limit.RF0808,
        ra_final_data_limit.RF0809,
        ra_final_data_limit.RF0810,
        ra_final_data_limit.RF0811,
        ra_final_data_limit.RF0812,
        ra_final_data_limit.RF0813,
        ra_final_data_limit.RF0814,
        ra_final_data_limit.RF0901,
        ra_final_data_limit.RF0902,
        ra_final_data_limit.RF1001,
        ra_final_data_limit.RF1002,
        ra_final_data_limit.RF1003,
        ra_final_data_limit.RF1101,
        ra_final_data_limit.RF1102,
        ra_final_data_limit.RF1103,
        ra_final_data_limit.RF1104,
        ra_final_data_limit.RF1105,
        ra_final_data_limit.RF1301,
        ra_final_data_limit.RF1302,
        ra_final_data_limit.RF1303,
        ra_final_data_limit.RF1304,
        ra_final_data_limit.RF1305,
        ra_final_data_limit.RF1306,
        ra_final_data_limit.RF1308,
        ra_final_data_limit.RF1309,
        ra_final_data_limit.RF1310,
        ra_final_data_limit.RF1401,
        ra_final_data_limit.RF1402,
        ra_final_data_limit.RF1403,
        ra_final_data_limit.RF1406,
        ra_final_data_limit.RF1407,
        ra_final_data_limit.RF1408,
        ra_final_data_limit.RF1409,
        ra_final_data_limit.RF1410,
        ra_final_data_limit.RF1412,
        ra_final_data_limit.RF1413,
        ra_final_data_limit.RF1414,
        ra_final_data_limit.RF1415,
        ra_final_data_limit.RF1416,
        ra_final_data_limit.RF1417,
        ra_final_data_limit.RF1418,
        ra_final_data_limit.RF1419,
        ra_final_data_limit.RF1421,
        ra_final_data_limit.RF1436,
        ra_final_data_limit.RF1441,
        ra_final_data_limit.RF1450,
        ra_final_data_limit.RF1454,
        ra_final_data_limit.RF1458,
        ra_final_data_limit.RF1460,
        ra_final_data_limit.RF1461,
        ra_final_data_limit.RF1462,
        ra_final_data_limit.RF1467,
        ra_final_data_limit.RF1601,
        ra_final_data_limit.RF1602,
        ra_final_data_limit.RF1603,
        ra_final_data_limit.RF1604,
        ra_final_data_limit.RF1605,
        ra_final_data_limit.RF1701,
        ra_final_data_limit.RF1702,
        ra_final_data_limit.RF1703,
        ra_final_data_limit.RF1704,
        ra_final_data_limit.RF1705,
        ra_final_data_limit.RF1706,
        ra_final_data_limit.RF1707,
        ra_final_data_limit.RF1708,
        ra_final_data_limit.RF1709,
        ra_final_data_limit.RF1710,
        ra_final_data_limit.RF1711,
        ra_final_data_limit.RF1712,
        ra_final_data_limit.RF1801,
        ra_final_data_limit.RF1901,
        ra_final_data_limit.RF1902,
        ra_final_data_limit.RF2001,
        ra_final_data_limit.RF2002,
        ra_final_data_limit.RF2101,
        ra_final_data_limit.RF2102,
        ra_final_data_limit.RF2201,
        ra_final_data_limit.RF2301,
        ra_final_data_limit.RF2302,
        ra_final_data_limit.RF2303,
        ra_final_data_limit.RF2304,
        ra_final_data_limit.RF2305,
        ra_final_data_limit.RF2306,
        ra_final_data_limit.RF2307,
        ra_final_data_limit.RF2308,
        ra_final_data_limit.RF2309,
        ra_final_data_limit.RF2310,
        ra_final_data_limit.RF2311,
        ra_final_data_limit.RF2401,
        ra_final_data_limit.RF2402,
        ra_final_data_limit.RF2403,
        ra_final_data_limit.RF2404,
        ra_final_data_limit.RF2405,
        ra_final_data_limit.RF2501,
        ra_final_data_limit.STCT0001,
        ra_final_data_limit.STCT0002,
        ra_final_data_limit.STCT0003,
        ra_final_data_limit.STCT00100,
        ra_final_data_limit.STCT0018,
        ra_final_data_limit.STCT0020,
        ra_final_data_limit.STCT0021,
        ra_final_data_limit.STCT0022,
        ra_final_data_limit.STCT0023,
        ra_final_data_limit.STCT0027,
        ra_final_data_limit.STCT0040,
        ra_final_data_limit.STCT0045,
        ra_final_data_limit.STCT0050,
        ra_final_data_limit.STCT0053,
        ra_final_data_limit.STCT0054,
        ra_final_data_limit.STCT0062,
        ra_final_data_limit.STCT0063,
        ra_final_data_limit.STCT0064,
        ra_final_data_limit.STCT0065,
        ra_final_data_limit.STCT0066,
        ra_final_data_limit.STCT0067,
        ra_final_data_limit.STCT0068,
        ra_final_data_limit.STCT0069,
        ra_final_data_limit.STCT0070,
        ra_final_data_limit.STCT0073,
        ra_final_data_limit.STCT0074,
        ra_final_data_limit.STCT0098,
        ra_final_data_limit.STCT0099,
        ra_final_data_limit.STCT0254,
        ra_final_data_limit.STCT0255,
        ra_final_data_limit.STCT0387,
        ra_final_data_limit.STCT0388,
        ra_final_data_limit.STCT04285,
        ra_final_data_limit.STCT04286,
        ra_final_data_limit.STCT04287,
        ra_final_data_limit.STCT04288,
        ra_final_data_limit.STCT04290,
        ra_final_data_limit.STCT04291,
        ra_final_data_limit.STCT04292,
        ra_final_data_limit.STCT04294,
        ra_final_data_limit.STCT05263,
        ra_final_data_limit.STCT05264,
        ra_final_data_limit.STCT05265,
        ra_final_data_limit.STCT05266,
        ra_final_data_limit.STCT05267,
        ra_final_data_limit.STCT05268,
        ra_final_data_limit.STCT06124,
        ra_final_data_limit.STCT06125,
        ra_final_data_limit.STCT06127,
        ra_final_data_limit.STCT06128,
        ra_final_data_limit.STCT06131,
        ra_final_data_limit.STCT06132,
        ra_final_data_limit.STCT06133,
        ra_final_data_limit.STCT06134,
        ra_final_data_limit.STCT06135,
        ra_final_data_limit.STCT06136,
        ra_final_data_limit.STCT06137,
        ra_final_data_limit.STCT06138,
        ra_final_data_limit.STCT06139,
        ra_final_data_limit.STCT06140,
        ra_final_data_limit.STCT06305,
        ra_final_data_limit.STCT06306,
        ra_final_data_limit.STCT06307,
        ra_final_data_limit.STCT06312,
        ra_final_data_limit.STCT0738,
        ra_final_data_limit.STCT0739,
        ra_final_data_limit.STCT08126,
        ra_final_data_limit.STCT08127,
        ra_final_data_limit.STCT08129,
        ra_final_data_limit.STCT08130,
        ra_final_data_limit.STCT08131,
        ra_final_data_limit.STCT08132,
        ra_final_data_limit.STCT08133,
        ra_final_data_limit.STCT08134,
        ra_final_data_limit.STCT08136,
        ra_final_data_limit.STCT08138,
        ra_final_data_limit.STCT08140,
        ra_final_data_limit.STCT08141,
        ra_final_data_limit.STCT08142,
        ra_final_data_limit.STCT08143,
        ra_final_data_limit.STCT08144,
        ra_final_data_limit.STCT08145,
        ra_final_data_limit.STCT08309,
        ra_final_data_limit.STCT09100,
        ra_final_data_limit.STCT09101,
        ra_final_data_limit.STCT0996,
        ra_final_data_limit.STCT0998,
        ra_final_data_limit.STCT0999,
        ra_final_data_limit.STCT1342,
        ra_final_data_limit.STCT1343,
        ra_final_data_limit.STCT1344,
        ra_final_data_limit.STCT1345,
        ra_final_data_limit.STCT1346,
        ra_final_data_limit.STCT1347,
        ra_final_data_limit.STCT1348,
        ra_final_data_limit.STCT1349,
        ra_final_data_limit.STCT14296,
        ra_final_data_limit.STCT1455,
        ra_final_data_limit.STCT17102,
        ra_final_data_limit.STCT17104,
        ra_final_data_limit.STCT1798,
        ra_final_data_limit.STCT1799,
        ra_final_data_limit.STCT2005,
        ra_final_data_limit.STCT23308,
        ra_final_data_limit.STDX01384,
        ra_final_data_limit.STDX01385,
        ra_final_data_limit.STDX01388,
        ra_final_data_limit.STDX01398,
        ra_final_data_limit.STDX01399,
        ra_final_data_limit.STDX01400,
        ra_final_data_limit.STDX01405,
        ra_final_data_limit.STDX01409,
        ra_final_data_limit.STDX02135,
        ra_final_data_limit.STDX02157,
        ra_final_data_limit.STDX02158,
        ra_final_data_limit.STDX02159,
        ra_final_data_limit.STDX02162,
        ra_final_data_limit.STDX02163,
        ra_final_data_limit.STDX02164,
        ra_final_data_limit.STDX02165,
        ra_final_data_limit.STDX02166,
        ra_final_data_limit.STDX02167,
        ra_final_data_limit.STDX03252,
        ra_final_data_limit.STDX03254,
        ra_final_data_limit.STDX03256,
        ra_final_data_limit.STDX03257,
        ra_final_data_limit.STDX03258,
        ra_final_data_limit.STDX03259,
        ra_final_data_limit.STDX03260,
        ra_final_data_limit.STDX03261,
        ra_final_data_limit.STDX03262,
        ra_final_data_limit.STDX03264,
        ra_final_data_limit.STDX03266,
        ra_final_data_limit.STDX03267,
        ra_final_data_limit.STDX04138,
        ra_final_data_limit.STDX04163,
        ra_final_data_limit.STDX04165,
        ra_final_data_limit.STDX04167,
        ra_final_data_limit.STDX04168,
        ra_final_data_limit.STDX04172,
        ra_final_data_limit.STDX04173,
        ra_final_data_limit.STDX04174,
        ra_final_data_limit.STDX04180,
        ra_final_data_limit.STDX04181,
        ra_final_data_limit.STDX04182,
        ra_final_data_limit.STDX04183,
        ra_final_data_limit.STDX04184,
        ra_final_data_limit.STDX04185,
        ra_final_data_limit.STDX04186,
        ra_final_data_limit.STDX04188,
        ra_final_data_limit.STDX04190,
        ra_final_data_limit.STDX05281,
        ra_final_data_limit.STDX05282,
        ra_final_data_limit.STDX05283,
        ra_final_data_limit.STDX05284,
        ra_final_data_limit.STDX05285,
        ra_final_data_limit.STDX05287,
        ra_final_data_limit.STDX05288,
        ra_final_data_limit.STDX05289,
        ra_final_data_limit.STDX05291,
        ra_final_data_limit.STDX05313,
        ra_final_data_limit.STDX05314,
        ra_final_data_limit.STDX05316,
        ra_final_data_limit.STDX05318,
        ra_final_data_limit.STDX05320,
        ra_final_data_limit.STDX05323,
        ra_final_data_limit.STDX05325,
        ra_final_data_limit.STDX05326,
        ra_final_data_limit.STDX05327,
        ra_final_data_limit.STDX05328,
        ra_final_data_limit.STDX05329,
        ra_final_data_limit.STDX05330,
        ra_final_data_limit.STDX05331,
        ra_final_data_limit.STDX05332,
        ra_final_data_limit.STDX05333,
        ra_final_data_limit.STDX05334,
        ra_final_data_limit.STDX05335,
        ra_final_data_limit.STDX05336,
        ra_final_data_limit.STDX05337,
        ra_final_data_limit.STDX05339,
        ra_final_data_limit.STDX05340,
        ra_final_data_limit.STDX05343,
        ra_final_data_limit.STDX05344,
        ra_final_data_limit.STDX05345,
        ra_final_data_limit.STDX06267,
        ra_final_data_limit.STDX06268,
        ra_final_data_limit.STDX06277,
        ra_final_data_limit.STDX06281,
        ra_final_data_limit.STDX06283,
        ra_final_data_limit.STDX06285,
        ra_final_data_limit.STDX06286,
        ra_final_data_limit.STDX06287,
        ra_final_data_limit.STDX06288,
        ra_final_data_limit.STDX06290,
        ra_final_data_limit.STDX06291,
        ra_final_data_limit.STDX06295,
        ra_final_data_limit.STDX06297,
        ra_final_data_limit.STDX06298,
        ra_final_data_limit.STDX06301,
        ra_final_data_limit.STDX06306,
        ra_final_data_limit.STDX06307,
        ra_final_data_limit.STDX06308,
        ra_final_data_limit.STDX06311,
        ra_final_data_limit.STDX06312,
        ra_final_data_limit.STDX06313,
        ra_final_data_limit.STDX06314,
        ra_final_data_limit.STDX06315,
        ra_final_data_limit.STDX06316,
        ra_final_data_limit.STDX06317,
        ra_final_data_limit.STDX06318,
        ra_final_data_limit.STDX06319,
        ra_final_data_limit.STDX06320,
        ra_final_data_limit.STDX06321,
        ra_final_data_limit.STDX06322,
        ra_final_data_limit.STDX06323,
        ra_final_data_limit.STDX06324,
        ra_final_data_limit.STDX06325,
        ra_final_data_limit.STDX06326,
        ra_final_data_limit.STDX06327,
        ra_final_data_limit.STDX06328,
        ra_final_data_limit.STDX06331,
        ra_final_data_limit.STDX06332,
        ra_final_data_limit.STDX06335,
        ra_final_data_limit.STDX06336,
        ra_final_data_limit.STDX06337,
        ra_final_data_limit.STDX06340,
        ra_final_data_limit.STDX06341,
        ra_final_data_limit.STDX06342,
        ra_final_data_limit.STDX06343,
        ra_final_data_limit.STDX06344,
        ra_final_data_limit.STDX06345,
        ra_final_data_limit.STDX06346,
        ra_final_data_limit.STDX06347,
        ra_final_data_limit.STDX06348,
        ra_final_data_limit.STDX06349,
        ra_final_data_limit.STDX06350,
        ra_final_data_limit.STDX06351,
        ra_final_data_limit.STDX06353,
        ra_final_data_limit.STDX06356,
        ra_final_data_limit.STDX07101,
        ra_final_data_limit.STDX07102,
        ra_final_data_limit.STDX07103,
        ra_final_data_limit.STDX07104,
        ra_final_data_limit.STDX07105,
        ra_final_data_limit.STDX07106,
        ra_final_data_limit.STDX07107,
        ra_final_data_limit.STDX07111,
        ra_final_data_limit.STDX07112,
        ra_final_data_limit.STDX07113,
        ra_final_data_limit.STDX07116,
        ra_final_data_limit.STDX081040,
        ra_final_data_limit.STDX081041,
        ra_final_data_limit.STDX081042,
        ra_final_data_limit.STDX081043,
        ra_final_data_limit.STDX081044,
        ra_final_data_limit.STDX081045,
        ra_final_data_limit.STDX081046,
        ra_final_data_limit.STDX081047,
        ra_final_data_limit.STDX081048,
        ra_final_data_limit.STDX081050,
        ra_final_data_limit.STDX081052,
        ra_final_data_limit.STDX081053,
        ra_final_data_limit.STDX081054,
        ra_final_data_limit.STDX081055,
        ra_final_data_limit.STDX081056,
        ra_final_data_limit.STDX081057,
        ra_final_data_limit.STDX081058,
        ra_final_data_limit.STDX081059,
        ra_final_data_limit.STDX081060,
        ra_final_data_limit.STDX081061,
        ra_final_data_limit.STDX081062,
        ra_final_data_limit.STDX081063,
        ra_final_data_limit.STDX081064,
        ra_final_data_limit.STDX081065,
        ra_final_data_limit.STDX081066,
        ra_final_data_limit.STDX081067,
        ra_final_data_limit.STDX081071,
        ra_final_data_limit.STDX081073,
        ra_final_data_limit.STDX081074,
        ra_final_data_limit.STDX081075,
        ra_final_data_limit.STDX081085,
        ra_final_data_limit.STDX081086,
        ra_final_data_limit.STDX081087,
        ra_final_data_limit.STDX081089,
        ra_final_data_limit.STDX081092,
        ra_final_data_limit.STDX081093,
        ra_final_data_limit.STDX081094,
        ra_final_data_limit.STDX081095,
        ra_final_data_limit.STDX081096,
        ra_final_data_limit.STDX081097,
        ra_final_data_limit.STDX09196,
        ra_final_data_limit.STDX09197,
        ra_final_data_limit.STDX09198,
        ra_final_data_limit.STDX09199,
        ra_final_data_limit.STDX09200,
        ra_final_data_limit.STDX1007,
        ra_final_data_limit.STDX10101,
        ra_final_data_limit.STDX10102,
        ra_final_data_limit.STDX10103,
        ra_final_data_limit.STDX10104,
        ra_final_data_limit.STDX10105,
        ra_final_data_limit.STDX10106,
        ra_final_data_limit.STDX10107,
        ra_final_data_limit.STDX10108,
        ra_final_data_limit.STDX10109,
        ra_final_data_limit.STDX10111,
        ra_final_data_limit.STDX10112,
        ra_final_data_limit.STDX10113,
        ra_final_data_limit.STDX10114,
        ra_final_data_limit.STDX10115,
        ra_final_data_limit.STDX10116,
        ra_final_data_limit.STDX1019,
        ra_final_data_limit.STDX11154,
        ra_final_data_limit.STDX1194,
        ra_final_data_limit.STDX1195,
        ra_final_data_limit.STDX1234,
        ra_final_data_limit.STDX1238,
        ra_final_data_limit.STDX13100,
        ra_final_data_limit.STDX13101,
        ra_final_data_limit.STDX13102,
        ra_final_data_limit.STDX13103,
        ra_final_data_limit.STDX13104,
        ra_final_data_limit.STDX13105,
        ra_final_data_limit.STDX13106,
        ra_final_data_limit.STDX13107,
        ra_final_data_limit.STDX13111,
        ra_final_data_limit.STDX1394,
        ra_final_data_limit.STDX1395,
        ra_final_data_limit.STDX1396,
        ra_final_data_limit.STDX1397,
        ra_final_data_limit.STDX1398,
        ra_final_data_limit.STDX1399,
        ra_final_data_limit.STDX1406,
        ra_final_data_limit.STDX1407,
        ra_final_data_limit.STDX1409,
        ra_final_data_limit.STDX14100,
        ra_final_data_limit.STDX1413,
        ra_final_data_limit.STDX1422,
        ra_final_data_limit.STDX1432,
        ra_final_data_limit.STDX1436,
        ra_final_data_limit.STDX1442,
        ra_final_data_limit.STDX1445,
        ra_final_data_limit.STDX1447,
        ra_final_data_limit.STDX1448,
        ra_final_data_limit.STDX1451,
        ra_final_data_limit.STDX1453,
        ra_final_data_limit.STDX1454,
        ra_final_data_limit.STDX1455,
        ra_final_data_limit.STDX1459,
        ra_final_data_limit.STDX1460,
        ra_final_data_limit.STDX1461,
        ra_final_data_limit.STDX1462,
        ra_final_data_limit.STDX1465,
        ra_final_data_limit.STDX1466,
        ra_final_data_limit.STDX1468,
        ra_final_data_limit.STDX1470,
        ra_final_data_limit.STDX1472,
        ra_final_data_limit.STDX1474,
        ra_final_data_limit.STDX1482,
        ra_final_data_limit.STDX1483,
        ra_final_data_limit.STDX1484,
        ra_final_data_limit.STDX1487,
        ra_final_data_limit.STDX1488,
        ra_final_data_limit.STDX1490,
        ra_final_data_limit.STDX1491,
        ra_final_data_limit.STDX1492,
        ra_final_data_limit.STDX1494,
        ra_final_data_limit.STDX1495,
        ra_final_data_limit.STDX1497,
        ra_final_data_limit.STDX1498,
        ra_final_data_limit.STDX1499,
        ra_final_data_limit.STDX1583,
        ra_final_data_limit.STDX1587,
        ra_final_data_limit.STDX16293,
        ra_final_data_limit.STDX1767,
        ra_final_data_limit.STDX1768,
        ra_final_data_limit.STDX1771,
        ra_final_data_limit.STDX1772,
        ra_final_data_limit.STDX1773,
        ra_final_data_limit.STDX1846,
        ra_final_data_limit.STDX1942,
        ra_final_data_limit.STDX1953,
        ra_final_data_limit.STDX1954,
        ra_final_data_limit.STDX1955,
        ra_final_data_limit.STDX1961,
        ra_final_data_limit.STDX1963,
        ra_final_data_limit.STDX1964,
        ra_final_data_limit.STDX1965,
        ra_final_data_limit.STDX1966,
        ra_final_data_limit.STDX1969,
        ra_final_data_limit.STDX1972,
        ra_final_data_limit.STDX1975,
        ra_final_data_limit.STDX1976,
        ra_final_data_limit.STDX1977,
        ra_final_data_limit.STDX1978,
        ra_final_data_limit.STDX1979,
        ra_final_data_limit.STDX1980,
        ra_final_data_limit.STDX1981,
        ra_final_data_limit.STDX1982,
        ra_final_data_limit.STDX1983,
        ra_final_data_limit.STDX1984,
        ra_final_data_limit.STDX1985,
        ra_final_data_limit.STDX1986,
        ra_final_data_limit.STDX1987,
        ra_final_data_limit.STDX1988,
        ra_final_data_limit.STDX1990,
        ra_final_data_limit.STDX1991,
        ra_final_data_limit.STDX1992,
        ra_final_data_limit.STDX1993,
        ra_final_data_limit.STDX2008,
        ra_final_data_limit.STDX2009,
        ra_final_data_limit.STDX2012,
        ra_final_data_limit.STDX2013,
        ra_final_data_limit.STDX2014,
        ra_final_data_limit.STDX2015,
        ra_final_data_limit.STDX2016,
        ra_final_data_limit.STDX2017,
        ra_final_data_limit.STDX2018,
        ra_final_data_limit.STDX2019,
        ra_final_data_limit.STDX2020,
        ra_final_data_limit.STDX2021,
        ra_final_data_limit.STDX2023,
        ra_final_data_limit.STDX2159,
        ra_final_data_limit.STDX2361,
        ra_final_data_limit.STDX2367,
        ra_final_data_limit.STDX2369,
        ra_final_data_limit.STDX2371,
        ra_final_data_limit.STPX0002,
        ra_final_data_limit.STPX04219,
        ra_final_data_limit.STPX04220,
        ra_final_data_limit.STPX04221,
        ra_final_data_limit.STPX04222,
        ra_final_data_limit.STPX04224,
        ra_final_data_limit.STPX04225,
        ra_final_data_limit.STPX04226,
        ra_final_data_limit.STPX05228,
        ra_final_data_limit.STPX05229,
        ra_final_data_limit.STPX05230,
        ra_final_data_limit.STPX05231,
        ra_final_data_limit.STPX05236,
        ra_final_data_limit.STPX05238,
        ra_final_data_limit.STPX05239,
        ra_final_data_limit.STPX05240,
        ra_final_data_limit.STPX0544,
        ra_final_data_limit.STPX06224,
        ra_final_data_limit.STPX06226,
        ra_final_data_limit.STPX06230,
        ra_final_data_limit.STPX06231,
        ra_final_data_limit.STPX06234,
        ra_final_data_limit.STPX06240,
        ra_final_data_limit.STPX06241,
        ra_final_data_limit.STPX06243,
        ra_final_data_limit.STPX06244,
        ra_final_data_limit.STPX06245,
        ra_final_data_limit.STPX06246,
        ra_final_data_limit.STPX06247,
        ra_final_data_limit.STPX06248,
        ra_final_data_limit.STPX06249,
        ra_final_data_limit.STPX06251,
        ra_final_data_limit.STPX07231,
        ra_final_data_limit.STPX07232,
        ra_final_data_limit.STPX08217,
        ra_final_data_limit.STPX08219,
        ra_final_data_limit.STPX08220,
        ra_final_data_limit.STPX08221,
        ra_final_data_limit.STPX08226,
        ra_final_data_limit.STPX08227,
        ra_final_data_limit.STPX08228,
        ra_final_data_limit.STPX08230,
        ra_final_data_limit.STPX08231,
        ra_final_data_limit.STPX08232,
        ra_final_data_limit.STPX08233,
        ra_final_data_limit.STPX08234,
        ra_final_data_limit.STPX08235,
        ra_final_data_limit.STPX09183,
        ra_final_data_limit.STPX09184,
        ra_final_data_limit.STPX09185,
        ra_final_data_limit.STPX09186,
        ra_final_data_limit.STPX09187,
        ra_final_data_limit.STPX13133,
        ra_final_data_limit.STPX13134,
        ra_final_data_limit.STPX13135,
        ra_final_data_limit.STPX13136,
        ra_final_data_limit.STPX13137,
        ra_final_data_limit.STPX13138,
        ra_final_data_limit.STPX13139,
        ra_final_data_limit.STPX13140,
        ra_final_data_limit.STPX14146,
        ra_final_data_limit.STPX17225
 FROM epbuilder.ra_final_data_limit
 ORDER BY ra_final_data_limit.row_names,
          ra_final_data_limit.epi_id,
          ra_final_data_limit.member_id,
          ra_final_data_limit.epi_number,
          ra_final_data_limit.epi_name,
          ra_final_data_limit.female,
          ra_final_data_limit.age,
          ra_final_data_limit.rec_enr,
          ra_final_data_limit.eol_ind,
          ra_final_data_limit.cost_ra_comp_l1,
          ra_final_data_limit.cost_ra_comp_l5,
          ra_final_data_limit.cost_ra_comp_other_l1,
          ra_final_data_limit.cost_ra_comp_other_l3,
          ra_final_data_limit.cost_ra_comp_other_l4,
          ra_final_data_limit.cost_ra_typ_ip_l1,
          ra_final_data_limit.cost_ra_typ_ip_l3,
          ra_final_data_limit.cost_ra_typ_ip_l4,
          ra_final_data_limit.cost_ra_typ_l1,
          ra_final_data_limit.cost_ra_typ_l5,
          ra_final_data_limit.cost_ra_typ_other_l3,
          ra_final_data_limit.cost_ra_typ_other_l4,
          ra_final_data_limit.cost_sa_comp_l1,
          ra_final_data_limit.cost_sa_comp_l5,
          ra_final_data_limit.cost_sa_comp_other_l1,
          ra_final_data_limit.cost_sa_comp_other_l3,
          ra_final_data_limit.cost_sa_comp_other_l4,
          ra_final_data_limit.cost_sa_typ_ip_l1,
          ra_final_data_limit.cost_sa_typ_ip_l3,
          ra_final_data_limit.cost_sa_typ_ip_l4,
          ra_final_data_limit.cost_sa_typ_l1,
          ra_final_data_limit.cost_sa_typ_l5,
          ra_final_data_limit.cost_sa_typ_other_l1,
          ra_final_data_limit.cost_sa_typ_other_l3,
          ra_final_data_limit.cost_sa_typ_other_l4,
          ra_final_data_limit.typ_ip_ind,
          ra_final_data_limit.use_comp_l1,
          ra_final_data_limit.use_comp_l5,
          ra_final_data_limit.use_comp_other_l1,
          ra_final_data_limit.use_comp_other_l3,
          ra_final_data_limit.use_comp_other_l4,
          ra_final_data_limit.use_typ_ip_l1,
          ra_final_data_limit.use_typ_ip_l3,
          ra_final_data_limit.use_typ_ip_l4,
          ra_final_data_limit.use_typ_l1,
          ra_final_data_limit.use_typ_l5,
          ra_final_data_limit.use_typ_other_l1,
          ra_final_data_limit.use_typ_other_l3,
          ra_final_data_limit.use_typ_other_l4,
          ra_final_data_limit.RF0101,
          ra_final_data_limit.RF0102,
          ra_final_data_limit.RF0103,
          ra_final_data_limit.RF0104,
          ra_final_data_limit.RF0105,
          ra_final_data_limit.RF0106,
          ra_final_data_limit.RF0107,
          ra_final_data_limit.RF0108,
          ra_final_data_limit.RF0109,
          ra_final_data_limit.RF0110,
          ra_final_data_limit.RF0111,
          ra_final_data_limit.RF0115,
          ra_final_data_limit.RF0201,
          ra_final_data_limit.RF0301,
          ra_final_data_limit.RF0401,
          ra_final_data_limit.RF0402,
          ra_final_data_limit.RF0403,
          ra_final_data_limit.RF0404,
          ra_final_data_limit.RF0406,
          ra_final_data_limit.RF0407,
          ra_final_data_limit.RF0408,
          ra_final_data_limit.RF0501,
          ra_final_data_limit.RF0502,
          ra_final_data_limit.RF0503,
          ra_final_data_limit.RF0504,
          ra_final_data_limit.RF0505,
          ra_final_data_limit.RF0506,
          ra_final_data_limit.RF0507,
          ra_final_data_limit.RF0508,
          ra_final_data_limit.RF0509,
          ra_final_data_limit.RF0510,
          ra_final_data_limit.RF0511,
          ra_final_data_limit.RF0512,
          ra_final_data_limit.RF0513,
          ra_final_data_limit.RF0514,
          ra_final_data_limit.RF0518,
          ra_final_data_limit.RF0519,
          ra_final_data_limit.RF0520,
          ra_final_data_limit.RF0521,
          ra_final_data_limit.RF0524,
          ra_final_data_limit.RF0525,
          ra_final_data_limit.RF0601,
          ra_final_data_limit.RF0602,
          ra_final_data_limit.RF0603,
          ra_final_data_limit.RF0604,
          ra_final_data_limit.RF0606,
          ra_final_data_limit.RF0607,
          ra_final_data_limit.RF0608,
          ra_final_data_limit.RF0609,
          ra_final_data_limit.RF0610,
          ra_final_data_limit.RF0611,
          ra_final_data_limit.RF0612,
          ra_final_data_limit.RF0613,
          ra_final_data_limit.RF0614,
          ra_final_data_limit.RF0615,
          ra_final_data_limit.RF0616,
          ra_final_data_limit.RF0617,
          ra_final_data_limit.RF0618,
          ra_final_data_limit.RF0619,
          ra_final_data_limit.RF0620,
          ra_final_data_limit.RF0701,
          ra_final_data_limit.RF0702,
          ra_final_data_limit.RF0703,
          ra_final_data_limit.RF0704,
          ra_final_data_limit.RF0705,
          ra_final_data_limit.RF0801,
          ra_final_data_limit.RF0802,
          ra_final_data_limit.RF0803,
          ra_final_data_limit.RF0804,
          ra_final_data_limit.RF0805,
          ra_final_data_limit.RF0806,
          ra_final_data_limit.RF0807,
          ra_final_data_limit.RF0808,
          ra_final_data_limit.RF0809,
          ra_final_data_limit.RF0810,
          ra_final_data_limit.RF0811,
          ra_final_data_limit.RF0812,
          ra_final_data_limit.RF0813,
          ra_final_data_limit.RF0814,
          ra_final_data_limit.RF0901,
          ra_final_data_limit.RF0902,
          ra_final_data_limit.RF1001,
          ra_final_data_limit.RF1002,
          ra_final_data_limit.RF1003,
          ra_final_data_limit.RF1101,
          ra_final_data_limit.RF1102,
          ra_final_data_limit.RF1103,
          ra_final_data_limit.RF1104,
          ra_final_data_limit.RF1105,
          ra_final_data_limit.RF1301,
          ra_final_data_limit.RF1302,
          ra_final_data_limit.RF1303,
          ra_final_data_limit.RF1304,
          ra_final_data_limit.RF1305,
          ra_final_data_limit.RF1306,
          ra_final_data_limit.RF1308,
          ra_final_data_limit.RF1309,
          ra_final_data_limit.RF1310,
          ra_final_data_limit.RF1401,
          ra_final_data_limit.RF1402,
          ra_final_data_limit.RF1403,
          ra_final_data_limit.RF1406,
          ra_final_data_limit.RF1407,
          ra_final_data_limit.RF1408,
          ra_final_data_limit.RF1409,
          ra_final_data_limit.RF1410,
          ra_final_data_limit.RF1412,
          ra_final_data_limit.RF1413,
          ra_final_data_limit.RF1414,
          ra_final_data_limit.RF1415,
          ra_final_data_limit.RF1416,
          ra_final_data_limit.RF1417,
          ra_final_data_limit.RF1418,
          ra_final_data_limit.RF1419,
          ra_final_data_limit.RF1421,
          ra_final_data_limit.RF1436,
          ra_final_data_limit.RF1441,
          ra_final_data_limit.RF1450,
          ra_final_data_limit.RF1454,
          ra_final_data_limit.RF1458,
          ra_final_data_limit.RF1460,
          ra_final_data_limit.RF1461,
          ra_final_data_limit.RF1462,
          ra_final_data_limit.RF1467,
          ra_final_data_limit.RF1601,
          ra_final_data_limit.RF1602,
          ra_final_data_limit.RF1603,
          ra_final_data_limit.RF1604,
          ra_final_data_limit.RF1605,
          ra_final_data_limit.RF1701,
          ra_final_data_limit.RF1702,
          ra_final_data_limit.RF1703,
          ra_final_data_limit.RF1704,
          ra_final_data_limit.RF1705,
          ra_final_data_limit.RF1706,
          ra_final_data_limit.RF1707,
          ra_final_data_limit.RF1708,
          ra_final_data_limit.RF1709,
          ra_final_data_limit.RF1710,
          ra_final_data_limit.RF1711,
          ra_final_data_limit.RF1712,
          ra_final_data_limit.RF1801,
          ra_final_data_limit.RF1901,
          ra_final_data_limit.RF1902,
          ra_final_data_limit.RF2001,
          ra_final_data_limit.RF2002,
          ra_final_data_limit.RF2101,
          ra_final_data_limit.RF2102,
          ra_final_data_limit.RF2201,
          ra_final_data_limit.RF2301,
          ra_final_data_limit.RF2302,
          ra_final_data_limit.RF2303,
          ra_final_data_limit.RF2304,
          ra_final_data_limit.RF2305,
          ra_final_data_limit.RF2306,
          ra_final_data_limit.RF2307,
          ra_final_data_limit.RF2308,
          ra_final_data_limit.RF2309,
          ra_final_data_limit.RF2310,
          ra_final_data_limit.RF2311,
          ra_final_data_limit.RF2401,
          ra_final_data_limit.RF2402,
          ra_final_data_limit.RF2403,
          ra_final_data_limit.RF2404,
          ra_final_data_limit.RF2405,
          ra_final_data_limit.RF2501,
          ra_final_data_limit.STCT0001,
          ra_final_data_limit.STCT0002,
          ra_final_data_limit.STCT0003,
          ra_final_data_limit.STCT00100,
          ra_final_data_limit.STCT0018,
          ra_final_data_limit.STCT0020,
          ra_final_data_limit.STCT0021,
          ra_final_data_limit.STCT0022,
          ra_final_data_limit.STCT0023,
          ra_final_data_limit.STCT0027,
          ra_final_data_limit.STCT0040,
          ra_final_data_limit.STCT0045,
          ra_final_data_limit.STCT0050,
          ra_final_data_limit.STCT0053,
          ra_final_data_limit.STCT0054,
          ra_final_data_limit.STCT0062,
          ra_final_data_limit.STCT0063,
          ra_final_data_limit.STCT0064,
          ra_final_data_limit.STCT0065,
          ra_final_data_limit.STCT0066,
          ra_final_data_limit.STCT0067,
          ra_final_data_limit.STCT0068,
          ra_final_data_limit.STCT0069,
          ra_final_data_limit.STCT0070,
          ra_final_data_limit.STCT0073,
          ra_final_data_limit.STCT0074,
          ra_final_data_limit.STCT0098,
          ra_final_data_limit.STCT0099,
          ra_final_data_limit.STCT0254,
          ra_final_data_limit.STCT0255,
          ra_final_data_limit.STCT0387,
          ra_final_data_limit.STCT0388,
          ra_final_data_limit.STCT04285,
          ra_final_data_limit.STCT04286,
          ra_final_data_limit.STCT04287,
          ra_final_data_limit.STCT04288,
          ra_final_data_limit.STCT04290,
          ra_final_data_limit.STCT04291,
          ra_final_data_limit.STCT04292,
          ra_final_data_limit.STCT04294,
          ra_final_data_limit.STCT05263,
          ra_final_data_limit.STCT05264,
          ra_final_data_limit.STCT05265,
          ra_final_data_limit.STCT05266,
          ra_final_data_limit.STCT05267,
          ra_final_data_limit.STCT05268,
          ra_final_data_limit.STCT06124,
          ra_final_data_limit.STCT06125,
          ra_final_data_limit.STCT06127,
          ra_final_data_limit.STCT06128,
          ra_final_data_limit.STCT06131,
          ra_final_data_limit.STCT06132,
          ra_final_data_limit.STCT06133,
          ra_final_data_limit.STCT06134,
          ra_final_data_limit.STCT06135,
          ra_final_data_limit.STCT06136,
          ra_final_data_limit.STCT06137,
          ra_final_data_limit.STCT06138,
          ra_final_data_limit.STCT06139,
          ra_final_data_limit.STCT06140,
          ra_final_data_limit.STCT06305,
          ra_final_data_limit.STCT06306,
          ra_final_data_limit.STCT06307,
          ra_final_data_limit.STCT06312,
          ra_final_data_limit.STCT0738,
          ra_final_data_limit.STCT0739,
          ra_final_data_limit.STCT08126,
          ra_final_data_limit.STCT08127,
          ra_final_data_limit.STCT08129,
          ra_final_data_limit.STCT08130,
          ra_final_data_limit.STCT08131,
          ra_final_data_limit.STCT08132,
          ra_final_data_limit.STCT08133,
          ra_final_data_limit.STCT08134,
          ra_final_data_limit.STCT08136,
          ra_final_data_limit.STCT08138,
          ra_final_data_limit.STCT08140,
          ra_final_data_limit.STCT08141,
          ra_final_data_limit.STCT08142,
          ra_final_data_limit.STCT08143,
          ra_final_data_limit.STCT08144,
          ra_final_data_limit.STCT08145,
          ra_final_data_limit.STCT08309,
          ra_final_data_limit.STCT09100,
          ra_final_data_limit.STCT09101,
          ra_final_data_limit.STCT0996,
          ra_final_data_limit.STCT0998,
          ra_final_data_limit.STCT0999,
          ra_final_data_limit.STCT1342,
          ra_final_data_limit.STCT1343,
          ra_final_data_limit.STCT1344,
          ra_final_data_limit.STCT1345,
          ra_final_data_limit.STCT1346,
          ra_final_data_limit.STCT1347,
          ra_final_data_limit.STCT1348,
          ra_final_data_limit.STCT1349,
          ra_final_data_limit.STCT14296,
          ra_final_data_limit.STCT1455,
          ra_final_data_limit.STCT17102,
          ra_final_data_limit.STCT17104,
          ra_final_data_limit.STCT1798,
          ra_final_data_limit.STCT1799,
          ra_final_data_limit.STCT2005,
          ra_final_data_limit.STCT23308,
          ra_final_data_limit.STDX01384,
          ra_final_data_limit.STDX01385,
          ra_final_data_limit.STDX01388,
          ra_final_data_limit.STDX01398,
          ra_final_data_limit.STDX01399,
          ra_final_data_limit.STDX01400,
          ra_final_data_limit.STDX01405,
          ra_final_data_limit.STDX01409,
          ra_final_data_limit.STDX02135,
          ra_final_data_limit.STDX02157,
          ra_final_data_limit.STDX02158,
          ra_final_data_limit.STDX02159,
          ra_final_data_limit.STDX02162,
          ra_final_data_limit.STDX02163,
          ra_final_data_limit.STDX02164,
          ra_final_data_limit.STDX02165,
          ra_final_data_limit.STDX02166,
          ra_final_data_limit.STDX02167,
          ra_final_data_limit.STDX03252,
          ra_final_data_limit.STDX03254,
          ra_final_data_limit.STDX03256,
          ra_final_data_limit.STDX03257,
          ra_final_data_limit.STDX03258,
          ra_final_data_limit.STDX03259,
          ra_final_data_limit.STDX03260,
          ra_final_data_limit.STDX03261,
          ra_final_data_limit.STDX03262,
          ra_final_data_limit.STDX03264,
          ra_final_data_limit.STDX03266,
          ra_final_data_limit.STDX03267,
          ra_final_data_limit.STDX04138,
          ra_final_data_limit.STDX04163,
          ra_final_data_limit.STDX04165,
          ra_final_data_limit.STDX04167,
          ra_final_data_limit.STDX04168,
          ra_final_data_limit.STDX04172,
          ra_final_data_limit.STDX04173,
          ra_final_data_limit.STDX04174,
          ra_final_data_limit.STDX04180,
          ra_final_data_limit.STDX04181,
          ra_final_data_limit.STDX04182,
          ra_final_data_limit.STDX04183,
          ra_final_data_limit.STDX04184,
          ra_final_data_limit.STDX04185,
          ra_final_data_limit.STDX04186,
          ra_final_data_limit.STDX04188,
          ra_final_data_limit.STDX04190,
          ra_final_data_limit.STDX05281,
          ra_final_data_limit.STDX05282,
          ra_final_data_limit.STDX05283,
          ra_final_data_limit.STDX05284,
          ra_final_data_limit.STDX05285,
          ra_final_data_limit.STDX05287,
          ra_final_data_limit.STDX05288,
          ra_final_data_limit.STDX05289,
          ra_final_data_limit.STDX05291,
          ra_final_data_limit.STDX05313,
          ra_final_data_limit.STDX05314,
          ra_final_data_limit.STDX05316,
          ra_final_data_limit.STDX05318,
          ra_final_data_limit.STDX05320,
          ra_final_data_limit.STDX05323,
          ra_final_data_limit.STDX05325,
          ra_final_data_limit.STDX05326,
          ra_final_data_limit.STDX05327,
          ra_final_data_limit.STDX05328,
          ra_final_data_limit.STDX05329,
          ra_final_data_limit.STDX05330,
          ra_final_data_limit.STDX05331,
          ra_final_data_limit.STDX05332,
          ra_final_data_limit.STDX05333,
          ra_final_data_limit.STDX05334,
          ra_final_data_limit.STDX05335,
          ra_final_data_limit.STDX05336,
          ra_final_data_limit.STDX05337,
          ra_final_data_limit.STDX05339,
          ra_final_data_limit.STDX05340,
          ra_final_data_limit.STDX05343,
          ra_final_data_limit.STDX05344,
          ra_final_data_limit.STDX05345,
          ra_final_data_limit.STDX06267,
          ra_final_data_limit.STDX06268,
          ra_final_data_limit.STDX06277,
          ra_final_data_limit.STDX06281,
          ra_final_data_limit.STDX06283,
          ra_final_data_limit.STDX06285,
          ra_final_data_limit.STDX06286,
          ra_final_data_limit.STDX06287,
          ra_final_data_limit.STDX06288,
          ra_final_data_limit.STDX06290,
          ra_final_data_limit.STDX06291,
          ra_final_data_limit.STDX06295,
          ra_final_data_limit.STDX06297,
          ra_final_data_limit.STDX06298,
          ra_final_data_limit.STDX06301,
          ra_final_data_limit.STDX06306,
          ra_final_data_limit.STDX06307,
          ra_final_data_limit.STDX06308,
          ra_final_data_limit.STDX06311,
          ra_final_data_limit.STDX06312,
          ra_final_data_limit.STDX06313,
          ra_final_data_limit.STDX06314,
          ra_final_data_limit.STDX06315,
          ra_final_data_limit.STDX06316,
          ra_final_data_limit.STDX06317,
          ra_final_data_limit.STDX06318,
          ra_final_data_limit.STDX06319,
          ra_final_data_limit.STDX06320,
          ra_final_data_limit.STDX06321,
          ra_final_data_limit.STDX06322,
          ra_final_data_limit.STDX06323,
          ra_final_data_limit.STDX06324,
          ra_final_data_limit.STDX06325,
          ra_final_data_limit.STDX06326,
          ra_final_data_limit.STDX06327,
          ra_final_data_limit.STDX06328,
          ra_final_data_limit.STDX06331,
          ra_final_data_limit.STDX06332,
          ra_final_data_limit.STDX06335,
          ra_final_data_limit.STDX06336,
          ra_final_data_limit.STDX06337,
          ra_final_data_limit.STDX06340,
          ra_final_data_limit.STDX06341,
          ra_final_data_limit.STDX06342,
          ra_final_data_limit.STDX06343,
          ra_final_data_limit.STDX06344,
          ra_final_data_limit.STDX06345,
          ra_final_data_limit.STDX06346,
          ra_final_data_limit.STDX06347,
          ra_final_data_limit.STDX06348,
          ra_final_data_limit.STDX06349,
          ra_final_data_limit.STDX06350,
          ra_final_data_limit.STDX06351,
          ra_final_data_limit.STDX06353,
          ra_final_data_limit.STDX06356,
          ra_final_data_limit.STDX07101,
          ra_final_data_limit.STDX07102,
          ra_final_data_limit.STDX07103,
          ra_final_data_limit.STDX07104,
          ra_final_data_limit.STDX07105,
          ra_final_data_limit.STDX07106,
          ra_final_data_limit.STDX07107,
          ra_final_data_limit.STDX07111,
          ra_final_data_limit.STDX07112,
          ra_final_data_limit.STDX07113,
          ra_final_data_limit.STDX07116,
          ra_final_data_limit.STDX081040,
          ra_final_data_limit.STDX081041,
          ra_final_data_limit.STDX081042,
          ra_final_data_limit.STDX081043,
          ra_final_data_limit.STDX081044,
          ra_final_data_limit.STDX081045,
          ra_final_data_limit.STDX081046,
          ra_final_data_limit.STDX081047,
          ra_final_data_limit.STDX081048,
          ra_final_data_limit.STDX081050,
          ra_final_data_limit.STDX081052,
          ra_final_data_limit.STDX081053,
          ra_final_data_limit.STDX081054,
          ra_final_data_limit.STDX081055,
          ra_final_data_limit.STDX081056,
          ra_final_data_limit.STDX081057,
          ra_final_data_limit.STDX081058,
          ra_final_data_limit.STDX081059,
          ra_final_data_limit.STDX081060,
          ra_final_data_limit.STDX081061,
          ra_final_data_limit.STDX081062,
          ra_final_data_limit.STDX081063,
          ra_final_data_limit.STDX081064,
          ra_final_data_limit.STDX081065,
          ra_final_data_limit.STDX081066,
          ra_final_data_limit.STDX081067,
          ra_final_data_limit.STDX081071,
          ra_final_data_limit.STDX081073,
          ra_final_data_limit.STDX081074,
          ra_final_data_limit.STDX081075,
          ra_final_data_limit.STDX081085,
          ra_final_data_limit.STDX081086,
          ra_final_data_limit.STDX081087,
          ra_final_data_limit.STDX081089,
          ra_final_data_limit.STDX081092,
          ra_final_data_limit.STDX081093,
          ra_final_data_limit.STDX081094,
          ra_final_data_limit.STDX081095,
          ra_final_data_limit.STDX081096,
          ra_final_data_limit.STDX081097,
          ra_final_data_limit.STDX09196,
          ra_final_data_limit.STDX09197,
          ra_final_data_limit.STDX09198,
          ra_final_data_limit.STDX09199,
          ra_final_data_limit.STDX09200,
          ra_final_data_limit.STDX1007,
          ra_final_data_limit.STDX10101,
          ra_final_data_limit.STDX10102,
          ra_final_data_limit.STDX10103,
          ra_final_data_limit.STDX10104,
          ra_final_data_limit.STDX10105,
          ra_final_data_limit.STDX10106,
          ra_final_data_limit.STDX10107,
          ra_final_data_limit.STDX10108,
          ra_final_data_limit.STDX10109,
          ra_final_data_limit.STDX10111,
          ra_final_data_limit.STDX10112,
          ra_final_data_limit.STDX10113,
          ra_final_data_limit.STDX10114,
          ra_final_data_limit.STDX10115,
          ra_final_data_limit.STDX10116,
          ra_final_data_limit.STDX1019,
          ra_final_data_limit.STDX11154,
          ra_final_data_limit.STDX1194,
          ra_final_data_limit.STDX1195,
          ra_final_data_limit.STDX1234,
          ra_final_data_limit.STDX1238,
          ra_final_data_limit.STDX13100,
          ra_final_data_limit.STDX13101,
          ra_final_data_limit.STDX13102,
          ra_final_data_limit.STDX13103,
          ra_final_data_limit.STDX13104,
          ra_final_data_limit.STDX13105,
          ra_final_data_limit.STDX13106,
          ra_final_data_limit.STDX13107,
          ra_final_data_limit.STDX13111,
          ra_final_data_limit.STDX1394,
          ra_final_data_limit.STDX1395,
          ra_final_data_limit.STDX1396,
          ra_final_data_limit.STDX1397,
          ra_final_data_limit.STDX1398,
          ra_final_data_limit.STDX1399,
          ra_final_data_limit.STDX1406,
          ra_final_data_limit.STDX1407,
          ra_final_data_limit.STDX1409,
          ra_final_data_limit.STDX14100,
          ra_final_data_limit.STDX1413,
          ra_final_data_limit.STDX1422,
          ra_final_data_limit.STDX1432,
          ra_final_data_limit.STDX1436,
          ra_final_data_limit.STDX1442,
          ra_final_data_limit.STDX1445,
          ra_final_data_limit.STDX1447,
          ra_final_data_limit.STDX1448,
          ra_final_data_limit.STDX1451,
          ra_final_data_limit.STDX1453,
          ra_final_data_limit.STDX1454,
          ra_final_data_limit.STDX1455,
          ra_final_data_limit.STDX1459,
          ra_final_data_limit.STDX1460,
          ra_final_data_limit.STDX1461,
          ra_final_data_limit.STDX1462,
          ra_final_data_limit.STDX1465,
          ra_final_data_limit.STDX1466,
          ra_final_data_limit.STDX1468,
          ra_final_data_limit.STDX1470,
          ra_final_data_limit.STDX1472,
          ra_final_data_limit.STDX1474,
          ra_final_data_limit.STDX1482,
          ra_final_data_limit.STDX1483,
          ra_final_data_limit.STDX1484,
          ra_final_data_limit.STDX1487,
          ra_final_data_limit.STDX1488,
          ra_final_data_limit.STDX1490,
          ra_final_data_limit.STDX1491,
          ra_final_data_limit.STDX1492,
          ra_final_data_limit.STDX1494,
          ra_final_data_limit.STDX1495,
          ra_final_data_limit.STDX1497,
          ra_final_data_limit.STDX1498,
          ra_final_data_limit.STDX1499,
          ra_final_data_limit.STDX1583,
          ra_final_data_limit.STDX1587,
          ra_final_data_limit.STDX16293,
          ra_final_data_limit.STDX1767,
          ra_final_data_limit.STDX1768,
          ra_final_data_limit.STDX1771,
          ra_final_data_limit.STDX1772,
          ra_final_data_limit.STDX1773,
          ra_final_data_limit.STDX1846,
          ra_final_data_limit.STDX1942,
          ra_final_data_limit.STDX1953,
          ra_final_data_limit.STDX1954,
          ra_final_data_limit.STDX1955,
          ra_final_data_limit.STDX1961,
          ra_final_data_limit.STDX1963,
          ra_final_data_limit.STDX1964,
          ra_final_data_limit.STDX1965,
          ra_final_data_limit.STDX1966,
          ra_final_data_limit.STDX1969,
          ra_final_data_limit.STDX1972,
          ra_final_data_limit.STDX1975,
          ra_final_data_limit.STDX1976,
          ra_final_data_limit.STDX1977,
          ra_final_data_limit.STDX1978,
          ra_final_data_limit.STDX1979,
          ra_final_data_limit.STDX1980,
          ra_final_data_limit.STDX1981,
          ra_final_data_limit.STDX1982,
          ra_final_data_limit.STDX1983,
          ra_final_data_limit.STDX1984,
          ra_final_data_limit.STDX1985,
          ra_final_data_limit.STDX1986,
          ra_final_data_limit.STDX1987,
          ra_final_data_limit.STDX1988,
          ra_final_data_limit.STDX1990,
          ra_final_data_limit.STDX1991,
          ra_final_data_limit.STDX1992,
          ra_final_data_limit.STDX1993,
          ra_final_data_limit.STDX2008,
          ra_final_data_limit.STDX2009,
          ra_final_data_limit.STDX2012,
          ra_final_data_limit.STDX2013,
          ra_final_data_limit.STDX2014,
          ra_final_data_limit.STDX2015,
          ra_final_data_limit.STDX2016,
          ra_final_data_limit.STDX2017,
          ra_final_data_limit.STDX2018,
          ra_final_data_limit.STDX2019,
          ra_final_data_limit.STDX2020,
          ra_final_data_limit.STDX2021,
          ra_final_data_limit.STDX2023,
          ra_final_data_limit.STDX2159,
          ra_final_data_limit.STDX2361,
          ra_final_data_limit.STDX2367,
          ra_final_data_limit.STDX2369,
          ra_final_data_limit.STDX2371,
          ra_final_data_limit.STPX0002,
          ra_final_data_limit.STPX04219,
          ra_final_data_limit.STPX04220,
          ra_final_data_limit.STPX04221,
          ra_final_data_limit.STPX04222,
          ra_final_data_limit.STPX04224,
          ra_final_data_limit.STPX04225,
          ra_final_data_limit.STPX04226,
          ra_final_data_limit.STPX05228,
          ra_final_data_limit.STPX05229,
          ra_final_data_limit.STPX05230,
          ra_final_data_limit.STPX05231,
          ra_final_data_limit.STPX05236,
          ra_final_data_limit.STPX05238,
          ra_final_data_limit.STPX05239,
          ra_final_data_limit.STPX05240,
          ra_final_data_limit.STPX0544,
          ra_final_data_limit.STPX06224,
          ra_final_data_limit.STPX06226,
          ra_final_data_limit.STPX06230,
          ra_final_data_limit.STPX06231,
          ra_final_data_limit.STPX06234,
          ra_final_data_limit.STPX06240,
          ra_final_data_limit.STPX06241,
          ra_final_data_limit.STPX06243,
          ra_final_data_limit.STPX06244,
          ra_final_data_limit.STPX06245,
          ra_final_data_limit.STPX06246,
          ra_final_data_limit.STPX06247,
          ra_final_data_limit.STPX06248,
          ra_final_data_limit.STPX06249,
          ra_final_data_limit.STPX06251,
          ra_final_data_limit.STPX07231,
          ra_final_data_limit.STPX07232,
          ra_final_data_limit.STPX08217,
          ra_final_data_limit.STPX08219,
          ra_final_data_limit.STPX08220,
          ra_final_data_limit.STPX08221,
          ra_final_data_limit.STPX08226,
          ra_final_data_limit.STPX08227,
          ra_final_data_limit.STPX08228,
          ra_final_data_limit.STPX08230,
          ra_final_data_limit.STPX08231,
          ra_final_data_limit.STPX08232,
          ra_final_data_limit.STPX08233,
          ra_final_data_limit.STPX08234,
          ra_final_data_limit.STPX08235,
          ra_final_data_limit.STPX09183,
          ra_final_data_limit.STPX09184,
          ra_final_data_limit.STPX09185,
          ra_final_data_limit.STPX09186,
          ra_final_data_limit.STPX09187,
          ra_final_data_limit.STPX13133,
          ra_final_data_limit.STPX13134,
          ra_final_data_limit.STPX13135,
          ra_final_data_limit.STPX13136,
          ra_final_data_limit.STPX13137,
          ra_final_data_limit.STPX13138,
          ra_final_data_limit.STPX13139,
          ra_final_data_limit.STPX13140,
          ra_final_data_limit.STPX14146,
          ra_final_data_limit.STPX17225
SEGMENTED BY hash(ra_final_data_limit.female, ra_final_data_limit.age, ra_final_data_limit.rec_enr, ra_final_data_limit.eol_ind, ra_final_data_limit.cost_ra_comp_l1, ra_final_data_limit.cost_ra_comp_l5, ra_final_data_limit.cost_ra_comp_other_l1, ra_final_data_limit.cost_ra_comp_other_l3, ra_final_data_limit.cost_ra_comp_other_l4, ra_final_data_limit.cost_ra_typ_ip_l1, ra_final_data_limit.cost_ra_typ_ip_l3, ra_final_data_limit.cost_ra_typ_ip_l4, ra_final_data_limit.cost_ra_typ_l1, ra_final_data_limit.cost_ra_typ_l5, ra_final_data_limit.cost_ra_typ_other_l3, ra_final_data_limit.cost_ra_typ_other_l4, ra_final_data_limit.cost_sa_comp_l1, ra_final_data_limit.cost_sa_comp_l5, ra_final_data_limit.cost_sa_comp_other_l1, ra_final_data_limit.cost_sa_comp_other_l3, ra_final_data_limit.cost_sa_comp_other_l4, ra_final_data_limit.cost_sa_typ_ip_l1, ra_final_data_limit.cost_sa_typ_ip_l3, ra_final_data_limit.cost_sa_typ_ip_l4, ra_final_data_limit.cost_sa_typ_l1, ra_final_data_limit.cost_sa_typ_l5, ra_final_data_limit.cost_sa_typ_other_l1, ra_final_data_limit.cost_sa_typ_other_l3, ra_final_data_limit.cost_sa_typ_other_l4, ra_final_data_limit.typ_ip_ind, ra_final_data_limit.use_comp_l1, ra_final_data_limit.use_comp_l5) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider_PACs_all /*+createtype(A)*/ 
(
 episode,
 level_assignment,
 pac_proc_acute,
 pac_chronic,
 Attributed_Provider
)
AS
 SELECT provider_PACs_all.episode,
        provider_PACs_all.level_assignment,
        provider_PACs_all.pac_proc_acute,
        provider_PACs_all.pac_chronic,
        provider_PACs_all.Attributed_Provider
 FROM epbuilder.provider_PACs_all
 ORDER BY provider_PACs_all.episode,
          provider_PACs_all.level_assignment,
          provider_PACs_all.pac_proc_acute,
          provider_PACs_all.pac_chronic
SEGMENTED BY hash(provider_PACs_all.episode) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider_PACs /*+createtype(A)*/ 
(
 episode_id,
 level_assignment,
 Attributed_Provider,
 Number_of_Episodes,
 Number_of_Epis_w_PAC,
 PAC_Rate
)
AS
 SELECT provider_PACs.episode_id,
        provider_PACs.level_assignment,
        provider_PACs.Attributed_Provider,
        provider_PACs.Number_of_Episodes,
        provider_PACs.Number_of_Epis_w_PAC,
        provider_PACs.PAC_Rate
 FROM epbuilder.provider_PACs
 ORDER BY provider_PACs.Attributed_Provider,
          provider_PACs.episode_id,
          provider_PACs.level_assignment
SEGMENTED BY hash(provider_PACs.Attributed_Provider, provider_PACs.episode_id, provider_PACs.level_assignment) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.epi_pacs_lvl1 /*+createtype(A)*/ 
(
 epi_id,
 pac_proc_acute,
 pac_chronic
)
AS
 SELECT epi_pacs_lvl1.epi_id,
        epi_pacs_lvl1.pac_proc_acute,
        epi_pacs_lvl1.pac_chronic
 FROM epbuilder.epi_pacs_lvl1
 ORDER BY epi_pacs_lvl1.epi_id
SEGMENTED BY hash(epi_pacs_lvl1.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_episode_data_rspr /*+createtype(A)*/ 
(
 epi_id,
 member_id,
 epi_number,
 epi_name,
 female,
 age,
 rec_enr,
 eol_ind
)
AS
 SELECT ra_episode_data_rspr.epi_id,
        ra_episode_data_rspr.member_id,
        ra_episode_data_rspr.epi_number,
        ra_episode_data_rspr.epi_name,
        ra_episode_data_rspr.female,
        ra_episode_data_rspr.age,
        ra_episode_data_rspr.rec_enr,
        ra_episode_data_rspr.eol_ind
 FROM epbuilder.ra_episode_data_rspr
 ORDER BY ra_episode_data_rspr.epi_id,
          ra_episode_data_rspr.member_id
SEGMENTED BY hash(ra_episode_data_rspr.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_subtypes_rspr /*+createtype(A)*/ 
(
 epi_id,
 epi_number,
 name,
 value
)
AS
 SELECT ra_subtypes_rspr.epi_id,
        ra_subtypes_rspr.epi_number,
        ra_subtypes_rspr.name,
        ra_subtypes_rspr.value
 FROM epbuilder.ra_subtypes_rspr
 ORDER BY ra_subtypes_rspr.epi_id,
          ra_subtypes_rspr.name
SEGMENTED BY hash(ra_subtypes_rspr.epi_number, ra_subtypes_rspr.value, ra_subtypes_rspr.name, ra_subtypes_rspr.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_riskfactors_rspr /*+createtype(A)*/ 
(
 epi_id,
 epi_number,
 name,
 value
)
AS
 SELECT ra_riskfactors_rspr.epi_id,
        ra_riskfactors_rspr.epi_number,
        ra_riskfactors_rspr.name,
        ra_riskfactors_rspr.value
 FROM epbuilder.ra_riskfactors_rspr
 ORDER BY ra_riskfactors_rspr.epi_id,
          ra_riskfactors_rspr.epi_number,
          ra_riskfactors_rspr.name,
          ra_riskfactors_rspr.value
SEGMENTED BY hash(ra_riskfactors_rspr.epi_number, ra_riskfactors_rspr.value, ra_riskfactors_rspr.name, ra_riskfactors_rspr.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ASTHMA_CLAIM_EXAMPLE_filtered /*+createtype(A)*/ 
(
 member_id,
 master_episode_id,
 trigger_master_claim_id,
 episode_id,
 episode_type,
 episode_begin_date,
 episode_end_date,
 episode_length_days,
 trig_begin_date,
 trig_end_date,
 truncated,
 master_claim_id,
 claim_source,
 assigned_type,
 assigned_count,
 rule,
 LEVEL_1,
 LEVEL_2,
 LEVEL_3,
 LEVEL_4,
 LEVEL_5,
 begin_date,
 end_date,
 allowed_amt
)
AS
 SELECT ASTHMA_CLAIM_EXAMPLE_filtered.member_id,
        ASTHMA_CLAIM_EXAMPLE_filtered.master_episode_id,
        ASTHMA_CLAIM_EXAMPLE_filtered.trigger_master_claim_id,
        ASTHMA_CLAIM_EXAMPLE_filtered.episode_id,
        ASTHMA_CLAIM_EXAMPLE_filtered.episode_type,
        ASTHMA_CLAIM_EXAMPLE_filtered.episode_begin_date,
        ASTHMA_CLAIM_EXAMPLE_filtered.episode_end_date,
        ASTHMA_CLAIM_EXAMPLE_filtered.episode_length_days,
        ASTHMA_CLAIM_EXAMPLE_filtered.trig_begin_date,
        ASTHMA_CLAIM_EXAMPLE_filtered.trig_end_date,
        ASTHMA_CLAIM_EXAMPLE_filtered.truncated,
        ASTHMA_CLAIM_EXAMPLE_filtered.master_claim_id,
        ASTHMA_CLAIM_EXAMPLE_filtered.claim_source,
        ASTHMA_CLAIM_EXAMPLE_filtered.assigned_type,
        ASTHMA_CLAIM_EXAMPLE_filtered.assigned_count,
        ASTHMA_CLAIM_EXAMPLE_filtered.rule,
        ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_1,
        ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_2,
        ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_3,
        ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_4,
        ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_5,
        ASTHMA_CLAIM_EXAMPLE_filtered.begin_date,
        ASTHMA_CLAIM_EXAMPLE_filtered.end_date,
        ASTHMA_CLAIM_EXAMPLE_filtered.allowed_amt
 FROM epbuilder.ASTHMA_CLAIM_EXAMPLE_filtered
 ORDER BY ASTHMA_CLAIM_EXAMPLE_filtered.master_episode_id
SEGMENTED BY hash(ASTHMA_CLAIM_EXAMPLE_filtered.episode_type, ASTHMA_CLAIM_EXAMPLE_filtered.episode_begin_date, ASTHMA_CLAIM_EXAMPLE_filtered.episode_end_date, ASTHMA_CLAIM_EXAMPLE_filtered.episode_length_days, ASTHMA_CLAIM_EXAMPLE_filtered.trig_begin_date, ASTHMA_CLAIM_EXAMPLE_filtered.trig_end_date, ASTHMA_CLAIM_EXAMPLE_filtered.truncated, ASTHMA_CLAIM_EXAMPLE_filtered.assigned_type, ASTHMA_CLAIM_EXAMPLE_filtered.assigned_count, ASTHMA_CLAIM_EXAMPLE_filtered.rule, ASTHMA_CLAIM_EXAMPLE_filtered.begin_date, ASTHMA_CLAIM_EXAMPLE_filtered.end_date, ASTHMA_CLAIM_EXAMPLE_filtered.episode_id, ASTHMA_CLAIM_EXAMPLE_filtered.claim_source, ASTHMA_CLAIM_EXAMPLE_filtered.allowed_amt, ASTHMA_CLAIM_EXAMPLE_filtered.member_id, ASTHMA_CLAIM_EXAMPLE_filtered.master_episode_id, ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_1, ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_2, ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_3, ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_4, ASTHMA_CLAIM_EXAMPLE_filtered.LEVEL_5, ASTHMA_CLAIM_EXAMPLE_filtered.trigger_master_claim_id, ASTHMA_CLAIM_EXAMPLE_filtered.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.pac_pred_probs_lvl1 /*+createtype(L)*/ 
(
 epi_id,
 pac_prob
)
AS
 SELECT pac_pred_probs_lvl1.epi_id,
        pac_pred_probs_lvl1.pac_prob
 FROM epbuilder.pac_pred_probs_lvl1
 ORDER BY pac_pred_probs_lvl1.epi_id,
          pac_pred_probs_lvl1.pac_prob
SEGMENTED BY hash(pac_pred_probs_lvl1.pac_prob, pac_pred_probs_lvl1.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ASTHMA_CLAIM_EXAMPLE /*+createtype(A)*/ 
(
 member_id,
 master_episode_id,
 trigger_master_claim_id,
 episode_id,
 episode_type,
 episode_begin_date,
 episode_end_date,
 episode_length_days,
 trig_begin_date,
 trig_end_date,
 truncated,
 master_claim_id,
 claim_source,
 assigned_type,
 assigned_count,
 rule,
 LEVEL_1,
 LEVEL_2,
 LEVEL_3,
 LEVEL_4,
 LEVEL_5,
 begin_date,
 end_date,
 allowed_amt
)
AS
 SELECT ASTHMA_CLAIM_EXAMPLE.member_id,
        ASTHMA_CLAIM_EXAMPLE.master_episode_id,
        ASTHMA_CLAIM_EXAMPLE.trigger_master_claim_id,
        ASTHMA_CLAIM_EXAMPLE.episode_id,
        ASTHMA_CLAIM_EXAMPLE.episode_type,
        ASTHMA_CLAIM_EXAMPLE.episode_begin_date,
        ASTHMA_CLAIM_EXAMPLE.episode_end_date,
        ASTHMA_CLAIM_EXAMPLE.episode_length_days,
        ASTHMA_CLAIM_EXAMPLE.trig_begin_date,
        ASTHMA_CLAIM_EXAMPLE.trig_end_date,
        ASTHMA_CLAIM_EXAMPLE.truncated,
        ASTHMA_CLAIM_EXAMPLE.master_claim_id,
        ASTHMA_CLAIM_EXAMPLE.claim_source,
        ASTHMA_CLAIM_EXAMPLE.assigned_type,
        ASTHMA_CLAIM_EXAMPLE.assigned_count,
        ASTHMA_CLAIM_EXAMPLE.rule,
        ASTHMA_CLAIM_EXAMPLE.LEVEL_1,
        ASTHMA_CLAIM_EXAMPLE.LEVEL_2,
        ASTHMA_CLAIM_EXAMPLE.LEVEL_3,
        ASTHMA_CLAIM_EXAMPLE.LEVEL_4,
        ASTHMA_CLAIM_EXAMPLE.LEVEL_5,
        ASTHMA_CLAIM_EXAMPLE.begin_date,
        ASTHMA_CLAIM_EXAMPLE.end_date,
        ASTHMA_CLAIM_EXAMPLE.allowed_amt
 FROM epbuilder.ASTHMA_CLAIM_EXAMPLE
 ORDER BY ASTHMA_CLAIM_EXAMPLE.member_id,
          ASTHMA_CLAIM_EXAMPLE.master_episode_id,
          ASTHMA_CLAIM_EXAMPLE.trigger_master_claim_id,
          ASTHMA_CLAIM_EXAMPLE.episode_id,
          ASTHMA_CLAIM_EXAMPLE.episode_type,
          ASTHMA_CLAIM_EXAMPLE.episode_begin_date,
          ASTHMA_CLAIM_EXAMPLE.episode_end_date,
          ASTHMA_CLAIM_EXAMPLE.episode_length_days,
          ASTHMA_CLAIM_EXAMPLE.trig_begin_date,
          ASTHMA_CLAIM_EXAMPLE.trig_end_date,
          ASTHMA_CLAIM_EXAMPLE.truncated,
          ASTHMA_CLAIM_EXAMPLE.master_claim_id,
          ASTHMA_CLAIM_EXAMPLE.claim_source,
          ASTHMA_CLAIM_EXAMPLE.assigned_type,
          ASTHMA_CLAIM_EXAMPLE.assigned_count,
          ASTHMA_CLAIM_EXAMPLE.rule,
          ASTHMA_CLAIM_EXAMPLE.LEVEL_1,
          ASTHMA_CLAIM_EXAMPLE.LEVEL_2,
          ASTHMA_CLAIM_EXAMPLE.LEVEL_3,
          ASTHMA_CLAIM_EXAMPLE.LEVEL_4,
          ASTHMA_CLAIM_EXAMPLE.LEVEL_5,
          ASTHMA_CLAIM_EXAMPLE.begin_date,
          ASTHMA_CLAIM_EXAMPLE.end_date,
          ASTHMA_CLAIM_EXAMPLE.allowed_amt
SEGMENTED BY hash(ASTHMA_CLAIM_EXAMPLE.LEVEL_1) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.Provider_PAC_Rates /*+createtype(A)*/ 
(
 Attributed_Provider,
 VBP_Contractor,
 Episode_ID,
 level_assignment,
 Number_of_Episodes,
 Number_of_Epis_w_PAC,
 PAC_Rate,
 N_Pred,
 Exp_PAC,
 Exp_PAC_Rate,
 OE
)
AS
 SELECT Provider_PAC_Rates.Attributed_Provider,
        Provider_PAC_Rates.VBP_Contractor,
        Provider_PAC_Rates.Episode_ID,
        Provider_PAC_Rates.level_assignment,
        Provider_PAC_Rates.Number_of_Episodes,
        Provider_PAC_Rates.Number_of_Epis_w_PAC,
        Provider_PAC_Rates.PAC_Rate,
        Provider_PAC_Rates.N_Pred,
        Provider_PAC_Rates.Exp_PAC,
        Provider_PAC_Rates.Exp_PAC_Rate,
        Provider_PAC_Rates.OE
 FROM epbuilder.Provider_PAC_Rates
 ORDER BY Provider_PAC_Rates.Attributed_Provider,
          Provider_PAC_Rates.VBP_Contractor,
          Provider_PAC_Rates.Episode_ID,
          Provider_PAC_Rates.level_assignment
SEGMENTED BY hash(Provider_PAC_Rates.Attributed_Provider, Provider_PAC_Rates.VBP_Contractor, Provider_PAC_Rates.Episode_ID, Provider_PAC_Rates.level_assignment) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_cost_use_rspr /*+createtype(A)*/ 
(
 epi_id,
 epi_number,
 name,
 value
)
AS
 SELECT ra_cost_use_rspr.epi_id,
        ra_cost_use_rspr.epi_number,
        ra_cost_use_rspr.name,
        ra_cost_use_rspr.value
 FROM epbuilder.ra_cost_use_rspr
 ORDER BY ra_cost_use_rspr.epi_id,
          ra_cost_use_rspr.epi_number,
          ra_cost_use_rspr.name,
          ra_cost_use_rspr.value
SEGMENTED BY hash(ra_cost_use_rspr.epi_number, ra_cost_use_rspr.value, ra_cost_use_rspr.name, ra_cost_use_rspr.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_exp_cost_rspr /*+createtype(L)*/ 
(
 row_names,
 epi_number,
 epi_name,
 epi_id,
 eol_prob,
 use_prob_ra_typ_l1,
 cost_pred_ra_typ_l1,
 exp_cost_ra_typ_l1,
 use_prob_sa_typ_l1,
 cost_pred_sa_typ_l1,
 exp_cost_sa_typ_l1,
 use_prob_ra_comp_l1,
 cost_pred_ra_comp_l1,
 exp_cost_ra_comp_l1,
 use_prob_sa_comp_l1,
 cost_pred_sa_comp_l1,
 exp_cost_sa_comp_l1,
 use_prob_ra_typ_l5,
 cost_pred_ra_typ_l5,
 exp_cost_ra_typ_l5,
 use_prob_sa_typ_l5,
 cost_pred_sa_typ_l5,
 exp_cost_sa_typ_l5,
 use_prob_ra_comp_l5,
 cost_pred_ra_comp_l5,
 exp_cost_ra_comp_l5,
 use_prob_sa_comp_l5,
 cost_pred_sa_comp_l5,
 exp_cost_sa_comp_l5,
 total_exp_cost_ra_l1,
 total_exp_cost_sa_l1,
 total_exp_cost_ra_l5,
 total_exp_cost_sa_l5
)
AS
 SELECT ra_exp_cost_rspr.row_names,
        ra_exp_cost_rspr.epi_number,
        ra_exp_cost_rspr.epi_name,
        ra_exp_cost_rspr.epi_id,
        ra_exp_cost_rspr.eol_prob,
        ra_exp_cost_rspr.use_prob_ra_typ_l1,
        ra_exp_cost_rspr.cost_pred_ra_typ_l1,
        ra_exp_cost_rspr.exp_cost_ra_typ_l1,
        ra_exp_cost_rspr.use_prob_sa_typ_l1,
        ra_exp_cost_rspr.cost_pred_sa_typ_l1,
        ra_exp_cost_rspr.exp_cost_sa_typ_l1,
        ra_exp_cost_rspr.use_prob_ra_comp_l1,
        ra_exp_cost_rspr.cost_pred_ra_comp_l1,
        ra_exp_cost_rspr.exp_cost_ra_comp_l1,
        ra_exp_cost_rspr.use_prob_sa_comp_l1,
        ra_exp_cost_rspr.cost_pred_sa_comp_l1,
        ra_exp_cost_rspr.exp_cost_sa_comp_l1,
        ra_exp_cost_rspr.use_prob_ra_typ_l5,
        ra_exp_cost_rspr.cost_pred_ra_typ_l5,
        ra_exp_cost_rspr.exp_cost_ra_typ_l5,
        ra_exp_cost_rspr.use_prob_sa_typ_l5,
        ra_exp_cost_rspr.cost_pred_sa_typ_l5,
        ra_exp_cost_rspr.exp_cost_sa_typ_l5,
        ra_exp_cost_rspr.use_prob_ra_comp_l5,
        ra_exp_cost_rspr.cost_pred_ra_comp_l5,
        ra_exp_cost_rspr.exp_cost_ra_comp_l5,
        ra_exp_cost_rspr.use_prob_sa_comp_l5,
        ra_exp_cost_rspr.cost_pred_sa_comp_l5,
        ra_exp_cost_rspr.exp_cost_sa_comp_l5,
        ra_exp_cost_rspr.total_exp_cost_ra_l1,
        ra_exp_cost_rspr.total_exp_cost_sa_l1,
        ra_exp_cost_rspr.total_exp_cost_ra_l5,
        ra_exp_cost_rspr.total_exp_cost_sa_l5
 FROM epbuilder.ra_exp_cost_rspr
 ORDER BY ra_exp_cost_rspr.row_names,
          ra_exp_cost_rspr.epi_number,
          ra_exp_cost_rspr.epi_name,
          ra_exp_cost_rspr.epi_id,
          ra_exp_cost_rspr.eol_prob,
          ra_exp_cost_rspr.use_prob_ra_typ_l1,
          ra_exp_cost_rspr.cost_pred_ra_typ_l1,
          ra_exp_cost_rspr.exp_cost_ra_typ_l1,
          ra_exp_cost_rspr.use_prob_sa_typ_l1,
          ra_exp_cost_rspr.cost_pred_sa_typ_l1,
          ra_exp_cost_rspr.exp_cost_sa_typ_l1,
          ra_exp_cost_rspr.use_prob_ra_comp_l1,
          ra_exp_cost_rspr.cost_pred_ra_comp_l1,
          ra_exp_cost_rspr.exp_cost_ra_comp_l1,
          ra_exp_cost_rspr.use_prob_sa_comp_l1,
          ra_exp_cost_rspr.cost_pred_sa_comp_l1,
          ra_exp_cost_rspr.exp_cost_sa_comp_l1,
          ra_exp_cost_rspr.use_prob_ra_typ_l5,
          ra_exp_cost_rspr.cost_pred_ra_typ_l5,
          ra_exp_cost_rspr.exp_cost_ra_typ_l5,
          ra_exp_cost_rspr.use_prob_sa_typ_l5,
          ra_exp_cost_rspr.cost_pred_sa_typ_l5,
          ra_exp_cost_rspr.exp_cost_sa_typ_l5,
          ra_exp_cost_rspr.use_prob_ra_comp_l5,
          ra_exp_cost_rspr.cost_pred_ra_comp_l5,
          ra_exp_cost_rspr.exp_cost_ra_comp_l5,
          ra_exp_cost_rspr.use_prob_sa_comp_l5,
          ra_exp_cost_rspr.cost_pred_sa_comp_l5,
          ra_exp_cost_rspr.exp_cost_sa_comp_l5,
          ra_exp_cost_rspr.total_exp_cost_ra_l1,
          ra_exp_cost_rspr.total_exp_cost_sa_l1,
          ra_exp_cost_rspr.total_exp_cost_ra_l5,
          ra_exp_cost_rspr.total_exp_cost_sa_l5
SEGMENTED BY hash(ra_exp_cost_rspr.eol_prob, ra_exp_cost_rspr.use_prob_ra_typ_l1, ra_exp_cost_rspr.cost_pred_ra_typ_l1, ra_exp_cost_rspr.exp_cost_ra_typ_l1, ra_exp_cost_rspr.use_prob_sa_typ_l1, ra_exp_cost_rspr.cost_pred_sa_typ_l1, ra_exp_cost_rspr.exp_cost_sa_typ_l1, ra_exp_cost_rspr.use_prob_ra_comp_l1, ra_exp_cost_rspr.cost_pred_ra_comp_l1, ra_exp_cost_rspr.exp_cost_ra_comp_l1, ra_exp_cost_rspr.use_prob_sa_comp_l1, ra_exp_cost_rspr.cost_pred_sa_comp_l1, ra_exp_cost_rspr.exp_cost_sa_comp_l1, ra_exp_cost_rspr.use_prob_ra_typ_l5, ra_exp_cost_rspr.cost_pred_ra_typ_l5, ra_exp_cost_rspr.exp_cost_ra_typ_l5, ra_exp_cost_rspr.use_prob_sa_typ_l5, ra_exp_cost_rspr.cost_pred_sa_typ_l5, ra_exp_cost_rspr.exp_cost_sa_typ_l5, ra_exp_cost_rspr.use_prob_ra_comp_l5, ra_exp_cost_rspr.cost_pred_ra_comp_l5, ra_exp_cost_rspr.exp_cost_ra_comp_l5, ra_exp_cost_rspr.use_prob_sa_comp_l5, ra_exp_cost_rspr.cost_pred_sa_comp_l5, ra_exp_cost_rspr.exp_cost_sa_comp_l5, ra_exp_cost_rspr.total_exp_cost_ra_l1, ra_exp_cost_rspr.total_exp_cost_sa_l1, ra_exp_cost_rspr.total_exp_cost_ra_l5, ra_exp_cost_rspr.total_exp_cost_sa_l5, ra_exp_cost_rspr.row_names, ra_exp_cost_rspr.epi_number, ra_exp_cost_rspr.epi_name) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.VBP_Costs /*+createtype(A)*/ 
(
 episode_id,
 VBP_Contractor,
 N,
 Act_Cost,
 Exp_Cost,
 RA_Cost
)
AS
 SELECT VBP_Costs.episode_id,
        VBP_Costs.VBP_Contractor,
        VBP_Costs.N,
        VBP_Costs.Act_Cost,
        VBP_Costs.Exp_Cost,
        VBP_Costs.RA_Cost
 FROM epbuilder.VBP_Costs
 ORDER BY VBP_Costs.episode_id,
          VBP_Costs.VBP_Contractor
SEGMENTED BY hash(VBP_Costs.N, VBP_Costs.Act_Cost, VBP_Costs.Exp_Cost, VBP_Costs.RA_Cost, VBP_Costs.episode_id, VBP_Costs.VBP_Contractor) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.VBP_PAC_Rates2 /*+createtype(A)*/ 
(
 VBP_Contractor,
 Episode_ID,
 level_assignment,
 Number_of_Episodes,
 Number_of_Epis_w_PAC,
 PAC_Rate,
 N_Pred,
 Exp_PAC,
 Exp_PAC_Rate,
 OE
)
AS
 SELECT VBP_PAC_Rates2.VBP_Contractor,
        VBP_PAC_Rates2.Episode_ID,
        VBP_PAC_Rates2.level_assignment,
        VBP_PAC_Rates2.Number_of_Episodes,
        VBP_PAC_Rates2.Number_of_Epis_w_PAC,
        VBP_PAC_Rates2.PAC_Rate,
        VBP_PAC_Rates2.N_Pred,
        VBP_PAC_Rates2.Exp_PAC,
        VBP_PAC_Rates2.Exp_PAC_Rate,
        VBP_PAC_Rates2.OE
 FROM epbuilder.VBP_PAC_Rates2
 ORDER BY VBP_PAC_Rates2.VBP_Contractor,
          VBP_PAC_Rates2.Episode_ID,
          VBP_PAC_Rates2.level_assignment
SEGMENTED BY hash(VBP_PAC_Rates2.VBP_Contractor, VBP_PAC_Rates2.Episode_ID, VBP_PAC_Rates2.level_assignment) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.VBP_PAC_Rates /*+createtype(A)*/ 
(
 VBP_Contractor,
 Episode_ID,
 level_assignment,
 Number_of_Episodes,
 Number_of_Epis_w_PAC,
 PAC_Rate,
 N_Pred,
 Exp_PAC,
 Exp_PAC_Rate,
 OE
)
AS
 SELECT VBP_PAC_Rates.VBP_Contractor,
        VBP_PAC_Rates.Episode_ID,
        VBP_PAC_Rates.level_assignment,
        VBP_PAC_Rates.Number_of_Episodes,
        VBP_PAC_Rates.Number_of_Epis_w_PAC,
        VBP_PAC_Rates.PAC_Rate,
        VBP_PAC_Rates.N_Pred,
        VBP_PAC_Rates.Exp_PAC,
        VBP_PAC_Rates.Exp_PAC_Rate,
        VBP_PAC_Rates.OE
 FROM epbuilder.VBP_PAC_Rates
 ORDER BY VBP_PAC_Rates.VBP_Contractor,
          VBP_PAC_Rates.Episode_ID,
          VBP_PAC_Rates.level_assignment
SEGMENTED BY hash(VBP_PAC_Rates.VBP_Contractor, VBP_PAC_Rates.Episode_ID, VBP_PAC_Rates.level_assignment) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.avg_cost /*+createtype(A)*/ 
(
 episode_id,
 avg_cost,
 avg_exp
)
AS
 SELECT avg_cost.episode_id,
        avg_cost.avg_cost,
        avg_cost.avg_exp
 FROM epbuilder.avg_cost
 ORDER BY avg_cost.episode_id
SEGMENTED BY hash(avg_cost.episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.epi_cost_rel_input_lvl1 /*+createtype(A)*/ 
(
 member_id,
 episode_id,
 episode,
 attrib_default_physician,
 attrib_default_facility,
 vbp_contractor,
 cost,
 exp_cost
)
AS
 SELECT epi_cost_rel_input_lvl1.member_id,
        epi_cost_rel_input_lvl1.episode_id,
        epi_cost_rel_input_lvl1.episode,
        epi_cost_rel_input_lvl1.attrib_default_physician,
        epi_cost_rel_input_lvl1.attrib_default_facility,
        epi_cost_rel_input_lvl1.vbp_contractor,
        epi_cost_rel_input_lvl1.cost,
        epi_cost_rel_input_lvl1.exp_cost
 FROM epbuilder.epi_cost_rel_input_lvl1
 ORDER BY epi_cost_rel_input_lvl1.member_id,
          epi_cost_rel_input_lvl1.episode_id,
          epi_cost_rel_input_lvl1.episode,
          epi_cost_rel_input_lvl1.attrib_default_physician,
          epi_cost_rel_input_lvl1.attrib_default_facility,
          epi_cost_rel_input_lvl1.vbp_contractor,
          epi_cost_rel_input_lvl1.cost,
          epi_cost_rel_input_lvl1.exp_cost
SEGMENTED BY hash(epi_cost_rel_input_lvl1.episode) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.HIV_PATIENTS /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT HIV_PATIENTS.member_id
 FROM epbuilder.HIV_PATIENTS
 ORDER BY HIV_PATIENTS.member_id
SEGMENTED BY hash(HIV_PATIENTS.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ALL_MAT_NBRON_FLAGs /*+createtype(A)*/ 
(
 u_c_id,
 member_id,
 episode_id,
 master_episode_id,
 master_claim_id,
 GROUP_ID,
 RF_NAME,
 csect_rf,
 induction_rf,
 NICU_LVL_4
)
AS
 SELECT ALL_MAT_NBRON_FLAGs.u_c_id,
        ALL_MAT_NBRON_FLAGs.member_id,
        ALL_MAT_NBRON_FLAGs.episode_id,
        ALL_MAT_NBRON_FLAGs.master_episode_id,
        ALL_MAT_NBRON_FLAGs.master_claim_id,
        ALL_MAT_NBRON_FLAGs.GROUP_ID,
        ALL_MAT_NBRON_FLAGs.RF_NAME,
        ALL_MAT_NBRON_FLAGs.csect_rf,
        ALL_MAT_NBRON_FLAGs.induction_rf,
        ALL_MAT_NBRON_FLAGs.NICU_LVL_4
 FROM epbuilder.ALL_MAT_NBRON_FLAGs
 ORDER BY ALL_MAT_NBRON_FLAGs.u_c_id,
          ALL_MAT_NBRON_FLAGs.member_id,
          ALL_MAT_NBRON_FLAGs.episode_id,
          ALL_MAT_NBRON_FLAGs.master_episode_id,
          ALL_MAT_NBRON_FLAGs.master_claim_id,
          ALL_MAT_NBRON_FLAGs.GROUP_ID,
          ALL_MAT_NBRON_FLAGs.RF_NAME,
          ALL_MAT_NBRON_FLAGs.csect_rf,
          ALL_MAT_NBRON_FLAGs.induction_rf,
          ALL_MAT_NBRON_FLAGs.NICU_LVL_4
SEGMENTED BY hash(ALL_MAT_NBRON_FLAGs.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ALL_MAT_NBRON_FLAGs2 /*+createtype(A)*/ 
(
 u_c_id,
 member_id,
 episode_id,
 master_episode_id,
 master_claim_id,
 GROUP_ID,
 RF_NAME,
 csect_rf,
 induction_rf,
 NICU_LVL_4,
 level_2,
 level_5,
 DELIVERY_EPISODE
)
AS
 SELECT ALL_MAT_NBRON_FLAGs2.u_c_id,
        ALL_MAT_NBRON_FLAGs2.member_id,
        ALL_MAT_NBRON_FLAGs2.episode_id,
        ALL_MAT_NBRON_FLAGs2.master_episode_id,
        ALL_MAT_NBRON_FLAGs2.master_claim_id,
        ALL_MAT_NBRON_FLAGs2.GROUP_ID,
        ALL_MAT_NBRON_FLAGs2.RF_NAME,
        ALL_MAT_NBRON_FLAGs2.csect_rf,
        ALL_MAT_NBRON_FLAGs2.induction_rf,
        ALL_MAT_NBRON_FLAGs2.NICU_LVL_4,
        ALL_MAT_NBRON_FLAGs2.level_2,
        ALL_MAT_NBRON_FLAGs2.level_5,
        ALL_MAT_NBRON_FLAGs2.DELIVERY_EPISODE
 FROM epbuilder.ALL_MAT_NBRON_FLAGs2
 ORDER BY ALL_MAT_NBRON_FLAGs2.u_c_id,
          ALL_MAT_NBRON_FLAGs2.member_id,
          ALL_MAT_NBRON_FLAGs2.episode_id,
          ALL_MAT_NBRON_FLAGs2.master_episode_id,
          ALL_MAT_NBRON_FLAGs2.master_claim_id,
          ALL_MAT_NBRON_FLAGs2.GROUP_ID,
          ALL_MAT_NBRON_FLAGs2.RF_NAME,
          ALL_MAT_NBRON_FLAGs2.csect_rf,
          ALL_MAT_NBRON_FLAGs2.induction_rf,
          ALL_MAT_NBRON_FLAGs2.NICU_LVL_4,
          ALL_MAT_NBRON_FLAGs2.level_2,
          ALL_MAT_NBRON_FLAGs2.level_5,
          ALL_MAT_NBRON_FLAGs2.DELIVERY_EPISODE
SEGMENTED BY hash(ALL_MAT_NBRON_FLAGs2.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.filters_MAT_NBORN /*+createtype(A)*/ 
(
 member_id,
 episode_id,
 master_episode_id,
 CSECT_WARRANTED_FILTER,
 INDUCTION_WARRANTED_FILTER,
 NICU_LVL_4_FILTER
)
AS
 SELECT filters_MAT_NBORN.member_id,
        filters_MAT_NBORN.episode_id,
        filters_MAT_NBORN.master_episode_id,
        filters_MAT_NBORN.CSECT_WARRANTED_FILTER,
        filters_MAT_NBORN.INDUCTION_WARRANTED_FILTER,
        filters_MAT_NBORN.NICU_LVL_4_FILTER
 FROM epbuilder.filters_MAT_NBORN
 ORDER BY filters_MAT_NBORN.member_id,
          filters_MAT_NBORN.master_episode_id
SEGMENTED BY hash(filters_MAT_NBORN.member_id, filters_MAT_NBORN.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.cost_compare_diab /*+createtype(A)*/ 
(
 member_id,
 episode_id,
 episode,
 attrib_default_physician,
 attrib_default_facility,
 vbp_contractor,
 tot_cost,
 t_cost,
 c_cost,
 exp_cost
)
AS
 SELECT cost_compare_diab.member_id,
        cost_compare_diab.episode_id,
        cost_compare_diab.episode,
        cost_compare_diab.attrib_default_physician,
        cost_compare_diab.attrib_default_facility,
        cost_compare_diab.vbp_contractor,
        cost_compare_diab.tot_cost,
        cost_compare_diab.t_cost,
        cost_compare_diab.c_cost,
        cost_compare_diab.exp_cost
 FROM epbuilder.cost_compare_diab
 ORDER BY cost_compare_diab.member_id,
          cost_compare_diab.episode_id,
          cost_compare_diab.episode,
          cost_compare_diab.attrib_default_physician,
          cost_compare_diab.attrib_default_facility,
          cost_compare_diab.vbp_contractor,
          cost_compare_diab.tot_cost,
          cost_compare_diab.t_cost,
          cost_compare_diab.c_cost,
          cost_compare_diab.exp_cost
SEGMENTED BY hash(cost_compare_diab.attrib_default_facility, cost_compare_diab.attrib_default_physician, cost_compare_diab.episode_id, cost_compare_diab.tot_cost, cost_compare_diab.t_cost, cost_compare_diab.c_cost, cost_compare_diab.exp_cost, cost_compare_diab.member_id, cost_compare_diab.episode, cost_compare_diab.vbp_contractor) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.VBP_Costs_diab /*+createtype(A)*/ 
(
 episode_id,
 VBP_Contractor,
 N,
 Act_Cost,
 Exp_Cost,
 RA_Cost,
 t_cost,
 c_cost,
 pac_pct
)
AS
 SELECT VBP_Costs_diab.episode_id,
        VBP_Costs_diab.VBP_Contractor,
        VBP_Costs_diab.N,
        VBP_Costs_diab.Act_Cost,
        VBP_Costs_diab.Exp_Cost,
        VBP_Costs_diab.RA_Cost,
        VBP_Costs_diab.t_cost,
        VBP_Costs_diab.c_cost,
        VBP_Costs_diab.pac_pct
 FROM epbuilder.VBP_Costs_diab
 ORDER BY VBP_Costs_diab.episode_id,
          VBP_Costs_diab.VBP_Contractor
SEGMENTED BY hash(VBP_Costs_diab.N, VBP_Costs_diab.Act_Cost, VBP_Costs_diab.Exp_Cost, VBP_Costs_diab.RA_Cost, VBP_Costs_diab.t_cost, VBP_Costs_diab.c_cost, VBP_Costs_diab.pac_pct, VBP_Costs_diab.episode_id, VBP_Costs_diab.VBP_Contractor) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_claim_line /*+createtype(A)*/ 
(
 id,
 master_claim_id,
 member_id,
 claim_id,
 claim_line_id,
 sequence_key,
 final_version_flag,
 claim_encounter_flag,
 provider_npi,
 provider_id,
 physician_id,
 facility_id,
 allowed_amt,
 facility_type_code,
 begin_date,
 end_date,
 place_of_svc_code,
 claim_line_type_code,
 assigned,
 assigned_count,
 quantity,
 standard_payment_amt,
 charge_amt,
 paid_amt,
 prepaid_amt,
 copay_amt,
 coinsurance_amt,
 deductible_amt,
 insurance_product,
 plan_id,
 admission_date,
 admission_src_code,
 admit_type_code,
 discharge_status_code,
 discharge_date,
 type_of_bill,
 rev_count,
 drg_version,
 ms_drg_code,
 apr_drg_code,
 readmission,
 office_visit,
 trigger,
 ed_visit,
 ed_visit_id,
 core_service,
 pas
)
AS
 SELECT salient_claim_line.id,
        salient_claim_line.master_claim_id,
        salient_claim_line.member_id,
        salient_claim_line.claim_id,
        salient_claim_line.claim_line_id,
        salient_claim_line.sequence_key,
        salient_claim_line.final_version_flag,
        salient_claim_line.claim_encounter_flag,
        salient_claim_line.provider_npi,
        salient_claim_line.provider_id,
        salient_claim_line.physician_id,
        salient_claim_line.facility_id,
        salient_claim_line.allowed_amt,
        salient_claim_line.facility_type_code,
        salient_claim_line.begin_date,
        salient_claim_line.end_date,
        salient_claim_line.place_of_svc_code,
        salient_claim_line.claim_line_type_code,
        salient_claim_line.assigned,
        salient_claim_line.assigned_count,
        salient_claim_line.quantity,
        salient_claim_line.standard_payment_amt,
        salient_claim_line.charge_amt,
        salient_claim_line.paid_amt,
        salient_claim_line.prepaid_amt,
        salient_claim_line.copay_amt,
        salient_claim_line.coinsurance_amt,
        salient_claim_line.deductible_amt,
        salient_claim_line.insurance_product,
        salient_claim_line.plan_id,
        salient_claim_line.admission_date,
        salient_claim_line.admission_src_code,
        salient_claim_line.admit_type_code,
        salient_claim_line.discharge_status_code,
        salient_claim_line.discharge_date,
        salient_claim_line.type_of_bill,
        salient_claim_line.rev_count,
        salient_claim_line.drg_version,
        salient_claim_line.ms_drg_code,
        salient_claim_line.apr_drg_code,
        salient_claim_line.readmission,
        salient_claim_line.office_visit,
        salient_claim_line.trigger,
        salient_claim_line.ed_visit,
        salient_claim_line.ed_visit_id,
        salient_claim_line.core_service,
        salient_claim_line.pas
 FROM epbuilder.salient_claim_line
 ORDER BY salient_claim_line.id
SEGMENTED BY hash(salient_claim_line.id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_mom_baby /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 year
)
AS
 SELECT salient_mom_baby.ENCRYPT_RECIP_ID_MOM,
        salient_mom_baby.ENCRYPT_RECIP_ID_BABY,
        salient_mom_baby.year
 FROM epbuilder.salient_mom_baby
 ORDER BY salient_mom_baby.ENCRYPT_RECIP_ID_MOM,
          salient_mom_baby.ENCRYPT_RECIP_ID_BABY
SEGMENTED BY hash(salient_mom_baby.ENCRYPT_RECIP_ID_MOM, salient_mom_baby.ENCRYPT_RECIP_ID_BABY) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_member_id_final /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT salient_member_id_final.member_id
 FROM epbuilder.salient_member_id_final
 ORDER BY salient_member_id_final.member_id
SEGMENTED BY hash(salient_member_id_final.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.deliveries_level3 /*+createtype(A)*/ 
(
 Filter_ID,
 Member_ID,
 Master_Episode_ID,
 Episode_ID,
 Episode_Name,
 Episode_Description,
 Episode_Type,
 MDC,
 MDC_Description,
 Episode_Begin_Date,
 Episode_End_Date,
 Episode_Length,
 Level,
 Split_Total_Cost,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_80thPercentile_Cost,
 Split_Total_PAC_Cost,
 Split_Total_Typical_Cost,
 Split_Total_TypicalwPAC_Cost,
 Annualized_Split_Total_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_80thPercentile_Cost,
 Annualized_Split_Total_PAC_Cost,
 Annualized_Split_Total_Typical_Cost,
 Annualized_Split_Total_TypicalwPAC_Cost,
 Unsplit_Total_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Unsplit_Total_PAC_Cost,
 Unsplit_Total_Typical_Cost,
 Unsplit_Total_TypicalwPAC_Cost,
 Annualized_Unsplit_Total_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_Total_PAC_Cost,
 Annualized_Unsplit_Total_Typical_Cost,
 Annualized_Unsplit_Total_TypicalwPAC_Cost,
 Facility_ID,
 Physician_ID,
 Physician_Specialty,
 Split_Expected_Total_Cost,
 Split_Expected_Typical_IP_Cost,
 Split_Expected_Typical_Other_Cost,
 Split_Expected_PAC_Cost,
 Unsplit_Expected_Total_Cost,
 Unsplit_Expected_Typical_IP_Cost,
 Unsplit_Expected_Typical_Other_Cost,
 Unsplit_Expected_PAC_Cost,
 IP_PAC_Count,
 OP_PAC_Count,
 PB_PAC_Count,
 RX_PAC_Count,
 year,
 enrolled_month,
 co_occurence_count_DEPANX,
 co_occurence_count_DIVERT,
 co_occurence_count_RHNTS,
 co_occurence_count_OSTEOA,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_GLCOMA,
 co_occurence_count_HTN,
 co_occurence_count_GERD,
 co_occurence_count_BIPLR,
 co_occurence_count_ULCLTS,
 co_occurence_count_CAD,
 co_occurence_count_COPD,
 co_occurence_count_HF,
 co_occurence_count_ARRBLK,
 co_occurence_count_ASTHMA,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_count_chronic,
 co_count_all,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 END_OF_STUDY,
 Facility_ID_provider_name,
 Facility_ID_provider_zipcode,
 Facility_ID_provider_type,
 Physician_ID_provider_name,
 Physician_ID_provider_zipcode,
 Physician_ID_provider_type,
 state_wide_avg_split_exp_cost,
 state_wide_avg_unsplit_exp_cost,
 state_wide_avg_split_total_cost,
 state_wide_avg_unsplit_total_cost,
 split_state_wide_pac_percent,
 unsplit_state_wide_pac_percent
)
AS
 SELECT deliveries_level3.Filter_ID,
        deliveries_level3.Member_ID,
        deliveries_level3.Master_Episode_ID,
        deliveries_level3.Episode_ID,
        deliveries_level3.Episode_Name,
        deliveries_level3.Episode_Description,
        deliveries_level3.Episode_Type,
        deliveries_level3.MDC,
        deliveries_level3.MDC_Description,
        deliveries_level3.Episode_Begin_Date,
        deliveries_level3.Episode_End_Date,
        deliveries_level3.Episode_Length,
        deliveries_level3.Level,
        deliveries_level3.Split_Total_Cost,
        deliveries_level3.Split_1stPercentile_Cost,
        deliveries_level3.Split_99thPercentile_Cost,
        deliveries_level3.Split_80thPercentile_Cost,
        deliveries_level3.Split_Total_PAC_Cost,
        deliveries_level3.Split_Total_Typical_Cost,
        deliveries_level3.Split_Total_TypicalwPAC_Cost,
        deliveries_level3.Annualized_Split_Total_Cost,
        deliveries_level3.Annualized_Split_1stPercentile_Cost,
        deliveries_level3.Annualized_Split_99thPercentile_Cost,
        deliveries_level3.Annualized_Split_80thPercentile_Cost,
        deliveries_level3.Annualized_Split_Total_PAC_Cost,
        deliveries_level3.Annualized_Split_Total_Typical_Cost,
        deliveries_level3.Annualized_Split_Total_TypicalwPAC_Cost,
        deliveries_level3.Unsplit_Total_Cost,
        deliveries_level3.Unsplit_1stPercentile_Cost,
        deliveries_level3.Unsplit_99thPercentile_Cost,
        deliveries_level3.Unsplit_Total_PAC_Cost,
        deliveries_level3.Unsplit_Total_Typical_Cost,
        deliveries_level3.Unsplit_Total_TypicalwPAC_Cost,
        deliveries_level3.Annualized_Unsplit_Total_Cost,
        deliveries_level3.Annualized_Unsplit_1stPercentile_Cost,
        deliveries_level3.Annualized_Unsplit_99thPercentile_Cost,
        deliveries_level3.Annualized_Unsplit_Total_PAC_Cost,
        deliveries_level3.Annualized_Unsplit_Total_Typical_Cost,
        deliveries_level3.Annualized_Unsplit_Total_TypicalwPAC_Cost,
        deliveries_level3.Facility_ID,
        deliveries_level3.Physician_ID,
        deliveries_level3.Physician_Specialty,
        deliveries_level3.Split_Expected_Total_Cost,
        deliveries_level3.Split_Expected_Typical_IP_Cost,
        deliveries_level3.Split_Expected_Typical_Other_Cost,
        deliveries_level3.Split_Expected_PAC_Cost,
        deliveries_level3.Unsplit_Expected_Total_Cost,
        deliveries_level3.Unsplit_Expected_Typical_IP_Cost,
        deliveries_level3.Unsplit_Expected_Typical_Other_Cost,
        deliveries_level3.Unsplit_Expected_PAC_Cost,
        deliveries_level3.IP_PAC_Count,
        deliveries_level3.OP_PAC_Count,
        deliveries_level3.PB_PAC_Count,
        deliveries_level3.RX_PAC_Count,
        deliveries_level3.year,
        deliveries_level3.enrolled_month,
        deliveries_level3.co_occurence_count_DEPANX,
        deliveries_level3.co_occurence_count_DIVERT,
        deliveries_level3.co_occurence_count_RHNTS,
        deliveries_level3.co_occurence_count_OSTEOA,
        deliveries_level3.co_occurence_count_DIAB,
        deliveries_level3.co_occurence_count_DEPRSN,
        deliveries_level3.co_occurence_count_LBP,
        deliveries_level3.co_occurence_count_CROHNS,
        deliveries_level3.co_occurence_count_GLCOMA,
        deliveries_level3.co_occurence_count_HTN,
        deliveries_level3.co_occurence_count_GERD,
        deliveries_level3.co_occurence_count_BIPLR,
        deliveries_level3.co_occurence_count_ULCLTS,
        deliveries_level3.co_occurence_count_CAD,
        deliveries_level3.co_occurence_count_COPD,
        deliveries_level3.co_occurence_count_HF,
        deliveries_level3.co_occurence_count_ARRBLK,
        deliveries_level3.co_occurence_count_ASTHMA,
        deliveries_level3.co_occurence_count_PTSD,
        deliveries_level3.co_occurence_count_SCHIZO,
        deliveries_level3.co_occurence_count_SUDS,
        deliveries_level3.co_count_chronic,
        deliveries_level3.co_count_all,
        deliveries_level3.ip_cost,
        deliveries_level3.op_cost,
        deliveries_level3.pb_cost,
        deliveries_level3.rx_cost,
        deliveries_level3.END_OF_STUDY,
        deliveries_level3.Facility_ID_provider_name,
        deliveries_level3.Facility_ID_provider_zipcode,
        deliveries_level3.Facility_ID_provider_type,
        deliveries_level3.Physician_ID_provider_name,
        deliveries_level3.Physician_ID_provider_zipcode,
        deliveries_level3.Physician_ID_provider_type,
        deliveries_level3.state_wide_avg_split_exp_cost,
        deliveries_level3.state_wide_avg_unsplit_exp_cost,
        deliveries_level3.state_wide_avg_split_total_cost,
        deliveries_level3.state_wide_avg_unsplit_total_cost,
        deliveries_level3.split_state_wide_pac_percent,
        deliveries_level3.unsplit_state_wide_pac_percent
 FROM epbuilder.deliveries_level3
 ORDER BY deliveries_level3.Filter_ID,
          deliveries_level3.Member_ID,
          deliveries_level3.Master_Episode_ID,
          deliveries_level3.Episode_ID,
          deliveries_level3.Episode_Name,
          deliveries_level3.Episode_Description,
          deliveries_level3.Episode_Type,
          deliveries_level3.MDC,
          deliveries_level3.MDC_Description,
          deliveries_level3.Episode_Begin_Date,
          deliveries_level3.Episode_End_Date,
          deliveries_level3.Episode_Length,
          deliveries_level3.Split_Total_Cost,
          deliveries_level3.Split_1stPercentile_Cost,
          deliveries_level3.Split_99thPercentile_Cost,
          deliveries_level3.Split_80thPercentile_Cost,
          deliveries_level3.Split_Total_PAC_Cost,
          deliveries_level3.Split_Total_Typical_Cost,
          deliveries_level3.Split_Total_TypicalwPAC_Cost,
          deliveries_level3.Annualized_Split_Total_Cost,
          deliveries_level3.Annualized_Split_1stPercentile_Cost,
          deliveries_level3.Annualized_Split_99thPercentile_Cost,
          deliveries_level3.Annualized_Split_80thPercentile_Cost,
          deliveries_level3.Annualized_Split_Total_PAC_Cost,
          deliveries_level3.Annualized_Split_Total_Typical_Cost,
          deliveries_level3.Annualized_Split_Total_TypicalwPAC_Cost,
          deliveries_level3.Unsplit_Total_Cost,
          deliveries_level3.Unsplit_1stPercentile_Cost,
          deliveries_level3.Unsplit_99thPercentile_Cost,
          deliveries_level3.Unsplit_Total_PAC_Cost,
          deliveries_level3.Unsplit_Total_Typical_Cost,
          deliveries_level3.Unsplit_Total_TypicalwPAC_Cost,
          deliveries_level3.Annualized_Unsplit_Total_Cost,
          deliveries_level3.Annualized_Unsplit_1stPercentile_Cost,
          deliveries_level3.Annualized_Unsplit_99thPercentile_Cost,
          deliveries_level3.Annualized_Unsplit_Total_PAC_Cost,
          deliveries_level3.Annualized_Unsplit_Total_Typical_Cost,
          deliveries_level3.Annualized_Unsplit_Total_TypicalwPAC_Cost,
          deliveries_level3.Facility_ID,
          deliveries_level3.Physician_ID,
          deliveries_level3.Physician_Specialty,
          deliveries_level3.Split_Expected_Total_Cost,
          deliveries_level3.Split_Expected_Typical_IP_Cost,
          deliveries_level3.Split_Expected_Typical_Other_Cost,
          deliveries_level3.Split_Expected_PAC_Cost,
          deliveries_level3.Unsplit_Expected_Total_Cost,
          deliveries_level3.Unsplit_Expected_Typical_IP_Cost,
          deliveries_level3.Unsplit_Expected_Typical_Other_Cost,
          deliveries_level3.Unsplit_Expected_PAC_Cost,
          deliveries_level3.IP_PAC_Count,
          deliveries_level3.OP_PAC_Count,
          deliveries_level3.PB_PAC_Count,
          deliveries_level3.RX_PAC_Count
SEGMENTED BY hash(deliveries_level3.Filter_ID, deliveries_level3.Episode_ID, deliveries_level3.Episode_Name, deliveries_level3.MDC, deliveries_level3.Episode_Length, deliveries_level3.Level, deliveries_level3.Split_Total_Cost, deliveries_level3.Split_1stPercentile_Cost, deliveries_level3.Split_99thPercentile_Cost, deliveries_level3.Split_80thPercentile_Cost, deliveries_level3.Split_Total_PAC_Cost, deliveries_level3.Split_Total_Typical_Cost, deliveries_level3.Split_Total_TypicalwPAC_Cost, deliveries_level3.Annualized_Split_Total_Cost, deliveries_level3.Annualized_Split_1stPercentile_Cost, deliveries_level3.Annualized_Split_99thPercentile_Cost, deliveries_level3.Annualized_Split_80thPercentile_Cost, deliveries_level3.Annualized_Split_Total_PAC_Cost, deliveries_level3.Annualized_Split_Total_Typical_Cost, deliveries_level3.Annualized_Split_Total_TypicalwPAC_Cost, deliveries_level3.Unsplit_Total_Cost, deliveries_level3.Unsplit_1stPercentile_Cost, deliveries_level3.Unsplit_99thPercentile_Cost, deliveries_level3.Unsplit_Total_PAC_Cost, deliveries_level3.Unsplit_Total_Typical_Cost, deliveries_level3.Unsplit_Total_TypicalwPAC_Cost, deliveries_level3.Annualized_Unsplit_Total_Cost, deliveries_level3.Annualized_Unsplit_1stPercentile_Cost, deliveries_level3.Annualized_Unsplit_99thPercentile_Cost, deliveries_level3.Annualized_Unsplit_Total_PAC_Cost, deliveries_level3.Annualized_Unsplit_Total_Typical_Cost, deliveries_level3.Annualized_Unsplit_Total_TypicalwPAC_Cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_ra_exp_cost /*+createtype(A)*/ 
(
 row_names,
 epi_number,
 epi_name,
 epi_id,
 eol_prob,
 use_prob_ra_typ_ip_l1,
 cost_pred_ra_typ_ip_l1,
 exp_cost_ra_typ_ip_l1,
 use_prob_sa_typ_ip_l1,
 cost_pred_sa_typ_ip_l1,
 exp_cost_sa_typ_ip_l1,
 use_prob_sa_typ_other_l1,
 cost_pred_sa_typ_other_l1,
 exp_cost_sa_typ_other_l1,
 use_prob_ra_comp_other_l1,
 cost_pred_ra_comp_other_l1,
 exp_cost_ra_comp_other_l1,
 use_prob_sa_comp_other_l1,
 cost_pred_sa_comp_other_l1,
 exp_cost_sa_comp_other_l1,
 use_prob_ra_typ_ip_l4,
 cost_pred_ra_typ_ip_l4,
 exp_cost_ra_typ_ip_l4,
 use_prob_sa_typ_ip_l4,
 cost_pred_sa_typ_ip_l4,
 exp_cost_sa_typ_ip_l4,
 use_prob_ra_typ_other_l4,
 cost_pred_ra_typ_other_l4,
 exp_cost_ra_typ_other_l4,
 use_prob_sa_typ_other_l4,
 cost_pred_sa_typ_other_l4,
 exp_cost_sa_typ_other_l4,
 use_prob_ra_comp_other_l4,
 cost_pred_ra_comp_other_l4,
 exp_cost_ra_comp_other_l4,
 use_prob_sa_comp_other_l4,
 cost_pred_sa_comp_other_l4,
 exp_cost_sa_comp_other_l4,
 total_exp_cost_ra_l1,
 total_exp_cost_sa_l1,
 total_exp_cost_ra_l4,
 total_exp_cost_sa_l4,
 use_prob_ra_typ_l1,
 cost_pred_ra_typ_l1,
 exp_cost_ra_typ_l1,
 use_prob_sa_typ_l1,
 cost_pred_sa_typ_l1,
 exp_cost_sa_typ_l1,
 use_prob_ra_comp_l1,
 cost_pred_ra_comp_l1,
 exp_cost_ra_comp_l1,
 use_prob_sa_comp_l1,
 cost_pred_sa_comp_l1,
 exp_cost_sa_comp_l1,
 use_prob_ra_typ_l5,
 cost_pred_ra_typ_l5,
 exp_cost_ra_typ_l5,
 use_prob_sa_typ_l5,
 cost_pred_sa_typ_l5,
 exp_cost_sa_typ_l5,
 use_prob_ra_comp_l5,
 cost_pred_ra_comp_l5,
 exp_cost_ra_comp_l5,
 use_prob_sa_comp_l5,
 cost_pred_sa_comp_l5,
 exp_cost_sa_comp_l5,
 total_exp_cost_ra_l5,
 total_exp_cost_sa_l5,
 use_prob_ra_typ_ip_l3,
 cost_pred_ra_typ_ip_l3,
 exp_cost_ra_typ_ip_l3,
 use_prob_sa_typ_ip_l3,
 cost_pred_sa_typ_ip_l3,
 exp_cost_sa_typ_ip_l3,
 use_prob_ra_typ_other_l3,
 cost_pred_ra_typ_other_l3,
 exp_cost_ra_typ_other_l3,
 use_prob_sa_typ_other_l3,
 cost_pred_sa_typ_other_l3,
 exp_cost_sa_typ_other_l3,
 use_prob_ra_comp_other_l3,
 cost_pred_ra_comp_other_l3,
 exp_cost_ra_comp_other_l3,
 use_prob_sa_comp_other_l3,
 cost_pred_sa_comp_other_l3,
 exp_cost_sa_comp_other_l3,
 total_exp_cost_ra_l3,
 total_exp_cost_sa_l3
)
AS
 SELECT salient_ra_exp_cost.row_names,
        salient_ra_exp_cost.epi_number,
        salient_ra_exp_cost.epi_name,
        salient_ra_exp_cost.epi_id,
        salient_ra_exp_cost.eol_prob,
        salient_ra_exp_cost.use_prob_ra_typ_ip_l1,
        salient_ra_exp_cost.cost_pred_ra_typ_ip_l1,
        salient_ra_exp_cost.exp_cost_ra_typ_ip_l1,
        salient_ra_exp_cost.use_prob_sa_typ_ip_l1,
        salient_ra_exp_cost.cost_pred_sa_typ_ip_l1,
        salient_ra_exp_cost.exp_cost_sa_typ_ip_l1,
        salient_ra_exp_cost.use_prob_sa_typ_other_l1,
        salient_ra_exp_cost.cost_pred_sa_typ_other_l1,
        salient_ra_exp_cost.exp_cost_sa_typ_other_l1,
        salient_ra_exp_cost.use_prob_ra_comp_other_l1,
        salient_ra_exp_cost.cost_pred_ra_comp_other_l1,
        salient_ra_exp_cost.exp_cost_ra_comp_other_l1,
        salient_ra_exp_cost.use_prob_sa_comp_other_l1,
        salient_ra_exp_cost.cost_pred_sa_comp_other_l1,
        salient_ra_exp_cost.exp_cost_sa_comp_other_l1,
        salient_ra_exp_cost.use_prob_ra_typ_ip_l4,
        salient_ra_exp_cost.cost_pred_ra_typ_ip_l4,
        salient_ra_exp_cost.exp_cost_ra_typ_ip_l4,
        salient_ra_exp_cost.use_prob_sa_typ_ip_l4,
        salient_ra_exp_cost.cost_pred_sa_typ_ip_l4,
        salient_ra_exp_cost.exp_cost_sa_typ_ip_l4,
        salient_ra_exp_cost.use_prob_ra_typ_other_l4,
        salient_ra_exp_cost.cost_pred_ra_typ_other_l4,
        salient_ra_exp_cost.exp_cost_ra_typ_other_l4,
        salient_ra_exp_cost.use_prob_sa_typ_other_l4,
        salient_ra_exp_cost.cost_pred_sa_typ_other_l4,
        salient_ra_exp_cost.exp_cost_sa_typ_other_l4,
        salient_ra_exp_cost.use_prob_ra_comp_other_l4,
        salient_ra_exp_cost.cost_pred_ra_comp_other_l4,
        salient_ra_exp_cost.exp_cost_ra_comp_other_l4,
        salient_ra_exp_cost.use_prob_sa_comp_other_l4,
        salient_ra_exp_cost.cost_pred_sa_comp_other_l4,
        salient_ra_exp_cost.exp_cost_sa_comp_other_l4,
        salient_ra_exp_cost.total_exp_cost_ra_l1,
        salient_ra_exp_cost.total_exp_cost_sa_l1,
        salient_ra_exp_cost.total_exp_cost_ra_l4,
        salient_ra_exp_cost.total_exp_cost_sa_l4,
        salient_ra_exp_cost.use_prob_ra_typ_l1,
        salient_ra_exp_cost.cost_pred_ra_typ_l1,
        salient_ra_exp_cost.exp_cost_ra_typ_l1,
        salient_ra_exp_cost.use_prob_sa_typ_l1,
        salient_ra_exp_cost.cost_pred_sa_typ_l1,
        salient_ra_exp_cost.exp_cost_sa_typ_l1,
        salient_ra_exp_cost.use_prob_ra_comp_l1,
        salient_ra_exp_cost.cost_pred_ra_comp_l1,
        salient_ra_exp_cost.exp_cost_ra_comp_l1,
        salient_ra_exp_cost.use_prob_sa_comp_l1,
        salient_ra_exp_cost.cost_pred_sa_comp_l1,
        salient_ra_exp_cost.exp_cost_sa_comp_l1,
        salient_ra_exp_cost.use_prob_ra_typ_l5,
        salient_ra_exp_cost.cost_pred_ra_typ_l5,
        salient_ra_exp_cost.exp_cost_ra_typ_l5,
        salient_ra_exp_cost.use_prob_sa_typ_l5,
        salient_ra_exp_cost.cost_pred_sa_typ_l5,
        salient_ra_exp_cost.exp_cost_sa_typ_l5,
        salient_ra_exp_cost.use_prob_ra_comp_l5,
        salient_ra_exp_cost.cost_pred_ra_comp_l5,
        salient_ra_exp_cost.exp_cost_ra_comp_l5,
        salient_ra_exp_cost.use_prob_sa_comp_l5,
        salient_ra_exp_cost.cost_pred_sa_comp_l5,
        salient_ra_exp_cost.exp_cost_sa_comp_l5,
        salient_ra_exp_cost.total_exp_cost_ra_l5,
        salient_ra_exp_cost.total_exp_cost_sa_l5,
        salient_ra_exp_cost.use_prob_ra_typ_ip_l3,
        salient_ra_exp_cost.cost_pred_ra_typ_ip_l3,
        salient_ra_exp_cost.exp_cost_ra_typ_ip_l3,
        salient_ra_exp_cost.use_prob_sa_typ_ip_l3,
        salient_ra_exp_cost.cost_pred_sa_typ_ip_l3,
        salient_ra_exp_cost.exp_cost_sa_typ_ip_l3,
        salient_ra_exp_cost.use_prob_ra_typ_other_l3,
        salient_ra_exp_cost.cost_pred_ra_typ_other_l3,
        salient_ra_exp_cost.exp_cost_ra_typ_other_l3,
        salient_ra_exp_cost.use_prob_sa_typ_other_l3,
        salient_ra_exp_cost.cost_pred_sa_typ_other_l3,
        salient_ra_exp_cost.exp_cost_sa_typ_other_l3,
        salient_ra_exp_cost.use_prob_ra_comp_other_l3,
        salient_ra_exp_cost.cost_pred_ra_comp_other_l3,
        salient_ra_exp_cost.exp_cost_ra_comp_other_l3,
        salient_ra_exp_cost.use_prob_sa_comp_other_l3,
        salient_ra_exp_cost.cost_pred_sa_comp_other_l3,
        salient_ra_exp_cost.exp_cost_sa_comp_other_l3,
        salient_ra_exp_cost.total_exp_cost_ra_l3,
        salient_ra_exp_cost.total_exp_cost_sa_l3
 FROM epbuilder.salient_ra_exp_cost
 ORDER BY salient_ra_exp_cost.row_names,
          salient_ra_exp_cost.epi_number,
          salient_ra_exp_cost.epi_name,
          salient_ra_exp_cost.epi_id,
          salient_ra_exp_cost.eol_prob,
          salient_ra_exp_cost.use_prob_ra_typ_ip_l1,
          salient_ra_exp_cost.cost_pred_ra_typ_ip_l1,
          salient_ra_exp_cost.exp_cost_ra_typ_ip_l1,
          salient_ra_exp_cost.use_prob_sa_typ_ip_l1,
          salient_ra_exp_cost.cost_pred_sa_typ_ip_l1,
          salient_ra_exp_cost.exp_cost_sa_typ_ip_l1,
          salient_ra_exp_cost.use_prob_sa_typ_other_l1,
          salient_ra_exp_cost.cost_pred_sa_typ_other_l1,
          salient_ra_exp_cost.exp_cost_sa_typ_other_l1,
          salient_ra_exp_cost.use_prob_ra_comp_other_l1,
          salient_ra_exp_cost.cost_pred_ra_comp_other_l1,
          salient_ra_exp_cost.exp_cost_ra_comp_other_l1,
          salient_ra_exp_cost.use_prob_sa_comp_other_l1,
          salient_ra_exp_cost.cost_pred_sa_comp_other_l1,
          salient_ra_exp_cost.exp_cost_sa_comp_other_l1,
          salient_ra_exp_cost.use_prob_ra_typ_ip_l4,
          salient_ra_exp_cost.cost_pred_ra_typ_ip_l4,
          salient_ra_exp_cost.exp_cost_ra_typ_ip_l4,
          salient_ra_exp_cost.use_prob_sa_typ_ip_l4,
          salient_ra_exp_cost.cost_pred_sa_typ_ip_l4,
          salient_ra_exp_cost.exp_cost_sa_typ_ip_l4,
          salient_ra_exp_cost.use_prob_ra_typ_other_l4,
          salient_ra_exp_cost.cost_pred_ra_typ_other_l4,
          salient_ra_exp_cost.exp_cost_ra_typ_other_l4,
          salient_ra_exp_cost.use_prob_sa_typ_other_l4,
          salient_ra_exp_cost.cost_pred_sa_typ_other_l4,
          salient_ra_exp_cost.exp_cost_sa_typ_other_l4,
          salient_ra_exp_cost.use_prob_ra_comp_other_l4,
          salient_ra_exp_cost.cost_pred_ra_comp_other_l4,
          salient_ra_exp_cost.exp_cost_ra_comp_other_l4,
          salient_ra_exp_cost.use_prob_sa_comp_other_l4,
          salient_ra_exp_cost.cost_pred_sa_comp_other_l4,
          salient_ra_exp_cost.exp_cost_sa_comp_other_l4,
          salient_ra_exp_cost.total_exp_cost_ra_l1,
          salient_ra_exp_cost.total_exp_cost_sa_l1,
          salient_ra_exp_cost.total_exp_cost_ra_l4,
          salient_ra_exp_cost.total_exp_cost_sa_l4,
          salient_ra_exp_cost.use_prob_ra_typ_l1,
          salient_ra_exp_cost.cost_pred_ra_typ_l1,
          salient_ra_exp_cost.exp_cost_ra_typ_l1,
          salient_ra_exp_cost.use_prob_sa_typ_l1,
          salient_ra_exp_cost.cost_pred_sa_typ_l1,
          salient_ra_exp_cost.exp_cost_sa_typ_l1,
          salient_ra_exp_cost.use_prob_ra_comp_l1,
          salient_ra_exp_cost.cost_pred_ra_comp_l1,
          salient_ra_exp_cost.exp_cost_ra_comp_l1,
          salient_ra_exp_cost.use_prob_sa_comp_l1,
          salient_ra_exp_cost.cost_pred_sa_comp_l1,
          salient_ra_exp_cost.exp_cost_sa_comp_l1,
          salient_ra_exp_cost.use_prob_ra_typ_l5,
          salient_ra_exp_cost.cost_pred_ra_typ_l5,
          salient_ra_exp_cost.exp_cost_ra_typ_l5,
          salient_ra_exp_cost.use_prob_sa_typ_l5,
          salient_ra_exp_cost.cost_pred_sa_typ_l5,
          salient_ra_exp_cost.exp_cost_sa_typ_l5,
          salient_ra_exp_cost.use_prob_ra_comp_l5,
          salient_ra_exp_cost.cost_pred_ra_comp_l5,
          salient_ra_exp_cost.exp_cost_ra_comp_l5,
          salient_ra_exp_cost.use_prob_sa_comp_l5,
          salient_ra_exp_cost.cost_pred_sa_comp_l5,
          salient_ra_exp_cost.exp_cost_sa_comp_l5,
          salient_ra_exp_cost.total_exp_cost_ra_l5,
          salient_ra_exp_cost.total_exp_cost_sa_l5,
          salient_ra_exp_cost.use_prob_ra_typ_ip_l3,
          salient_ra_exp_cost.cost_pred_ra_typ_ip_l3,
          salient_ra_exp_cost.exp_cost_ra_typ_ip_l3,
          salient_ra_exp_cost.use_prob_sa_typ_ip_l3,
          salient_ra_exp_cost.cost_pred_sa_typ_ip_l3,
          salient_ra_exp_cost.exp_cost_sa_typ_ip_l3,
          salient_ra_exp_cost.use_prob_ra_typ_other_l3,
          salient_ra_exp_cost.cost_pred_ra_typ_other_l3,
          salient_ra_exp_cost.exp_cost_ra_typ_other_l3,
          salient_ra_exp_cost.use_prob_sa_typ_other_l3,
          salient_ra_exp_cost.cost_pred_sa_typ_other_l3,
          salient_ra_exp_cost.exp_cost_sa_typ_other_l3,
          salient_ra_exp_cost.use_prob_ra_comp_other_l3,
          salient_ra_exp_cost.cost_pred_ra_comp_other_l3,
          salient_ra_exp_cost.exp_cost_ra_comp_other_l3,
          salient_ra_exp_cost.use_prob_sa_comp_other_l3,
          salient_ra_exp_cost.cost_pred_sa_comp_other_l3,
          salient_ra_exp_cost.exp_cost_sa_comp_other_l3,
          salient_ra_exp_cost.total_exp_cost_ra_l3,
          salient_ra_exp_cost.total_exp_cost_sa_l3
SEGMENTED BY hash(salient_ra_exp_cost.eol_prob, salient_ra_exp_cost.use_prob_ra_typ_ip_l1, salient_ra_exp_cost.cost_pred_ra_typ_ip_l1, salient_ra_exp_cost.exp_cost_ra_typ_ip_l1, salient_ra_exp_cost.use_prob_sa_typ_ip_l1, salient_ra_exp_cost.cost_pred_sa_typ_ip_l1, salient_ra_exp_cost.exp_cost_sa_typ_ip_l1, salient_ra_exp_cost.use_prob_sa_typ_other_l1, salient_ra_exp_cost.cost_pred_sa_typ_other_l1, salient_ra_exp_cost.exp_cost_sa_typ_other_l1, salient_ra_exp_cost.use_prob_ra_comp_other_l1, salient_ra_exp_cost.cost_pred_ra_comp_other_l1, salient_ra_exp_cost.exp_cost_ra_comp_other_l1, salient_ra_exp_cost.use_prob_sa_comp_other_l1, salient_ra_exp_cost.cost_pred_sa_comp_other_l1, salient_ra_exp_cost.exp_cost_sa_comp_other_l1, salient_ra_exp_cost.use_prob_ra_typ_ip_l4, salient_ra_exp_cost.cost_pred_ra_typ_ip_l4, salient_ra_exp_cost.exp_cost_ra_typ_ip_l4, salient_ra_exp_cost.use_prob_sa_typ_ip_l4, salient_ra_exp_cost.cost_pred_sa_typ_ip_l4, salient_ra_exp_cost.exp_cost_sa_typ_ip_l4, salient_ra_exp_cost.use_prob_ra_typ_other_l4, salient_ra_exp_cost.cost_pred_ra_typ_other_l4, salient_ra_exp_cost.exp_cost_ra_typ_other_l4, salient_ra_exp_cost.use_prob_sa_typ_other_l4, salient_ra_exp_cost.cost_pred_sa_typ_other_l4, salient_ra_exp_cost.exp_cost_sa_typ_other_l4, salient_ra_exp_cost.use_prob_ra_comp_other_l4, salient_ra_exp_cost.cost_pred_ra_comp_other_l4, salient_ra_exp_cost.exp_cost_ra_comp_other_l4, salient_ra_exp_cost.use_prob_sa_comp_other_l4) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_episodes_all /*+createtype(A)*/ 
(
 member_id,
 master_episode_id
)
AS
 SELECT salient_episodes_all.member_id,
        salient_episodes_all.master_episode_id
 FROM epbuilder.salient_episodes_all
 ORDER BY salient_episodes_all.member_id,
          salient_episodes_all.master_episode_id
SEGMENTED BY hash(salient_episodes_all.member_id, salient_episodes_all.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_association /*+createtype(A)*/ 
(
 id,
 parent_master_episode_id,
 child_master_episode_id,
 association_type,
 association_level,
 association_count,
 association_start_day,
 association_end_day
)
AS
 SELECT salient_association.id,
        salient_association.parent_master_episode_id,
        salient_association.child_master_episode_id,
        salient_association.association_type,
        salient_association.association_level,
        salient_association.association_count,
        salient_association.association_start_day,
        salient_association.association_end_day
 FROM epbuilder.salient_association
 ORDER BY salient_association.id,
          salient_association.parent_master_episode_id,
          salient_association.child_master_episode_id,
          salient_association.association_type,
          salient_association.association_level,
          salient_association.association_count,
          salient_association.association_start_day,
          salient_association.association_end_day
SEGMENTED BY hash(salient_association.id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_episode /*+createtype(A)*/ 
(
 id,
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
 attrib_default_physician,
 attrib_default_facility
)
AS
 SELECT salient_episode.id,
        salient_episode.member_id,
        salient_episode.claim_id,
        salient_episode.claim_line_id,
        salient_episode.master_episode_id,
        salient_episode.master_claim_id,
        salient_episode.episode_id,
        salient_episode.episode_type,
        salient_episode.episode_begin_date,
        salient_episode.episode_end_date,
        salient_episode.episode_length_days,
        salient_episode.trig_begin_date,
        salient_episode.trig_end_date,
        salient_episode.associated,
        salient_episode.association_count,
        salient_episode.association_level,
        salient_episode.truncated,
        salient_episode.attrib_default_physician,
        salient_episode.attrib_default_facility
 FROM epbuilder.salient_episode
 ORDER BY salient_episode.id
SEGMENTED BY hash(salient_episode.id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_claims_combined /*+createtype(A)*/ 
(
 id,
 master_claim_id,
 member_id,
 allowed_amt,
 assigned_count,
 claim_line_type_code,
 begin_date,
 end_date
)
AS
 SELECT salient_claims_combined.id,
        salient_claims_combined.master_claim_id,
        salient_claims_combined.member_id,
        salient_claims_combined.allowed_amt,
        salient_claims_combined.assigned_count,
        salient_claims_combined.claim_line_type_code,
        salient_claims_combined.begin_date,
        salient_claims_combined.end_date
 FROM epbuilder.salient_claims_combined
 ORDER BY salient_claims_combined.id
SEGMENTED BY hash(salient_claims_combined.id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_filtered_episodes /*+createtype(A)*/ 
(
 filter_id,
 master_episode_id,
 filter_fail,
 age_limit_lower,
 age_limit_upper,
 episode_cost_upper,
 episode_cost_lower,
 coverage_gap,
 episode_complete,
 drg,
 proc_ep_orphan,
 proc_ep_orph_triggered
)
AS
 SELECT salient_filtered_episodes.filter_id,
        salient_filtered_episodes.master_episode_id,
        salient_filtered_episodes.filter_fail,
        salient_filtered_episodes.age_limit_lower,
        salient_filtered_episodes.age_limit_upper,
        salient_filtered_episodes.episode_cost_upper,
        salient_filtered_episodes.episode_cost_lower,
        salient_filtered_episodes.coverage_gap,
        salient_filtered_episodes.episode_complete,
        salient_filtered_episodes.drg,
        salient_filtered_episodes.proc_ep_orphan,
        salient_filtered_episodes.proc_ep_orph_triggered
 FROM epbuilder.salient_filtered_episodes
 ORDER BY salient_filtered_episodes.filter_id,
          salient_filtered_episodes.master_episode_id,
          salient_filtered_episodes.filter_fail,
          salient_filtered_episodes.age_limit_lower,
          salient_filtered_episodes.age_limit_upper,
          salient_filtered_episodes.episode_cost_upper,
          salient_filtered_episodes.episode_cost_lower,
          salient_filtered_episodes.coverage_gap,
          salient_filtered_episodes.episode_complete,
          salient_filtered_episodes.drg,
          salient_filtered_episodes.proc_ep_orphan,
          salient_filtered_episodes.proc_ep_orph_triggered
SEGMENTED BY hash(salient_filtered_episodes.filter_id, salient_filtered_episodes.filter_fail, salient_filtered_episodes.age_limit_lower, salient_filtered_episodes.age_limit_upper, salient_filtered_episodes.episode_cost_upper, salient_filtered_episodes.episode_cost_lower, salient_filtered_episodes.coverage_gap, salient_filtered_episodes.episode_complete, salient_filtered_episodes.drg, salient_filtered_episodes.proc_ep_orphan, salient_filtered_episodes.proc_ep_orph_triggered, salient_filtered_episodes.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_assignment /*+createtype(A)*/ 
(
 id,
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
 IP_period,
 id_new
)
AS
 SELECT salient_assignment.id,
        salient_assignment.member_id,
        salient_assignment.master_claim_id,
        salient_assignment.master_episode_id,
        salient_assignment.claim_source,
        salient_assignment.assigned_type,
        salient_assignment.assigned_count,
        salient_assignment.rule,
        salient_assignment.pac,
        salient_assignment.pac_type,
        salient_assignment.episode_period,
        salient_assignment.IP_period,
        salient_assignment.id_new
 FROM epbuilder.salient_assignment
 ORDER BY salient_assignment.id,
          salient_assignment.member_id,
          salient_assignment.master_claim_id,
          salient_assignment.master_episode_id,
          salient_assignment.claim_source,
          salient_assignment.assigned_type,
          salient_assignment.assigned_count,
          salient_assignment.rule,
          salient_assignment.pac,
          salient_assignment.pac_type,
          salient_assignment.episode_period,
          salient_assignment.IP_period,
          salient_assignment.id_new
SEGMENTED BY hash(salient_assignment.id, salient_assignment.assigned_type, salient_assignment.assigned_count, salient_assignment.rule, salient_assignment.pac, salient_assignment.pac_type, salient_assignment.episode_period, salient_assignment.IP_period, salient_assignment.id_new, salient_assignment.claim_source, salient_assignment.member_id, salient_assignment.master_episode_id, salient_assignment.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.salient_mel /*+createtype(A)*/ 
(
 id,
 filter_id,
 master_episode_id,
 claim_type,
 level,
 split,
 annualized,
 cost,
 cost_t,
 cost_tc,
 cost_c,
 risk_factor_count,
 sub_type_count,
 probability_of_complications,
 IP_stay_count,
 IP_stay_facility_costs,
 IP_stay_prof_costs,
 IP_stay_total_costs,
 IP_stay_bed_days,
 IP_stay_avg_length
)
AS
 SELECT salient_mel.id,
        salient_mel.filter_id,
        salient_mel.master_episode_id,
        salient_mel.claim_type,
        salient_mel.level,
        salient_mel.split,
        salient_mel.annualized,
        salient_mel.cost,
        salient_mel.cost_t,
        salient_mel.cost_tc,
        salient_mel.cost_c,
        salient_mel.risk_factor_count,
        salient_mel.sub_type_count,
        salient_mel.probability_of_complications,
        salient_mel.IP_stay_count,
        salient_mel.IP_stay_facility_costs,
        salient_mel.IP_stay_prof_costs,
        salient_mel.IP_stay_total_costs,
        salient_mel.IP_stay_bed_days,
        salient_mel.IP_stay_avg_length
 FROM epbuilder.salient_mel
 ORDER BY salient_mel.id
SEGMENTED BY hash(salient_mel.id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PAC_groups /*+createtype(A)*/ 
(
 episode_id,
 episode_name,
 episode_description,
 code_id,
 type_id,
 code_name,
 group_id,
 group_name
)
AS
 SELECT PAC_groups.episode_id,
        PAC_groups.episode_name,
        PAC_groups.episode_description,
        PAC_groups.code_id,
        PAC_groups.type_id,
        PAC_groups.code_name,
        PAC_groups.group_id,
        PAC_groups.group_name
 FROM epbuilder.PAC_groups
 ORDER BY PAC_groups.code_id,
          PAC_groups.type_id
SEGMENTED BY hash(PAC_groups.code_id, PAC_groups.type_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.MBR_MOTHER_BABY /*+createtype(L)*/ 
(
 MOTHER_MBR_ID,
 BABY_MBR_ID,
 BIRTH_YR,
 MDW_EFF_DT,
 MDW_EXP_DT,
 INS_ROW_PRCS_DT,
 MDW_INS_DT,
 MDW_INS_ID,
 MDW_UPDT_DT,
 MDW_UPDT_ID
)
AS
 SELECT MBR_MOTHER_BABY.MOTHER_MBR_ID,
        MBR_MOTHER_BABY.BABY_MBR_ID,
        MBR_MOTHER_BABY.BIRTH_YR,
        MBR_MOTHER_BABY.MDW_EFF_DT,
        MBR_MOTHER_BABY.MDW_EXP_DT,
        MBR_MOTHER_BABY.INS_ROW_PRCS_DT,
        MBR_MOTHER_BABY.MDW_INS_DT,
        MBR_MOTHER_BABY.MDW_INS_ID,
        MBR_MOTHER_BABY.MDW_UPDT_DT,
        MBR_MOTHER_BABY.MDW_UPDT_ID
 FROM epbuilder.MBR_MOTHER_BABY
 ORDER BY MBR_MOTHER_BABY.MOTHER_MBR_ID,
          MBR_MOTHER_BABY.BABY_MBR_ID,
          MBR_MOTHER_BABY.BIRTH_YR,
          MBR_MOTHER_BABY.MDW_EFF_DT
SEGMENTED BY hash(MBR_MOTHER_BABY.MOTHER_MBR_ID, MBR_MOTHER_BABY.BABY_MBR_ID, MBR_MOTHER_BABY.BIRTH_YR, MBR_MOTHER_BABY.MDW_EFF_DT) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PCP_TABLE_20160825 /*+createtype(L)*/ 
(
 MEMBER_ID,
 PCP,
 MMIS,
 NPI
)
AS
 SELECT PCP_TABLE_20160825.MEMBER_ID,
        PCP_TABLE_20160825.PCP,
        PCP_TABLE_20160825.MMIS,
        PCP_TABLE_20160825.NPI
 FROM epbuilder.PCP_TABLE_20160825
 ORDER BY PCP_TABLE_20160825.MEMBER_ID,
          PCP_TABLE_20160825.PCP,
          PCP_TABLE_20160825.MMIS,
          PCP_TABLE_20160825.NPI
SEGMENTED BY hash(PCP_TABLE_20160825.MMIS, PCP_TABLE_20160825.NPI, PCP_TABLE_20160825.MEMBER_ID, PCP_TABLE_20160825.PCP) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.V_NUM_EVENTS_RP1100A /*+createtype(L)*/ 
(
 MSR_PUB_YR,
 MSR_ID,
 MSR_COMP_ID,
 MBR_KEY,
 MSR_YR,
 CLAIM_TRANS_ID,
 TCN,
 DSCH_DT,
 MBR_AGE,
 MDW_INS_DT
)
AS
 SELECT V_NUM_EVENTS_RP1100A.MSR_PUB_YR,
        V_NUM_EVENTS_RP1100A.MSR_ID,
        V_NUM_EVENTS_RP1100A.MSR_COMP_ID,
        V_NUM_EVENTS_RP1100A.MBR_KEY,
        V_NUM_EVENTS_RP1100A.MSR_YR,
        V_NUM_EVENTS_RP1100A.CLAIM_TRANS_ID,
        V_NUM_EVENTS_RP1100A.TCN,
        V_NUM_EVENTS_RP1100A.DSCH_DT,
        V_NUM_EVENTS_RP1100A.MBR_AGE,
        V_NUM_EVENTS_RP1100A.MDW_INS_DT
 FROM epbuilder.V_NUM_EVENTS_RP1100A
 ORDER BY V_NUM_EVENTS_RP1100A.MSR_PUB_YR,
          V_NUM_EVENTS_RP1100A.MSR_ID,
          V_NUM_EVENTS_RP1100A.MSR_COMP_ID,
          V_NUM_EVENTS_RP1100A.MBR_KEY,
          V_NUM_EVENTS_RP1100A.MSR_YR,
          V_NUM_EVENTS_RP1100A.CLAIM_TRANS_ID,
          V_NUM_EVENTS_RP1100A.TCN,
          V_NUM_EVENTS_RP1100A.DSCH_DT,
          V_NUM_EVENTS_RP1100A.MBR_AGE,
          V_NUM_EVENTS_RP1100A.MDW_INS_DT
SEGMENTED BY hash(V_NUM_EVENTS_RP1100A.MBR_KEY, V_NUM_EVENTS_RP1100A.CLAIM_TRANS_ID, V_NUM_EVENTS_RP1100A.DSCH_DT, V_NUM_EVENTS_RP1100A.MBR_AGE, V_NUM_EVENTS_RP1100A.MDW_INS_DT, V_NUM_EVENTS_RP1100A.MSR_PUB_YR, V_NUM_EVENTS_RP1100A.MSR_ID, V_NUM_EVENTS_RP1100A.MSR_COMP_ID, V_NUM_EVENTS_RP1100A.MSR_YR, V_NUM_EVENTS_RP1100A.TCN) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.V_MSR_RESULT_RY_RP1100A /*+createtype(L)*/ 
(
 MSR_PUB_YR,
 MSR_ID,
 MSR_COMP_ID,
 MBR_KEY,
 MSR_YR,
 MSR_VAL,
 MBR_AGE,
 MDW_INS_DT,
 MDW_UPDT_DT
)
AS
 SELECT V_MSR_RESULT_RY_RP1100A.MSR_PUB_YR,
        V_MSR_RESULT_RY_RP1100A.MSR_ID,
        V_MSR_RESULT_RY_RP1100A.MSR_COMP_ID,
        V_MSR_RESULT_RY_RP1100A.MBR_KEY,
        V_MSR_RESULT_RY_RP1100A.MSR_YR,
        V_MSR_RESULT_RY_RP1100A.MSR_VAL,
        V_MSR_RESULT_RY_RP1100A.MBR_AGE,
        V_MSR_RESULT_RY_RP1100A.MDW_INS_DT,
        V_MSR_RESULT_RY_RP1100A.MDW_UPDT_DT
 FROM epbuilder.V_MSR_RESULT_RY_RP1100A
 ORDER BY V_MSR_RESULT_RY_RP1100A.MSR_PUB_YR,
          V_MSR_RESULT_RY_RP1100A.MSR_ID,
          V_MSR_RESULT_RY_RP1100A.MSR_COMP_ID,
          V_MSR_RESULT_RY_RP1100A.MBR_KEY,
          V_MSR_RESULT_RY_RP1100A.MSR_YR,
          V_MSR_RESULT_RY_RP1100A.MSR_VAL,
          V_MSR_RESULT_RY_RP1100A.MBR_AGE,
          V_MSR_RESULT_RY_RP1100A.MDW_INS_DT,
          V_MSR_RESULT_RY_RP1100A.MDW_UPDT_DT
SEGMENTED BY hash(V_MSR_RESULT_RY_RP1100A.MBR_KEY, V_MSR_RESULT_RY_RP1100A.MSR_VAL, V_MSR_RESULT_RY_RP1100A.MBR_AGE, V_MSR_RESULT_RY_RP1100A.MDW_INS_DT, V_MSR_RESULT_RY_RP1100A.MDW_UPDT_DT, V_MSR_RESULT_RY_RP1100A.MSR_PUB_YR, V_MSR_RESULT_RY_RP1100A.MSR_ID, V_MSR_RESULT_RY_RP1100A.MSR_COMP_ID, V_MSR_RESULT_RY_RP1100A.MSR_YR) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.run /*+createtype(L)*/ 
(
 uid,
 submitter_name,
 submitted_date,
 run_name,
 data_start_date,
 data_end_date,
 data_latest_begin_date,
 enrolled_population,
 run_date,
 data_approved_date,
 metadata_version,
 metadata_custom,
 episode_count,
 episode_cost,
 unassigned_cost,
 jobUid
)
AS
 SELECT run.uid,
        run.submitter_name,
        run.submitted_date,
        run.run_name,
        run.data_start_date,
        run.data_end_date,
        run.data_latest_begin_date,
        run.enrolled_population,
        run.run_date,
        run.data_approved_date,
        run.metadata_version,
        run.metadata_custom,
        run.episode_count,
        run.episode_cost,
        run.unassigned_cost,
        run.jobUid
 FROM epbuilder.run
 ORDER BY run.uid,
          run.submitter_name,
          run.submitted_date,
          run.run_name,
          run.data_start_date,
          run.data_end_date,
          run.data_latest_begin_date,
          run.enrolled_population,
          run.run_date,
          run.data_approved_date,
          run.metadata_version,
          run.metadata_custom,
          run.episode_count,
          run.episode_cost,
          run.unassigned_cost,
          run.jobUid
SEGMENTED BY hash(run.uid, run.submitted_date, run.data_start_date, run.data_end_date, run.data_latest_begin_date, run.enrolled_population, run.run_date, run.data_approved_date, run.metadata_custom, run.episode_count, run.episode_cost, run.unassigned_cost, run.jobUid, run.submitter_name, run.run_name, run.metadata_version) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.episode /*+createtype(L)*/ 
(
 id,
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
 attrib_default_physician,
 attrib_default_facility
)
AS
 SELECT episode.id,
        episode.member_id,
        episode.claim_id,
        episode.claim_line_id,
        episode.master_episode_id,
        episode.master_claim_id,
        episode.episode_id,
        episode.episode_type,
        episode.episode_begin_date,
        episode.episode_end_date,
        episode.episode_length_days,
        episode.trig_begin_date,
        episode.trig_end_date,
        episode.associated,
        episode.association_count,
        episode.association_level,
        episode.truncated,
        episode.attrib_default_physician,
        episode.attrib_default_facility
 FROM epbuilder.episode
 ORDER BY episode.id,
          episode.member_id,
          episode.claim_id,
          episode.claim_line_id,
          episode.master_episode_id,
          episode.master_claim_id,
          episode.episode_id,
          episode.episode_type,
          episode.episode_begin_date,
          episode.episode_end_date,
          episode.episode_length_days,
          episode.trig_begin_date,
          episode.trig_end_date,
          episode.associated,
          episode.association_count,
          episode.association_level,
          episode.truncated,
          episode.attrib_default_physician,
          episode.attrib_default_facility
SEGMENTED BY hash(episode.id, episode.episode_type, episode.episode_begin_date, episode.episode_end_date, episode.episode_length_days, episode.trig_begin_date, episode.trig_end_date, episode.associated, episode.association_count, episode.association_level, episode.truncated, episode.episode_id, episode.claim_line_id, episode.attrib_default_physician, episode.attrib_default_facility, episode.member_id, episode.claim_id, episode.master_episode_id, episode.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.claims_combined /*+createtype(L)*/ 
(
 id,
 master_claim_id,
 member_id,
 allowed_amt,
 assigned_count,
 claim_line_type_code,
 begin_date,
 end_date,
 real_allowed_amt,
 proxy_allowed_amt
)
AS
 SELECT claims_combined.id,
        claims_combined.master_claim_id,
        claims_combined.member_id,
        claims_combined.allowed_amt,
        claims_combined.assigned_count,
        claims_combined.claim_line_type_code,
        claims_combined.begin_date,
        claims_combined.end_date,
        claims_combined.real_allowed_amt,
        claims_combined.proxy_allowed_amt
 FROM epbuilder.claims_combined
 ORDER BY claims_combined.id,
          claims_combined.master_claim_id,
          claims_combined.member_id,
          claims_combined.allowed_amt,
          claims_combined.assigned_count,
          claims_combined.claim_line_type_code,
          claims_combined.begin_date,
          claims_combined.end_date,
          claims_combined.real_allowed_amt,
          claims_combined.proxy_allowed_amt
SEGMENTED BY hash(claims_combined.id, claims_combined.assigned_count, claims_combined.begin_date, claims_combined.end_date, claims_combined.claim_line_type_code, claims_combined.allowed_amt, claims_combined.real_allowed_amt, claims_combined.proxy_allowed_amt, claims_combined.member_id, claims_combined.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.code /*+createtype(L)*/ 
(
 id,
 u_c_id,
 master_claim_id,
 function_code,
 code_value,
 nomen,
 principal
)
AS
 SELECT code.id,
        code.u_c_id,
        code.master_claim_id,
        code.function_code,
        code.code_value,
        code.nomen,
        code.principal
 FROM epbuilder.code
 ORDER BY code.id,
          code.u_c_id,
          code.master_claim_id,
          code.function_code,
          code.code_value,
          code.nomen,
          code.principal
SEGMENTED BY hash(code.id, code.u_c_id, code.nomen, code.principal, code.function_code, code.code_value, code.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.claim_line /*+createtype(L)*/ 
(
 id,
 master_claim_id,
 member_id,
 claim_id,
 claim_line_id,
 sequence_key,
 final_version_flag,
 claim_encounter_flag,
 provider_npi,
 provider_id,
 physician_id,
 facility_id,
 allowed_amt,
 real_allowed_amt,
 proxy_allowed_amt,
 facility_type_code,
 begin_date,
 end_date,
 place_of_svc_code,
 claim_line_type_code,
 assigned,
 assigned_count,
 quantity,
 standard_payment_amt,
 charge_amt,
 paid_amt,
 prepaid_amt,
 copay_amt,
 coinsurance_amt,
 deductible_amt,
 insurance_product,
 plan_id,
 admission_date,
 admission_src_code,
 admit_type_code,
 discharge_status_code,
 discharge_date,
 type_of_bill,
 rev_count,
 drg_version,
 ms_drg_code,
 apr_drg_code,
 readmission,
 office_visit,
 trigger,
 ed_visit,
 ed_visit_id,
 core_service,
 pas
)
AS
 SELECT claim_line.id,
        claim_line.master_claim_id,
        claim_line.member_id,
        claim_line.claim_id,
        claim_line.claim_line_id,
        claim_line.sequence_key,
        claim_line.final_version_flag,
        claim_line.claim_encounter_flag,
        claim_line.provider_npi,
        claim_line.provider_id,
        claim_line.physician_id,
        claim_line.facility_id,
        claim_line.allowed_amt,
        claim_line.real_allowed_amt,
        claim_line.proxy_allowed_amt,
        claim_line.facility_type_code,
        claim_line.begin_date,
        claim_line.end_date,
        claim_line.place_of_svc_code,
        claim_line.claim_line_type_code,
        claim_line.assigned,
        claim_line.assigned_count,
        claim_line.quantity,
        claim_line.standard_payment_amt,
        claim_line.charge_amt,
        claim_line.paid_amt,
        claim_line.prepaid_amt,
        claim_line.copay_amt,
        claim_line.coinsurance_amt,
        claim_line.deductible_amt,
        claim_line.insurance_product,
        claim_line.plan_id,
        claim_line.admission_date,
        claim_line.admission_src_code,
        claim_line.admit_type_code,
        claim_line.discharge_status_code,
        claim_line.discharge_date,
        claim_line.type_of_bill,
        claim_line.rev_count,
        claim_line.drg_version,
        claim_line.ms_drg_code,
        claim_line.apr_drg_code,
        claim_line.readmission,
        claim_line.office_visit,
        claim_line.trigger,
        claim_line.ed_visit,
        claim_line.ed_visit_id,
        claim_line.core_service,
        claim_line.pas
 FROM epbuilder.claim_line
 ORDER BY claim_line.id,
          claim_line.master_claim_id,
          claim_line.member_id,
          claim_line.claim_id,
          claim_line.claim_line_id,
          claim_line.sequence_key,
          claim_line.final_version_flag,
          claim_line.claim_encounter_flag,
          claim_line.provider_npi,
          claim_line.provider_id,
          claim_line.physician_id,
          claim_line.facility_id,
          claim_line.allowed_amt,
          claim_line.real_allowed_amt,
          claim_line.proxy_allowed_amt,
          claim_line.facility_type_code,
          claim_line.begin_date,
          claim_line.end_date,
          claim_line.place_of_svc_code,
          claim_line.claim_line_type_code,
          claim_line.assigned,
          claim_line.assigned_count,
          claim_line.quantity,
          claim_line.standard_payment_amt,
          claim_line.charge_amt,
          claim_line.paid_amt,
          claim_line.prepaid_amt,
          claim_line.copay_amt,
          claim_line.coinsurance_amt,
          claim_line.deductible_amt,
          claim_line.insurance_product,
          claim_line.plan_id,
          claim_line.admission_date,
          claim_line.admission_src_code,
          claim_line.admit_type_code,
          claim_line.discharge_status_code,
          claim_line.discharge_date,
          claim_line.type_of_bill,
          claim_line.rev_count,
          claim_line.drg_version,
          claim_line.ms_drg_code,
          claim_line.apr_drg_code,
          claim_line.readmission,
          claim_line.office_visit,
          claim_line.trigger,
          claim_line.ed_visit,
          claim_line.ed_visit_id,
          claim_line.core_service,
          claim_line.pas
SEGMENTED BY hash(claim_line.id, claim_line.final_version_flag, claim_line.claim_encounter_flag, claim_line.facility_type_code, claim_line.begin_date, claim_line.end_date, claim_line.assigned, claim_line.assigned_count, claim_line.quantity, claim_line.paid_amt, claim_line.prepaid_amt, claim_line.copay_amt, claim_line.coinsurance_amt, claim_line.deductible_amt, claim_line.admission_date, claim_line.discharge_date, claim_line.rev_count, claim_line.readmission, claim_line.office_visit, claim_line.trigger, claim_line.ed_visit, claim_line.core_service, claim_line.pas, claim_line.sequence_key, claim_line.provider_npi, claim_line.claim_line_type_code, claim_line.admission_src_code, claim_line.admit_type_code, claim_line.discharge_status_code, claim_line.type_of_bill, claim_line.drg_version, claim_line.ms_drg_code) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.claim_line_rx /*+createtype(L)*/ 
(
 id,
 master_claim_id,
 member_id,
 claim_id,
 sequence_key,
 final_version_flag,
 claim_encounter_flag,
 allowed_amt,
 real_allowed_amt,
 proxy_allowed_amt,
 assigned,
 assigned_count,
 line_counter,
 charge_amt,
 paid_amt,
 prepaid_amt,
 copay_amt,
 coinsurance_amt,
 deductible_amt,
 drug_nomen,
 drug_code,
 drug_name,
 builder_match_code,
 days_supply_amt,
 quantityDispensed,
 rx_fill_date,
 prescribing_provider_id,
 prescribing_provider_npi,
 prescribing_provider_dea,
 pharmacy_zip_code,
 insurance_product,
 genericDrugIndicator,
 national_pharmacy_Id,
 orig_adj_rev,
 plan_id
)
AS
 SELECT claim_line_rx.id,
        claim_line_rx.master_claim_id,
        claim_line_rx.member_id,
        claim_line_rx.claim_id,
        claim_line_rx.sequence_key,
        claim_line_rx.final_version_flag,
        claim_line_rx.claim_encounter_flag,
        claim_line_rx.allowed_amt,
        claim_line_rx.real_allowed_amt,
        claim_line_rx.proxy_allowed_amt,
        claim_line_rx.assigned,
        claim_line_rx.assigned_count,
        claim_line_rx.line_counter,
        claim_line_rx.charge_amt,
        claim_line_rx.paid_amt,
        claim_line_rx.prepaid_amt,
        claim_line_rx.copay_amt,
        claim_line_rx.coinsurance_amt,
        claim_line_rx.deductible_amt,
        claim_line_rx.drug_nomen,
        claim_line_rx.drug_code,
        claim_line_rx.drug_name,
        claim_line_rx.builder_match_code,
        claim_line_rx.days_supply_amt,
        claim_line_rx.quantityDispensed,
        claim_line_rx.rx_fill_date,
        claim_line_rx.prescribing_provider_id,
        claim_line_rx.prescribing_provider_npi,
        claim_line_rx.prescribing_provider_dea,
        claim_line_rx.pharmacy_zip_code,
        claim_line_rx.insurance_product,
        claim_line_rx.genericDrugIndicator,
        claim_line_rx.national_pharmacy_Id,
        claim_line_rx.orig_adj_rev,
        claim_line_rx.plan_id
 FROM epbuilder.claim_line_rx
 ORDER BY claim_line_rx.id,
          claim_line_rx.master_claim_id,
          claim_line_rx.member_id,
          claim_line_rx.claim_id,
          claim_line_rx.sequence_key,
          claim_line_rx.final_version_flag,
          claim_line_rx.claim_encounter_flag,
          claim_line_rx.allowed_amt,
          claim_line_rx.real_allowed_amt,
          claim_line_rx.proxy_allowed_amt,
          claim_line_rx.assigned,
          claim_line_rx.assigned_count,
          claim_line_rx.line_counter,
          claim_line_rx.charge_amt,
          claim_line_rx.paid_amt,
          claim_line_rx.prepaid_amt,
          claim_line_rx.copay_amt,
          claim_line_rx.coinsurance_amt,
          claim_line_rx.deductible_amt,
          claim_line_rx.drug_nomen,
          claim_line_rx.drug_code,
          claim_line_rx.drug_name,
          claim_line_rx.builder_match_code,
          claim_line_rx.days_supply_amt,
          claim_line_rx.quantityDispensed,
          claim_line_rx.rx_fill_date,
          claim_line_rx.prescribing_provider_id,
          claim_line_rx.prescribing_provider_npi,
          claim_line_rx.prescribing_provider_dea,
          claim_line_rx.pharmacy_zip_code,
          claim_line_rx.insurance_product,
          claim_line_rx.genericDrugIndicator,
          claim_line_rx.national_pharmacy_Id,
          claim_line_rx.orig_adj_rev,
          claim_line_rx.plan_id
SEGMENTED BY hash(claim_line_rx.id, claim_line_rx.final_version_flag, claim_line_rx.claim_encounter_flag, claim_line_rx.assigned, claim_line_rx.assigned_count, claim_line_rx.line_counter, claim_line_rx.days_supply_amt, claim_line_rx.rx_fill_date, claim_line_rx.genericDrugIndicator, claim_line_rx.drug_nomen, claim_line_rx.sequence_key, claim_line_rx.prescribing_provider_npi, claim_line_rx.orig_adj_rev, claim_line_rx.pharmacy_zip_code, claim_line_rx.prescribing_provider_dea, claim_line_rx.national_pharmacy_Id, claim_line_rx.allowed_amt, claim_line_rx.real_allowed_amt, claim_line_rx.proxy_allowed_amt, claim_line_rx.charge_amt, claim_line_rx.paid_amt, claim_line_rx.prepaid_amt, claim_line_rx.copay_amt, claim_line_rx.coinsurance_amt, claim_line_rx.deductible_amt, claim_line_rx.quantityDispensed, claim_line_rx.prescribing_provider_id, claim_line_rx.insurance_product, claim_line_rx.plan_id, claim_line_rx.drug_code, claim_line_rx.member_id, claim_line_rx.claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.master_epid_level /*+createtype(L)*/ 
(
 id,
 filter_id,
 master_episode_id,
 claim_type,
 level,
 split,
 annualized,
 cost,
 cost_t,
 cost_tc,
 cost_c,
 risk_factor_count,
 sub_type_count,
 probability_of_complications,
 IP_stay_count,
 IP_stay_facility_costs,
 IP_stay_prof_costs,
 IP_stay_total_costs,
 IP_stay_bed_days,
 IP_stay_avg_length
)
AS
 SELECT master_epid_level.id,
        master_epid_level.filter_id,
        master_epid_level.master_episode_id,
        master_epid_level.claim_type,
        master_epid_level.level,
        master_epid_level.split,
        master_epid_level.annualized,
        master_epid_level.cost,
        master_epid_level.cost_t,
        master_epid_level.cost_tc,
        master_epid_level.cost_c,
        master_epid_level.risk_factor_count,
        master_epid_level.sub_type_count,
        master_epid_level.probability_of_complications,
        master_epid_level.IP_stay_count,
        master_epid_level.IP_stay_facility_costs,
        master_epid_level.IP_stay_prof_costs,
        master_epid_level.IP_stay_total_costs,
        master_epid_level.IP_stay_bed_days,
        master_epid_level.IP_stay_avg_length
 FROM epbuilder.master_epid_level
 ORDER BY master_epid_level.id,
          master_epid_level.filter_id,
          master_epid_level.master_episode_id,
          master_epid_level.claim_type,
          master_epid_level.level,
          master_epid_level.split,
          master_epid_level.annualized,
          master_epid_level.cost,
          master_epid_level.cost_t,
          master_epid_level.cost_tc,
          master_epid_level.cost_c,
          master_epid_level.risk_factor_count,
          master_epid_level.sub_type_count,
          master_epid_level.probability_of_complications,
          master_epid_level.IP_stay_count,
          master_epid_level.IP_stay_facility_costs,
          master_epid_level.IP_stay_prof_costs,
          master_epid_level.IP_stay_total_costs,
          master_epid_level.IP_stay_bed_days,
          master_epid_level.IP_stay_avg_length
SEGMENTED BY hash(master_epid_level.id, master_epid_level.filter_id, master_epid_level.claim_type, master_epid_level.level, master_epid_level.split, master_epid_level.annualized, master_epid_level.risk_factor_count, master_epid_level.sub_type_count, master_epid_level.probability_of_complications, master_epid_level.IP_stay_count, master_epid_level.IP_stay_bed_days, master_epid_level.IP_stay_avg_length, master_epid_level.cost, master_epid_level.cost_t, master_epid_level.cost_tc, master_epid_level.cost_c, master_epid_level.IP_stay_facility_costs, master_epid_level.IP_stay_prof_costs, master_epid_level.IP_stay_total_costs, master_epid_level.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.episode_aggregates /*+createtype(L)*/ 
(
 episode_id,
 filter_id,
 level,
 episode_count,
 total_cost_unsplit,
 total_cost_split,
 avg_cost_unsplit,
 avg_cost_split,
 standard_deviation_unsplit,
 standard_deviation_split,
 cv_unsplit,
 cv_split,
 percent_costs_unsplit,
 percent_costs_split,
 typical_costs_unsplit,
 typical_costs_split,
 typ_w_comp_costs_unsplit,
 typ_w_comp_costs_split,
 pac_costs_unsplit,
 pac_costs_split,
 complication_rate_unsplit,
 complication_rate_split,
 percentile_1_costs_split,
 percentile_80_costs_split,
 percentile_99_costs_split,
 percentile_1_ann_costs_split,
 percentile_80_ann_costs_split,
 percentile_99_ann_costs_split,
 percentile_1_costs_unsplit,
 percentile_99_costs_unsplit,
 percentile_1_ann_costs_unsplit,
 percentile_99_ann_costs_unsplit
)
AS
 SELECT episode_aggregates.episode_id,
        episode_aggregates.filter_id,
        episode_aggregates.level,
        episode_aggregates.episode_count,
        episode_aggregates.total_cost_unsplit,
        episode_aggregates.total_cost_split,
        episode_aggregates.avg_cost_unsplit,
        episode_aggregates.avg_cost_split,
        episode_aggregates.standard_deviation_unsplit,
        episode_aggregates.standard_deviation_split,
        episode_aggregates.cv_unsplit,
        episode_aggregates.cv_split,
        episode_aggregates.percent_costs_unsplit,
        episode_aggregates.percent_costs_split,
        episode_aggregates.typical_costs_unsplit,
        episode_aggregates.typical_costs_split,
        episode_aggregates.typ_w_comp_costs_unsplit,
        episode_aggregates.typ_w_comp_costs_split,
        episode_aggregates.pac_costs_unsplit,
        episode_aggregates.pac_costs_split,
        episode_aggregates.complication_rate_unsplit,
        episode_aggregates.complication_rate_split,
        episode_aggregates.percentile_1_costs_split,
        episode_aggregates.percentile_80_costs_split,
        episode_aggregates.percentile_99_costs_split,
        episode_aggregates.percentile_1_ann_costs_split,
        episode_aggregates.percentile_80_ann_costs_split,
        episode_aggregates.percentile_99_ann_costs_split,
        episode_aggregates.percentile_1_costs_unsplit,
        episode_aggregates.percentile_99_costs_unsplit,
        episode_aggregates.percentile_1_ann_costs_unsplit,
        episode_aggregates.percentile_99_ann_costs_unsplit
 FROM epbuilder.episode_aggregates
 ORDER BY episode_aggregates.episode_id,
          episode_aggregates.filter_id,
          episode_aggregates.level,
          episode_aggregates.episode_count,
          episode_aggregates.total_cost_unsplit,
          episode_aggregates.total_cost_split,
          episode_aggregates.avg_cost_unsplit,
          episode_aggregates.avg_cost_split,
          episode_aggregates.standard_deviation_unsplit,
          episode_aggregates.standard_deviation_split,
          episode_aggregates.cv_unsplit,
          episode_aggregates.cv_split,
          episode_aggregates.percent_costs_unsplit,
          episode_aggregates.percent_costs_split,
          episode_aggregates.typical_costs_unsplit,
          episode_aggregates.typical_costs_split,
          episode_aggregates.typ_w_comp_costs_unsplit,
          episode_aggregates.typ_w_comp_costs_split,
          episode_aggregates.pac_costs_unsplit,
          episode_aggregates.pac_costs_split,
          episode_aggregates.complication_rate_unsplit,
          episode_aggregates.complication_rate_split,
          episode_aggregates.percentile_1_costs_split,
          episode_aggregates.percentile_80_costs_split,
          episode_aggregates.percentile_99_costs_split,
          episode_aggregates.percentile_1_ann_costs_split,
          episode_aggregates.percentile_80_ann_costs_split,
          episode_aggregates.percentile_99_ann_costs_split,
          episode_aggregates.percentile_1_costs_unsplit,
          episode_aggregates.percentile_99_costs_unsplit,
          episode_aggregates.percentile_1_ann_costs_unsplit,
          episode_aggregates.percentile_99_ann_costs_unsplit
SEGMENTED BY hash(episode_aggregates.filter_id, episode_aggregates.level, episode_aggregates.episode_count, episode_aggregates.episode_id, episode_aggregates.total_cost_unsplit, episode_aggregates.total_cost_split, episode_aggregates.avg_cost_unsplit, episode_aggregates.avg_cost_split, episode_aggregates.standard_deviation_unsplit, episode_aggregates.standard_deviation_split, episode_aggregates.cv_unsplit, episode_aggregates.cv_split, episode_aggregates.percent_costs_unsplit, episode_aggregates.percent_costs_split, episode_aggregates.typical_costs_unsplit, episode_aggregates.typical_costs_split, episode_aggregates.typ_w_comp_costs_unsplit, episode_aggregates.typ_w_comp_costs_split, episode_aggregates.pac_costs_unsplit, episode_aggregates.pac_costs_split, episode_aggregates.complication_rate_unsplit, episode_aggregates.complication_rate_split, episode_aggregates.percentile_1_costs_split, episode_aggregates.percentile_80_costs_split, episode_aggregates.percentile_99_costs_split, episode_aggregates.percentile_1_ann_costs_split, episode_aggregates.percentile_80_ann_costs_split, episode_aggregates.percentile_99_ann_costs_split, episode_aggregates.percentile_1_costs_unsplit, episode_aggregates.percentile_99_costs_unsplit, episode_aggregates.percentile_1_ann_costs_unsplit, episode_aggregates.percentile_99_ann_costs_unsplit) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_epid_level /*+createtype(L)*/ 
(
 id,
 member_id,
 episode_id,
 level,
 split,
 annualized,
 cost,
 cost_t,
 cost_tc,
 cost_c
)
AS
 SELECT member_epid_level.id,
        member_epid_level.member_id,
        member_epid_level.episode_id,
        member_epid_level.level,
        member_epid_level.split,
        member_epid_level.annualized,
        member_epid_level.cost,
        member_epid_level.cost_t,
        member_epid_level.cost_tc,
        member_epid_level.cost_c
 FROM epbuilder.member_epid_level
 ORDER BY member_epid_level.id,
          member_epid_level.member_id,
          member_epid_level.episode_id,
          member_epid_level.level,
          member_epid_level.split,
          member_epid_level.annualized,
          member_epid_level.cost,
          member_epid_level.cost_t,
          member_epid_level.cost_tc,
          member_epid_level.cost_c
SEGMENTED BY hash(member_epid_level.id, member_epid_level.level, member_epid_level.split, member_epid_level.annualized, member_epid_level.episode_id, member_epid_level.cost, member_epid_level.cost_t, member_epid_level.cost_tc, member_epid_level.cost_c, member_epid_level.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.pmp_su /*+createtype(L)*/ 
(
 pmp_su_id,
 begin_date,
 end_date,
 period,
 member_count,
 total_costs,
 total_costs_IP,
 total_costs_OP,
 total_costs_PB,
 total_costs_RX,
 pmp_costs,
 pmp_costs_IP,
 pmp_costs_OP,
 pmp_costs_PB,
 pmp_costs_RX,
 member_count_IP_stay,
 member_count_ED_visit,
 member_percent_IP_stay,
 member_percent_ED_visit,
 member_count_SU,
 member_count_SU_IP,
 member_count_SU_ED,
 member_count_SU_ED_and_IP,
 member_percent_SU,
 member_percent_SU_IP,
 member_percent_SU_ED,
 member_percent_SU_ED_and_IP,
 costs_ED_and_IP,
 costs_SU_ED_or_IP,
 costs_SU_ED,
 costs_SU_IP,
 costs_SU_ED_and_IP,
 percent_costs_SU_ED_or_IP,
 percent_ED_costs_SU_ED,
 percent_IP_costs_SU_IP,
 percent_costs_SU_ED_and_IP,
 ED_SU_avg_ED_count,
 ED_SU_avg_IP_count,
 ED_SU_avg_ED_LOS,
 IP_SU_avg_ED_count,
 IP_SU_avg_IP_count,
 IP_SU_avg_ED_LOS,
 ED_and_IP_SU_avg_ED_count,
 ED_and_IP_SU_avg_IP_count,
 ED_and_IP_SU_avg_ED_LOS,
 ED_SU_avg_ED_cost,
 IP_SU_avg_IP_cost,
 ED_and_IP_SU_avg_ED_and_IP_cost
)
AS
 SELECT pmp_su.pmp_su_id,
        pmp_su.begin_date,
        pmp_su.end_date,
        pmp_su.period,
        pmp_su.member_count,
        pmp_su.total_costs,
        pmp_su.total_costs_IP,
        pmp_su.total_costs_OP,
        pmp_su.total_costs_PB,
        pmp_su.total_costs_RX,
        pmp_su.pmp_costs,
        pmp_su.pmp_costs_IP,
        pmp_su.pmp_costs_OP,
        pmp_su.pmp_costs_PB,
        pmp_su.pmp_costs_RX,
        pmp_su.member_count_IP_stay,
        pmp_su.member_count_ED_visit,
        pmp_su.member_percent_IP_stay,
        pmp_su.member_percent_ED_visit,
        pmp_su.member_count_SU,
        pmp_su.member_count_SU_IP,
        pmp_su.member_count_SU_ED,
        pmp_su.member_count_SU_ED_and_IP,
        pmp_su.member_percent_SU,
        pmp_su.member_percent_SU_IP,
        pmp_su.member_percent_SU_ED,
        pmp_su.member_percent_SU_ED_and_IP,
        pmp_su.costs_ED_and_IP,
        pmp_su.costs_SU_ED_or_IP,
        pmp_su.costs_SU_ED,
        pmp_su.costs_SU_IP,
        pmp_su.costs_SU_ED_and_IP,
        pmp_su.percent_costs_SU_ED_or_IP,
        pmp_su.percent_ED_costs_SU_ED,
        pmp_su.percent_IP_costs_SU_IP,
        pmp_su.percent_costs_SU_ED_and_IP,
        pmp_su.ED_SU_avg_ED_count,
        pmp_su.ED_SU_avg_IP_count,
        pmp_su.ED_SU_avg_ED_LOS,
        pmp_su.IP_SU_avg_ED_count,
        pmp_su.IP_SU_avg_IP_count,
        pmp_su.IP_SU_avg_ED_LOS,
        pmp_su.ED_and_IP_SU_avg_ED_count,
        pmp_su.ED_and_IP_SU_avg_IP_count,
        pmp_su.ED_and_IP_SU_avg_ED_LOS,
        pmp_su.ED_SU_avg_ED_cost,
        pmp_su.IP_SU_avg_IP_cost,
        pmp_su.ED_and_IP_SU_avg_ED_and_IP_cost
 FROM epbuilder.pmp_su
 ORDER BY pmp_su.pmp_su_id,
          pmp_su.begin_date,
          pmp_su.end_date,
          pmp_su.period,
          pmp_su.member_count,
          pmp_su.total_costs,
          pmp_su.total_costs_IP,
          pmp_su.total_costs_OP,
          pmp_su.total_costs_PB,
          pmp_su.total_costs_RX,
          pmp_su.pmp_costs,
          pmp_su.pmp_costs_IP,
          pmp_su.pmp_costs_OP,
          pmp_su.pmp_costs_PB,
          pmp_su.pmp_costs_RX,
          pmp_su.member_count_IP_stay,
          pmp_su.member_count_ED_visit,
          pmp_su.member_percent_IP_stay,
          pmp_su.member_percent_ED_visit,
          pmp_su.member_count_SU,
          pmp_su.member_count_SU_IP,
          pmp_su.member_count_SU_ED,
          pmp_su.member_count_SU_ED_and_IP,
          pmp_su.member_percent_SU,
          pmp_su.member_percent_SU_IP,
          pmp_su.member_percent_SU_ED,
          pmp_su.member_percent_SU_ED_and_IP,
          pmp_su.costs_ED_and_IP,
          pmp_su.costs_SU_ED_or_IP,
          pmp_su.costs_SU_ED,
          pmp_su.costs_SU_IP,
          pmp_su.costs_SU_ED_and_IP,
          pmp_su.percent_costs_SU_ED_or_IP,
          pmp_su.percent_ED_costs_SU_ED,
          pmp_su.percent_IP_costs_SU_IP,
          pmp_su.percent_costs_SU_ED_and_IP,
          pmp_su.ED_SU_avg_ED_count,
          pmp_su.ED_SU_avg_IP_count,
          pmp_su.ED_SU_avg_ED_LOS,
          pmp_su.IP_SU_avg_ED_count,
          pmp_su.IP_SU_avg_IP_count,
          pmp_su.IP_SU_avg_ED_LOS,
          pmp_su.ED_and_IP_SU_avg_ED_count,
          pmp_su.ED_and_IP_SU_avg_IP_count,
          pmp_su.ED_and_IP_SU_avg_ED_LOS,
          pmp_su.ED_SU_avg_ED_cost,
          pmp_su.IP_SU_avg_IP_cost,
          pmp_su.ED_and_IP_SU_avg_ED_and_IP_cost
SEGMENTED BY hash(pmp_su.pmp_su_id, pmp_su.begin_date, pmp_su.end_date, pmp_su.member_count, pmp_su.member_count_IP_stay, pmp_su.member_count_ED_visit, pmp_su.member_percent_IP_stay, pmp_su.member_percent_ED_visit, pmp_su.member_count_SU, pmp_su.member_count_SU_IP, pmp_su.member_count_SU_ED, pmp_su.member_count_SU_ED_and_IP, pmp_su.member_percent_SU, pmp_su.member_percent_SU_IP, pmp_su.member_percent_SU_ED, pmp_su.member_percent_SU_ED_and_IP, pmp_su.percent_costs_SU_ED_or_IP, pmp_su.percent_ED_costs_SU_ED, pmp_su.percent_IP_costs_SU_IP, pmp_su.percent_costs_SU_ED_and_IP, pmp_su.ED_SU_avg_ED_count, pmp_su.ED_SU_avg_IP_count, pmp_su.ED_SU_avg_ED_LOS, pmp_su.IP_SU_avg_ED_count, pmp_su.IP_SU_avg_IP_count, pmp_su.IP_SU_avg_ED_LOS, pmp_su.ED_and_IP_SU_avg_ED_count, pmp_su.ED_and_IP_SU_avg_IP_count, pmp_su.ED_and_IP_SU_avg_ED_LOS, pmp_su.ED_SU_avg_ED_cost, pmp_su.IP_SU_avg_IP_cost, pmp_su.ED_and_IP_SU_avg_ED_and_IP_cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider /*+createtype(L)*/ 
(
 id,
 provider_id,
 npi,
 dea_no,
 group_name,
 practice_name,
 provider_name,
 system_name,
 tax_id,
 medicare_id,
 zipcode,
 pilot_name,
 aco_name,
 provider_type,
 provider_attribution_code,
 provider_aco,
 provider_health_system,
 provider_medical_group,
 facility_id,
 member_count,
 episode_count,
 severity_score,
 performance_score
)
AS
 SELECT provider.id,
        provider.provider_id,
        provider.npi,
        provider.dea_no,
        provider.group_name,
        provider.practice_name,
        provider.provider_name,
        provider.system_name,
        provider.tax_id,
        provider.medicare_id,
        provider.zipcode,
        provider.pilot_name,
        provider.aco_name,
        provider.provider_type,
        provider.provider_attribution_code,
        provider.provider_aco,
        provider.provider_health_system,
        provider.provider_medical_group,
        provider.facility_id,
        provider.member_count,
        provider.episode_count,
        provider.severity_score,
        provider.performance_score
 FROM epbuilder.provider
 ORDER BY provider.id,
          provider.provider_id,
          provider.npi,
          provider.dea_no,
          provider.group_name,
          provider.practice_name,
          provider.provider_name,
          provider.system_name,
          provider.tax_id,
          provider.medicare_id,
          provider.zipcode,
          provider.pilot_name,
          provider.aco_name,
          provider.provider_type,
          provider.provider_attribution_code,
          provider.provider_aco,
          provider.provider_health_system,
          provider.provider_medical_group,
          provider.facility_id,
          provider.member_count,
          provider.episode_count,
          provider.severity_score,
          provider.performance_score
SEGMENTED BY hash(provider.id, provider.member_count, provider.episode_count, provider.severity_score, provider.performance_score, provider.tax_id, provider.npi, provider.dea_no, provider.medicare_id, provider.provider_id, provider.provider_type, provider.provider_attribution_code, provider.provider_aco, provider.zipcode, provider.facility_id, provider.group_name, provider.practice_name, provider.provider_name, provider.system_name, provider.pilot_name, provider.aco_name, provider.provider_health_system, provider.provider_medical_group) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider_specialty /*+createtype(L)*/ 
(
 id,
 provider_uid,
 specialty_id,
 code_source
)
AS
 SELECT provider_specialty.id,
        provider_specialty.provider_uid,
        provider_specialty.specialty_id,
        provider_specialty.code_source
 FROM epbuilder.provider_specialty
 ORDER BY provider_specialty.id,
          provider_specialty.provider_uid,
          provider_specialty.specialty_id,
          provider_specialty.code_source
SEGMENTED BY hash(provider_specialty.id, provider_specialty.provider_uid, provider_specialty.code_source, provider_specialty.specialty_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member /*+createtype(L)*/ 
(
 id,
 member_id,
 gender_code,
 race_code,
 zip_code,
 birth_year,
 age,
 enforced_provider_id,
 primary_care_provider_id,
 primary_care_provider_npi,
 pcp_effective_date,
 date_of_death,
 insurance_type,
 insurance_carrier,
 dual_eligible,
 months_eligible_total,
 cost_of_care,
 unassigned_cost,
 assigned_cost,
 ed_visits,
 ed_cost,
 ip_stay_count,
 ip_stay_cost,
 bed_days,
 alos,
 claim_lines,
 claim_lines_t,
 claim_lines_tc,
 claim_lines_c,
 ip_readmit_count,
 ip_readmit_cost,
 designated_pcp,
 plan_id,
 rf_count,
 st_count
)
AS
 SELECT member.id,
        member.member_id,
        member.gender_code,
        member.race_code,
        member.zip_code,
        member.birth_year,
        member.age,
        member.enforced_provider_id,
        member.primary_care_provider_id,
        member.primary_care_provider_npi,
        member.pcp_effective_date,
        member.date_of_death,
        member.insurance_type,
        member.insurance_carrier,
        member.dual_eligible,
        member.months_eligible_total,
        member.cost_of_care,
        member.unassigned_cost,
        member.assigned_cost,
        member.ed_visits,
        member.ed_cost,
        member.ip_stay_count,
        member.ip_stay_cost,
        member.bed_days,
        member.alos,
        member.claim_lines,
        member.claim_lines_t,
        member.claim_lines_tc,
        member.claim_lines_c,
        member.ip_readmit_count,
        member.ip_readmit_cost,
        member.designated_pcp,
        member.plan_id,
        member.rf_count,
        member.st_count
 FROM epbuilder.member
 ORDER BY member.id,
          member.member_id,
          member.gender_code,
          member.race_code,
          member.zip_code,
          member.birth_year,
          member.age,
          member.enforced_provider_id,
          member.primary_care_provider_id,
          member.primary_care_provider_npi,
          member.pcp_effective_date,
          member.date_of_death,
          member.insurance_type,
          member.insurance_carrier,
          member.dual_eligible,
          member.months_eligible_total,
          member.cost_of_care,
          member.unassigned_cost,
          member.assigned_cost,
          member.ed_visits,
          member.ed_cost,
          member.ip_stay_count,
          member.ip_stay_cost,
          member.bed_days,
          member.alos,
          member.claim_lines,
          member.claim_lines_t,
          member.claim_lines_tc,
          member.claim_lines_c,
          member.ip_readmit_count,
          member.ip_readmit_cost,
          member.designated_pcp,
          member.plan_id,
          member.rf_count,
          member.st_count
SEGMENTED BY hash(member.id, member.gender_code, member.race_code, member.birth_year, member.age, member.pcp_effective_date, member.date_of_death, member.dual_eligible, member.months_eligible_total, member.ed_visits, member.ip_stay_count, member.bed_days, member.alos, member.claim_lines, member.claim_lines_t, member.claim_lines_tc, member.claim_lines_c, member.ip_readmit_count, member.rf_count, member.st_count, member.primary_care_provider_npi, member.zip_code, member.enforced_provider_id, member.primary_care_provider_id, member.insurance_type, member.insurance_carrier, member.cost_of_care, member.unassigned_cost, member.assigned_cost, member.ed_cost, member.ip_stay_cost, member.ip_readmit_cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.assignment /*+createtype(L)*/ 
(
 id,
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
)
AS
 SELECT assignment.id,
        assignment.member_id,
        assignment.master_claim_id,
        assignment.master_episode_id,
        assignment.claim_source,
        assignment.assigned_type,
        assignment.assigned_count,
        assignment.rule,
        assignment.pac,
        assignment.pac_type,
        assignment.episode_period,
        assignment.IP_period
 FROM epbuilder.assignment
 ORDER BY assignment.id,
          assignment.member_id,
          assignment.master_claim_id,
          assignment.master_episode_id,
          assignment.claim_source,
          assignment.assigned_type,
          assignment.assigned_count,
          assignment.rule,
          assignment.pac,
          assignment.pac_type,
          assignment.episode_period,
          assignment.IP_period
SEGMENTED BY hash(assignment.id, assignment.assigned_type, assignment.assigned_count, assignment.rule, assignment.pac, assignment.pac_type, assignment.episode_period, assignment.IP_period, assignment.claim_source, assignment.member_id, assignment.master_episode_id, assignment.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_master_epid_level /*+createtype(L)*/ 
(
 id,
 member_id,
 master_episode_id,
 level,
 split,
 annualized,
 cost,
 cost_t,
 cost_tc,
 cost_c
)
AS
 SELECT member_master_epid_level.id,
        member_master_epid_level.member_id,
        member_master_epid_level.master_episode_id,
        member_master_epid_level.level,
        member_master_epid_level.split,
        member_master_epid_level.annualized,
        member_master_epid_level.cost,
        member_master_epid_level.cost_t,
        member_master_epid_level.cost_tc,
        member_master_epid_level.cost_c
 FROM epbuilder.member_master_epid_level
 ORDER BY member_master_epid_level.id,
          member_master_epid_level.member_id,
          member_master_epid_level.master_episode_id,
          member_master_epid_level.level,
          member_master_epid_level.split,
          member_master_epid_level.annualized,
          member_master_epid_level.cost,
          member_master_epid_level.cost_t,
          member_master_epid_level.cost_tc,
          member_master_epid_level.cost_c
SEGMENTED BY hash(member_master_epid_level.id, member_master_epid_level.level, member_master_epid_level.split, member_master_epid_level.annualized, member_master_epid_level.cost, member_master_epid_level.cost_t, member_master_epid_level.cost_tc, member_master_epid_level.cost_c, member_master_epid_level.member_id, member_master_epid_level.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.pmp_su_metric_desc /*+createtype(L)*/ 
(
 pmp_su_metric_id,
 metric_name,
 metric_desc
)
AS
 SELECT pmp_su_metric_desc.pmp_su_metric_id,
        pmp_su_metric_desc.metric_name,
        pmp_su_metric_desc.metric_desc
 FROM epbuilder.pmp_su_metric_desc
 ORDER BY pmp_su_metric_desc.pmp_su_metric_id,
          pmp_su_metric_desc.metric_name,
          pmp_su_metric_desc.metric_desc
SEGMENTED BY hash(pmp_su_metric_desc.pmp_su_metric_id, pmp_su_metric_desc.metric_name, pmp_su_metric_desc.metric_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider_epid /*+createtype(L)*/ 
(
 id,
 provider_id,
 episode_id,
 member_count,
 episode_count,
 severity_score
)
AS
 SELECT provider_epid.id,
        provider_epid.provider_id,
        provider_epid.episode_id,
        provider_epid.member_count,
        provider_epid.episode_count,
        provider_epid.severity_score
 FROM epbuilder.provider_epid
 ORDER BY provider_epid.id,
          provider_epid.provider_id,
          provider_epid.episode_id,
          provider_epid.member_count,
          provider_epid.episode_count,
          provider_epid.severity_score
SEGMENTED BY hash(provider_epid.id, provider_epid.member_count, provider_epid.episode_count, provider_epid.severity_score, provider_epid.episode_id, provider_epid.provider_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.filtered_episodes /*+createtype(L)*/ 
(
 filter_id,
 master_episode_id,
 filter_fail,
 age_limit_lower,
 age_limit_upper,
 episode_cost_upper,
 episode_cost_lower,
 coverage_gap,
 episode_complete,
 drg,
 proc_ep_orphan,
 proc_ep_orph_triggered,
 dual_eligable
)
AS
 SELECT filtered_episodes.filter_id,
        filtered_episodes.master_episode_id,
        filtered_episodes.filter_fail,
        filtered_episodes.age_limit_lower,
        filtered_episodes.age_limit_upper,
        filtered_episodes.episode_cost_upper,
        filtered_episodes.episode_cost_lower,
        filtered_episodes.coverage_gap,
        filtered_episodes.episode_complete,
        filtered_episodes.drg,
        filtered_episodes.proc_ep_orphan,
        filtered_episodes.proc_ep_orph_triggered,
        filtered_episodes.dual_eligable
 FROM epbuilder.filtered_episodes
 ORDER BY filtered_episodes.filter_id,
          filtered_episodes.master_episode_id,
          filtered_episodes.filter_fail,
          filtered_episodes.age_limit_lower,
          filtered_episodes.age_limit_upper,
          filtered_episodes.episode_cost_upper,
          filtered_episodes.episode_cost_lower,
          filtered_episodes.coverage_gap,
          filtered_episodes.episode_complete,
          filtered_episodes.drg,
          filtered_episodes.proc_ep_orphan,
          filtered_episodes.proc_ep_orph_triggered
SEGMENTED BY hash(filtered_episodes.filter_id, filtered_episodes.filter_fail, filtered_episodes.age_limit_lower, filtered_episodes.age_limit_upper, filtered_episodes.episode_cost_upper, filtered_episodes.episode_cost_lower, filtered_episodes.coverage_gap, filtered_episodes.episode_complete, filtered_episodes.drg, filtered_episodes.proc_ep_orphan, filtered_episodes.proc_ep_orph_triggered, filtered_episodes.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.association /*+createtype(L)*/ 
(
 id,
 parent_master_episode_id,
 child_master_episode_id,
 association_type,
 association_level,
 association_count,
 association_start_day,
 association_end_day
)
AS
 SELECT association.id,
        association.parent_master_episode_id,
        association.child_master_episode_id,
        association.association_type,
        association.association_level,
        association.association_count,
        association.association_start_day,
        association.association_end_day
 FROM epbuilder.association
 ORDER BY association.id,
          association.parent_master_episode_id,
          association.child_master_episode_id,
          association.association_type,
          association.association_level,
          association.association_count,
          association.association_start_day,
          association.association_end_day
SEGMENTED BY hash(association.id, association.association_level, association.association_count, association.association_start_day, association.association_end_day, association.association_type, association.parent_master_episode_id, association.child_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.master_epid_code /*+createtype(L)*/ 
(
 id,
 master_episode_id,
 master_claim_id,
 code_value,
 nomen,
 pas,
 core_service,
 pac,
 pac_type,
 risk_factor,
 sub_type,
 betos
)
AS
 SELECT master_epid_code.id,
        master_epid_code.master_episode_id,
        master_epid_code.master_claim_id,
        master_epid_code.code_value,
        master_epid_code.nomen,
        master_epid_code.pas,
        master_epid_code.core_service,
        master_epid_code.pac,
        master_epid_code.pac_type,
        master_epid_code.risk_factor,
        master_epid_code.sub_type,
        master_epid_code.betos
 FROM epbuilder.master_epid_code
 ORDER BY master_epid_code.id,
          master_epid_code.master_episode_id,
          master_epid_code.master_claim_id,
          master_epid_code.code_value,
          master_epid_code.nomen,
          master_epid_code.pas,
          master_epid_code.core_service,
          master_epid_code.pac,
          master_epid_code.pac_type,
          master_epid_code.risk_factor,
          master_epid_code.sub_type,
          master_epid_code.betos
SEGMENTED BY hash(master_epid_code.id, master_epid_code.nomen, master_epid_code.pas, master_epid_code.core_service, master_epid_code.pac, master_epid_code.pac_type, master_epid_code.code_value, master_epid_code.risk_factor, master_epid_code.sub_type, master_epid_code.betos, master_epid_code.master_episode_id, master_epid_code.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ndc_to_multum /*+createtype(L)*/ 
(
 id,
 multum,
 ndc,
 drug_category
)
AS
 SELECT ndc_to_multum.id,
        ndc_to_multum.multum,
        ndc_to_multum.ndc,
        ndc_to_multum.drug_category
 FROM epbuilder.ndc_to_multum
 ORDER BY ndc_to_multum.id,
          ndc_to_multum.multum,
          ndc_to_multum.ndc,
          ndc_to_multum.drug_category
SEGMENTED BY hash(ndc_to_multum.id, ndc_to_multum.multum, ndc_to_multum.ndc, ndc_to_multum.drug_category) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.pmp_su_ranks /*+createtype(L)*/ 
(
 id,
 pmp_su_id,
 pmp_su_metric_id,
 rank,
 count,
 cost
)
AS
 SELECT pmp_su_ranks.id,
        pmp_su_ranks.pmp_su_id,
        pmp_su_ranks.pmp_su_metric_id,
        pmp_su_ranks.rank,
        pmp_su_ranks.count,
        pmp_su_ranks.cost
 FROM epbuilder.pmp_su_ranks
 ORDER BY pmp_su_ranks.id,
          pmp_su_ranks.pmp_su_id,
          pmp_su_ranks.pmp_su_metric_id,
          pmp_su_ranks.rank,
          pmp_su_ranks.count,
          pmp_su_ranks.cost
SEGMENTED BY hash(pmp_su_ranks.id, pmp_su_ranks.pmp_su_id, pmp_su_ranks.pmp_su_metric_id, pmp_su_ranks.rank, pmp_su_ranks.count, pmp_su_ranks.cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider_epid_level /*+createtype(L)*/ 
(
 id,
 provider_id,
 episode_id,
 level,
 split,
 annualized,
 cost,
 cost_t,
 cost_tc,
 cost_c
)
AS
 SELECT provider_epid_level.id,
        provider_epid_level.provider_id,
        provider_epid_level.episode_id,
        provider_epid_level.level,
        provider_epid_level.split,
        provider_epid_level.annualized,
        provider_epid_level.cost,
        provider_epid_level.cost_t,
        provider_epid_level.cost_tc,
        provider_epid_level.cost_c
 FROM epbuilder.provider_epid_level
 ORDER BY provider_epid_level.id,
          provider_epid_level.provider_id,
          provider_epid_level.episode_id,
          provider_epid_level.level,
          provider_epid_level.split,
          provider_epid_level.annualized,
          provider_epid_level.cost,
          provider_epid_level.cost_t,
          provider_epid_level.cost_tc,
          provider_epid_level.cost_c
SEGMENTED BY hash(provider_epid_level.id, provider_epid_level.level, provider_epid_level.split, provider_epid_level.annualized, provider_epid_level.episode_id, provider_epid_level.cost, provider_epid_level.cost_t, provider_epid_level.cost_tc, provider_epid_level.cost_c, provider_epid_level.provider_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.triggers /*+createtype(L)*/ 
(
 id,
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
AS
 SELECT triggers.id,
        triggers.member_id,
        triggers.claim_id,
        triggers.claim_line_id,
        triggers.master_episode_id,
        triggers.master_claim_id,
        triggers.episode_id,
        triggers.episode_type,
        triggers.trig_begin_date,
        triggers.trig_end_date,
        triggers.episode_begin_date,
        triggers.episode_end_date,
        triggers.orig_episode_begin_date,
        triggers.orig_episode_end_date,
        triggers.look_back,
        triggers.look_ahead,
        triggers.req_conf_claim,
        triggers.conf_claim_id,
        triggers.conf_claim_line_id,
        triggers.min_code_separation,
        triggers.max_code_separation,
        triggers.trig_by_episode_id,
        triggers.trig_by_master_episode_id,
        triggers.dx_pass_code,
        triggers.px_pass_code,
        triggers.em_pass_code,
        triggers.conf_dx_pass_code,
        triggers.conf_px_pass_code,
        triggers.conf_em_pass_code,
        triggers.dropped,
        triggers.truncated,
        triggers.win_claim_id,
        triggers.win_master_claim_id
 FROM epbuilder.triggers
 ORDER BY triggers.id,
          triggers.member_id,
          triggers.claim_id,
          triggers.claim_line_id,
          triggers.master_episode_id,
          triggers.master_claim_id,
          triggers.episode_id,
          triggers.episode_type,
          triggers.trig_begin_date,
          triggers.trig_end_date,
          triggers.episode_begin_date,
          triggers.episode_end_date,
          triggers.orig_episode_begin_date,
          triggers.orig_episode_end_date,
          triggers.look_back,
          triggers.look_ahead,
          triggers.req_conf_claim,
          triggers.conf_claim_id,
          triggers.conf_claim_line_id,
          triggers.min_code_separation,
          triggers.max_code_separation,
          triggers.trig_by_episode_id,
          triggers.trig_by_master_episode_id,
          triggers.dx_pass_code,
          triggers.px_pass_code,
          triggers.em_pass_code,
          triggers.conf_dx_pass_code,
          triggers.conf_px_pass_code,
          triggers.conf_em_pass_code,
          triggers.dropped,
          triggers.truncated,
          triggers.win_claim_id,
          triggers.win_master_claim_id
SEGMENTED BY hash(triggers.id, triggers.episode_type, triggers.trig_begin_date, triggers.trig_end_date, triggers.episode_begin_date, triggers.episode_end_date, triggers.orig_episode_begin_date, triggers.orig_episode_end_date, triggers.look_back, triggers.look_ahead, triggers.req_conf_claim, triggers.min_code_separation, triggers.max_code_separation, triggers.dropped, triggers.truncated, triggers.episode_id, triggers.conf_claim_line_id, triggers.trig_by_episode_id, triggers.dx_pass_code, triggers.px_pass_code, triggers.em_pass_code, triggers.conf_dx_pass_code, triggers.conf_px_pass_code, triggers.conf_em_pass_code, triggers.claim_line_id, triggers.member_id, triggers.claim_id, triggers.conf_claim_id, triggers.win_claim_id, triggers.master_episode_id, triggers.trig_by_master_episode_id, triggers.win_master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider_master_epid_level /*+createtype(L)*/ 
(
 id,
 provider_id,
 master_episode_id,
 level,
 split,
 annualized,
 cost,
 cost_t,
 cost_tc,
 cost_c
)
AS
 SELECT provider_master_epid_level.id,
        provider_master_epid_level.provider_id,
        provider_master_epid_level.master_episode_id,
        provider_master_epid_level.level,
        provider_master_epid_level.split,
        provider_master_epid_level.annualized,
        provider_master_epid_level.cost,
        provider_master_epid_level.cost_t,
        provider_master_epid_level.cost_tc,
        provider_master_epid_level.cost_c
 FROM epbuilder.provider_master_epid_level
 ORDER BY provider_master_epid_level.id,
          provider_master_epid_level.provider_id,
          provider_master_epid_level.master_episode_id,
          provider_master_epid_level.level,
          provider_master_epid_level.split,
          provider_master_epid_level.annualized,
          provider_master_epid_level.cost,
          provider_master_epid_level.cost_t,
          provider_master_epid_level.cost_tc,
          provider_master_epid_level.cost_c
SEGMENTED BY hash(provider_master_epid_level.id, provider_master_epid_level.level, provider_master_epid_level.split, provider_master_epid_level.annualized, provider_master_epid_level.provider_id, provider_master_epid_level.cost, provider_master_epid_level.cost_t, provider_master_epid_level.cost_tc, provider_master_epid_level.cost_c, provider_master_epid_level.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.unassigned_episodes /*+createtype(L)*/ 
(
 member_id,
 episode_id,
 episode_begin_date,
 cost
)
AS
 SELECT unassigned_episodes.member_id,
        unassigned_episodes.episode_id,
        unassigned_episodes.episode_begin_date,
        unassigned_episodes.cost
 FROM epbuilder.unassigned_episodes
 ORDER BY unassigned_episodes.member_id
SEGMENTED BY hash(unassigned_episodes.episode_begin_date, unassigned_episodes.episode_id, unassigned_episodes.cost, unassigned_episodes.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_uber /*+createtype(L)*/ 
(
 id,
 parent_master_episode_id,
 child_master_episode_id,
 association_type,
 association_level,
 association_count,
 association_start_day,
 association_end_day
)
AS
 SELECT tmp_uber.id,
        tmp_uber.parent_master_episode_id,
        tmp_uber.child_master_episode_id,
        tmp_uber.association_type,
        tmp_uber.association_level,
        tmp_uber.association_count,
        tmp_uber.association_start_day,
        tmp_uber.association_end_day
 FROM epbuilder.tmp_uber
 ORDER BY tmp_uber.id,
          tmp_uber.parent_master_episode_id,
          tmp_uber.child_master_episode_id,
          tmp_uber.association_type,
          tmp_uber.association_count,
          tmp_uber.association_start_day,
          tmp_uber.association_end_day
SEGMENTED BY hash(tmp_uber.id, tmp_uber.association_level, tmp_uber.association_count, tmp_uber.association_start_day, tmp_uber.association_end_day, tmp_uber.association_type, tmp_uber.parent_master_episode_id, tmp_uber.child_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_sub_association /*+createtype(L)*/ 
(
 uber_master_episode_id,
 parent_master_episode_id,
 child_master_episode_id,
 association_type,
 association_level,
 association_count,
 association_sub_level,
 cost,
 cost_t,
 cost_tc,
 cost_c,
 association_sub_level_inv
)
AS
 SELECT tmp_sub_association.uber_master_episode_id,
        tmp_sub_association.parent_master_episode_id,
        tmp_sub_association.child_master_episode_id,
        tmp_sub_association.association_type,
        tmp_sub_association.association_level,
        tmp_sub_association.association_count,
        tmp_sub_association.association_sub_level,
        tmp_sub_association.cost,
        tmp_sub_association.cost_t,
        tmp_sub_association.cost_tc,
        tmp_sub_association.cost_c,
        tmp_sub_association.association_sub_level_inv
 FROM epbuilder.tmp_sub_association
 ORDER BY tmp_sub_association.uber_master_episode_id,
          tmp_sub_association.parent_master_episode_id,
          tmp_sub_association.child_master_episode_id,
          tmp_sub_association.association_type,
          tmp_sub_association.association_level,
          tmp_sub_association.association_count,
          tmp_sub_association.association_sub_level,
          tmp_sub_association.cost,
          tmp_sub_association.cost_t,
          tmp_sub_association.cost_tc,
          tmp_sub_association.cost_c,
          tmp_sub_association.association_sub_level_inv
SEGMENTED BY hash(tmp_sub_association.association_level, tmp_sub_association.association_count, tmp_sub_association.association_sub_level, tmp_sub_association.association_sub_level_inv, tmp_sub_association.association_type, tmp_sub_association.cost, tmp_sub_association.cost_t, tmp_sub_association.cost_tc, tmp_sub_association.cost_c, tmp_sub_association.uber_master_episode_id, tmp_sub_association.parent_master_episode_id, tmp_sub_association.child_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_mel1 /*+createtype(L)*/ 
(
 master_episode_id,
 cost,
 cost_t,
 cost_tc,
 cost_c
)
AS
 SELECT tmp_mel1.master_episode_id,
        tmp_mel1.cost,
        tmp_mel1.cost_t,
        tmp_mel1.cost_tc,
        tmp_mel1.cost_c
 FROM epbuilder.tmp_mel1
 ORDER BY tmp_mel1.master_episode_id,
          tmp_mel1.cost,
          tmp_mel1.cost_t,
          tmp_mel1.cost_tc,
          tmp_mel1.cost_c
SEGMENTED BY hash(tmp_mel1.cost, tmp_mel1.cost_t, tmp_mel1.cost_tc, tmp_mel1.cost_c, tmp_mel1.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_enroll_gap /*+createtype(L)*/ 
(
 episode_begin_date,
 episode_end_date,
 gap_begin_date,
 gap_end_date,
 episode_length,
 gap_length,
 master_episode_id,
 member_id,
 gap_length_in_episode,
 filter_fail,
 data_latest_begin_date,
 annualize_begin_date,
 data_start_date,
 end_of_study,
 annualizable
)
AS
 SELECT tmp_enroll_gap.episode_begin_date,
        tmp_enroll_gap.episode_end_date,
        tmp_enroll_gap.gap_begin_date,
        tmp_enroll_gap.gap_end_date,
        tmp_enroll_gap.episode_length,
        tmp_enroll_gap.gap_length,
        tmp_enroll_gap.master_episode_id,
        tmp_enroll_gap.member_id,
        tmp_enroll_gap.gap_length_in_episode,
        tmp_enroll_gap.filter_fail,
        tmp_enroll_gap.data_latest_begin_date,
        tmp_enroll_gap.annualize_begin_date,
        tmp_enroll_gap.data_start_date,
        tmp_enroll_gap.end_of_study,
        tmp_enroll_gap.annualizable
 FROM epbuilder.tmp_enroll_gap
 ORDER BY tmp_enroll_gap.episode_begin_date,
          tmp_enroll_gap.episode_end_date,
          tmp_enroll_gap.episode_length,
          tmp_enroll_gap.master_episode_id,
          tmp_enroll_gap.member_id
SEGMENTED BY hash(tmp_enroll_gap.episode_begin_date, tmp_enroll_gap.episode_end_date, tmp_enroll_gap.episode_length, tmp_enroll_gap.member_id, tmp_enroll_gap.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_proc_orph /*+createtype(L)*/ 
(
 master_episode_id,
 master_claim_id,
 claim_line_type_code,
 begin_date,
 end_date,
 confirmed
)
AS
 SELECT tmp_proc_orph.master_episode_id,
        tmp_proc_orph.master_claim_id,
        tmp_proc_orph.claim_line_type_code,
        tmp_proc_orph.begin_date,
        tmp_proc_orph.end_date,
        tmp_proc_orph.confirmed
 FROM epbuilder.tmp_proc_orph
 ORDER BY tmp_proc_orph.master_episode_id,
          tmp_proc_orph.master_claim_id,
          tmp_proc_orph.claim_line_type_code,
          tmp_proc_orph.begin_date,
          tmp_proc_orph.end_date
SEGMENTED BY hash(tmp_proc_orph.master_episode_id, tmp_proc_orph.master_claim_id, tmp_proc_orph.claim_line_type_code, tmp_proc_orph.begin_date, tmp_proc_orph.end_date) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_filt_proc_orp_trig /*+createtype(L)*/ 
(
 master_episode_id
)
AS
 SELECT tmp_filt_proc_orp_trig.master_episode_id
 FROM epbuilder.tmp_filt_proc_orp_trig
 ORDER BY tmp_filt_proc_orp_trig.master_episode_id
SEGMENTED BY hash(tmp_filt_proc_orp_trig.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_hlow /*+createtype(L)*/ 
(
 episode_id,
 low,
 high
)
AS
 SELECT tmp_hlow.episode_id,
        tmp_hlow.low,
        tmp_hlow.high
 FROM epbuilder.tmp_hlow
 ORDER BY tmp_hlow.episode_id,
          tmp_hlow.low,
          tmp_hlow.high
SEGMENTED BY hash(tmp_hlow.episode_id, tmp_hlow.low, tmp_hlow.high) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_annids /*+createtype(L)*/ 
(
 master_episode_id,
 parent_child_id
)
AS
 SELECT tmp_annids.master_episode_id,
        tmp_annids.parent_child_id
 FROM epbuilder.tmp_annids
 ORDER BY tmp_annids.master_episode_id,
          tmp_annids.parent_child_id
SEGMENTED BY hash(tmp_annids.parent_child_id, tmp_annids.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_annids_c /*+createtype(L)*/ 
(
 master_episode_id,
 lvl
)
AS
 SELECT tmp_annids_c.master_episode_id,
        tmp_annids_c.lvl
 FROM epbuilder.tmp_annids_c
 ORDER BY tmp_annids_c.master_episode_id,
          tmp_annids_c.lvl
SEGMENTED BY hash(tmp_annids_c.lvl, tmp_annids_c.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.build_episode_reference /*+createtype(L)*/ 
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
 FROM epbuilder.build_episode_reference
 ORDER BY build_episode_reference.EPISODE_ID
SEGMENTED BY hash(build_episode_reference.EPISODE_ID, build_episode_reference.NAME, build_episode_reference.CREATED_DATE, build_episode_reference.MODIFIED_DATE, build_episode_reference.MDC_CATEGORY, build_episode_reference.PARM_SET, build_episode_reference.TRIGGER_TYPE, build_episode_reference.TRIGGER_NUMBER, build_episode_reference.SEPARATION_MIN, build_episode_reference.SEPARATION_MAX, build_episode_reference.BOUND_OFFSET, build_episode_reference.BOUND_LENGTH, build_episode_reference.CONDITION_MIN, build_episode_reference.END_OF_STUDY, build_episode_reference.DESCRIPTION, build_episode_reference.TYPE, build_episode_reference.STATUS, build_episode_reference.USERS_USER_ID, build_episode_reference.VERSION) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.filter_params /*+createtype(L)*/ 
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
 FROM epbuilder.filter_params
 ORDER BY filter_params.filter_id,
          filter_params.episode_id,
          filter_params.lower_age_limit,
          filter_params.upper_age_limit
SEGMENTED BY hash(filter_params.filter_id, filter_params.lower_age_limit, filter_params.upper_age_limit, filter_params.episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.filters /*+createtype(L)*/ 
(
 filter_id,
 fiilter_name
)
AS
 SELECT filters.filter_id,
        filters.fiilter_name
 FROM epbuilder.filters
 ORDER BY filters.filter_id
SEGMENTED BY hash(filters.filter_id, filters.fiilter_name) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider_attribution /*+createtype(L)*/ 
(
 id,
 master_episode_id,
 master_claim_id,
 episode_id,
 episode_type,
 claim_line_type_code,
 trig_begin_date,
 trig_end_date,
 triggering_physician,
 triggering_facility,
 trig_claim_attr_phys,
 trig_claim_attr_fac,
 em_count_attr,
 em_cost_attr
)
AS
 SELECT provider_attribution.id,
        provider_attribution.master_episode_id,
        provider_attribution.master_claim_id,
        provider_attribution.episode_id,
        provider_attribution.episode_type,
        provider_attribution.claim_line_type_code,
        provider_attribution.trig_begin_date,
        provider_attribution.trig_end_date,
        provider_attribution.triggering_physician,
        provider_attribution.triggering_facility,
        provider_attribution.trig_claim_attr_phys,
        provider_attribution.trig_claim_attr_fac,
        provider_attribution.em_count_attr,
        provider_attribution.em_cost_attr
 FROM epbuilder.provider_attribution
 ORDER BY provider_attribution.id,
          provider_attribution.master_episode_id,
          provider_attribution.master_claim_id,
          provider_attribution.episode_id,
          provider_attribution.episode_type,
          provider_attribution.claim_line_type_code,
          provider_attribution.trig_begin_date,
          provider_attribution.trig_end_date,
          provider_attribution.triggering_physician,
          provider_attribution.triggering_facility,
          provider_attribution.trig_claim_attr_phys,
          provider_attribution.trig_claim_attr_fac,
          provider_attribution.em_count_attr,
          provider_attribution.em_cost_attr
SEGMENTED BY hash(provider_attribution.id, provider_attribution.episode_type, provider_attribution.trig_begin_date, provider_attribution.trig_end_date, provider_attribution.episode_id, provider_attribution.claim_line_type_code, provider_attribution.triggering_physician, provider_attribution.triggering_facility, provider_attribution.trig_claim_attr_phys, provider_attribution.trig_claim_attr_fac, provider_attribution.em_count_attr, provider_attribution.em_cost_attr, provider_attribution.master_episode_id, provider_attribution.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.Assign_PAC_Totals /*+createtype(A)*/ 
(
 EPISODE,
 Level_Assignment,
 IP_PAC_Count,
 OP_PAC_Count,
 PB_PAC_Count,
 RX_PAC_Count
)
AS
 SELECT Assign_PAC_Totals.EPISODE,
        Assign_PAC_Totals.Level_Assignment,
        Assign_PAC_Totals.IP_PAC_Count,
        Assign_PAC_Totals.OP_PAC_Count,
        Assign_PAC_Totals.PB_PAC_Count,
        Assign_PAC_Totals.RX_PAC_Count
 FROM epbuilder.Assign_PAC_Totals
 ORDER BY Assign_PAC_Totals.EPISODE,
          Assign_PAC_Totals.Level_Assignment,
          Assign_PAC_Totals.IP_PAC_Count,
          Assign_PAC_Totals.OP_PAC_Count,
          Assign_PAC_Totals.PB_PAC_Count,
          Assign_PAC_Totals.RX_PAC_Count
SEGMENTED BY hash(Assign_PAC_Totals.EPISODE) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_episode_data /*+createtype(L)*/ 
(
 epi_id,
 member_id,
 epi_number,
 epi_name,
 female,
 age,
 rec_enr,
 eol_ind
)
AS
 SELECT ra_episode_data.epi_id,
        ra_episode_data.member_id,
        ra_episode_data.epi_number,
        ra_episode_data.epi_name,
        ra_episode_data.female,
        ra_episode_data.age,
        ra_episode_data.rec_enr,
        ra_episode_data.eol_ind
 FROM epbuilder.ra_episode_data
 ORDER BY ra_episode_data.epi_id,
          ra_episode_data.member_id
SEGMENTED BY hash(ra_episode_data.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrollment /*+createtype(L)*/ 
(
 id,
 member_id,
 begin_date,
 end_date,
 age_at_enrollment,
 insurance_product,
 coverage_type,
 isGap
)
AS
 SELECT enrollment.id,
        enrollment.member_id,
        enrollment.begin_date,
        enrollment.end_date,
        enrollment.age_at_enrollment,
        enrollment.insurance_product,
        enrollment.coverage_type,
        enrollment.isGap
 FROM epbuilder.enrollment
 ORDER BY enrollment.id,
          enrollment.member_id,
          enrollment.begin_date,
          enrollment.end_date,
          enrollment.age_at_enrollment,
          enrollment.insurance_product,
          enrollment.coverage_type,
          enrollment.isGap
SEGMENTED BY hash(enrollment.id, enrollment.begin_date, enrollment.end_date, enrollment.age_at_enrollment, enrollment.isGap, enrollment.insurance_product, enrollment.coverage_type, enrollment.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_enroll /*+createtype(L)*/ 
(
 member_id,
 begin_date
)
AS
 SELECT tmp_enroll.member_id,
        tmp_enroll.begin_date
 FROM epbuilder.tmp_enroll
 ORDER BY tmp_enroll.member_id,
          tmp_enroll.begin_date
SEGMENTED BY hash(tmp_enroll.begin_date, tmp_enroll.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.risk_factors /*+createtype(A)*/ 
(
 dx,
 codename,
 factor_id,
 factorname
)
AS
 SELECT risk_factors.dx,
        risk_factors.codename,
        risk_factors.factor_id,
        risk_factors.factorname
 FROM epbuilder.risk_factors
 ORDER BY risk_factors.dx,
          risk_factors.codename,
          risk_factors.factor_id,
          risk_factors.factorname
SEGMENTED BY hash(risk_factors.dx, risk_factors.factor_id, risk_factors.codename, risk_factors.factorname) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.global_risk_factors /*+createtype(L)*/ 
(
 member_id,
 master_claim_id,
 claim_date,
 code_value,
 factor_id
)
AS
 SELECT global_risk_factors.member_id,
        global_risk_factors.master_claim_id,
        global_risk_factors.claim_date,
        global_risk_factors.code_value,
        global_risk_factors.factor_id
 FROM epbuilder.global_risk_factors
 ORDER BY global_risk_factors.member_id,
          global_risk_factors.master_claim_id,
          global_risk_factors.claim_date,
          global_risk_factors.code_value,
          global_risk_factors.factor_id
SEGMENTED BY hash(global_risk_factors.claim_date, global_risk_factors.code_value, global_risk_factors.factor_id, global_risk_factors.master_claim_id, global_risk_factors.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.sub_type_codes /*+createtype(A)*/ 
(
 EPISODE_ID,
 EPISODE_NAME,
 CODE_VALUE,
 TYPE_ID,
 GROUP_ID,
 GROUP_NAME,
 SUBTYPE_GROUP_ID
)
AS
 SELECT sub_type_codes.EPISODE_ID,
        sub_type_codes.EPISODE_NAME,
        sub_type_codes.CODE_VALUE,
        sub_type_codes.TYPE_ID,
        sub_type_codes.GROUP_ID,
        sub_type_codes.GROUP_NAME,
        sub_type_codes.SUBTYPE_GROUP_ID
 FROM epbuilder.sub_type_codes
 ORDER BY sub_type_codes.EPISODE_ID,
          sub_type_codes.EPISODE_NAME,
          sub_type_codes.CODE_VALUE,
          sub_type_codes.TYPE_ID,
          sub_type_codes.GROUP_ID,
          sub_type_codes.GROUP_NAME,
          sub_type_codes.SUBTYPE_GROUP_ID
SEGMENTED BY hash(sub_type_codes.TYPE_ID, sub_type_codes.EPISODE_ID, sub_type_codes.GROUP_ID, sub_type_codes.CODE_VALUE, sub_type_codes.SUBTYPE_GROUP_ID, sub_type_codes.EPISODE_NAME, sub_type_codes.GROUP_NAME) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_riskfactors /*+createtype(L)*/ 
(
 epi_id,
 epi_number,
 name,
 value
)
AS
 SELECT ra_riskfactors.epi_id,
        ra_riskfactors.epi_number,
        ra_riskfactors.name,
        ra_riskfactors.value
 FROM epbuilder.ra_riskfactors
 ORDER BY ra_riskfactors.epi_id,
          ra_riskfactors.epi_number,
          ra_riskfactors.name,
          ra_riskfactors.value
SEGMENTED BY hash(ra_riskfactors.epi_number, ra_riskfactors.value, ra_riskfactors.name, ra_riskfactors.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_subt_claims /*+createtype(L)*/ 
(
 master_claim_id,
 id,
 epi,
 trig,
 begin_date,
 master_episode_id,
 episode_id
)
AS
 SELECT tmp_subt_claims.master_claim_id,
        tmp_subt_claims.id,
        tmp_subt_claims.epi,
        tmp_subt_claims.trig,
        tmp_subt_claims.begin_date,
        tmp_subt_claims.master_episode_id,
        tmp_subt_claims.episode_id
 FROM epbuilder.tmp_subt_claims
 ORDER BY tmp_subt_claims.master_claim_id,
          tmp_subt_claims.id,
          tmp_subt_claims.epi,
          tmp_subt_claims.trig,
          tmp_subt_claims.begin_date,
          tmp_subt_claims.master_episode_id,
          tmp_subt_claims.episode_id
SEGMENTED BY hash(tmp_subt_claims.id, tmp_subt_claims.begin_date, tmp_subt_claims.epi, tmp_subt_claims.trig, tmp_subt_claims.episode_id, tmp_subt_claims.master_episode_id, tmp_subt_claims.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.mom_baby /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 year
)
AS
 SELECT mom_baby.ENCRYPT_RECIP_ID_MOM,
        mom_baby.ENCRYPT_RECIP_ID_BABY,
        mom_baby.year
 FROM epbuilder.mom_baby
 ORDER BY mom_baby.ENCRYPT_RECIP_ID_MOM,
          mom_baby.ENCRYPT_RECIP_ID_BABY
SEGMENTED BY hash(mom_baby.ENCRYPT_RECIP_ID_MOM, mom_baby.ENCRYPT_RECIP_ID_BABY) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.vbp_ppe_ppa /*+createtype(A)*/ 
(
 claim_key,
 claim_id,
 claim_trans_id,
 dsch_dt,
 claim_type_code,
 preventable_group,
 pfp_preventable_group_name,
 pfp_preventable_group_desc,
 preventable_status,
 pfp_preventable_status_desc,
 preventable_reason,
 pfp_preventable_reason_desc,
 mdw_insert_dt,
 mdw_update_dt
)
AS
 SELECT vbp_ppe_ppa.claim_key,
        vbp_ppe_ppa.claim_id,
        vbp_ppe_ppa.claim_trans_id,
        vbp_ppe_ppa.dsch_dt,
        vbp_ppe_ppa.claim_type_code,
        vbp_ppe_ppa.preventable_group,
        vbp_ppe_ppa.pfp_preventable_group_name,
        vbp_ppe_ppa.pfp_preventable_group_desc,
        vbp_ppe_ppa.preventable_status,
        vbp_ppe_ppa.pfp_preventable_status_desc,
        vbp_ppe_ppa.preventable_reason,
        vbp_ppe_ppa.pfp_preventable_reason_desc,
        vbp_ppe_ppa.mdw_insert_dt,
        vbp_ppe_ppa.mdw_update_dt
 FROM epbuilder.vbp_ppe_ppa
 ORDER BY vbp_ppe_ppa.claim_key,
          vbp_ppe_ppa.claim_id,
          vbp_ppe_ppa.claim_trans_id,
          vbp_ppe_ppa.dsch_dt,
          vbp_ppe_ppa.claim_type_code,
          vbp_ppe_ppa.preventable_group,
          vbp_ppe_ppa.pfp_preventable_group_name,
          vbp_ppe_ppa.pfp_preventable_group_desc,
          vbp_ppe_ppa.preventable_status,
          vbp_ppe_ppa.pfp_preventable_status_desc,
          vbp_ppe_ppa.preventable_reason,
          vbp_ppe_ppa.pfp_preventable_reason_desc,
          vbp_ppe_ppa.mdw_insert_dt,
          vbp_ppe_ppa.mdw_update_dt
SEGMENTED BY hash(vbp_ppe_ppa.dsch_dt, vbp_ppe_ppa.claim_type_code, vbp_ppe_ppa.preventable_group, vbp_ppe_ppa.preventable_status, vbp_ppe_ppa.preventable_reason, vbp_ppe_ppa.mdw_insert_dt, vbp_ppe_ppa.mdw_update_dt, vbp_ppe_ppa.claim_key, vbp_ppe_ppa.claim_trans_id, vbp_ppe_ppa.claim_id, vbp_ppe_ppa.pfp_preventable_group_name, vbp_ppe_ppa.pfp_preventable_group_desc, vbp_ppe_ppa.pfp_preventable_status_desc, vbp_ppe_ppa.pfp_preventable_reason_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.vbp_ppe_ppr /*+createtype(A)*/ 
(
 claim_id,
 claim_trans_id,
 dsch_dt,
 claim_type_code,
 preventable_group,
 preventable_group_desc,
 ppr_type_code,
 ppr_type_code_desc,
 mdw_insert_dt,
 mdw_update_dt
)
AS
 SELECT vbp_ppe_ppr.claim_id,
        vbp_ppe_ppr.claim_trans_id,
        vbp_ppe_ppr.dsch_dt,
        vbp_ppe_ppr.claim_type_code,
        vbp_ppe_ppr.preventable_group,
        vbp_ppe_ppr.preventable_group_desc,
        vbp_ppe_ppr.ppr_type_code,
        vbp_ppe_ppr.ppr_type_code_desc,
        vbp_ppe_ppr.mdw_insert_dt,
        vbp_ppe_ppr.mdw_update_dt
 FROM epbuilder.vbp_ppe_ppr
 ORDER BY vbp_ppe_ppr.claim_id,
          vbp_ppe_ppr.claim_trans_id,
          vbp_ppe_ppr.dsch_dt,
          vbp_ppe_ppr.claim_type_code,
          vbp_ppe_ppr.preventable_group,
          vbp_ppe_ppr.preventable_group_desc,
          vbp_ppe_ppr.ppr_type_code,
          vbp_ppe_ppr.ppr_type_code_desc,
          vbp_ppe_ppr.mdw_insert_dt,
          vbp_ppe_ppr.mdw_update_dt
SEGMENTED BY hash(vbp_ppe_ppr.dsch_dt, vbp_ppe_ppr.claim_type_code, vbp_ppe_ppr.preventable_group, vbp_ppe_ppr.ppr_type_code, vbp_ppe_ppr.mdw_insert_dt, vbp_ppe_ppr.mdw_update_dt, vbp_ppe_ppr.claim_trans_id, vbp_ppe_ppr.claim_id, vbp_ppe_ppr.preventable_group_desc, vbp_ppe_ppr.ppr_type_code_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.vbp_ppe_pps /*+createtype(A)*/ 
(
 claim_key,
 claim_id,
 claim_trans_id,
 claim_line_num,
 srv_dt,
 claim_type_code,
 preventbl_group_code,
 pfp_preventbl_group_name,
 pfp_preventbl_group_desc,
 item_preventbl_status_code,
 pfp_preventbl_status_item_desc,
 item_preventbl_rsn_code,
 pfp_preventbl_rsn_item_desc,
 mdw_insert_dt,
 mdw_update_dt
)
AS
 SELECT vbp_ppe_pps.claim_key,
        vbp_ppe_pps.claim_id,
        vbp_ppe_pps.claim_trans_id,
        vbp_ppe_pps.claim_line_num,
        vbp_ppe_pps.srv_dt,
        vbp_ppe_pps.claim_type_code,
        vbp_ppe_pps.preventbl_group_code,
        vbp_ppe_pps.pfp_preventbl_group_name,
        vbp_ppe_pps.pfp_preventbl_group_desc,
        vbp_ppe_pps.item_preventbl_status_code,
        vbp_ppe_pps.pfp_preventbl_status_item_desc,
        vbp_ppe_pps.item_preventbl_rsn_code,
        vbp_ppe_pps.pfp_preventbl_rsn_item_desc,
        vbp_ppe_pps.mdw_insert_dt,
        vbp_ppe_pps.mdw_update_dt
 FROM epbuilder.vbp_ppe_pps
 ORDER BY vbp_ppe_pps.claim_key,
          vbp_ppe_pps.claim_id,
          vbp_ppe_pps.claim_trans_id,
          vbp_ppe_pps.claim_line_num,
          vbp_ppe_pps.srv_dt,
          vbp_ppe_pps.claim_type_code,
          vbp_ppe_pps.preventbl_group_code,
          vbp_ppe_pps.pfp_preventbl_group_name,
          vbp_ppe_pps.pfp_preventbl_group_desc,
          vbp_ppe_pps.item_preventbl_status_code,
          vbp_ppe_pps.pfp_preventbl_status_item_desc,
          vbp_ppe_pps.item_preventbl_rsn_code,
          vbp_ppe_pps.pfp_preventbl_rsn_item_desc,
          vbp_ppe_pps.mdw_insert_dt,
          vbp_ppe_pps.mdw_update_dt
SEGMENTED BY hash(vbp_ppe_pps.srv_dt, vbp_ppe_pps.claim_type_code, vbp_ppe_pps.preventbl_group_code, vbp_ppe_pps.item_preventbl_status_code, vbp_ppe_pps.item_preventbl_rsn_code, vbp_ppe_pps.mdw_insert_dt, vbp_ppe_pps.mdw_update_dt, vbp_ppe_pps.claim_key, vbp_ppe_pps.claim_trans_id, vbp_ppe_pps.claim_line_num, vbp_ppe_pps.claim_id, vbp_ppe_pps.pfp_preventbl_group_name, vbp_ppe_pps.pfp_preventbl_group_desc, vbp_ppe_pps.pfp_preventbl_status_item_desc, vbp_ppe_pps.pfp_preventbl_rsn_item_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.vbp_ppe_ppv /*+createtype(A)*/ 
(
 claim_key,
 claim_id,
 claim_trans_id,
 dsch_dt,
 claim_type_code,
 preventable_group,
 pfp_preventable_group_name,
 pfp_preventable_group_desc,
 preventable_status,
 pfp_preventable_status_desc,
 preventable_reason,
 pfp_preventable_reason_desc,
 mdw_insert_dt,
 mdw_update_dt
)
AS
 SELECT vbp_ppe_ppv.claim_key,
        vbp_ppe_ppv.claim_id,
        vbp_ppe_ppv.claim_trans_id,
        vbp_ppe_ppv.dsch_dt,
        vbp_ppe_ppv.claim_type_code,
        vbp_ppe_ppv.preventable_group,
        vbp_ppe_ppv.pfp_preventable_group_name,
        vbp_ppe_ppv.pfp_preventable_group_desc,
        vbp_ppe_ppv.preventable_status,
        vbp_ppe_ppv.pfp_preventable_status_desc,
        vbp_ppe_ppv.preventable_reason,
        vbp_ppe_ppv.pfp_preventable_reason_desc,
        vbp_ppe_ppv.mdw_insert_dt,
        vbp_ppe_ppv.mdw_update_dt
 FROM epbuilder.vbp_ppe_ppv
 ORDER BY vbp_ppe_ppv.claim_key,
          vbp_ppe_ppv.claim_id,
          vbp_ppe_ppv.claim_trans_id,
          vbp_ppe_ppv.dsch_dt,
          vbp_ppe_ppv.claim_type_code,
          vbp_ppe_ppv.preventable_group,
          vbp_ppe_ppv.pfp_preventable_group_name,
          vbp_ppe_ppv.pfp_preventable_group_desc,
          vbp_ppe_ppv.preventable_status,
          vbp_ppe_ppv.pfp_preventable_status_desc,
          vbp_ppe_ppv.preventable_reason,
          vbp_ppe_ppv.pfp_preventable_reason_desc,
          vbp_ppe_ppv.mdw_insert_dt,
          vbp_ppe_ppv.mdw_update_dt
SEGMENTED BY hash(vbp_ppe_ppv.dsch_dt, vbp_ppe_ppv.claim_type_code, vbp_ppe_ppv.preventable_group, vbp_ppe_ppv.preventable_status, vbp_ppe_ppv.preventable_reason, vbp_ppe_ppv.mdw_insert_dt, vbp_ppe_ppv.mdw_update_dt, vbp_ppe_ppv.claim_key, vbp_ppe_ppv.claim_trans_id, vbp_ppe_ppv.claim_id, vbp_ppe_ppv.pfp_preventable_group_name, vbp_ppe_ppv.pfp_preventable_group_desc, vbp_ppe_ppv.pfp_preventable_status_desc, vbp_ppe_ppv.pfp_preventable_reason_desc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.county_mcregion /*+createtype(A)*/ 
(
 county,
 mcregion
)
AS
 SELECT county_mcregion.county,
        county_mcregion.mcregion
 FROM epbuilder.county_mcregion
 ORDER BY county_mcregion.county,
          county_mcregion.mcregion
SEGMENTED BY hash(county_mcregion.county, county_mcregion.mcregion) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.crg /*+createtype(A)*/ 
(
 year,
 quarter,
 crg_version,
 claim_begin_date,
 claim_end_date,
 claims_as_of_date,
 recip_id,
 qcrg_code,
 qcrg_desc,
 qacrg1_code,
 qacrg1_desc,
 qacrg2_code,
 qacrg2_desc,
 qacrg3_code,
 qacrg3_desc,
 fincrg_q,
 q_base,
 q_severity,
 crg_code,
 crg_desc,
 acrg1_code,
 acrg1_desc,
 acrg2_code,
 acrg2_desc,
 acrg3_code,
 acrg3_desc,
 fincrg_g,
 g_base,
 g_severity,
 fincrg,
 base,
 severity,
 total_allowed_amount,
 total_medical_allowed,
 total_pharmacy_allowed,
 total_episode_costs,
 acrg_weight,
 qcrg_weight,
 acrg_average,
 qcrg_average
)
AS
 SELECT crg.year,
        crg.quarter,
        crg.crg_version,
        crg.claim_begin_date,
        crg.claim_end_date,
        crg.claims_as_of_date,
        crg.recip_id,
        crg.qcrg_code,
        crg.qcrg_desc,
        crg.qacrg1_code,
        crg.qacrg1_desc,
        crg.qacrg2_code,
        crg.qacrg2_desc,
        crg.qacrg3_code,
        crg.qacrg3_desc,
        crg.fincrg_q,
        crg.q_base,
        crg.q_severity,
        crg.crg_code,
        crg.crg_desc,
        crg.acrg1_code,
        crg.acrg1_desc,
        crg.acrg2_code,
        crg.acrg2_desc,
        crg.acrg3_code,
        crg.acrg3_desc,
        crg.fincrg_g,
        crg.g_base,
        crg.g_severity,
        crg.fincrg,
        crg.base,
        crg.severity,
        crg.total_allowed_amount,
        crg.total_medical_allowed,
        crg.total_pharmacy_allowed,
        crg.total_episode_costs,
        crg.acrg_weight,
        crg.qcrg_weight,
        crg.acrg_average,
        crg.qcrg_average
 FROM epbuilder.crg
 ORDER BY crg.year,
          crg.quarter,
          crg.crg_version,
          crg.claim_begin_date,
          crg.claim_end_date,
          crg.claims_as_of_date,
          crg.recip_id
SEGMENTED BY hash(crg.year, crg.quarter, crg.claim_begin_date, crg.claim_end_date, crg.claims_as_of_date, crg.qcrg_code, crg.qacrg1_code, crg.qacrg2_code, crg.qacrg3_code, crg.fincrg_q, crg.q_base, crg.q_severity, crg.crg_code, crg.acrg1_code, crg.acrg2_code, crg.acrg3_code, crg.fincrg_g, crg.g_base, crg.g_severity, crg.fincrg, crg.base, crg.severity, crg.total_allowed_amount, crg.total_medical_allowed, crg.total_pharmacy_allowed, crg.total_episode_costs, crg.acrg_weight, crg.qcrg_weight, crg.acrg_average, crg.qcrg_average, crg.recip_id, crg.crg_version) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.crg_2012_2013 /*+createtype(A)*/ 
(
 YEAR,
 QUARTER,
 CRG_VERSION,
 CLAIM_BEGIN_DATE,
 CLAIM_END_DATE,
 CLAIMS_AS_OF_DATE,
 RECIP_ID,
 QCRG_CODE,
 QACRG1_CODE,
 QACRG2_CODE,
 QACRG3_CODE,
 FINCRG_Q,
 Q_BASE,
 Q_SEVERITY,
 CRG_CODE,
 ACRG1_CODE,
 ACRG2_CODE,
 ACRG3_CODE,
 FINCRG_G,
 G_BASE,
 G_SEVERITY,
 FINCRG,
 BASE,
 SEVERITY,
 acrg1_desc,
 acrg2_desc,
 acrg3_desc,
 crg_desc,
 qacrg1_desc,
 qacrg2_desc,
 qacrg3_desc,
 qcrg_desc
)
AS
 SELECT crg_2012_2013.YEAR,
        crg_2012_2013.QUARTER,
        crg_2012_2013.CRG_VERSION,
        crg_2012_2013.CLAIM_BEGIN_DATE,
        crg_2012_2013.CLAIM_END_DATE,
        crg_2012_2013.CLAIMS_AS_OF_DATE,
        crg_2012_2013.RECIP_ID,
        crg_2012_2013.QCRG_CODE,
        crg_2012_2013.QACRG1_CODE,
        crg_2012_2013.QACRG2_CODE,
        crg_2012_2013.QACRG3_CODE,
        crg_2012_2013.FINCRG_Q,
        crg_2012_2013.Q_BASE,
        crg_2012_2013.Q_SEVERITY,
        crg_2012_2013.CRG_CODE,
        crg_2012_2013.ACRG1_CODE,
        crg_2012_2013.ACRG2_CODE,
        crg_2012_2013.ACRG3_CODE,
        crg_2012_2013.FINCRG_G,
        crg_2012_2013.G_BASE,
        crg_2012_2013.G_SEVERITY,
        crg_2012_2013.FINCRG,
        crg_2012_2013.BASE,
        crg_2012_2013.SEVERITY,
        crg_2012_2013.acrg1_desc,
        crg_2012_2013.acrg2_desc,
        crg_2012_2013.acrg3_desc,
        crg_2012_2013.crg_desc,
        crg_2012_2013.qacrg1_desc,
        crg_2012_2013.qacrg2_desc,
        crg_2012_2013.qacrg3_desc,
        crg_2012_2013.qcrg_desc
 FROM epbuilder.crg_2012_2013
 ORDER BY crg_2012_2013.YEAR,
          crg_2012_2013.QUARTER,
          crg_2012_2013.CRG_VERSION,
          crg_2012_2013.CLAIM_BEGIN_DATE,
          crg_2012_2013.CLAIM_END_DATE,
          crg_2012_2013.CLAIMS_AS_OF_DATE,
          crg_2012_2013.RECIP_ID,
          crg_2012_2013.QCRG_CODE,
          crg_2012_2013.QACRG1_CODE,
          crg_2012_2013.QACRG2_CODE,
          crg_2012_2013.QACRG3_CODE,
          crg_2012_2013.FINCRG_Q,
          crg_2012_2013.Q_BASE,
          crg_2012_2013.Q_SEVERITY,
          crg_2012_2013.CRG_CODE,
          crg_2012_2013.ACRG1_CODE,
          crg_2012_2013.ACRG2_CODE,
          crg_2012_2013.ACRG3_CODE,
          crg_2012_2013.FINCRG_G,
          crg_2012_2013.G_BASE,
          crg_2012_2013.G_SEVERITY,
          crg_2012_2013.FINCRG,
          crg_2012_2013.BASE,
          crg_2012_2013.SEVERITY
SEGMENTED BY hash(crg_2012_2013.YEAR, crg_2012_2013.QUARTER, crg_2012_2013.CLAIM_BEGIN_DATE, crg_2012_2013.CLAIM_END_DATE, crg_2012_2013.CLAIMS_AS_OF_DATE, crg_2012_2013.QCRG_CODE, crg_2012_2013.QACRG2_CODE, crg_2012_2013.QACRG3_CODE, crg_2012_2013.FINCRG_Q, crg_2012_2013.Q_BASE, crg_2012_2013.Q_SEVERITY, crg_2012_2013.CRG_CODE, crg_2012_2013.ACRG1_CODE, crg_2012_2013.ACRG2_CODE, crg_2012_2013.ACRG3_CODE, crg_2012_2013.FINCRG_G, crg_2012_2013.G_BASE, crg_2012_2013.G_SEVERITY, crg_2012_2013.FINCRG, crg_2012_2013.BASE, crg_2012_2013.SEVERITY, crg_2012_2013.QACRG1_CODE, crg_2012_2013.RECIP_ID, crg_2012_2013.CRG_VERSION) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.harp_recip /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT harp_recip.member_id
 FROM epbuilder.harp_recip
 ORDER BY harp_recip.member_id
SEGMENTED BY hash(harp_recip.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.hh_table /*+createtype(A)*/ 
(
 member_id,
 hh,
 mmis,
 npi
)
AS
 SELECT hh_table.member_id,
        hh_table.hh,
        hh_table.mmis,
        hh_table.npi
 FROM epbuilder.hh_table
 ORDER BY hh_table.member_id
SEGMENTED BY hash(hh_table.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.hiv_recip /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT hiv_recip.member_id
 FROM epbuilder.hiv_recip
 ORDER BY hiv_recip.member_id
SEGMENTED BY hash(hiv_recip.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.maimo_harp_member /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT maimo_harp_member.member_id
 FROM epbuilder.maimo_harp_member
 ORDER BY maimo_harp_member.member_id
SEGMENTED BY hash(maimo_harp_member.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.mltc_recip /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT mltc_recip.member_id
 FROM epbuilder.mltc_recip
 ORDER BY mltc_recip.member_id
SEGMENTED BY hash(mltc_recip.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.mdc_desc /*+createtype(A)*/ 
(
 mdc,
 mdc_description
)
AS
 SELECT mdc_desc.mdc,
        mdc_desc.mdc_description
 FROM epbuilder.mdc_desc
 ORDER BY mdc_desc.mdc
SEGMENTED BY hash(mdc_desc.mdc) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.mco_mmis /*+createtype(A)*/ 
(
 mco,
 mmis
)
AS
 SELECT mco_mmis.mco,
        mco_mmis.mmis
 FROM epbuilder.mco_mmis
 ORDER BY mco_mmis.mco,
          mco_mmis.mmis
SEGMENTED BY hash(mco_mmis.mmis, mco_mmis.mco) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.mco_table /*+createtype(A)*/ 
(
 member_id,
 mco,
 mmis,
 npi
)
AS
 SELECT mco_table.member_id,
        mco_table.mco,
        mco_table.mmis,
        mco_table.npi
 FROM epbuilder.mco_table
 ORDER BY mco_table.member_id,
          mco_table.mco,
          mco_table.mmis,
          mco_table.npi
SEGMENTED BY hash(mco_table.member_id, mco_table.mmis, mco_table.npi, mco_table.mco) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.MMCOR_provider_type /*+createtype(A)*/ 
(
 provider_id,
 provider_npi,
 provider_type
)
AS
 SELECT MMCOR_provider_type.provider_id,
        MMCOR_provider_type.provider_npi,
        MMCOR_provider_type.provider_type
 FROM epbuilder.MMCOR_provider_type
 ORDER BY MMCOR_provider_type.provider_id,
          MMCOR_provider_type.provider_npi,
          MMCOR_provider_type.provider_type
SEGMENTED BY hash(MMCOR_provider_type.provider_id, MMCOR_provider_type.provider_npi, MMCOR_provider_type.provider_type) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.mom_baby_2014 /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY
)
AS
 SELECT mom_baby_2014.ENCRYPT_RECIP_ID_MOM,
        mom_baby_2014.ENCRYPT_RECIP_ID_BABY
 FROM epbuilder.mom_baby_2014
 ORDER BY mom_baby_2014.ENCRYPT_RECIP_ID_MOM,
          mom_baby_2014.ENCRYPT_RECIP_ID_BABY
SEGMENTED BY hash(mom_baby_2014.ENCRYPT_RECIP_ID_MOM, mom_baby_2014.ENCRYPT_RECIP_ID_BABY) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.npi_mmcor_category /*+createtype(A)*/ 
(
 npi,
 category,
 category_description
)
AS
 SELECT npi_mmcor_category.npi,
        npi_mmcor_category.category,
        npi_mmcor_category.category_description
 FROM epbuilder.npi_mmcor_category
 ORDER BY npi_mmcor_category.npi,
          npi_mmcor_category.category,
          npi_mmcor_category.category_description
SEGMENTED BY hash(npi_mmcor_category.category, npi_mmcor_category.npi, npi_mmcor_category.category_description) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ny_zip_to_county /*+createtype(A)*/ 
(
 zipcode,
 state,
 county
)
AS
 SELECT ny_zip_to_county.zipcode,
        ny_zip_to_county.state,
        ny_zip_to_county.county
 FROM epbuilder.ny_zip_to_county
 ORDER BY ny_zip_to_county.zipcode,
          ny_zip_to_county.state,
          ny_zip_to_county.county
SEGMENTED BY hash(ny_zip_to_county.zipcode, ny_zip_to_county.state, ny_zip_to_county.county) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.dd_recip /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT dd_recip.member_id
 FROM epbuilder.dd_recip
 ORDER BY dd_recip.member_id
SEGMENTED BY hash(dd_recip.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PPS_table /*+createtype(A)*/ 
(
 member_id,
 PPS
)
AS
 SELECT PPS_table.member_id,
        PPS_table.PPS
 FROM epbuilder.PPS_table
 ORDER BY PPS_table.member_id,
          PPS_table.PPS
SEGMENTED BY hash(PPS_table.member_id, PPS_table.PPS) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.PCP_table /*+createtype(A)*/ 
(
 member_id,
 pcp,
 mmis,
 npi
)
AS
 SELECT PCP_table.member_id,
        PCP_table.pcp,
        PCP_table.mmis,
        PCP_table.npi
 FROM epbuilder.PCP_table
 ORDER BY PCP_table.member_id,
          PCP_table.pcp,
          PCP_table.mmis,
          PCP_table.npi
SEGMENTED BY hash(PCP_table.member_id, PCP_table.mmis, PCP_table.npi, PCP_table.pcp) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.vbp_contractor_provider_npi /*+createtype(A)*/ 
(
 provider_npi,
 vbp_contractor,
 PGroup
)
AS
 SELECT vbp_contractor_provider_npi.provider_npi,
        vbp_contractor_provider_npi.vbp_contractor,
        vbp_contractor_provider_npi.PGroup
 FROM epbuilder.vbp_contractor_provider_npi
 ORDER BY vbp_contractor_provider_npi.provider_npi,
          vbp_contractor_provider_npi.vbp_contractor,
          vbp_contractor_provider_npi.PGroup
SEGMENTED BY hash(vbp_contractor_provider_npi.provider_npi, vbp_contractor_provider_npi.vbp_contractor, vbp_contractor_provider_npi.PGroup) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.episode_sub_types /*+createtype(L)*/ 
(
 master_episode_id,
 master_claim_id,
 code_value,
 subtype_group_id,
 group_name,
 episode_id
)
AS
 SELECT episode_sub_types.master_episode_id,
        episode_sub_types.master_claim_id,
        episode_sub_types.code_value,
        episode_sub_types.subtype_group_id,
        episode_sub_types.group_name,
        episode_sub_types.episode_id
 FROM epbuilder.episode_sub_types
 ORDER BY episode_sub_types.master_episode_id,
          episode_sub_types.master_claim_id,
          episode_sub_types.code_value,
          episode_sub_types.subtype_group_id,
          episode_sub_types.group_name,
          episode_sub_types.episode_id
SEGMENTED BY hash(episode_sub_types.episode_id, episode_sub_types.code_value, episode_sub_types.subtype_group_id, episode_sub_types.master_episode_id, episode_sub_types.master_claim_id, episode_sub_types.group_name) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_subtypes /*+createtype(L)*/ 
(
 epi_id,
 epi_number,
 name,
 value
)
AS
 SELECT ra_subtypes.epi_id,
        ra_subtypes.epi_number,
        ra_subtypes.name,
        ra_subtypes.value
 FROM epbuilder.ra_subtypes
 ORDER BY ra_subtypes.epi_id,
          ra_subtypes.name
SEGMENTED BY hash(ra_subtypes.epi_number, ra_subtypes.value, ra_subtypes.name, ra_subtypes.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_trig_claims /*+createtype(L)*/ 
(
 master_episode_id,
 episode_id,
 episode_type,
 assigned_type,
 claim_line_type_code,
 allowed_amt,
 split_allowed_amt
)
AS
 SELECT tmp_trig_claims.master_episode_id,
        tmp_trig_claims.episode_id,
        tmp_trig_claims.episode_type,
        tmp_trig_claims.assigned_type,
        tmp_trig_claims.claim_line_type_code,
        tmp_trig_claims.allowed_amt,
        tmp_trig_claims.split_allowed_amt
 FROM epbuilder.tmp_trig_claims
 ORDER BY tmp_trig_claims.master_episode_id,
          tmp_trig_claims.episode_id,
          tmp_trig_claims.episode_type,
          tmp_trig_claims.claim_line_type_code
SEGMENTED BY hash(tmp_trig_claims.master_episode_id, tmp_trig_claims.episode_id, tmp_trig_claims.episode_type, tmp_trig_claims.claim_line_type_code) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.QC_mom_baby /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 year
)
AS
 SELECT QC_mom_baby.ENCRYPT_RECIP_ID_MOM,
        QC_mom_baby.ENCRYPT_RECIP_ID_BABY,
        QC_mom_baby.year
 FROM epbuilder.QC_mom_baby
 ORDER BY QC_mom_baby.ENCRYPT_RECIP_ID_MOM,
          QC_mom_baby.ENCRYPT_RECIP_ID_BABY,
          QC_mom_baby.year
SEGMENTED BY hash(QC_mom_baby.year, QC_mom_baby.ENCRYPT_RECIP_ID_MOM, QC_mom_baby.ENCRYPT_RECIP_ID_BABY) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.QC_SAMPLE_PT_LIST_final /*+createtype(A)*/ 
(
 member_id
)
AS
 SELECT QC_SAMPLE_PT_LIST_final.member_id
 FROM epbuilder.QC_SAMPLE_PT_LIST_final
 ORDER BY QC_SAMPLE_PT_LIST_final.member_id
SEGMENTED BY hash(QC_SAMPLE_PT_LIST_final.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.sub_distinct /*+createtype(A)*/ 
(
 child_master_episode_id,
 association_level
)
AS
 SELECT sub_distinct.child_master_episode_id,
        sub_distinct.association_level
 FROM epbuilder.sub_distinct
 ORDER BY sub_distinct.child_master_episode_id
SEGMENTED BY hash(sub_distinct.child_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.report_episode_detail /*+createtype(L)*/ 
(
 Filter_ID,
 Member_ID,
 Master_Episode_ID,
 Episode_ID,
 Episode_Name,
 Episode_Description,
 Episode_Type,
 MDC,
 MDC_Description,
 Episode_Begin_Date,
 Episode_End_Date,
 Episode_Length,
 Level,
 Split_Total_Cost,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_80thPercentile_Cost,
 Split_Total_PAC_Cost,
 Split_Total_Typical_Cost,
 Split_Total_TypicalwPAC_Cost,
 Annualized_Split_Total_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_80thPercentile_Cost,
 Annualized_Split_Total_PAC_Cost,
 Annualized_Split_Total_Typical_Cost,
 Annualized_Split_Total_TypicalwPAC_Cost,
 Unsplit_Total_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Unsplit_Total_PAC_Cost,
 Unsplit_Total_Typical_Cost,
 Unsplit_Total_TypicalwPAC_Cost,
 Annualized_Unsplit_Total_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_Total_PAC_Cost,
 Annualized_Unsplit_Total_Typical_Cost,
 Annualized_Unsplit_Total_TypicalwPAC_Cost,
 Facility_ID,
 Physician_ID,
 Physician_Specialty,
 Split_Expected_Total_Cost,
 Split_Expected_Typical_IP_Cost,
 Split_Expected_Typical_Other_Cost,
 Split_Expected_PAC_Cost,
 Unsplit_Expected_Total_Cost,
 Unsplit_Expected_Typical_IP_Cost,
 Unsplit_Expected_Typical_Other_Cost,
 Unsplit_Expected_PAC_Cost,
 IP_PAC_Count,
 OP_PAC_Count,
 PB_PAC_Count,
 RX_PAC_Count,
 year,
 enrolled_month,
 co_occurence_count_DEPANX,
 co_occurence_count_DIVERT,
 co_occurence_count_RHNTS,
 co_occurence_count_OSTEOA,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_GLCOMA,
 co_occurence_count_HTN,
 co_occurence_count_GERD,
 co_occurence_count_BIPLR,
 co_occurence_count_ULCLTS,
 co_occurence_count_CAD,
 co_occurence_count_COPD,
 co_occurence_count_HF,
 co_occurence_count_ARRBLK,
 co_occurence_count_ASTHMA,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_count_chronic,
 co_count_all,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 END_OF_STUDY,
 Facility_ID_provider_name,
 Facility_ID_provider_zipcode,
 Facility_ID_provider_type,
 Physician_ID_provider_name,
 Physician_ID_provider_zipcode,
 Physician_ID_provider_type
)
AS
 SELECT report_episode_detail.Filter_ID,
        report_episode_detail.Member_ID,
        report_episode_detail.Master_Episode_ID,
        report_episode_detail.Episode_ID,
        report_episode_detail.Episode_Name,
        report_episode_detail.Episode_Description,
        report_episode_detail.Episode_Type,
        report_episode_detail.MDC,
        report_episode_detail.MDC_Description,
        report_episode_detail.Episode_Begin_Date,
        report_episode_detail.Episode_End_Date,
        report_episode_detail.Episode_Length,
        report_episode_detail.Level,
        report_episode_detail.Split_Total_Cost,
        report_episode_detail.Split_1stPercentile_Cost,
        report_episode_detail.Split_99thPercentile_Cost,
        report_episode_detail.Split_80thPercentile_Cost,
        report_episode_detail.Split_Total_PAC_Cost,
        report_episode_detail.Split_Total_Typical_Cost,
        report_episode_detail.Split_Total_TypicalwPAC_Cost,
        report_episode_detail.Annualized_Split_Total_Cost,
        report_episode_detail.Annualized_Split_1stPercentile_Cost,
        report_episode_detail.Annualized_Split_99thPercentile_Cost,
        report_episode_detail.Annualized_Split_80thPercentile_Cost,
        report_episode_detail.Annualized_Split_Total_PAC_Cost,
        report_episode_detail.Annualized_Split_Total_Typical_Cost,
        report_episode_detail.Annualized_Split_Total_TypicalwPAC_Cost,
        report_episode_detail.Unsplit_Total_Cost,
        report_episode_detail.Unsplit_1stPercentile_Cost,
        report_episode_detail.Unsplit_99thPercentile_Cost,
        report_episode_detail.Unsplit_Total_PAC_Cost,
        report_episode_detail.Unsplit_Total_Typical_Cost,
        report_episode_detail.Unsplit_Total_TypicalwPAC_Cost,
        report_episode_detail.Annualized_Unsplit_Total_Cost,
        report_episode_detail.Annualized_Unsplit_1stPercentile_Cost,
        report_episode_detail.Annualized_Unsplit_99thPercentile_Cost,
        report_episode_detail.Annualized_Unsplit_Total_PAC_Cost,
        report_episode_detail.Annualized_Unsplit_Total_Typical_Cost,
        report_episode_detail.Annualized_Unsplit_Total_TypicalwPAC_Cost,
        report_episode_detail.Facility_ID,
        report_episode_detail.Physician_ID,
        report_episode_detail.Physician_Specialty,
        report_episode_detail.Split_Expected_Total_Cost,
        report_episode_detail.Split_Expected_Typical_IP_Cost,
        report_episode_detail.Split_Expected_Typical_Other_Cost,
        report_episode_detail.Split_Expected_PAC_Cost,
        report_episode_detail.Unsplit_Expected_Total_Cost,
        report_episode_detail.Unsplit_Expected_Typical_IP_Cost,
        report_episode_detail.Unsplit_Expected_Typical_Other_Cost,
        report_episode_detail.Unsplit_Expected_PAC_Cost,
        report_episode_detail.IP_PAC_Count,
        report_episode_detail.OP_PAC_Count,
        report_episode_detail.PB_PAC_Count,
        report_episode_detail.RX_PAC_Count,
        report_episode_detail.year,
        report_episode_detail.enrolled_month,
        report_episode_detail.co_occurence_count_DEPANX,
        report_episode_detail.co_occurence_count_DIVERT,
        report_episode_detail.co_occurence_count_RHNTS,
        report_episode_detail.co_occurence_count_OSTEOA,
        report_episode_detail.co_occurence_count_DIAB,
        report_episode_detail.co_occurence_count_DEPRSN,
        report_episode_detail.co_occurence_count_LBP,
        report_episode_detail.co_occurence_count_CROHNS,
        report_episode_detail.co_occurence_count_GLCOMA,
        report_episode_detail.co_occurence_count_HTN,
        report_episode_detail.co_occurence_count_GERD,
        report_episode_detail.co_occurence_count_BIPLR,
        report_episode_detail.co_occurence_count_ULCLTS,
        report_episode_detail.co_occurence_count_CAD,
        report_episode_detail.co_occurence_count_COPD,
        report_episode_detail.co_occurence_count_HF,
        report_episode_detail.co_occurence_count_ARRBLK,
        report_episode_detail.co_occurence_count_ASTHMA,
        report_episode_detail.co_occurence_count_PTSD,
        report_episode_detail.co_occurence_count_SCHIZO,
        report_episode_detail.co_occurence_count_SUDS,
        report_episode_detail.co_count_chronic,
        report_episode_detail.co_count_all,
        report_episode_detail.ip_cost,
        report_episode_detail.op_cost,
        report_episode_detail.pb_cost,
        report_episode_detail.rx_cost,
        report_episode_detail.END_OF_STUDY,
        report_episode_detail.Facility_ID_provider_name,
        report_episode_detail.Facility_ID_provider_zipcode,
        report_episode_detail.Facility_ID_provider_type,
        report_episode_detail.Physician_ID_provider_name,
        report_episode_detail.Physician_ID_provider_zipcode,
        report_episode_detail.Physician_ID_provider_type
 FROM epbuilder.report_episode_detail
 ORDER BY report_episode_detail.Filter_ID,
          report_episode_detail.Member_ID,
          report_episode_detail.Master_Episode_ID,
          report_episode_detail.Episode_ID,
          report_episode_detail.Episode_Name,
          report_episode_detail.Episode_Description,
          report_episode_detail.Episode_Type,
          report_episode_detail.MDC,
          report_episode_detail.MDC_Description,
          report_episode_detail.Episode_Begin_Date,
          report_episode_detail.Episode_End_Date,
          report_episode_detail.Episode_Length,
          report_episode_detail.Level,
          report_episode_detail.Split_Total_Cost,
          report_episode_detail.Split_1stPercentile_Cost,
          report_episode_detail.Split_99thPercentile_Cost,
          report_episode_detail.Split_80thPercentile_Cost,
          report_episode_detail.Split_Total_PAC_Cost,
          report_episode_detail.Split_Total_Typical_Cost,
          report_episode_detail.Split_Total_TypicalwPAC_Cost,
          report_episode_detail.Annualized_Split_Total_Cost,
          report_episode_detail.Annualized_Split_1stPercentile_Cost,
          report_episode_detail.Annualized_Split_99thPercentile_Cost,
          report_episode_detail.Annualized_Split_80thPercentile_Cost,
          report_episode_detail.Annualized_Split_Total_PAC_Cost,
          report_episode_detail.Annualized_Split_Total_Typical_Cost,
          report_episode_detail.Annualized_Split_Total_TypicalwPAC_Cost,
          report_episode_detail.Unsplit_Total_Cost,
          report_episode_detail.Unsplit_1stPercentile_Cost,
          report_episode_detail.Unsplit_99thPercentile_Cost,
          report_episode_detail.Unsplit_Total_PAC_Cost,
          report_episode_detail.Unsplit_Total_Typical_Cost,
          report_episode_detail.Unsplit_Total_TypicalwPAC_Cost,
          report_episode_detail.Annualized_Unsplit_Total_Cost,
          report_episode_detail.Annualized_Unsplit_1stPercentile_Cost,
          report_episode_detail.Annualized_Unsplit_99thPercentile_Cost,
          report_episode_detail.Annualized_Unsplit_Total_PAC_Cost,
          report_episode_detail.Annualized_Unsplit_Total_Typical_Cost,
          report_episode_detail.Annualized_Unsplit_Total_TypicalwPAC_Cost,
          report_episode_detail.Facility_ID,
          report_episode_detail.Physician_ID,
          report_episode_detail.Physician_Specialty,
          report_episode_detail.Split_Expected_Total_Cost,
          report_episode_detail.Split_Expected_Typical_IP_Cost,
          report_episode_detail.Split_Expected_Typical_Other_Cost,
          report_episode_detail.Split_Expected_PAC_Cost,
          report_episode_detail.Unsplit_Expected_Total_Cost,
          report_episode_detail.Unsplit_Expected_Typical_IP_Cost,
          report_episode_detail.Unsplit_Expected_Typical_Other_Cost,
          report_episode_detail.Unsplit_Expected_PAC_Cost,
          report_episode_detail.IP_PAC_Count,
          report_episode_detail.OP_PAC_Count,
          report_episode_detail.PB_PAC_Count,
          report_episode_detail.RX_PAC_Count
SEGMENTED BY hash(report_episode_detail.Filter_ID, report_episode_detail.Episode_ID, report_episode_detail.Episode_Name, report_episode_detail.MDC, report_episode_detail.Episode_Length, report_episode_detail.Level, report_episode_detail.Split_Total_Cost, report_episode_detail.Physician_Specialty, report_episode_detail.Episode_Begin_Date, report_episode_detail.Episode_End_Date, report_episode_detail.Split_1stPercentile_Cost, report_episode_detail.Split_99thPercentile_Cost, report_episode_detail.Split_80thPercentile_Cost, report_episode_detail.Split_Total_PAC_Cost, report_episode_detail.Split_Total_Typical_Cost, report_episode_detail.Split_Total_TypicalwPAC_Cost, report_episode_detail.Annualized_Split_Total_Cost, report_episode_detail.Annualized_Split_1stPercentile_Cost, report_episode_detail.Annualized_Split_99thPercentile_Cost, report_episode_detail.Annualized_Split_80thPercentile_Cost, report_episode_detail.Annualized_Split_Total_PAC_Cost, report_episode_detail.Annualized_Split_Total_Typical_Cost, report_episode_detail.Annualized_Split_Total_TypicalwPAC_Cost, report_episode_detail.Unsplit_Total_Cost, report_episode_detail.Unsplit_1stPercentile_Cost, report_episode_detail.Unsplit_99thPercentile_Cost, report_episode_detail.Unsplit_Total_PAC_Cost, report_episode_detail.Unsplit_Total_Typical_Cost, report_episode_detail.Unsplit_Total_TypicalwPAC_Cost, report_episode_detail.Annualized_Unsplit_Total_Cost, report_episode_detail.Annualized_Unsplit_1stPercentile_Cost, report_episode_detail.Annualized_Unsplit_99thPercentile_Cost) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.level_5 /*+createtype(A)*/ 
(
 member_id,
 LEVEL_1,
 LEVEL_2,
 LEVEL_3,
 LEVEL_4,
 LEVEL_5
)
AS
 SELECT level_5.member_id,
        level_5.LEVEL_1,
        level_5.LEVEL_2,
        level_5.LEVEL_3,
        level_5.LEVEL_4,
        level_5.LEVEL_5
 FROM epbuilder.level_5
 ORDER BY level_5.member_id,
          level_5.LEVEL_1,
          level_5.LEVEL_2,
          level_5.LEVEL_3,
          level_5.LEVEL_4,
          level_5.LEVEL_5
SEGMENTED BY hash(level_5.member_id, level_5.LEVEL_1, level_5.LEVEL_2, level_5.LEVEL_3, level_5.LEVEL_4, level_5.LEVEL_5) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.assign_1 /*+createtype(A)*/ 
(
 member_id,
 LEVEL_1,
 LEVEL_2,
 LEVEL_3,
 LEVEL_4,
 LEVEL_5,
 claim_source,
 assigned_type,
 master_claim_id
)
AS
 SELECT assign_1.member_id,
        assign_1.LEVEL_1,
        assign_1.LEVEL_2,
        assign_1.LEVEL_3,
        assign_1.LEVEL_4,
        assign_1.LEVEL_5,
        assign_1.claim_source,
        assign_1.assigned_type,
        assign_1.master_claim_id
 FROM epbuilder.assign_1
 ORDER BY assign_1.member_id,
          assign_1.LEVEL_1,
          assign_1.LEVEL_2,
          assign_1.LEVEL_3,
          assign_1.LEVEL_4,
          assign_1.LEVEL_5,
          assign_1.claim_source,
          assign_1.assigned_type,
          assign_1.master_claim_id
SEGMENTED BY hash(assign_1.assigned_type, assign_1.claim_source, assign_1.member_id, assign_1.LEVEL_1, assign_1.LEVEL_2, assign_1.LEVEL_3, assign_1.LEVEL_4, assign_1.LEVEL_5, assign_1.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.tmp_ann_mel /*+createtype(A)*/ 
(
 master_episode_id,
 level,
 parent_child_id,
 cost,
 cost_t,
 cost_tc,
 cost_c
)
AS
 SELECT tmp_ann_mel.master_episode_id,
        tmp_ann_mel.level,
        tmp_ann_mel.parent_child_id,
        tmp_ann_mel.cost,
        tmp_ann_mel.cost_t,
        tmp_ann_mel.cost_tc,
        tmp_ann_mel.cost_c
 FROM epbuilder.tmp_ann_mel
 ORDER BY tmp_ann_mel.master_episode_id,
          tmp_ann_mel.level,
          tmp_ann_mel.parent_child_id,
          tmp_ann_mel.cost,
          tmp_ann_mel.cost_t,
          tmp_ann_mel.cost_tc,
          tmp_ann_mel.cost_c
SEGMENTED BY hash(tmp_ann_mel.level, tmp_ann_mel.parent_child_id, tmp_ann_mel.cost, tmp_ann_mel.cost_t, tmp_ann_mel.cost_tc, tmp_ann_mel.cost_c, tmp_ann_mel.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.percentiles /*+createtype(A)*/ 
(
 Filter_id,
 Master_episode_id,
 Episode_ID,
 Level,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_80thPercentile_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_80thPercentile_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost
)
AS
 SELECT percentiles.Filter_id,
        percentiles.Master_episode_id,
        percentiles.Episode_ID,
        percentiles.Level,
        percentiles.Split_1stPercentile_Cost,
        percentiles.Split_99thPercentile_Cost,
        percentiles.Split_80thPercentile_Cost,
        percentiles.Unsplit_1stPercentile_Cost,
        percentiles.Unsplit_99thPercentile_Cost,
        percentiles.Annualized_Split_1stPercentile_Cost,
        percentiles.Annualized_Split_99thPercentile_Cost,
        percentiles.Annualized_Split_80thPercentile_Cost,
        percentiles.Annualized_Unsplit_1stPercentile_Cost,
        percentiles.Annualized_Unsplit_99thPercentile_Cost
 FROM epbuilder.percentiles
 ORDER BY percentiles.Filter_id,
          percentiles.Episode_ID,
          percentiles.Level
SEGMENTED BY hash(percentiles.Filter_id, percentiles.Episode_ID, percentiles.Level) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.run_month_year /*+createtype(A)*/ 
(
 month,
 year,
 date
)
AS
 SELECT run_month_year.month,
        run_month_year.year,
        run_month_year.date
 FROM epbuilder.run_month_year
 ORDER BY run_month_year.month,
          run_month_year.year,
          run_month_year.date
SEGMENTED BY hash(run_month_year.month, run_month_year.year, run_month_year.date) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrollment_raw /*+createtype(A)*/ 
(
 year,
 month,
 date,
 member_id,
 insurance_product,
 birth_year,
 sex,
 dod
)
AS
 SELECT enrollment_raw.year,
        enrollment_raw.month,
        enrollment_raw.date,
        enrollment_raw.member_id,
        enrollment_raw.insurance_product,
        enrollment_raw.birth_year,
        enrollment_raw.sex,
        enrollment_raw.dod
 FROM epbuilder.enrollment_raw
 ORDER BY enrollment_raw.month,
          enrollment_raw.year,
          enrollment_raw.date
SEGMENTED BY hash(enrollment_raw.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrollment_raw_year_1 /*+createtype(A)*/ 
(
 year,
 month,
 member_id,
 birth_year,
 sex,
 dod
)
AS
 SELECT enrollment_raw_year_1.year,
        enrollment_raw_year_1.month,
        enrollment_raw_year_1.member_id,
        enrollment_raw_year_1.birth_year,
        enrollment_raw_year_1.sex,
        enrollment_raw_year_1.dod
 FROM epbuilder.enrollment_raw_year_1
 ORDER BY enrollment_raw_year_1.year,
          enrollment_raw_year_1.month,
          enrollment_raw_year_1.member_id,
          enrollment_raw_year_1.birth_year,
          enrollment_raw_year_1.sex,
          enrollment_raw_year_1.dod
SEGMENTED BY hash(enrollment_raw_year_1.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrollment_raw_year_2 /*+createtype(A)*/ 
(
 year,
 month,
 member_id,
 birth_year,
 sex,
 dod
)
AS
 SELECT enrollment_raw_year_2.year,
        enrollment_raw_year_2.month,
        enrollment_raw_year_2.member_id,
        enrollment_raw_year_2.birth_year,
        enrollment_raw_year_2.sex,
        enrollment_raw_year_2.dod
 FROM epbuilder.enrollment_raw_year_2
 ORDER BY enrollment_raw_year_2.year,
          enrollment_raw_year_2.month,
          enrollment_raw_year_2.member_id,
          enrollment_raw_year_2.birth_year,
          enrollment_raw_year_2.sex,
          enrollment_raw_year_2.dod
SEGMENTED BY hash(enrollment_raw_year_2.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.enrolled_month /*+createtype(A)*/ 
(
 member_id,
 year,
 enrolled_month
)
AS
 SELECT enrolled_month.member_id,
        enrolled_month.year,
        enrolled_month.enrolled_month
 FROM epbuilder.enrolled_month
 ORDER BY enrolled_month.member_id,
          enrolled_month.year
SEGMENTED BY hash(enrolled_month.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.first_date_of_service /*+createtype(A)*/ 
(
 master_episode_id,
 min
)
AS
 SELECT first_date_of_service.master_episode_id,
        first_date_of_service.min
 FROM epbuilder.first_date_of_service
 ORDER BY first_date_of_service.master_episode_id
SEGMENTED BY hash(first_date_of_service.master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.BABY_LEVEL_NURSERY /*+createtype(A)*/ 
(
 member_id,
 code_value
)
AS
 SELECT BABY_LEVEL_NURSERY.member_id,
        BABY_LEVEL_NURSERY.code_value
 FROM epbuilder.BABY_LEVEL_NURSERY
 ORDER BY BABY_LEVEL_NURSERY.member_id,
          BABY_LEVEL_NURSERY.code_value
SEGMENTED BY hash(BABY_LEVEL_NURSERY.code_value, BABY_LEVEL_NURSERY.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.BABY_NURSERY /*+createtype(A)*/ 
(
 member_id,
 NURSERY_LEVEL
)
AS
 SELECT BABY_NURSERY.member_id,
        BABY_NURSERY.NURSERY_LEVEL
 FROM epbuilder.BABY_NURSERY
 ORDER BY BABY_NURSERY.member_id
SEGMENTED BY hash(BABY_NURSERY.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.del_cost_info /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 DEL_master_episode_id,
 DELIVERY_TYPE,
 trig_begin_date,
 year,
 SPLIT_TOTAL_DEL_COST,
 SPLIT_TYPICAL_DEL_COST,
 SPLIT_PAC_DEL_COST,
 UNSPLIT_TOTAL_DEL_COST,
 UNSPLIT_TYPICAL_DEL_COST,
 UNSPLIT_PAC_DEL_COST
)
AS
 SELECT del_cost_info.ENCRYPT_RECIP_ID_MOM,
        del_cost_info.ENCRYPT_RECIP_ID_BABY,
        del_cost_info.DEL_master_episode_id,
        del_cost_info.DELIVERY_TYPE,
        del_cost_info.trig_begin_date,
        del_cost_info.year,
        del_cost_info.SPLIT_TOTAL_DEL_COST,
        del_cost_info.SPLIT_TYPICAL_DEL_COST,
        del_cost_info.SPLIT_PAC_DEL_COST,
        del_cost_info.UNSPLIT_TOTAL_DEL_COST,
        del_cost_info.UNSPLIT_TYPICAL_DEL_COST,
        del_cost_info.UNSPLIT_PAC_DEL_COST
 FROM epbuilder.del_cost_info
 ORDER BY del_cost_info.ENCRYPT_RECIP_ID_MOM,
          del_cost_info.ENCRYPT_RECIP_ID_BABY,
          del_cost_info.DEL_master_episode_id,
          del_cost_info.DELIVERY_TYPE,
          del_cost_info.trig_begin_date,
          del_cost_info.year,
          del_cost_info.SPLIT_TOTAL_DEL_COST,
          del_cost_info.SPLIT_TYPICAL_DEL_COST,
          del_cost_info.SPLIT_PAC_DEL_COST,
          del_cost_info.UNSPLIT_TOTAL_DEL_COST,
          del_cost_info.UNSPLIT_TYPICAL_DEL_COST,
          del_cost_info.UNSPLIT_PAC_DEL_COST
SEGMENTED BY hash(del_cost_info.DEL_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.preg_cost_info /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 PREG_master_episode_id,
 trig_begin_date,
 year,
 SPLIT_TOTAL_PREGN_COST,
 SPLIT_TYPICAL_PREGN_COST,
 SPLIT_PAC_PREGN_COST,
 UNSPLIT_TOTAL_PREGN_COST,
 UNSPLIT_TYPICAL_PREGN_COST,
 UNSPLIT_PAC_PREGN_COST
)
AS
 SELECT preg_cost_info.ENCRYPT_RECIP_ID_MOM,
        preg_cost_info.ENCRYPT_RECIP_ID_BABY,
        preg_cost_info.PREG_master_episode_id,
        preg_cost_info.trig_begin_date,
        preg_cost_info.year,
        preg_cost_info.SPLIT_TOTAL_PREGN_COST,
        preg_cost_info.SPLIT_TYPICAL_PREGN_COST,
        preg_cost_info.SPLIT_PAC_PREGN_COST,
        preg_cost_info.UNSPLIT_TOTAL_PREGN_COST,
        preg_cost_info.UNSPLIT_TYPICAL_PREGN_COST,
        preg_cost_info.UNSPLIT_PAC_PREGN_COST
 FROM epbuilder.preg_cost_info
 ORDER BY preg_cost_info.ENCRYPT_RECIP_ID_MOM,
          preg_cost_info.ENCRYPT_RECIP_ID_BABY,
          preg_cost_info.PREG_master_episode_id,
          preg_cost_info.trig_begin_date,
          preg_cost_info.year,
          preg_cost_info.SPLIT_TOTAL_PREGN_COST,
          preg_cost_info.SPLIT_TYPICAL_PREGN_COST,
          preg_cost_info.SPLIT_PAC_PREGN_COST,
          preg_cost_info.UNSPLIT_TOTAL_PREGN_COST,
          preg_cost_info.UNSPLIT_TYPICAL_PREGN_COST,
          preg_cost_info.UNSPLIT_PAC_PREGN_COST
SEGMENTED BY hash(preg_cost_info.PREG_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.baby_cost_info /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 NEWBORN_master_episode_id,
 trig_begin_date,
 year,
 SPLIT_TOTAL_NEWBORN_COST,
 SPLIT_TYPICAL_NEWBORN_COST,
 SPLIT_PAC_NEWBORN_COST,
 UNSPLIT_TOTAL_NEWBORN_COST,
 UNSPLIT_TYPICAL_NEWBORN_COST,
 UNSPLIT_PAC_NEWBORN_COST,
 NURSERY_LEVEL
)
AS
 SELECT baby_cost_info.ENCRYPT_RECIP_ID_MOM,
        baby_cost_info.ENCRYPT_RECIP_ID_BABY,
        baby_cost_info.NEWBORN_master_episode_id,
        baby_cost_info.trig_begin_date,
        baby_cost_info.year,
        baby_cost_info.SPLIT_TOTAL_NEWBORN_COST,
        baby_cost_info.SPLIT_TYPICAL_NEWBORN_COST,
        baby_cost_info.SPLIT_PAC_NEWBORN_COST,
        baby_cost_info.UNSPLIT_TOTAL_NEWBORN_COST,
        baby_cost_info.UNSPLIT_TYPICAL_NEWBORN_COST,
        baby_cost_info.UNSPLIT_PAC_NEWBORN_COST,
        baby_cost_info.NURSERY_LEVEL
 FROM epbuilder.baby_cost_info
 ORDER BY baby_cost_info.ENCRYPT_RECIP_ID_MOM,
          baby_cost_info.ENCRYPT_RECIP_ID_BABY,
          baby_cost_info.NEWBORN_master_episode_id,
          baby_cost_info.trig_begin_date,
          baby_cost_info.year,
          baby_cost_info.SPLIT_TOTAL_NEWBORN_COST,
          baby_cost_info.SPLIT_TYPICAL_NEWBORN_COST,
          baby_cost_info.SPLIT_PAC_NEWBORN_COST,
          baby_cost_info.UNSPLIT_TOTAL_NEWBORN_COST,
          baby_cost_info.UNSPLIT_TYPICAL_NEWBORN_COST,
          baby_cost_info.UNSPLIT_PAC_NEWBORN_COST,
          baby_cost_info.NURSERY_LEVEL
SEGMENTED BY hash(baby_cost_info.NEWBORN_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.report_episode_summary /*+createtype(L)*/ 
(
 Filter_ID,
 Episode_ID,
 Episode_Name,
 Episode_Description,
 Episode_Type,
 MDC,
 MDC_Description,
 Level,
 Episode_Volume,
 Split_Total_Cost,
 Split_Average_Cost,
 Split_1stPercentile_Cost,
 Split_99thPercentile_Cost,
 Split_Min_Cost,
 Split_Max_Cost,
 Split_STDEV,
 Split_CV,
 Split_Total_PAC_Cost,
 Split_Average_PAC_Cost,
 Split_PAC_Percent,
 Split_Total_Typical_Cost,
 Split_Average_Typical_Cost,
 Split_Total_TypicalwPAC_Cost,
 Split_Average_TypicalwPAC_Cost,
 Annualized_Split_Total_Cost,
 Annualized_Split_Average_Cost,
 Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost,
 Annualized_Split_Min_Cost,
 Annualized_Split_Max_Cost,
 Annualized_Split_STDEV,
 Annualized_Split_CV,
 Annualized_Split_Total_PAC_Cost,
 Annualized_Split_Average_PAC_Cost,
 Annualized_Split_PAC_Percent,
 Annualized_Split_Total_Typical_Cost,
 Annualized_Split_Average_Typical_Cost,
 Annualized_Split_Total_TypicalwPAC_Cost,
 Annualized_Split_Average_TypicalwPAC_Cost,
 Unsplit_Total_Cost,
 Unsplit_Average_Cost,
 Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost,
 Unsplit_Min_Cost,
 Unsplit_Max_Cost,
 Unsplit_STDEV,
 Unsplit_CV,
 Unsplit_Total_PAC_Cost,
 Unsplit_Average_PAC_Cost,
 Unsplit_PAC_Percent,
 Unsplit_Total_Typical_Cost,
 Unsplit_Average_Typical_Cost,
 Unsplit_Total_TypicalwPAC_Cost,
 Unsplit_Average_TypicalwPAC_Cost,
 Annualized_Unsplit_Total_Cost,
 Annualized_Unsplit_Average_Cost,
 Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_Min_Cost,
 Annualized_Unsplit_Max_Cost,
 Annualized_Unsplit_STDEV,
 Annualized_Unsplit_CV,
 Annualized_Unsplit_Total_PAC_Cost,
 Annualized_Unsplit_Average_PAC_Cost,
 Annualized_Unsplit_PAC_Percent,
 Annualized_Unsplit_Total_Typical_Cost,
 Annualized_Unsplit_Average_Typical_Cost,
 Annualized_Unsplit_Total_TypicalwPAC_Cost,
 Annualized_Unsplit_Average_TypicalwPAC_Cost,
 Expected_Split_Average_Cost,
 Expected_Split_Typical_IP_Average_Cost,
 Expected_Split_Typical_Other_Average_Cost,
 Expected_Split_PAC_Average_Cost,
 Expected_Unsplit_Average_Cost,
 Expected_Unsplit_Typical_IP_Average_Cost,
 Expected_Unsplit_Typical_Other_Average_Cost,
 Expected_Unsplit_PAC_Average_Cost
)
AS
 SELECT report_episode_summary.Filter_ID,
        report_episode_summary.Episode_ID,
        report_episode_summary.Episode_Name,
        report_episode_summary.Episode_Description,
        report_episode_summary.Episode_Type,
        report_episode_summary.MDC,
        report_episode_summary.MDC_Description,
        report_episode_summary.Level,
        report_episode_summary.Episode_Volume,
        report_episode_summary.Split_Total_Cost,
        report_episode_summary.Split_Average_Cost,
        report_episode_summary.Split_1stPercentile_Cost,
        report_episode_summary.Split_99thPercentile_Cost,
        report_episode_summary.Split_Min_Cost,
        report_episode_summary.Split_Max_Cost,
        report_episode_summary.Split_STDEV,
        report_episode_summary.Split_CV,
        report_episode_summary.Split_Total_PAC_Cost,
        report_episode_summary.Split_Average_PAC_Cost,
        report_episode_summary.Split_PAC_Percent,
        report_episode_summary.Split_Total_Typical_Cost,
        report_episode_summary.Split_Average_Typical_Cost,
        report_episode_summary.Split_Total_TypicalwPAC_Cost,
        report_episode_summary.Split_Average_TypicalwPAC_Cost,
        report_episode_summary.Annualized_Split_Total_Cost,
        report_episode_summary.Annualized_Split_Average_Cost,
        report_episode_summary.Annualized_Split_1stPercentile_Cost,
        report_episode_summary.Annualized_Split_99thPercentile_Cost,
        report_episode_summary.Annualized_Split_Min_Cost,
        report_episode_summary.Annualized_Split_Max_Cost,
        report_episode_summary.Annualized_Split_STDEV,
        report_episode_summary.Annualized_Split_CV,
        report_episode_summary.Annualized_Split_Total_PAC_Cost,
        report_episode_summary.Annualized_Split_Average_PAC_Cost,
        report_episode_summary.Annualized_Split_PAC_Percent,
        report_episode_summary.Annualized_Split_Total_Typical_Cost,
        report_episode_summary.Annualized_Split_Average_Typical_Cost,
        report_episode_summary.Annualized_Split_Total_TypicalwPAC_Cost,
        report_episode_summary.Annualized_Split_Average_TypicalwPAC_Cost,
        report_episode_summary.Unsplit_Total_Cost,
        report_episode_summary.Unsplit_Average_Cost,
        report_episode_summary.Unsplit_1stPercentile_Cost,
        report_episode_summary.Unsplit_99thPercentile_Cost,
        report_episode_summary.Unsplit_Min_Cost,
        report_episode_summary.Unsplit_Max_Cost,
        report_episode_summary.Unsplit_STDEV,
        report_episode_summary.Unsplit_CV,
        report_episode_summary.Unsplit_Total_PAC_Cost,
        report_episode_summary.Unsplit_Average_PAC_Cost,
        report_episode_summary.Unsplit_PAC_Percent,
        report_episode_summary.Unsplit_Total_Typical_Cost,
        report_episode_summary.Unsplit_Average_Typical_Cost,
        report_episode_summary.Unsplit_Total_TypicalwPAC_Cost,
        report_episode_summary.Unsplit_Average_TypicalwPAC_Cost,
        report_episode_summary.Annualized_Unsplit_Total_Cost,
        report_episode_summary.Annualized_Unsplit_Average_Cost,
        report_episode_summary.Annualized_Unsplit_1stPercentile_Cost,
        report_episode_summary.Annualized_Unsplit_99thPercentile_Cost,
        report_episode_summary.Annualized_Unsplit_Min_Cost,
        report_episode_summary.Annualized_Unsplit_Max_Cost,
        report_episode_summary.Annualized_Unsplit_STDEV,
        report_episode_summary.Annualized_Unsplit_CV,
        report_episode_summary.Annualized_Unsplit_Total_PAC_Cost,
        report_episode_summary.Annualized_Unsplit_Average_PAC_Cost,
        report_episode_summary.Annualized_Unsplit_PAC_Percent,
        report_episode_summary.Annualized_Unsplit_Total_Typical_Cost,
        report_episode_summary.Annualized_Unsplit_Average_Typical_Cost,
        report_episode_summary.Annualized_Unsplit_Total_TypicalwPAC_Cost,
        report_episode_summary.Annualized_Unsplit_Average_TypicalwPAC_Cost,
        report_episode_summary.Expected_Split_Average_Cost,
        report_episode_summary.Expected_Split_Typical_IP_Average_Cost,
        report_episode_summary.Expected_Split_Typical_Other_Average_Cost,
        report_episode_summary.Expected_Split_PAC_Average_Cost,
        report_episode_summary.Expected_Unsplit_Average_Cost,
        report_episode_summary.Expected_Unsplit_Typical_IP_Average_Cost,
        report_episode_summary.Expected_Unsplit_Typical_Other_Average_Cost,
        report_episode_summary.Expected_Unsplit_PAC_Average_Cost
 FROM epbuilder.report_episode_summary
 ORDER BY report_episode_summary.Episode_ID,
          report_episode_summary.Level
SEGMENTED BY hash(report_episode_summary.Episode_ID, report_episode_summary.Level) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.del_cost_exp_info /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 DEL_master_episode_id,
 DELIVERY_TYPE,
 trig_begin_date,
 year,
 SPLIT_TOTAL_DEL_COST,
 SPLIT_TYPICAL_DEL_COST,
 SPLIT_PAC_DEL_COST,
 UNSPLIT_TOTAL_DEL_COST,
 UNSPLIT_TYPICAL_DEL_COST,
 UNSPLIT_PAC_DEL_COST,
 split_expected_total_cost,
 split_expected_typical_cost,
 split_expected_pac_cost,
 unsplit_expected_total_cost,
 unsplit_expected_typical_cost,
 unsplit_expected_pac_cost
)
AS
 SELECT del_cost_exp_info.ENCRYPT_RECIP_ID_MOM,
        del_cost_exp_info.ENCRYPT_RECIP_ID_BABY,
        del_cost_exp_info.DEL_master_episode_id,
        del_cost_exp_info.DELIVERY_TYPE,
        del_cost_exp_info.trig_begin_date,
        del_cost_exp_info.year,
        del_cost_exp_info.SPLIT_TOTAL_DEL_COST,
        del_cost_exp_info.SPLIT_TYPICAL_DEL_COST,
        del_cost_exp_info.SPLIT_PAC_DEL_COST,
        del_cost_exp_info.UNSPLIT_TOTAL_DEL_COST,
        del_cost_exp_info.UNSPLIT_TYPICAL_DEL_COST,
        del_cost_exp_info.UNSPLIT_PAC_DEL_COST,
        del_cost_exp_info.split_expected_total_cost,
        del_cost_exp_info.split_expected_typical_cost,
        del_cost_exp_info.split_expected_pac_cost,
        del_cost_exp_info.unsplit_expected_total_cost,
        del_cost_exp_info.unsplit_expected_typical_cost,
        del_cost_exp_info.unsplit_expected_pac_cost
 FROM epbuilder.del_cost_exp_info
 ORDER BY del_cost_exp_info.ENCRYPT_RECIP_ID_MOM,
          del_cost_exp_info.ENCRYPT_RECIP_ID_BABY,
          del_cost_exp_info.DEL_master_episode_id,
          del_cost_exp_info.DELIVERY_TYPE,
          del_cost_exp_info.trig_begin_date,
          del_cost_exp_info.year,
          del_cost_exp_info.SPLIT_TOTAL_DEL_COST,
          del_cost_exp_info.SPLIT_TYPICAL_DEL_COST,
          del_cost_exp_info.SPLIT_PAC_DEL_COST,
          del_cost_exp_info.UNSPLIT_TOTAL_DEL_COST,
          del_cost_exp_info.UNSPLIT_TYPICAL_DEL_COST,
          del_cost_exp_info.UNSPLIT_PAC_DEL_COST
SEGMENTED BY hash(del_cost_exp_info.DEL_master_episode_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.MATERNITY_BUNDLE_COSTFIELDS /*+createtype(A)*/ 
(
 ENCRYPT_RECIP_ID_MOM,
 ENCRYPT_RECIP_ID_BABY,
 PREG_master_episode_id,
 trig_begin_date,
 year,
 SPLIT_TOTAL_PREGN_COST,
 SPLIT_TYPICAL_PREGN_COST,
 SPLIT_PAC_PREGN_COST,
 UNSPLIT_TOTAL_PREGN_COST,
 UNSPLIT_TYPICAL_PREGN_COST,
 UNSPLIT_PAC_PREGN_COST,
 DEL_master_episode_id,
 DELIVERY_TYPE,
 SPLIT_TOTAL_DEL_COST,
 SPLIT_TYPICAL_DEL_COST,
 SPLIT_PAC_DEL_COST,
 UNSPLIT_TOTAL_DEL_COST,
 UNSPLIT_TYPICAL_DEL_COST,
 UNSPLIT_PAC_DEL_COST,
 split_expected_del_total_cost,
 split_expected_del_typical_cost,
 split_expected_del_cost_cost,
 unsplit_expected_del_total_cost,
 unsplit_expected_del_typical_cost,
  unsplit_expected_del_pac_cost,
 NEWBORN_master_episode_id,
 SPLIT_TOTAL_NEWBORN_COST,
 SPLIT_TYPICAL_NEWBORN_COST,
 SPLIT_PAC_NEWBORN_COST,
 UNSPLIT_TOTAL_NEWBORN_COST,
 UNSPLIT_TYPICAL_NEWBORN_COST,
 UNSPLIT_PAC_NEWBORN_COST,
 NURSERY_LEVEL,
 PREG_AVE_SPLIT_COSTS,
 PREG_AVE_UNSPLIT_COSTS,
 NEWBORN_AVE_SPLIT_COSTS,
 NEWBORN_AVE_UNSPLIT_COSTS,
 PREG_AVE_SPLIT_PAC_COSTS,
 PREG_AVE_UNSPLIT_PAC_COSTS,
 NEWBORN_AVE_SPLIT_PAC_COSTS,
 NEWBORN_AVE_UNSPLIT_PAC_COSTS,
 PREG_AVE_SPLIT_Typical_COSTS,
 PREG_AVE_UNSPLIT_Typical_COSTS,
 NEWBORN_AVE_SPLIT_Typical_COSTS,
 NEWBORN_AVE_UNSPLIT_Typical_COSTS
)
AS
 SELECT MATERNITY_BUNDLE_COSTFIELDS.ENCRYPT_RECIP_ID_MOM,
        MATERNITY_BUNDLE_COSTFIELDS.ENCRYPT_RECIP_ID_BABY,
        MATERNITY_BUNDLE_COSTFIELDS.PREG_master_episode_id,
        MATERNITY_BUNDLE_COSTFIELDS.trig_begin_date,
        MATERNITY_BUNDLE_COSTFIELDS.year,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TOTAL_PREGN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TYPICAL_PREGN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_PAC_PREGN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TOTAL_PREGN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TYPICAL_PREGN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_PAC_PREGN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.DEL_master_episode_id,
        MATERNITY_BUNDLE_COSTFIELDS.DELIVERY_TYPE,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TOTAL_DEL_COST,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TYPICAL_DEL_COST,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_PAC_DEL_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TOTAL_DEL_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TYPICAL_DEL_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_PAC_DEL_COST,
        MATERNITY_BUNDLE_COSTFIELDS.split_expected_del_total_cost,
        MATERNITY_BUNDLE_COSTFIELDS.split_expected_del_typical_cost,
        MATERNITY_BUNDLE_COSTFIELDS.split_expected_del_cost_cost,
        MATERNITY_BUNDLE_COSTFIELDS.unsplit_expected_del_total_cost,
        MATERNITY_BUNDLE_COSTFIELDS.unsplit_expected_del_typical_cost,
        MATERNITY_BUNDLE_COSTFIELDS. unsplit_expected_del_pac_cost,
        MATERNITY_BUNDLE_COSTFIELDS.NEWBORN_master_episode_id,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TOTAL_NEWBORN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TYPICAL_NEWBORN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.SPLIT_PAC_NEWBORN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TOTAL_NEWBORN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TYPICAL_NEWBORN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_PAC_NEWBORN_COST,
        MATERNITY_BUNDLE_COSTFIELDS.NURSERY_LEVEL,
        MATERNITY_BUNDLE_COSTFIELDS.PREG_AVE_SPLIT_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.PREG_AVE_UNSPLIT_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.NEWBORN_AVE_SPLIT_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.NEWBORN_AVE_UNSPLIT_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.PREG_AVE_SPLIT_PAC_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.PREG_AVE_UNSPLIT_PAC_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.NEWBORN_AVE_SPLIT_PAC_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.NEWBORN_AVE_UNSPLIT_PAC_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.PREG_AVE_SPLIT_Typical_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.PREG_AVE_UNSPLIT_Typical_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.NEWBORN_AVE_SPLIT_Typical_COSTS,
        MATERNITY_BUNDLE_COSTFIELDS.NEWBORN_AVE_UNSPLIT_Typical_COSTS
 FROM epbuilder.MATERNITY_BUNDLE_COSTFIELDS
 ORDER BY MATERNITY_BUNDLE_COSTFIELDS.ENCRYPT_RECIP_ID_MOM,
          MATERNITY_BUNDLE_COSTFIELDS.ENCRYPT_RECIP_ID_BABY,
          MATERNITY_BUNDLE_COSTFIELDS.PREG_master_episode_id,
          MATERNITY_BUNDLE_COSTFIELDS.trig_begin_date,
          MATERNITY_BUNDLE_COSTFIELDS.year,
          MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TOTAL_PREGN_COST,
          MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TYPICAL_PREGN_COST,
          MATERNITY_BUNDLE_COSTFIELDS.SPLIT_PAC_PREGN_COST,
          MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TOTAL_PREGN_COST,
          MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TYPICAL_PREGN_COST,
          MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_PAC_PREGN_COST
SEGMENTED BY hash(MATERNITY_BUNDLE_COSTFIELDS.ENCRYPT_RECIP_ID_BABY) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.provider_prep /*+createtype(A)*/ 
(
 provider_id,
 provider_name,
 provider_zipcode,
 provider_type,
 provider_npi
)
AS
 SELECT provider_prep.provider_id,
        provider_prep.provider_name,
        provider_prep.provider_zipcode,
        provider_prep.provider_type,
        provider_prep.provider_npi
 FROM epbuilder.provider_prep
 ORDER BY provider_prep.provider_id
SEGMENTED BY hash(provider_prep.provider_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.co_ocurrence_of_chronic_episodes /*+createtype(A)*/ 
(
 master_episode_id,
 level,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_count_chronic,
 co_count_all,
 year
)
AS
 SELECT co_ocurrence_of_chronic_episodes.master_episode_id,
        co_ocurrence_of_chronic_episodes.level,
        co_ocurrence_of_chronic_episodes.co_occurence_count_ASTHMA,
        co_ocurrence_of_chronic_episodes.co_occurence_count_ARRBLK,
        co_ocurrence_of_chronic_episodes.co_occurence_count_HF,
        co_ocurrence_of_chronic_episodes.co_occurence_count_COPD,
        co_ocurrence_of_chronic_episodes.co_occurence_count_CAD,
        co_ocurrence_of_chronic_episodes.co_occurence_count_ULCLTS,
        co_ocurrence_of_chronic_episodes.co_occurence_count_BIPLR,
        co_ocurrence_of_chronic_episodes.co_occurence_count_GERD,
        co_ocurrence_of_chronic_episodes.co_occurence_count_HTN,
        co_ocurrence_of_chronic_episodes.co_occurence_count_GLCOMA,
        co_ocurrence_of_chronic_episodes.co_occurence_count_LBP,
        co_ocurrence_of_chronic_episodes.co_occurence_count_CROHNS,
        co_ocurrence_of_chronic_episodes.co_occurence_count_DIAB,
        co_ocurrence_of_chronic_episodes.co_occurence_count_DEPRSN,
        co_ocurrence_of_chronic_episodes.co_occurence_count_OSTEOA,
        co_ocurrence_of_chronic_episodes.co_occurence_count_RHNTS,
        co_ocurrence_of_chronic_episodes.co_occurence_count_DIVERT,
        co_ocurrence_of_chronic_episodes.co_occurence_count_DEPANX,
        co_ocurrence_of_chronic_episodes.co_occurence_count_PTSD,
        co_ocurrence_of_chronic_episodes.co_occurence_count_SCHIZO,
        co_ocurrence_of_chronic_episodes.co_occurence_count_SUDS,
        co_ocurrence_of_chronic_episodes.co_count_chronic,
        co_ocurrence_of_chronic_episodes.co_count_all,
        co_ocurrence_of_chronic_episodes.year
 FROM epbuilder.co_ocurrence_of_chronic_episodes
 ORDER BY co_ocurrence_of_chronic_episodes.master_episode_id,
          co_ocurrence_of_chronic_episodes.level
SEGMENTED BY hash(co_ocurrence_of_chronic_episodes.master_episode_id, co_ocurrence_of_chronic_episodes.level) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_vistualization_claim_type_temp /*+createtype(A)*/ 
(
 master_claim_id,
 member_id,
 allowed_amt,
 assigned_count,
 claim_line_type_code,
 begin_date,
 end_date,
 filter_fail_total,
 maternity_flag,
 episode_count,
 ppr,
 ppv
)
AS
 SELECT member_vistualization_claim_type_temp.master_claim_id,
        member_vistualization_claim_type_temp.member_id,
        member_vistualization_claim_type_temp.allowed_amt,
        member_vistualization_claim_type_temp.assigned_count,
        member_vistualization_claim_type_temp.claim_line_type_code,
        member_vistualization_claim_type_temp.begin_date,
        member_vistualization_claim_type_temp.end_date,
        member_vistualization_claim_type_temp.filter_fail_total,
        member_vistualization_claim_type_temp.maternity_flag,
        member_vistualization_claim_type_temp.episode_count,
        member_vistualization_claim_type_temp.ppr,
        member_vistualization_claim_type_temp.ppv
 FROM epbuilder.member_vistualization_claim_type_temp
 ORDER BY member_vistualization_claim_type_temp.master_claim_id,
          member_vistualization_claim_type_temp.member_id,
          member_vistualization_claim_type_temp.allowed_amt,
          member_vistualization_claim_type_temp.assigned_count,
          member_vistualization_claim_type_temp.claim_line_type_code,
          member_vistualization_claim_type_temp.begin_date,
          member_vistualization_claim_type_temp.end_date
SEGMENTED BY hash(member_vistualization_claim_type_temp.master_claim_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_cost_use /*+createtype(L)*/ 
(
 epi_id,
 epi_number,
 name,
 value
)
AS
 SELECT ra_cost_use.epi_id,
        ra_cost_use.epi_number,
        ra_cost_use.name,
        ra_cost_use.value
 FROM epbuilder.ra_cost_use
 ORDER BY ra_cost_use.epi_id,
          ra_cost_use.epi_number,
          ra_cost_use.name,
          ra_cost_use.value
SEGMENTED BY hash(ra_cost_use.epi_number, ra_cost_use.value, ra_cost_use.name, ra_cost_use.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_cost_use_back_up /*+createtype(A)*/ 
(
 epi_id,
 epi_number,
 name,
 value
)
AS
 SELECT ra_cost_use_back_up.epi_id,
        ra_cost_use_back_up.epi_number,
        ra_cost_use_back_up.name,
        ra_cost_use_back_up.value
 FROM epbuilder.ra_cost_use_back_up
 ORDER BY ra_cost_use_back_up.epi_id,
          ra_cost_use_back_up.epi_number,
          ra_cost_use_back_up.name,
          ra_cost_use_back_up.value
SEGMENTED BY hash(ra_cost_use_back_up.epi_number, ra_cost_use_back_up.value, ra_cost_use_back_up.name, ra_cost_use_back_up.epi_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_vistualization_claim_type /*+createtype(A)*/ 
(
 member_id,
 year,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 assigned_cost,
 assigned_ip_cost,
 assigned_op_cost,
 assigned_pb_cost,
 assigned_rx_cost,
 assigned_cost_unfiltered,
 assigned_ip_cost_unfiltered,
 assigned_op_cost_unfiltered,
 assigned_pb_cost_unfiltered,
 assigned_rx_cost_unfiltered,
 PPR,
 PPV,
 pac_cost,
 cms_age_group,
 gender,
 member_population
)
AS
 SELECT member_vistualization_claim_type.member_id,
        member_vistualization_claim_type.year,
        member_vistualization_claim_type.ip_cost,
        member_vistualization_claim_type.op_cost,
        member_vistualization_claim_type.pb_cost,
        member_vistualization_claim_type.rx_cost,
        member_vistualization_claim_type.assigned_cost,
        member_vistualization_claim_type.assigned_ip_cost,
        member_vistualization_claim_type.assigned_op_cost,
        member_vistualization_claim_type.assigned_pb_cost,
        member_vistualization_claim_type.assigned_rx_cost,
        member_vistualization_claim_type.assigned_cost_unfiltered,
        member_vistualization_claim_type.assigned_ip_cost_unfiltered,
        member_vistualization_claim_type.assigned_op_cost_unfiltered,
        member_vistualization_claim_type.assigned_pb_cost_unfiltered,
        member_vistualization_claim_type.assigned_rx_cost_unfiltered,
        member_vistualization_claim_type.PPR,
        member_vistualization_claim_type.PPV,
        member_vistualization_claim_type.pac_cost,
        member_vistualization_claim_type.cms_age_group,
        member_vistualization_claim_type.gender,
        member_vistualization_claim_type.member_population
 FROM epbuilder.member_vistualization_claim_type
 ORDER BY member_vistualization_claim_type.member_id,
          member_vistualization_claim_type.year,
          member_vistualization_claim_type.ip_cost,
          member_vistualization_claim_type.op_cost,
          member_vistualization_claim_type.pb_cost,
          member_vistualization_claim_type.rx_cost,
          member_vistualization_claim_type.assigned_cost,
          member_vistualization_claim_type.assigned_ip_cost,
          member_vistualization_claim_type.assigned_op_cost,
          member_vistualization_claim_type.assigned_pb_cost,
          member_vistualization_claim_type.assigned_rx_cost,
          member_vistualization_claim_type.assigned_cost_unfiltered,
          member_vistualization_claim_type.assigned_ip_cost_unfiltered,
          member_vistualization_claim_type.assigned_op_cost_unfiltered,
          member_vistualization_claim_type.assigned_pb_cost_unfiltered,
          member_vistualization_claim_type.assigned_rx_cost_unfiltered,
          member_vistualization_claim_type.PPR,
          member_vistualization_claim_type.PPV,
          member_vistualization_claim_type.gender,
          member_vistualization_claim_type.member_population
SEGMENTED BY hash(member_vistualization_claim_type.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.exp_cost_qacrg3_age_gender /*+createtype(A)*/ 
(
 r_year,
 r_fincrg,
 r_cms_age_group,
 r_gender_code,
 sum,
 r_group_exp_cost,
 ppr_group_exp_cost,
 ppv_group_exp_cost
)
AS
 SELECT exp_cost_qacrg3_age_gender.r_year,
        exp_cost_qacrg3_age_gender.r_fincrg,
        exp_cost_qacrg3_age_gender.r_cms_age_group,
        exp_cost_qacrg3_age_gender.r_gender_code,
        exp_cost_qacrg3_age_gender.sum,
        exp_cost_qacrg3_age_gender.r_group_exp_cost,
        exp_cost_qacrg3_age_gender.ppr_group_exp_cost,
        exp_cost_qacrg3_age_gender.ppv_group_exp_cost
 FROM epbuilder.exp_cost_qacrg3_age_gender
 ORDER BY exp_cost_qacrg3_age_gender.r_year,
          exp_cost_qacrg3_age_gender.r_fincrg,
          exp_cost_qacrg3_age_gender.r_cms_age_group,
          exp_cost_qacrg3_age_gender.r_gender_code
SEGMENTED BY hash(exp_cost_qacrg3_age_gender.r_year, exp_cost_qacrg3_age_gender.r_fincrg, exp_cost_qacrg3_age_gender.r_cms_age_group, exp_cost_qacrg3_age_gender.r_gender_code) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.crg_cost_summary_by_member /*+createtype(L)*/ 
(
 member_id,
 year,
 cms_age_group,
 gender,
 fincrg,
 qacrg3_desc,
 actual_cost,
 expected_cost,
 ppr_expected_cost,
 ppv_expected_cost
)
AS
 SELECT crg_cost_summary_by_member.member_id,
        crg_cost_summary_by_member.year,
        crg_cost_summary_by_member.cms_age_group,
        crg_cost_summary_by_member.gender,
        crg_cost_summary_by_member.fincrg,
        crg_cost_summary_by_member.qacrg3_desc,
        crg_cost_summary_by_member.actual_cost,
        crg_cost_summary_by_member.expected_cost,
        crg_cost_summary_by_member.ppr_expected_cost,
        crg_cost_summary_by_member.ppv_expected_cost
 FROM epbuilder.crg_cost_summary_by_member
 ORDER BY crg_cost_summary_by_member.member_id
SEGMENTED BY hash(crg_cost_summary_by_member.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_exp_cost /*+createtype(L)*/ 
(
 row_names,
 epi_number,
 epi_name,
 epi_id,
 eol_prob,
 use_prob_ra_typ_l1,
 cost_pred_ra_typ_l1,
 exp_cost_ra_typ_l1,
 use_prob_sa_typ_l1,
 cost_pred_sa_typ_l1,
 exp_cost_sa_typ_l1,
 use_prob_ra_comp_l1,
 cost_pred_ra_comp_l1,
 exp_cost_ra_comp_l1,
 use_prob_sa_comp_l1,
 cost_pred_sa_comp_l1,
 exp_cost_sa_comp_l1,
 use_prob_ra_typ_l5,
 cost_pred_ra_typ_l5,
 exp_cost_ra_typ_l5,
 use_prob_sa_typ_l5,
 cost_pred_sa_typ_l5,
 exp_cost_sa_typ_l5,
 use_prob_ra_comp_l5,
 cost_pred_ra_comp_l5,
 exp_cost_ra_comp_l5,
 use_prob_sa_comp_l5,
 cost_pred_sa_comp_l5,
 exp_cost_sa_comp_l5,
 total_exp_cost_ra_l1,
 total_exp_cost_sa_l1,
 total_exp_cost_ra_l5,
 total_exp_cost_sa_l5,
 use_prob_ra_typ_ip_l1,
 cost_pred_ra_typ_ip_l1,
 exp_cost_ra_typ_ip_l1,
 use_prob_sa_typ_ip_l1,
 cost_pred_sa_typ_ip_l1,
 exp_cost_sa_typ_ip_l1,
 use_prob_ra_typ_other_l1,
 cost_pred_ra_typ_other_l1,
 exp_cost_ra_typ_other_l1,
 use_prob_sa_typ_other_l1,
 cost_pred_sa_typ_other_l1,
 exp_cost_sa_typ_other_l1,
 use_prob_ra_comp_other_l1,
 cost_pred_ra_comp_other_l1,
 exp_cost_ra_comp_other_l1,
 use_prob_sa_comp_other_l1,
 cost_pred_sa_comp_other_l1,
 exp_cost_sa_comp_other_l1,
 use_prob_ra_typ_ip_l3,
 cost_pred_ra_typ_ip_l3,
 exp_cost_ra_typ_ip_l3,
 use_prob_sa_typ_ip_l3,
 cost_pred_sa_typ_ip_l3,
 exp_cost_sa_typ_ip_l3,
 use_prob_ra_typ_other_l3,
 cost_pred_ra_typ_other_l3,
 exp_cost_ra_typ_other_l3,
 use_prob_sa_typ_other_l3,
 cost_pred_sa_typ_other_l3,
 exp_cost_sa_typ_other_l3,
 use_prob_ra_comp_other_l3,
 cost_pred_ra_comp_other_l3,
 exp_cost_ra_comp_other_l3,
 use_prob_sa_comp_other_l3,
 cost_pred_sa_comp_other_l3,
 exp_cost_sa_comp_other_l3,
 total_exp_cost_ra_l3,
 total_exp_cost_sa_l3
)
AS
 SELECT ra_exp_cost.row_names,
        ra_exp_cost.epi_number,
        ra_exp_cost.epi_name,
        ra_exp_cost.epi_id,
        ra_exp_cost.eol_prob,
        ra_exp_cost.use_prob_ra_typ_l1,
        ra_exp_cost.cost_pred_ra_typ_l1,
        ra_exp_cost.exp_cost_ra_typ_l1,
        ra_exp_cost.use_prob_sa_typ_l1,
        ra_exp_cost.cost_pred_sa_typ_l1,
        ra_exp_cost.exp_cost_sa_typ_l1,
        ra_exp_cost.use_prob_ra_comp_l1,
        ra_exp_cost.cost_pred_ra_comp_l1,
        ra_exp_cost.exp_cost_ra_comp_l1,
        ra_exp_cost.use_prob_sa_comp_l1,
        ra_exp_cost.cost_pred_sa_comp_l1,
        ra_exp_cost.exp_cost_sa_comp_l1,
        ra_exp_cost.use_prob_ra_typ_l5,
        ra_exp_cost.cost_pred_ra_typ_l5,
        ra_exp_cost.exp_cost_ra_typ_l5,
        ra_exp_cost.use_prob_sa_typ_l5,
        ra_exp_cost.cost_pred_sa_typ_l5,
        ra_exp_cost.exp_cost_sa_typ_l5,
        ra_exp_cost.use_prob_ra_comp_l5,
        ra_exp_cost.cost_pred_ra_comp_l5,
        ra_exp_cost.exp_cost_ra_comp_l5,
        ra_exp_cost.use_prob_sa_comp_l5,
        ra_exp_cost.cost_pred_sa_comp_l5,
        ra_exp_cost.exp_cost_sa_comp_l5,
        ra_exp_cost.total_exp_cost_ra_l1,
        ra_exp_cost.total_exp_cost_sa_l1,
        ra_exp_cost.total_exp_cost_ra_l5,
        ra_exp_cost.total_exp_cost_sa_l5,
        ra_exp_cost.use_prob_ra_typ_ip_l1,
        ra_exp_cost.cost_pred_ra_typ_ip_l1,
        ra_exp_cost.exp_cost_ra_typ_ip_l1,
        ra_exp_cost.use_prob_sa_typ_ip_l1,
        ra_exp_cost.cost_pred_sa_typ_ip_l1,
        ra_exp_cost.exp_cost_sa_typ_ip_l1,
        ra_exp_cost.use_prob_ra_typ_other_l1,
        ra_exp_cost.cost_pred_ra_typ_other_l1,
        ra_exp_cost.exp_cost_ra_typ_other_l1,
        ra_exp_cost.use_prob_sa_typ_other_l1,
        ra_exp_cost.cost_pred_sa_typ_other_l1,
        ra_exp_cost.exp_cost_sa_typ_other_l1,
        ra_exp_cost.use_prob_ra_comp_other_l1,
        ra_exp_cost.cost_pred_ra_comp_other_l1,
        ra_exp_cost.exp_cost_ra_comp_other_l1,
        ra_exp_cost.use_prob_sa_comp_other_l1,
        ra_exp_cost.cost_pred_sa_comp_other_l1,
        ra_exp_cost.exp_cost_sa_comp_other_l1,
        ra_exp_cost.use_prob_ra_typ_ip_l3,
        ra_exp_cost.cost_pred_ra_typ_ip_l3,
        ra_exp_cost.exp_cost_ra_typ_ip_l3,
        ra_exp_cost.use_prob_sa_typ_ip_l3,
        ra_exp_cost.cost_pred_sa_typ_ip_l3,
        ra_exp_cost.exp_cost_sa_typ_ip_l3,
        ra_exp_cost.use_prob_ra_typ_other_l3,
        ra_exp_cost.cost_pred_ra_typ_other_l3,
        ra_exp_cost.exp_cost_ra_typ_other_l3,
        ra_exp_cost.use_prob_sa_typ_other_l3,
        ra_exp_cost.cost_pred_sa_typ_other_l3,
        ra_exp_cost.exp_cost_sa_typ_other_l3,
        ra_exp_cost.use_prob_ra_comp_other_l3,
        ra_exp_cost.cost_pred_ra_comp_other_l3,
        ra_exp_cost.exp_cost_ra_comp_other_l3,
        ra_exp_cost.use_prob_sa_comp_other_l3,
        ra_exp_cost.cost_pred_sa_comp_other_l3,
        ra_exp_cost.exp_cost_sa_comp_other_l3,
        ra_exp_cost.total_exp_cost_ra_l3,
        ra_exp_cost.total_exp_cost_sa_l3
 FROM epbuilder.ra_exp_cost
 ORDER BY ra_exp_cost.row_names,
          ra_exp_cost.epi_number,
          ra_exp_cost.epi_name,
          ra_exp_cost.epi_id,
          ra_exp_cost.eol_prob,
          ra_exp_cost.use_prob_ra_typ_l1,
          ra_exp_cost.cost_pred_ra_typ_l1,
          ra_exp_cost.exp_cost_ra_typ_l1,
          ra_exp_cost.use_prob_sa_typ_l1,
          ra_exp_cost.cost_pred_sa_typ_l1,
          ra_exp_cost.exp_cost_sa_typ_l1,
          ra_exp_cost.use_prob_ra_comp_l1,
          ra_exp_cost.cost_pred_ra_comp_l1,
          ra_exp_cost.exp_cost_ra_comp_l1,
          ra_exp_cost.use_prob_sa_comp_l1,
          ra_exp_cost.cost_pred_sa_comp_l1,
          ra_exp_cost.exp_cost_sa_comp_l1,
          ra_exp_cost.use_prob_ra_typ_l5,
          ra_exp_cost.cost_pred_ra_typ_l5,
          ra_exp_cost.exp_cost_ra_typ_l5,
          ra_exp_cost.use_prob_sa_typ_l5,
          ra_exp_cost.cost_pred_sa_typ_l5,
          ra_exp_cost.exp_cost_sa_typ_l5,
          ra_exp_cost.use_prob_ra_comp_l5,
          ra_exp_cost.cost_pred_ra_comp_l5,
          ra_exp_cost.exp_cost_ra_comp_l5,
          ra_exp_cost.use_prob_sa_comp_l5,
          ra_exp_cost.cost_pred_sa_comp_l5,
          ra_exp_cost.exp_cost_sa_comp_l5,
          ra_exp_cost.total_exp_cost_ra_l1,
          ra_exp_cost.total_exp_cost_sa_l1,
          ra_exp_cost.total_exp_cost_ra_l5,
          ra_exp_cost.total_exp_cost_sa_l5,
          ra_exp_cost.use_prob_ra_typ_ip_l1,
          ra_exp_cost.cost_pred_ra_typ_ip_l1,
          ra_exp_cost.exp_cost_ra_typ_ip_l1,
          ra_exp_cost.use_prob_sa_typ_ip_l1,
          ra_exp_cost.cost_pred_sa_typ_ip_l1,
          ra_exp_cost.exp_cost_sa_typ_ip_l1,
          ra_exp_cost.use_prob_ra_typ_other_l1,
          ra_exp_cost.cost_pred_ra_typ_other_l1,
          ra_exp_cost.exp_cost_ra_typ_other_l1,
          ra_exp_cost.use_prob_sa_typ_other_l1,
          ra_exp_cost.cost_pred_sa_typ_other_l1,
          ra_exp_cost.exp_cost_sa_typ_other_l1,
          ra_exp_cost.use_prob_ra_comp_other_l1,
          ra_exp_cost.cost_pred_ra_comp_other_l1,
          ra_exp_cost.exp_cost_ra_comp_other_l1,
          ra_exp_cost.use_prob_sa_comp_other_l1,
          ra_exp_cost.cost_pred_sa_comp_other_l1,
          ra_exp_cost.exp_cost_sa_comp_other_l1,
          ra_exp_cost.use_prob_ra_typ_ip_l3,
          ra_exp_cost.cost_pred_ra_typ_ip_l3,
          ra_exp_cost.exp_cost_ra_typ_ip_l3,
          ra_exp_cost.use_prob_sa_typ_ip_l3,
          ra_exp_cost.cost_pred_sa_typ_ip_l3,
          ra_exp_cost.exp_cost_sa_typ_ip_l3,
          ra_exp_cost.use_prob_ra_typ_other_l3,
          ra_exp_cost.cost_pred_ra_typ_other_l3,
          ra_exp_cost.exp_cost_ra_typ_other_l3,
          ra_exp_cost.use_prob_sa_typ_other_l3,
          ra_exp_cost.cost_pred_sa_typ_other_l3,
          ra_exp_cost.exp_cost_sa_typ_other_l3,
          ra_exp_cost.use_prob_ra_comp_other_l3,
          ra_exp_cost.cost_pred_ra_comp_other_l3,
          ra_exp_cost.exp_cost_ra_comp_other_l3,
          ra_exp_cost.use_prob_sa_comp_other_l3,
          ra_exp_cost.cost_pred_sa_comp_other_l3,
          ra_exp_cost.exp_cost_sa_comp_other_l3,
          ra_exp_cost.total_exp_cost_ra_l3,
          ra_exp_cost.total_exp_cost_sa_l3
SEGMENTED BY hash(ra_exp_cost.eol_prob, ra_exp_cost.use_prob_ra_typ_l1, ra_exp_cost.cost_pred_ra_typ_l1, ra_exp_cost.exp_cost_ra_typ_l1, ra_exp_cost.use_prob_sa_typ_l1, ra_exp_cost.cost_pred_sa_typ_l1, ra_exp_cost.exp_cost_sa_typ_l1, ra_exp_cost.use_prob_ra_comp_l1, ra_exp_cost.cost_pred_ra_comp_l1, ra_exp_cost.exp_cost_ra_comp_l1, ra_exp_cost.use_prob_sa_comp_l1, ra_exp_cost.cost_pred_sa_comp_l1, ra_exp_cost.exp_cost_sa_comp_l1, ra_exp_cost.use_prob_ra_typ_l5, ra_exp_cost.cost_pred_ra_typ_l5, ra_exp_cost.exp_cost_ra_typ_l5, ra_exp_cost.use_prob_sa_typ_l5, ra_exp_cost.cost_pred_sa_typ_l5, ra_exp_cost.exp_cost_sa_typ_l5, ra_exp_cost.use_prob_ra_comp_l5, ra_exp_cost.cost_pred_ra_comp_l5, ra_exp_cost.exp_cost_ra_comp_l5, ra_exp_cost.use_prob_sa_comp_l5, ra_exp_cost.cost_pred_sa_comp_l5, ra_exp_cost.exp_cost_sa_comp_l5, ra_exp_cost.total_exp_cost_ra_l1, ra_exp_cost.total_exp_cost_sa_l1, ra_exp_cost.total_exp_cost_ra_l5, ra_exp_cost.total_exp_cost_sa_l5, ra_exp_cost.use_prob_ra_typ_ip_l1, ra_exp_cost.cost_pred_ra_typ_ip_l1, ra_exp_cost.exp_cost_ra_typ_ip_l1) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.ra_coeffs /*+createtype(L)*/ 
(
 row_names,
 epi_number,
 epi_name,
 parameter,
 eol,
 coef_ra_typ_l1_use,
 coef_ra_typ_l1_cost,
 coef_sa_typ_l1_use,
 coef_sa_typ_l1_cost,
 coef_ra_comp_l1_use,
 coef_ra_comp_l1_cost,
 coef_sa_comp_l1_use,
 coef_sa_comp_l1_cost,
 coef_ra_typ_l5_use,
 coef_ra_typ_l5_cost,
 coef_sa_typ_l5_use,
 coef_sa_typ_l5_cost,
 coef_ra_comp_l5_use,
 coef_ra_comp_l5_cost,
 coef_sa_comp_l5_use,
 coef_sa_comp_l5_cost,
 coef_ra_typ_ip_l1_use,
 coef_ra_typ_ip_l1_cost,
 coef_sa_typ_ip_l1_use,
 coef_sa_typ_ip_l1_cost,
 coef_ra_typ_other_l1_use,
 coef_ra_typ_other_l1_cost,
 coef_sa_typ_other_l1_use,
 coef_sa_typ_other_l1_cost,
 coef_ra_comp_other_l1_use,
 coef_ra_comp_other_l1_cost,
 coef_sa_comp_other_l1_use,
 coef_sa_comp_other_l1_cost,
 coef_ra_typ_ip_l3_use,
 coef_ra_typ_ip_l3_cost,
 coef_sa_typ_ip_l3_use,
 coef_sa_typ_ip_l3_cost,
 coef_ra_typ_other_l3_use,
 coef_ra_typ_other_l3_cost,
 coef_sa_typ_other_l3_use,
 coef_sa_typ_other_l3_cost,
 coef_ra_comp_other_l3_use,
 coef_ra_comp_other_l3_cost,
 coef_sa_comp_other_l3_use,
 coef_sa_comp_other_l3_cost
)
AS
 SELECT ra_coeffs.row_names,
        ra_coeffs.epi_number,
        ra_coeffs.epi_name,
        ra_coeffs.parameter,
        ra_coeffs.eol,
        ra_coeffs.coef_ra_typ_l1_use,
        ra_coeffs.coef_ra_typ_l1_cost,
        ra_coeffs.coef_sa_typ_l1_use,
        ra_coeffs.coef_sa_typ_l1_cost,
        ra_coeffs.coef_ra_comp_l1_use,
        ra_coeffs.coef_ra_comp_l1_cost,
        ra_coeffs.coef_sa_comp_l1_use,
        ra_coeffs.coef_sa_comp_l1_cost,
        ra_coeffs.coef_ra_typ_l5_use,
        ra_coeffs.coef_ra_typ_l5_cost,
        ra_coeffs.coef_sa_typ_l5_use,
        ra_coeffs.coef_sa_typ_l5_cost,
        ra_coeffs.coef_ra_comp_l5_use,
        ra_coeffs.coef_ra_comp_l5_cost,
        ra_coeffs.coef_sa_comp_l5_use,
        ra_coeffs.coef_sa_comp_l5_cost,
        ra_coeffs.coef_ra_typ_ip_l1_use,
        ra_coeffs.coef_ra_typ_ip_l1_cost,
        ra_coeffs.coef_sa_typ_ip_l1_use,
        ra_coeffs.coef_sa_typ_ip_l1_cost,
        ra_coeffs.coef_ra_typ_other_l1_use,
        ra_coeffs.coef_ra_typ_other_l1_cost,
        ra_coeffs.coef_sa_typ_other_l1_use,
        ra_coeffs.coef_sa_typ_other_l1_cost,
        ra_coeffs.coef_ra_comp_other_l1_use,
        ra_coeffs.coef_ra_comp_other_l1_cost,
        ra_coeffs.coef_sa_comp_other_l1_use,
        ra_coeffs.coef_sa_comp_other_l1_cost,
        ra_coeffs.coef_ra_typ_ip_l3_use,
        ra_coeffs.coef_ra_typ_ip_l3_cost,
        ra_coeffs.coef_sa_typ_ip_l3_use,
        ra_coeffs.coef_sa_typ_ip_l3_cost,
        ra_coeffs.coef_ra_typ_other_l3_use,
        ra_coeffs.coef_ra_typ_other_l3_cost,
        ra_coeffs.coef_sa_typ_other_l3_use,
        ra_coeffs.coef_sa_typ_other_l3_cost,
        ra_coeffs.coef_ra_comp_other_l3_use,
        ra_coeffs.coef_ra_comp_other_l3_cost,
        ra_coeffs.coef_sa_comp_other_l3_use,
        ra_coeffs.coef_sa_comp_other_l3_cost
 FROM epbuilder.ra_coeffs
 ORDER BY ra_coeffs.row_names,
          ra_coeffs.epi_number,
          ra_coeffs.epi_name,
          ra_coeffs.parameter,
          ra_coeffs.eol,
          ra_coeffs.coef_ra_typ_l1_use,
          ra_coeffs.coef_ra_typ_l1_cost,
          ra_coeffs.coef_sa_typ_l1_use,
          ra_coeffs.coef_sa_typ_l1_cost,
          ra_coeffs.coef_ra_comp_l1_use,
          ra_coeffs.coef_ra_comp_l1_cost,
          ra_coeffs.coef_sa_comp_l1_use,
          ra_coeffs.coef_sa_comp_l1_cost,
          ra_coeffs.coef_ra_typ_l5_use,
          ra_coeffs.coef_ra_typ_l5_cost,
          ra_coeffs.coef_sa_typ_l5_use,
          ra_coeffs.coef_sa_typ_l5_cost,
          ra_coeffs.coef_ra_comp_l5_use,
          ra_coeffs.coef_ra_comp_l5_cost,
          ra_coeffs.coef_sa_comp_l5_use,
          ra_coeffs.coef_sa_comp_l5_cost,
          ra_coeffs.coef_ra_typ_ip_l1_use,
          ra_coeffs.coef_ra_typ_ip_l1_cost,
          ra_coeffs.coef_sa_typ_ip_l1_use,
          ra_coeffs.coef_sa_typ_ip_l1_cost,
          ra_coeffs.coef_ra_typ_other_l1_use,
          ra_coeffs.coef_ra_typ_other_l1_cost,
          ra_coeffs.coef_sa_typ_other_l1_use,
          ra_coeffs.coef_sa_typ_other_l1_cost,
          ra_coeffs.coef_ra_comp_other_l1_use,
          ra_coeffs.coef_ra_comp_other_l1_cost,
          ra_coeffs.coef_sa_comp_other_l1_use,
          ra_coeffs.coef_sa_comp_other_l1_cost,
          ra_coeffs.coef_ra_typ_ip_l3_use,
          ra_coeffs.coef_ra_typ_ip_l3_cost,
          ra_coeffs.coef_sa_typ_ip_l3_use,
          ra_coeffs.coef_sa_typ_ip_l3_cost,
          ra_coeffs.coef_ra_typ_other_l3_use,
          ra_coeffs.coef_ra_typ_other_l3_cost,
          ra_coeffs.coef_sa_typ_other_l3_use,
          ra_coeffs.coef_sa_typ_other_l3_cost,
          ra_coeffs.coef_ra_comp_other_l3_use,
          ra_coeffs.coef_ra_comp_other_l3_cost,
          ra_coeffs.coef_sa_comp_other_l3_use,
          ra_coeffs.coef_sa_comp_other_l3_cost
SEGMENTED BY hash(ra_coeffs.row_names, ra_coeffs.epi_number, ra_coeffs.epi_name, ra_coeffs.parameter, ra_coeffs.eol, ra_coeffs.coef_ra_typ_l1_use, ra_coeffs.coef_ra_typ_l1_cost, ra_coeffs.coef_sa_typ_l1_use, ra_coeffs.coef_sa_typ_l1_cost, ra_coeffs.coef_ra_comp_l1_use, ra_coeffs.coef_ra_comp_l1_cost, ra_coeffs.coef_sa_comp_l1_use, ra_coeffs.coef_sa_comp_l1_cost, ra_coeffs.coef_ra_typ_l5_use, ra_coeffs.coef_ra_typ_l5_cost, ra_coeffs.coef_sa_typ_l5_use, ra_coeffs.coef_sa_typ_l5_cost, ra_coeffs.coef_ra_comp_l5_use, ra_coeffs.coef_ra_comp_l5_cost, ra_coeffs.coef_sa_comp_l5_use, ra_coeffs.coef_sa_comp_l5_cost, ra_coeffs.coef_ra_typ_ip_l1_use, ra_coeffs.coef_ra_typ_ip_l1_cost, ra_coeffs.coef_sa_typ_ip_l1_use, ra_coeffs.coef_sa_typ_ip_l1_cost, ra_coeffs.coef_ra_typ_other_l1_use, ra_coeffs.coef_ra_typ_other_l1_cost, ra_coeffs.coef_sa_typ_other_l1_use, ra_coeffs.coef_sa_typ_other_l1_cost, ra_coeffs.coef_ra_comp_other_l1_use, ra_coeffs.coef_ra_comp_other_l1_cost, ra_coeffs.coef_sa_comp_other_l1_use) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_sub_population2 /*+createtype(L)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group,
 zip_code,
 county,
 mcregion,
 PPS,
 MCO,
 HH,
 PCP,
 exclusive
)
AS
 SELECT member_sub_population2.member_id,
        member_sub_population2.sub_population,
        member_sub_population2.gender,
        member_sub_population2.birth_year,
        member_sub_population2.age_group,
        member_sub_population2.zip_code,
        member_sub_population2.county,
        member_sub_population2.mcregion,
        member_sub_population2.PPS,
        member_sub_population2.MCO,
        member_sub_population2.HH,
        member_sub_population2.PCP,
        member_sub_population2.exclusive
 FROM epbuilder.member_sub_population2
 ORDER BY member_sub_population2.member_id,
          member_sub_population2.sub_population,
          member_sub_population2.gender,
          member_sub_population2.birth_year,
          member_sub_population2.age_group,
          member_sub_population2.zip_code,
          member_sub_population2.county,
          member_sub_population2.mcregion,
          member_sub_population2.PPS,
          member_sub_population2.MCO,
          member_sub_population2.HH,
          member_sub_population2.PCP,
          member_sub_population2.exclusive
SEGMENTED BY hash(member_sub_population2.gender, member_sub_population2.birth_year, member_sub_population2.exclusive, member_sub_population2.zip_code, member_sub_population2.county, member_sub_population2.sub_population, member_sub_population2.age_group, member_sub_population2.member_id, member_sub_population2.mcregion, member_sub_population2.PPS, member_sub_population2.MCO, member_sub_population2.HH, member_sub_population2.PCP) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.member_sub_population2_exclusive /*+createtype(A)*/ 
(
 member_id,
 sub_population,
 gender,
 birth_year,
 age_group,
 zip_code,
 county,
 mcregion,
 PPS,
 MCO,
 HH,
 PCP,
 exclusive
)
AS
 SELECT member_sub_population2_exclusive.member_id,
        member_sub_population2_exclusive.sub_population,
        member_sub_population2_exclusive.gender,
        member_sub_population2_exclusive.birth_year,
        member_sub_population2_exclusive.age_group,
        member_sub_population2_exclusive.zip_code,
        member_sub_population2_exclusive.county,
        member_sub_population2_exclusive.mcregion,
        member_sub_population2_exclusive.PPS,
        member_sub_population2_exclusive.MCO,
        member_sub_population2_exclusive.HH,
        member_sub_population2_exclusive.PCP,
        member_sub_population2_exclusive.exclusive
 FROM epbuilder.member_sub_population2_exclusive
 ORDER BY member_sub_population2_exclusive.member_id,
          member_sub_population2_exclusive.sub_population,
          member_sub_population2_exclusive.gender,
          member_sub_population2_exclusive.birth_year,
          member_sub_population2_exclusive.age_group,
          member_sub_population2_exclusive.zip_code,
          member_sub_population2_exclusive.county,
          member_sub_population2_exclusive.mcregion,
          member_sub_population2_exclusive.PPS,
          member_sub_population2_exclusive.MCO,
          member_sub_population2_exclusive.HH,
          member_sub_population2_exclusive.PCP,
          member_sub_population2_exclusive.exclusive
SEGMENTED BY hash(member_sub_population2_exclusive.gender, member_sub_population2_exclusive.birth_year, member_sub_population2_exclusive.exclusive, member_sub_population2_exclusive.zip_code, member_sub_population2_exclusive.county, member_sub_population2_exclusive.sub_population, member_sub_population2_exclusive.age_group, member_sub_population2_exclusive.member_id, member_sub_population2_exclusive.mcregion, member_sub_population2_exclusive.PPS, member_sub_population2_exclusive.MCO, member_sub_population2_exclusive.HH, member_sub_population2_exclusive.PCP) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.visual_analysis_table_js /*+createtype(A)*/ 
(
 Analysis_type,
 id,
 episode_id,
 episode_name,
 episode_description,
 episode_type,
 episode_category,
 episode_level,
 member_id,
 member_age,
 cms_age_group,
 gender,
 member_zipcode,
 member_county,
 member_population,
 member_region,
 Split_Total_Cost,
 Split_Total_PAC_Cost,
 Split_Total_Typical_Cost,
 Unsplit_Total_Cost,
 Unsplit_Total_PAC_Cost,
 Unsplit_Total_Typical_Cost,
 Split_Expected_Total_Cost,
 Split_Expected_Typical_IP_Cost,
 Split_Expected_Typical_Other_Cost,
 Split_Expected_PAC_Cost,
 Unsplit_Expected_Total_Cost,
 Unsplit_Expected_Typical_IP_Cost,
 Unsplit_Expected_Typical_Other_Cost,
 Unsplit_Expected_PAC_Cost,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 assigned_cost,
 assigned_ip_cost,
 assigned_op_cost,
 assigned_pb_cost,
 assigned_rx_cost,
 assigned_cost_unfiltered,
 assigned_ip_cost_unfiltered,
 assigned_op_cost_unfiltered,
 assigned_pb_cost_unfiltered,
 assigned_rx_cost_unfiltered,
 pps,
 Facility_ID,
 Facility_ID_provider_name,
 Facility_ID_provider_zipcode,
 Facility_ID_provider_type,
 Physician_ID,
 Physician_ID_provider_name,
 Physician_ID_provider_zipcode,
 Physician_ID_provider_type,
 mco,
 hh,
 pcp,
 vbp_attrib_provider,
 vbp_attrib_provider_zipcode,
 vbp_contractor,
 subgroup,
 year,
 utilization,
 ppr,
 ppv,
 exclusive,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_occurence_count_chronic,
 co_occurence_count_all,
 episode_count,
 episode_count_unfiltered,
 qcrg_code,
 qcrg_desc,
 qacrg1_code,
 qacrg1_desc,
 qacrg2_code,
 qacrg2_desc,
 qacrg3_code,
 qacrg3_desc,
 fincrg,
 q_base,
 q_severity,
 enrolled_num_month,
 vbp_arrangement,
 state_wide_female_percent,
 state_wide_male_percent,
 state_wide_NU_percent,
 state_wide_LU_percent,
 state_wide_percent_co_ASTHMA,
 state_wide_percent_co_ARRBLK,
 state_wide_percent_co_HF,
 state_wide_percent_co_COPD,
 state_wide_percent_co_CAD,
 state_wide_percent_co_ULCLTS,
 state_wide_percent_co_BIPLR,
 state_wide_percent_co_GERD,
 state_wide_percent_co_HTN,
 state_wide_percent_co_GLCOMA,
 state_wide_percent_co_LBP,
 state_wide_percent_co_CROHNS,
 state_wide_percent_co_DIAB,
 state_wide_percent_co_DEPRSN,
 state_wide_percent_co_OSTEOA,
 state_wide_percent_co_RHNTS,
 state_wide_percent_co_DIVERT,
 state_wide_percent_co_DEPANX,
 state_wide_percent_co_PTSD,
 state_wide_percent_co_SCHIZO,
 state_wide_percent_co_SUDS,
 ppr_expected_cost,
 ppv_expected_cost,
 state_wide_avg_split_exp_cost,
 state_wide_avg_unsplit_exp_cost,
 state_wide_avg_split_total_cost,
 state_wide_avg_unsplit_total_cost,
 split_state_wide_pac_percent,
 unsplit_state_wide_pac_percent
)
AS
 SELECT visual_analysis_table_js.Analysis_type,
        visual_analysis_table_js.id,
        visual_analysis_table_js.episode_id,
        visual_analysis_table_js.episode_name,
        visual_analysis_table_js.episode_description,
        visual_analysis_table_js.episode_type,
        visual_analysis_table_js.episode_category,
        visual_analysis_table_js.episode_level,
        visual_analysis_table_js.member_id,
        visual_analysis_table_js.member_age,
        visual_analysis_table_js.cms_age_group,
        visual_analysis_table_js.gender,
        visual_analysis_table_js.member_zipcode,
        visual_analysis_table_js.member_county,
        visual_analysis_table_js.member_population,
        visual_analysis_table_js.member_region,
        visual_analysis_table_js.Split_Total_Cost,
        visual_analysis_table_js.Split_Total_PAC_Cost,
        visual_analysis_table_js.Split_Total_Typical_Cost,
        visual_analysis_table_js.Unsplit_Total_Cost,
        visual_analysis_table_js.Unsplit_Total_PAC_Cost,
        visual_analysis_table_js.Unsplit_Total_Typical_Cost,
        visual_analysis_table_js.Split_Expected_Total_Cost,
        visual_analysis_table_js.Split_Expected_Typical_IP_Cost,
        visual_analysis_table_js.Split_Expected_Typical_Other_Cost,
        visual_analysis_table_js.Split_Expected_PAC_Cost,
        visual_analysis_table_js.Unsplit_Expected_Total_Cost,
        visual_analysis_table_js.Unsplit_Expected_Typical_IP_Cost,
        visual_analysis_table_js.Unsplit_Expected_Typical_Other_Cost,
        visual_analysis_table_js.Unsplit_Expected_PAC_Cost,
        visual_analysis_table_js.ip_cost,
        visual_analysis_table_js.op_cost,
        visual_analysis_table_js.pb_cost,
        visual_analysis_table_js.rx_cost,
        visual_analysis_table_js.assigned_cost,
        visual_analysis_table_js.assigned_ip_cost,
        visual_analysis_table_js.assigned_op_cost,
        visual_analysis_table_js.assigned_pb_cost,
        visual_analysis_table_js.assigned_rx_cost,
        visual_analysis_table_js.assigned_cost_unfiltered,
        visual_analysis_table_js.assigned_ip_cost_unfiltered,
        visual_analysis_table_js.assigned_op_cost_unfiltered,
        visual_analysis_table_js.assigned_pb_cost_unfiltered,
        visual_analysis_table_js.assigned_rx_cost_unfiltered,
        visual_analysis_table_js.pps,
        visual_analysis_table_js.Facility_ID,
        visual_analysis_table_js.Facility_ID_provider_name,
        visual_analysis_table_js.Facility_ID_provider_zipcode,
        visual_analysis_table_js.Facility_ID_provider_type,
        visual_analysis_table_js.Physician_ID,
        visual_analysis_table_js.Physician_ID_provider_name,
        visual_analysis_table_js.Physician_ID_provider_zipcode,
        visual_analysis_table_js.Physician_ID_provider_type,
        visual_analysis_table_js.mco,
        visual_analysis_table_js.hh,
        visual_analysis_table_js.pcp,
        visual_analysis_table_js.vbp_attrib_provider,
        visual_analysis_table_js.vbp_attrib_provider_zipcode,
        visual_analysis_table_js.vbp_contractor,
        visual_analysis_table_js.subgroup,
        visual_analysis_table_js.year,
        visual_analysis_table_js.utilization,
        visual_analysis_table_js.ppr,
        visual_analysis_table_js.ppv,
        visual_analysis_table_js.exclusive,
        visual_analysis_table_js.co_occurence_count_ASTHMA,
        visual_analysis_table_js.co_occurence_count_ARRBLK,
        visual_analysis_table_js.co_occurence_count_HF,
        visual_analysis_table_js.co_occurence_count_COPD,
        visual_analysis_table_js.co_occurence_count_CAD,
        visual_analysis_table_js.co_occurence_count_ULCLTS,
        visual_analysis_table_js.co_occurence_count_BIPLR,
        visual_analysis_table_js.co_occurence_count_GERD,
        visual_analysis_table_js.co_occurence_count_HTN,
        visual_analysis_table_js.co_occurence_count_GLCOMA,
        visual_analysis_table_js.co_occurence_count_LBP,
        visual_analysis_table_js.co_occurence_count_CROHNS,
        visual_analysis_table_js.co_occurence_count_DIAB,
        visual_analysis_table_js.co_occurence_count_DEPRSN,
        visual_analysis_table_js.co_occurence_count_OSTEOA,
        visual_analysis_table_js.co_occurence_count_RHNTS,
        visual_analysis_table_js.co_occurence_count_DIVERT,
        visual_analysis_table_js.co_occurence_count_DEPANX,
        visual_analysis_table_js.co_occurence_count_PTSD,
        visual_analysis_table_js.co_occurence_count_SCHIZO,
        visual_analysis_table_js.co_occurence_count_SUDS,
        visual_analysis_table_js.co_occurence_count_chronic,
        visual_analysis_table_js.co_occurence_count_all,
        visual_analysis_table_js.episode_count,
        visual_analysis_table_js.episode_count_unfiltered,
        visual_analysis_table_js.qcrg_code,
        visual_analysis_table_js.qcrg_desc,
        visual_analysis_table_js.qacrg1_code,
        visual_analysis_table_js.qacrg1_desc,
        visual_analysis_table_js.qacrg2_code,
        visual_analysis_table_js.qacrg2_desc,
        visual_analysis_table_js.qacrg3_code,
        visual_analysis_table_js.qacrg3_desc,
        visual_analysis_table_js.fincrg,
        visual_analysis_table_js.q_base,
        visual_analysis_table_js.q_severity,
        visual_analysis_table_js.enrolled_num_month,
        visual_analysis_table_js.vbp_arrangement,
        visual_analysis_table_js.state_wide_female_percent,
        visual_analysis_table_js.state_wide_male_percent,
        visual_analysis_table_js.state_wide_NU_percent,
        visual_analysis_table_js.state_wide_LU_percent,
        visual_analysis_table_js.state_wide_percent_co_ASTHMA,
        visual_analysis_table_js.state_wide_percent_co_ARRBLK,
        visual_analysis_table_js.state_wide_percent_co_HF,
        visual_analysis_table_js.state_wide_percent_co_COPD,
        visual_analysis_table_js.state_wide_percent_co_CAD,
        visual_analysis_table_js.state_wide_percent_co_ULCLTS,
        visual_analysis_table_js.state_wide_percent_co_BIPLR,
        visual_analysis_table_js.state_wide_percent_co_GERD,
        visual_analysis_table_js.state_wide_percent_co_HTN,
        visual_analysis_table_js.state_wide_percent_co_GLCOMA,
        visual_analysis_table_js.state_wide_percent_co_LBP,
        visual_analysis_table_js.state_wide_percent_co_CROHNS,
        visual_analysis_table_js.state_wide_percent_co_DIAB,
        visual_analysis_table_js.state_wide_percent_co_DEPRSN,
        visual_analysis_table_js.state_wide_percent_co_OSTEOA,
        visual_analysis_table_js.state_wide_percent_co_RHNTS,
        visual_analysis_table_js.state_wide_percent_co_DIVERT,
        visual_analysis_table_js.state_wide_percent_co_DEPANX,
        visual_analysis_table_js.state_wide_percent_co_PTSD,
        visual_analysis_table_js.state_wide_percent_co_SCHIZO,
        visual_analysis_table_js.state_wide_percent_co_SUDS,
        visual_analysis_table_js.ppr_expected_cost,
        visual_analysis_table_js.ppv_expected_cost,
        visual_analysis_table_js.state_wide_avg_split_exp_cost,
        visual_analysis_table_js.state_wide_avg_unsplit_exp_cost,
        visual_analysis_table_js.state_wide_avg_split_total_cost,
        visual_analysis_table_js.state_wide_avg_unsplit_total_cost,
        visual_analysis_table_js.split_state_wide_pac_percent,
        visual_analysis_table_js.unsplit_state_wide_pac_percent
 FROM epbuilder.visual_analysis_table_js
 ORDER BY visual_analysis_table_js.member_id,
          visual_analysis_table_js.id,
          visual_analysis_table_js.episode_id
SEGMENTED BY hash(visual_analysis_table_js.episode_id, visual_analysis_table_js.episode_level, visual_analysis_table_js.member_age, visual_analysis_table_js.cms_age_group, visual_analysis_table_js.gender, visual_analysis_table_js.ip_cost, visual_analysis_table_js.op_cost, visual_analysis_table_js.pb_cost, visual_analysis_table_js.rx_cost, visual_analysis_table_js.year, visual_analysis_table_js.utilization, visual_analysis_table_js.exclusive, visual_analysis_table_js.co_occurence_count_ASTHMA, visual_analysis_table_js.co_occurence_count_ARRBLK, visual_analysis_table_js.co_occurence_count_HF, visual_analysis_table_js.co_occurence_count_COPD, visual_analysis_table_js.co_occurence_count_CAD, visual_analysis_table_js.co_occurence_count_ULCLTS, visual_analysis_table_js.co_occurence_count_BIPLR, visual_analysis_table_js.co_occurence_count_GERD, visual_analysis_table_js.co_occurence_count_HTN, visual_analysis_table_js.co_occurence_count_GLCOMA, visual_analysis_table_js.co_occurence_count_LBP, visual_analysis_table_js.co_occurence_count_CROHNS, visual_analysis_table_js.co_occurence_count_DIAB, visual_analysis_table_js.co_occurence_count_DEPRSN, visual_analysis_table_js.co_occurence_count_OSTEOA, visual_analysis_table_js.co_occurence_count_RHNTS, visual_analysis_table_js.co_occurence_count_DIVERT, visual_analysis_table_js.co_occurence_count_DEPANX, visual_analysis_table_js.co_occurence_count_PTSD, visual_analysis_table_js.co_occurence_count_SCHIZO) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.epi_counts /*+createtype(A)*/ 
(
 member_id,
 year,
 episode_count,
 filtered_episode_count
)
AS
 SELECT epi_counts.member_id,
        epi_counts.year,
        epi_counts.episode_count,
        epi_counts.filtered_episode_count
 FROM epbuilder.epi_counts
 ORDER BY epi_counts.member_id
SEGMENTED BY hash(epi_counts.year, epi_counts.episode_count, epi_counts.filtered_episode_count, epi_counts.member_id) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.temp_main_state_wide_values_2 /*+createtype(A)*/ 
(
 analysis_type,
 vbp_arrangement,
 member_population,
 episode_name,
 year,
 state_wide_female_percent,
 state_wide_male_percent,
 state_wide_LU_percent,
 state_wide_NU_percent,
 state_wide_total,
 state_wide_avg_split_exp_cost,
 state_wide_avg_unsplit_exp_cost,
 state_wide_avg_split_total_cost,
 state_wide_avg_unsplit_total_cost,
 split_state_wide_pac_percent,
 unsplit_state_wide_pac_percent,
 state_wide_percent_co_ASTHMA,
 state_wide_percent_co_ARRBLK,
 state_wide_percent_co_HF,
 state_wide_percent_co_COPD,
 state_wide_percent_co_CAD,
 state_wide_percent_co_ULCLTS,
 state_wide_percent_co_BIPLR,
 state_wide_percent_co_GERD,
 state_wide_percent_co_HTN,
 state_wide_percent_co_GLCOMA,
 state_wide_percent_co_LBP,
 state_wide_percent_co_CROHNS,
 state_wide_percent_co_DIAB,
 state_wide_percent_co_DEPRSN,
 state_wide_percent_co_OSTEOA,
 state_wide_percent_co_RHNTS,
 state_wide_percent_co_DIVERT,
 state_wide_percent_co_DEPANX,
 state_wide_percent_co_PTSD,
 state_wide_percent_co_SCHIZO,
 state_wide_percent_co_SUDS
)
AS
 SELECT temp_main_state_wide_values_2.analysis_type,
        temp_main_state_wide_values_2.vbp_arrangement,
        temp_main_state_wide_values_2.member_population,
        temp_main_state_wide_values_2.episode_name,
        temp_main_state_wide_values_2.year,
        temp_main_state_wide_values_2.state_wide_female_percent,
        temp_main_state_wide_values_2.state_wide_male_percent,
        temp_main_state_wide_values_2.state_wide_LU_percent,
        temp_main_state_wide_values_2.state_wide_NU_percent,
        temp_main_state_wide_values_2.state_wide_total,
        temp_main_state_wide_values_2.state_wide_avg_split_exp_cost,
        temp_main_state_wide_values_2.state_wide_avg_unsplit_exp_cost,
        temp_main_state_wide_values_2.state_wide_avg_split_total_cost,
        temp_main_state_wide_values_2.state_wide_avg_unsplit_total_cost,
        temp_main_state_wide_values_2.split_state_wide_pac_percent,
        temp_main_state_wide_values_2.unsplit_state_wide_pac_percent,
        temp_main_state_wide_values_2.state_wide_percent_co_ASTHMA,
        temp_main_state_wide_values_2.state_wide_percent_co_ARRBLK,
        temp_main_state_wide_values_2.state_wide_percent_co_HF,
        temp_main_state_wide_values_2.state_wide_percent_co_COPD,
        temp_main_state_wide_values_2.state_wide_percent_co_CAD,
        temp_main_state_wide_values_2.state_wide_percent_co_ULCLTS,
        temp_main_state_wide_values_2.state_wide_percent_co_BIPLR,
        temp_main_state_wide_values_2.state_wide_percent_co_GERD,
        temp_main_state_wide_values_2.state_wide_percent_co_HTN,
        temp_main_state_wide_values_2.state_wide_percent_co_GLCOMA,
        temp_main_state_wide_values_2.state_wide_percent_co_LBP,
        temp_main_state_wide_values_2.state_wide_percent_co_CROHNS,
        temp_main_state_wide_values_2.state_wide_percent_co_DIAB,
        temp_main_state_wide_values_2.state_wide_percent_co_DEPRSN,
        temp_main_state_wide_values_2.state_wide_percent_co_OSTEOA,
        temp_main_state_wide_values_2.state_wide_percent_co_RHNTS,
        temp_main_state_wide_values_2.state_wide_percent_co_DIVERT,
        temp_main_state_wide_values_2.state_wide_percent_co_DEPANX,
        temp_main_state_wide_values_2.state_wide_percent_co_PTSD,
        temp_main_state_wide_values_2.state_wide_percent_co_SCHIZO,
        temp_main_state_wide_values_2.state_wide_percent_co_SUDS
 FROM epbuilder.temp_main_state_wide_values_2
 ORDER BY temp_main_state_wide_values_2.analysis_type,
          temp_main_state_wide_values_2.vbp_arrangement,
          temp_main_state_wide_values_2.member_population,
          temp_main_state_wide_values_2.episode_name,
          temp_main_state_wide_values_2.year
SEGMENTED BY hash(temp_main_state_wide_values_2.analysis_type, temp_main_state_wide_values_2.vbp_arrangement, temp_main_state_wide_values_2.member_population, temp_main_state_wide_values_2.episode_name, temp_main_state_wide_values_2.year) ALL NODES KSAFE 1;

CREATE PROJECTION epbuilder.visual_analysis_table_js_subset /*+createtype(A)*/ 
(
 Analysis_type,
 id,
 episode_id,
 episode_name,
 episode_description,
 episode_type,
 episode_category,
 episode_level,
 member_id,
 member_age,
 cms_age_group,
 gender,
 member_zipcode,
 member_county,
 member_population,
 member_region,
 Split_Total_Cost,
 Split_Total_PAC_Cost,
 Split_Total_Typical_Cost,
 Unsplit_Total_Cost,
 Unsplit_Total_PAC_Cost,
 Unsplit_Total_Typical_Cost,
 Split_Expected_Total_Cost,
 Split_Expected_Typical_IP_Cost,
 Split_Expected_Typical_Other_Cost,
 Split_Expected_PAC_Cost,
 Unsplit_Expected_Total_Cost,
 Unsplit_Expected_Typical_IP_Cost,
 Unsplit_Expected_Typical_Other_Cost,
 Unsplit_Expected_PAC_Cost,
 ip_cost,
 op_cost,
 pb_cost,
 rx_cost,
 assigned_cost,
 assigned_ip_cost,
 assigned_op_cost,
 assigned_pb_cost,
 assigned_rx_cost,
 assigned_cost_unfiltered,
 assigned_ip_cost_unfiltered,
 assigned_op_cost_unfiltered,
 assigned_pb_cost_unfiltered,
 assigned_rx_cost_unfiltered,
 pps,
 Facility_ID,
 Facility_ID_provider_name,
 Facility_ID_provider_zipcode,
 Facility_ID_provider_type,
 Physician_ID,
 Physician_ID_provider_name,
 Physician_ID_provider_zipcode,
 Physician_ID_provider_type,
 mco,
 hh,
 pcp,
 vbp_attrib_provider,
 vbp_attrib_provider_zipcode,
 vbp_contractor,
 subgroup,
 year,
 utilization,
 ppr,
 ppv,
 exclusive,
 co_occurence_count_ASTHMA,
 co_occurence_count_ARRBLK,
 co_occurence_count_HF,
 co_occurence_count_COPD,
 co_occurence_count_CAD,
 co_occurence_count_ULCLTS,
 co_occurence_count_BIPLR,
 co_occurence_count_GERD,
 co_occurence_count_HTN,
 co_occurence_count_GLCOMA,
 co_occurence_count_LBP,
 co_occurence_count_CROHNS,
 co_occurence_count_DIAB,
 co_occurence_count_DEPRSN,
 co_occurence_count_OSTEOA,
 co_occurence_count_RHNTS,
 co_occurence_count_DIVERT,
 co_occurence_count_DEPANX,
 co_occurence_count_PTSD,
 co_occurence_count_SCHIZO,
 co_occurence_count_SUDS,
 co_occurence_count_chronic,
 co_occurence_count_all,
 episode_count,
 episode_count_unfiltered,
 qcrg_code,
 qcrg_desc,
 qacrg1_code,
 qacrg1_desc,
 qacrg2_code,
 qacrg2_desc,
 qacrg3_code,
 qacrg3_desc,
 fincrg,
 q_base,
 q_severity,
 enrolled_num_month,
 vbp_arrangement,
 state_wide_female_percent,
 state_wide_male_percent,
 state_wide_NU_percent,
 state_wide_LU_percent,
 state_wide_percent_co_ASTHMA,
 state_wide_percent_co_ARRBLK,
 state_wide_percent_co_HF,
 state_wide_percent_co_COPD,
 state_wide_percent_co_CAD,
 state_wide_percent_co_ULCLTS,
 state_wide_percent_co_BIPLR,
 state_wide_percent_co_GERD,
 state_wide_percent_co_HTN,
 state_wide_percent_co_GLCOMA,
 state_wide_percent_co_LBP,
 state_wide_percent_co_CROHNS,
 state_wide_percent_co_DIAB,
 state_wide_percent_co_DEPRSN,
 state_wide_percent_co_OSTEOA,
 state_wide_percent_co_RHNTS,
 state_wide_percent_co_DIVERT,
 state_wide_percent_co_DEPANX,
 state_wide_percent_co_PTSD,
 state_wide_percent_co_SCHIZO,
 state_wide_percent_co_SUDS,
 ppr_expected_cost,
 ppv_expected_cost,
 state_wide_avg_split_exp_cost,
 state_wide_avg_unsplit_exp_cost,
 state_wide_avg_split_total_cost,
 state_wide_avg_unsplit_total_cost,
 split_state_wide_pac_percent,
 unsplit_state_wide_pac_percent
)
AS
 SELECT visual_analysis_table_js_subset.Analysis_type,
        visual_analysis_table_js_subset.id,
        visual_analysis_table_js_subset.episode_id,
        visual_analysis_table_js_subset.episode_name,
        visual_analysis_table_js_subset.episode_description,
        visual_analysis_table_js_subset.episode_type,
        visual_analysis_table_js_subset.episode_category,
        visual_analysis_table_js_subset.episode_level,
        visual_analysis_table_js_subset.member_id,
        visual_analysis_table_js_subset.member_age,
        visual_analysis_table_js_subset.cms_age_group,
        visual_analysis_table_js_subset.gender,
        visual_analysis_table_js_subset.member_zipcode,
        visual_analysis_table_js_subset.member_county,
        visual_analysis_table_js_subset.member_population,
        visual_analysis_table_js_subset.member_region,
        visual_analysis_table_js_subset.Split_Total_Cost,
        visual_analysis_table_js_subset.Split_Total_PAC_Cost,
        visual_analysis_table_js_subset.Split_Total_Typical_Cost,
        visual_analysis_table_js_subset.Unsplit_Total_Cost,
        visual_analysis_table_js_subset.Unsplit_Total_PAC_Cost,
        visual_analysis_table_js_subset.Unsplit_Total_Typical_Cost,
        visual_analysis_table_js_subset.Split_Expected_Total_Cost,
        visual_analysis_table_js_subset.Split_Expected_Typical_IP_Cost,
        visual_analysis_table_js_subset.Split_Expected_Typical_Other_Cost,
        visual_analysis_table_js_subset.Split_Expected_PAC_Cost,
        visual_analysis_table_js_subset.Unsplit_Expected_Total_Cost,
        visual_analysis_table_js_subset.Unsplit_Expected_Typical_IP_Cost,
        visual_analysis_table_js_subset.Unsplit_Expected_Typical_Other_Cost,
        visual_analysis_table_js_subset.Unsplit_Expected_PAC_Cost,
        visual_analysis_table_js_subset.ip_cost,
        visual_analysis_table_js_subset.op_cost,
        visual_analysis_table_js_subset.pb_cost,
        visual_analysis_table_js_subset.rx_cost,
        visual_analysis_table_js_subset.assigned_cost,
        visual_analysis_table_js_subset.assigned_ip_cost,
        visual_analysis_table_js_subset.assigned_op_cost,
        visual_analysis_table_js_subset.assigned_pb_cost,
        visual_analysis_table_js_subset.assigned_rx_cost,
        visual_analysis_table_js_subset.assigned_cost_unfiltered,
        visual_analysis_table_js_subset.assigned_ip_cost_unfiltered,
        visual_analysis_table_js_subset.assigned_op_cost_unfiltered,
        visual_analysis_table_js_subset.assigned_pb_cost_unfiltered,
        visual_analysis_table_js_subset.assigned_rx_cost_unfiltered,
        visual_analysis_table_js_subset.pps,
        visual_analysis_table_js_subset.Facility_ID,
        visual_analysis_table_js_subset.Facility_ID_provider_name,
        visual_analysis_table_js_subset.Facility_ID_provider_zipcode,
        visual_analysis_table_js_subset.Facility_ID_provider_type,
        visual_analysis_table_js_subset.Physician_ID,
        visual_analysis_table_js_subset.Physician_ID_provider_name,
        visual_analysis_table_js_subset.Physician_ID_provider_zipcode,
        visual_analysis_table_js_subset.Physician_ID_provider_type,
        visual_analysis_table_js_subset.mco,
        visual_analysis_table_js_subset.hh,
        visual_analysis_table_js_subset.pcp,
        visual_analysis_table_js_subset.vbp_attrib_provider,
        visual_analysis_table_js_subset.vbp_attrib_provider_zipcode,
        visual_analysis_table_js_subset.vbp_contractor,
        visual_analysis_table_js_subset.subgroup,
        visual_analysis_table_js_subset.year,
        visual_analysis_table_js_subset.utilization,
        visual_analysis_table_js_subset.ppr,
        visual_analysis_table_js_subset.ppv,
        visual_analysis_table_js_subset.exclusive,
        visual_analysis_table_js_subset.co_occurence_count_ASTHMA,
        visual_analysis_table_js_subset.co_occurence_count_ARRBLK,
        visual_analysis_table_js_subset.co_occurence_count_HF,
        visual_analysis_table_js_subset.co_occurence_count_COPD,
        visual_analysis_table_js_subset.co_occurence_count_CAD,
        visual_analysis_table_js_subset.co_occurence_count_ULCLTS,
        visual_analysis_table_js_subset.co_occurence_count_BIPLR,
        visual_analysis_table_js_subset.co_occurence_count_GERD,
        visual_analysis_table_js_subset.co_occurence_count_HTN,
        visual_analysis_table_js_subset.co_occurence_count_GLCOMA,
        visual_analysis_table_js_subset.co_occurence_count_LBP,
        visual_analysis_table_js_subset.co_occurence_count_CROHNS,
        visual_analysis_table_js_subset.co_occurence_count_DIAB,
        visual_analysis_table_js_subset.co_occurence_count_DEPRSN,
        visual_analysis_table_js_subset.co_occurence_count_OSTEOA,
        visual_analysis_table_js_subset.co_occurence_count_RHNTS,
        visual_analysis_table_js_subset.co_occurence_count_DIVERT,
        visual_analysis_table_js_subset.co_occurence_count_DEPANX,
        visual_analysis_table_js_subset.co_occurence_count_PTSD,
        visual_analysis_table_js_subset.co_occurence_count_SCHIZO,
        visual_analysis_table_js_subset.co_occurence_count_SUDS,
        visual_analysis_table_js_subset.co_occurence_count_chronic,
        visual_analysis_table_js_subset.co_occurence_count_all,
        visual_analysis_table_js_subset.episode_count,
        visual_analysis_table_js_subset.episode_count_unfiltered,
        visual_analysis_table_js_subset.qcrg_code,
        visual_analysis_table_js_subset.qcrg_desc,
        visual_analysis_table_js_subset.qacrg1_code,
        visual_analysis_table_js_subset.qacrg1_desc,
        visual_analysis_table_js_subset.qacrg2_code,
        visual_analysis_table_js_subset.qacrg2_desc,
        visual_analysis_table_js_subset.qacrg3_code,
        visual_analysis_table_js_subset.qacrg3_desc,
        visual_analysis_table_js_subset.fincrg,
        visual_analysis_table_js_subset.q_base,
        visual_analysis_table_js_subset.q_severity,
        visual_analysis_table_js_subset.enrolled_num_month,
        visual_analysis_table_js_subset.vbp_arrangement,
        visual_analysis_table_js_subset.state_wide_female_percent,
        visual_analysis_table_js_subset.state_wide_male_percent,
        visual_analysis_table_js_subset.state_wide_NU_percent,
        visual_analysis_table_js_subset.state_wide_LU_percent,
        visual_analysis_table_js_subset.state_wide_percent_co_ASTHMA,
        visual_analysis_table_js_subset.state_wide_percent_co_ARRBLK,
        visual_analysis_table_js_subset.state_wide_percent_co_HF,
        visual_analysis_table_js_subset.state_wide_percent_co_COPD,
        visual_analysis_table_js_subset.state_wide_percent_co_CAD,
        visual_analysis_table_js_subset.state_wide_percent_co_ULCLTS,
        visual_analysis_table_js_subset.state_wide_percent_co_BIPLR,
        visual_analysis_table_js_subset.state_wide_percent_co_GERD,
        visual_analysis_table_js_subset.state_wide_percent_co_HTN,
        visual_analysis_table_js_subset.state_wide_percent_co_GLCOMA,
        visual_analysis_table_js_subset.state_wide_percent_co_LBP,
        visual_analysis_table_js_subset.state_wide_percent_co_CROHNS,
        visual_analysis_table_js_subset.state_wide_percent_co_DIAB,
        visual_analysis_table_js_subset.state_wide_percent_co_DEPRSN,
        visual_analysis_table_js_subset.state_wide_percent_co_OSTEOA,
        visual_analysis_table_js_subset.state_wide_percent_co_RHNTS,
        visual_analysis_table_js_subset.state_wide_percent_co_DIVERT,
        visual_analysis_table_js_subset.state_wide_percent_co_DEPANX,
        visual_analysis_table_js_subset.state_wide_percent_co_PTSD,
        visual_analysis_table_js_subset.state_wide_percent_co_SCHIZO,
        visual_analysis_table_js_subset.state_wide_percent_co_SUDS,
        visual_analysis_table_js_subset.ppr_expected_cost,
        visual_analysis_table_js_subset.ppv_expected_cost,
        visual_analysis_table_js_subset.state_wide_avg_split_exp_cost,
        visual_analysis_table_js_subset.state_wide_avg_unsplit_exp_cost,
        visual_analysis_table_js_subset.state_wide_avg_split_total_cost,
        visual_analysis_table_js_subset.state_wide_avg_unsplit_total_cost,
        visual_analysis_table_js_subset.split_state_wide_pac_percent,
        visual_analysis_table_js_subset.unsplit_state_wide_pac_percent
 FROM epbuilder.visual_analysis_table_js_subset
 ORDER BY visual_analysis_table_js_subset.Analysis_type,
          visual_analysis_table_js_subset.id,
          visual_analysis_table_js_subset.episode_id,
          visual_analysis_table_js_subset.episode_name,
          visual_analysis_table_js_subset.episode_description,
          visual_analysis_table_js_subset.episode_type,
          visual_analysis_table_js_subset.episode_category,
          visual_analysis_table_js_subset.episode_level,
          visual_analysis_table_js_subset.member_id,
          visual_analysis_table_js_subset.member_age,
          visual_analysis_table_js_subset.cms_age_group,
          visual_analysis_table_js_subset.gender,
          visual_analysis_table_js_subset.member_zipcode,
          visual_analysis_table_js_subset.member_county,
          visual_analysis_table_js_subset.member_population,
          visual_analysis_table_js_subset.member_region,
          visual_analysis_table_js_subset.Split_Total_Cost,
          visual_analysis_table_js_subset.Split_Total_PAC_Cost,
          visual_analysis_table_js_subset.Split_Total_Typical_Cost,
          visual_analysis_table_js_subset.Unsplit_Total_Cost,
          visual_analysis_table_js_subset.Unsplit_Total_PAC_Cost,
          visual_analysis_table_js_subset.Unsplit_Total_Typical_Cost,
          visual_analysis_table_js_subset.Split_Expected_Total_Cost,
          visual_analysis_table_js_subset.Split_Expected_Typical_IP_Cost,
          visual_analysis_table_js_subset.Split_Expected_Typical_Other_Cost,
          visual_analysis_table_js_subset.Split_Expected_PAC_Cost,
          visual_analysis_table_js_subset.Unsplit_Expected_Total_Cost,
          visual_analysis_table_js_subset.Unsplit_Expected_Typical_IP_Cost,
          visual_analysis_table_js_subset.Unsplit_Expected_Typical_Other_Cost,
          visual_analysis_table_js_subset.Unsplit_Expected_PAC_Cost,
          visual_analysis_table_js_subset.ip_cost,
          visual_analysis_table_js_subset.op_cost,
          visual_analysis_table_js_subset.pb_cost,
          visual_analysis_table_js_subset.rx_cost,
          visual_analysis_table_js_subset.assigned_cost,
          visual_analysis_table_js_subset.assigned_ip_cost,
          visual_analysis_table_js_subset.assigned_op_cost,
          visual_analysis_table_js_subset.assigned_pb_cost,
          visual_analysis_table_js_subset.assigned_rx_cost,
          visual_analysis_table_js_subset.assigned_cost_unfiltered,
          visual_analysis_table_js_subset.assigned_ip_cost_unfiltered,
          visual_analysis_table_js_subset.assigned_op_cost_unfiltered,
          visual_analysis_table_js_subset.assigned_pb_cost_unfiltered,
          visual_analysis_table_js_subset.assigned_rx_cost_unfiltered,
          visual_analysis_table_js_subset.pps,
          visual_analysis_table_js_subset.Facility_ID,
          visual_analysis_table_js_subset.Facility_ID_provider_name,
          visual_analysis_table_js_subset.Facility_ID_provider_zipcode,
          visual_analysis_table_js_subset.Facility_ID_provider_type,
          visual_analysis_table_js_subset.Physician_ID,
          visual_analysis_table_js_subset.Physician_ID_provider_name,
          visual_analysis_table_js_subset.Physician_ID_provider_zipcode,
          visual_analysis_table_js_subset.Physician_ID_provider_type,
          visual_analysis_table_js_subset.mco,
          visual_analysis_table_js_subset.hh,
          visual_analysis_table_js_subset.pcp,
          visual_analysis_table_js_subset.vbp_attrib_provider,
          visual_analysis_table_js_subset.vbp_attrib_provider_zipcode,
          visual_analysis_table_js_subset.vbp_contractor,
          visual_analysis_table_js_subset.subgroup,
          visual_analysis_table_js_subset.year,
          visual_analysis_table_js_subset.utilization,
          visual_analysis_table_js_subset.ppr,
          visual_analysis_table_js_subset.ppv,
          visual_analysis_table_js_subset.exclusive,
          visual_analysis_table_js_subset.co_occurence_count_ASTHMA,
          visual_analysis_table_js_subset.co_occurence_count_ARRBLK,
          visual_analysis_table_js_subset.co_occurence_count_HF,
          visual_analysis_table_js_subset.co_occurence_count_COPD,
          visual_analysis_table_js_subset.co_occurence_count_CAD,
          visual_analysis_table_js_subset.co_occurence_count_ULCLTS,
          visual_analysis_table_js_subset.co_occurence_count_BIPLR,
          visual_analysis_table_js_subset.co_occurence_count_GERD,
          visual_analysis_table_js_subset.co_occurence_count_HTN,
          visual_analysis_table_js_subset.co_occurence_count_GLCOMA,
          visual_analysis_table_js_subset.co_occurence_count_LBP,
          visual_analysis_table_js_subset.co_occurence_count_CROHNS,
          visual_analysis_table_js_subset.co_occurence_count_DIAB,
          visual_analysis_table_js_subset.co_occurence_count_DEPRSN,
          visual_analysis_table_js_subset.co_occurence_count_OSTEOA,
          visual_analysis_table_js_subset.co_occurence_count_RHNTS,
          visual_analysis_table_js_subset.co_occurence_count_DIVERT,
          visual_analysis_table_js_subset.co_occurence_count_DEPANX,
          visual_analysis_table_js_subset.co_occurence_count_PTSD,
          visual_analysis_table_js_subset.co_occurence_count_SCHIZO,
          visual_analysis_table_js_subset.co_occurence_count_SUDS,
          visual_analysis_table_js_subset.co_occurence_count_chronic,
          visual_analysis_table_js_subset.co_occurence_count_all,
          visual_analysis_table_js_subset.episode_count,
          visual_analysis_table_js_subset.episode_count_unfiltered,
          visual_analysis_table_js_subset.qcrg_code,
          visual_analysis_table_js_subset.qcrg_desc,
          visual_analysis_table_js_subset.qacrg1_code,
          visual_analysis_table_js_subset.qacrg1_desc,
          visual_analysis_table_js_subset.qacrg2_code,
          visual_analysis_table_js_subset.qacrg2_desc,
          visual_analysis_table_js_subset.qacrg3_code,
          visual_analysis_table_js_subset.qacrg3_desc,
          visual_analysis_table_js_subset.fincrg,
          visual_analysis_table_js_subset.q_base,
          visual_analysis_table_js_subset.q_severity,
          visual_analysis_table_js_subset.enrolled_num_month,
          visual_analysis_table_js_subset.vbp_arrangement,
          visual_analysis_table_js_subset.state_wide_female_percent,
          visual_analysis_table_js_subset.state_wide_male_percent,
          visual_analysis_table_js_subset.state_wide_NU_percent,
          visual_analysis_table_js_subset.state_wide_LU_percent,
          visual_analysis_table_js_subset.state_wide_percent_co_ASTHMA,
          visual_analysis_table_js_subset.state_wide_percent_co_ARRBLK,
          visual_analysis_table_js_subset.state_wide_percent_co_HF,
          visual_analysis_table_js_subset.state_wide_percent_co_COPD,
          visual_analysis_table_js_subset.state_wide_percent_co_CAD,
          visual_analysis_table_js_subset.state_wide_percent_co_ULCLTS,
          visual_analysis_table_js_subset.state_wide_percent_co_BIPLR,
          visual_analysis_table_js_subset.state_wide_percent_co_GERD,
          visual_analysis_table_js_subset.state_wide_percent_co_HTN,
          visual_analysis_table_js_subset.state_wide_percent_co_GLCOMA,
          visual_analysis_table_js_subset.state_wide_percent_co_LBP,
          visual_analysis_table_js_subset.state_wide_percent_co_CROHNS,
          visual_analysis_table_js_subset.state_wide_percent_co_DIAB,
          visual_analysis_table_js_subset.state_wide_percent_co_DEPRSN,
          visual_analysis_table_js_subset.state_wide_percent_co_OSTEOA,
          visual_analysis_table_js_subset.state_wide_percent_co_RHNTS,
          visual_analysis_table_js_subset.state_wide_percent_co_DIVERT,
          visual_analysis_table_js_subset.state_wide_percent_co_DEPANX,
          visual_analysis_table_js_subset.state_wide_percent_co_PTSD,
          visual_analysis_table_js_subset.state_wide_percent_co_SCHIZO,
          visual_analysis_table_js_subset.state_wide_percent_co_SUDS,
          visual_analysis_table_js_subset.ppr_expected_cost,
          visual_analysis_table_js_subset.ppv_expected_cost,
          visual_analysis_table_js_subset.state_wide_avg_split_exp_cost,
          visual_analysis_table_js_subset.state_wide_avg_unsplit_exp_cost,
          visual_analysis_table_js_subset.state_wide_avg_split_total_cost,
          visual_analysis_table_js_subset.state_wide_avg_unsplit_total_cost,
          visual_analysis_table_js_subset.split_state_wide_pac_percent,
          visual_analysis_table_js_subset.unsplit_state_wide_pac_percent
SEGMENTED BY hash(visual_analysis_table_js_subset.episode_id, visual_analysis_table_js_subset.episode_level, visual_analysis_table_js_subset.member_age, visual_analysis_table_js_subset.cms_age_group, visual_analysis_table_js_subset.gender, visual_analysis_table_js_subset.ip_cost, visual_analysis_table_js_subset.op_cost, visual_analysis_table_js_subset.pb_cost, visual_analysis_table_js_subset.rx_cost, visual_analysis_table_js_subset.year, visual_analysis_table_js_subset.utilization, visual_analysis_table_js_subset.exclusive, visual_analysis_table_js_subset.co_occurence_count_ASTHMA, visual_analysis_table_js_subset.co_occurence_count_ARRBLK, visual_analysis_table_js_subset.co_occurence_count_HF, visual_analysis_table_js_subset.co_occurence_count_COPD, visual_analysis_table_js_subset.co_occurence_count_CAD, visual_analysis_table_js_subset.co_occurence_count_ULCLTS, visual_analysis_table_js_subset.co_occurence_count_BIPLR, visual_analysis_table_js_subset.co_occurence_count_GERD, visual_analysis_table_js_subset.co_occurence_count_HTN, visual_analysis_table_js_subset.co_occurence_count_GLCOMA, visual_analysis_table_js_subset.co_occurence_count_LBP, visual_analysis_table_js_subset.co_occurence_count_CROHNS, visual_analysis_table_js_subset.co_occurence_count_DIAB, visual_analysis_table_js_subset.co_occurence_count_DEPRSN, visual_analysis_table_js_subset.co_occurence_count_OSTEOA, visual_analysis_table_js_subset.co_occurence_count_RHNTS, visual_analysis_table_js_subset.co_occurence_count_DIVERT, visual_analysis_table_js_subset.co_occurence_count_DEPANX, visual_analysis_table_js_subset.co_occurence_count_PTSD, visual_analysis_table_js_subset.co_occurence_count_SCHIZO) ALL NODES KSAFE 1;


CREATE  VIEW epbuilder.mbr_crg_fact_view AS
 SELECT (MBR_CRG_FACT.YEAR)::int AS YEAR,
        MBR_CRG_FACT.QUARTER,
        MBR_CRG_FACT.CRG_VERSION,
        MBR_CRG_FACT.CLAIM_BEGIN_DATE,
        MBR_CRG_FACT.CLAIM_END_DATE,
        MBR_CRG_FACT.CLAIMS_AS_OF_DATE,
        MBR_CRG_FACT.RECIP_ID,
        (MBR_CRG_FACT.QCRG_CODE)::int AS QCRG_CODE,
        (MBR_CRG_FACT.QACRG1_CODE)::int AS QACRG1_CODE,
        (MBR_CRG_FACT.QACRG2_CODE)::int AS QACRG2_CODE,
        (MBR_CRG_FACT.QACRG3_CODE)::int AS QACRG3_CODE,
        MBR_CRG_FACT.FINCRG_Q,
        MBR_CRG_FACT.Q_BASE,
        MBR_CRG_FACT.Q_SEVERITY,
        MBR_CRG_FACT.CRG_CODE,
        (MBR_CRG_FACT.ACRG1_CODE)::int AS ACRG1_CODE,
        (MBR_CRG_FACT.ACRG2_CODE)::int AS ACRG2_CODE,
        (MBR_CRG_FACT.ACRG3_CODE)::int AS ACRG3_CODE,
        MBR_CRG_FACT.FINCRG_G,
        MBR_CRG_FACT.G_BASE,
        MBR_CRG_FACT.G_SEVERITY
 FROM epbuilder.MBR_CRG_FACT;

CREATE  VIEW epbuilder.exp_cost_qacrg3_age_gender2 AS
 SELECT crg.year AS r_year,
        crg.fincrg_q AS r_fincrg_q,
        CASE WHEN ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) < 6::numeric(18,0)) THEN '< 6'::varchar(3) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 6::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 11::numeric(18,0))) THEN '6 - 11'::varchar(6) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 12::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 17::numeric(18,0))) THEN '12 - 17'::varchar(7) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 18::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 44::numeric(18,0))) THEN '18 - 44'::varchar(7) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 45::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 64::numeric(18,0))) THEN '45 - 64'::varchar(7) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 65::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 200::numeric(18,0))) THEN '>= 65'::varchar(5) WHEN ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) > 200::numeric(18,0)) THEN NULL ELSE NULL END AS r_cms_age_group,
        member.gender_code AS r_gender_code,
        (sum(cc.allowed_amt) / count(DISTINCT cc.member_id)) AS r_group_exp_cost
 FROM (((epbuilder.crg LEFT JOIN epbuilder.claims_combined cc ON (((cc.member_id = (crg.recip_id)::varchar(11)) AND (date_part('year'::varchar(4), cc.begin_date) = crg.year)))) LEFT JOIN epbuilder.member ON ((cc.member_id = member.member_id))) LEFT JOIN epbuilder.member_sub_population msp ON ((msp.member_id = cc.member_id)))
 WHERE (msp.sub_population <> ALL (ARRAY['MLTC'::varchar(4), 'DD'::varchar(2)]))
 GROUP BY crg.year,
          crg.fincrg_q,
          CASE WHEN ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) < 6::numeric(18,0)) THEN '< 6'::varchar(3) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 6::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 11::numeric(18,0))) THEN '6 - 11'::varchar(6) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 12::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 17::numeric(18,0))) THEN '12 - 17'::varchar(7) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 18::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 44::numeric(18,0))) THEN '18 - 44'::varchar(7) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 45::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 64::numeric(18,0))) THEN '45 - 64'::varchar(7) WHEN (((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) >= 65::numeric(18,0)) AND ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) <= 200::numeric(18,0))) THEN '>= 65'::varchar(5) WHEN ((date_part('year'::varchar(4), cc.begin_date) - member.birth_year) > 200::numeric(18,0)) THEN NULL ELSE NULL END,
          member.gender_code;

CREATE  VIEW epbuilder.mom_baby_wo_multi_mom AS
 SELECT max(mom_baby.ENCRYPT_RECIP_ID_MOM) AS ENCRYPT_RECIP_ID_MOM,
        mom_baby.ENCRYPT_RECIP_ID_BABY
 FROM epbuilder.mom_baby
 GROUP BY mom_baby.ENCRYPT_RECIP_ID_BABY
 HAVING (count(*) = 1);

CREATE  VIEW epbuilder.normal_delivery_mom AS
 SELECT all_mom.member_id
 FROM (((( SELECT episode.member_id
 FROM epbuilder.episode
 WHERE (episode.episode_id = ANY (ARRAY['EP1403'::varchar(6), 'EP1404'::varchar(6)]))
 GROUP BY episode.member_id) all_mom LEFT JOIN ( SELECT DISTINCT episode.member_id
 FROM epbuilder.episode
 WHERE (episode.episode_id = ANY (ARRAY['EP1403'::varchar(6), 'EP1404'::varchar(6)]))
 GROUP BY episode.member_id,
          episode.episode_begin_date
 HAVING (count(*) = 2)
 ORDER BY episode.member_id) abnormal_mom1 ON ((all_mom.member_id = abnormal_mom1.member_id))) LEFT JOIN ( SELECT episode.member_id
 FROM epbuilder.episode
 WHERE (episode.episode_id = ANY (ARRAY['EP1403'::varchar(6), 'EP1404'::varchar(6)]))
 GROUP BY episode.member_id
 HAVING ((count(*) > 1) AND ((max(episode.episode_begin_date) - min(episode.episode_begin_date)) <= 250))) abnormal_mom2 ON ((all_mom.member_id = abnormal_mom2.member_id))) LEFT JOIN ( SELECT episode.member_id
 FROM epbuilder.episode
 WHERE (episode.episode_id = ANY (ARRAY['EP1403'::varchar(6), 'EP1404'::varchar(6)]))
 GROUP BY episode.member_id
 HAVING ((count(*) > 1) AND ((max(episode.episode_end_date) - min(episode.episode_end_date)) <= 250))) abnormal_mom3 ON ((all_mom.member_id = abnormal_mom3.member_id)))
 WHERE ((abnormal_mom1.member_id IS NULL) AND (abnormal_mom2.member_id IS NULL) AND (abnormal_mom3.member_id IS NULL));

CREATE  VIEW epbuilder.baby_wo_multiple_birth AS
 SELECT episode.member_id AS baby_id
 FROM epbuilder.episode
 WHERE (episode.episode_id = 'EX1502'::varchar(6))
 GROUP BY episode.member_id
 HAVING (count(*) = 1);

CREATE  VIEW epbuilder.visual_analysis_table_view AS
 SELECT visual_analysis_table.analysis_type,
        visual_analysis_table.id,
        visual_analysis_table.episode_id,
        visual_analysis_table.episode_name,
        visual_analysis_table.episode_description,
        visual_analysis_table.episode_type,
        visual_analysis_table.episode_category,
        visual_analysis_table.episode_level,
        visual_analysis_table.member_id,
        visual_analysis_table.member_age,
        visual_analysis_table.cms_age_group,
        visual_analysis_table.gender,
        visual_analysis_table.member_zipcode,
        visual_analysis_table.member_county,
        visual_analysis_table.member_population,
        visual_analysis_table.total_cost,
        visual_analysis_table.expected_cost,
        visual_analysis_table.pac_cost,
        visual_analysis_table.typical_cost,
        visual_analysis_table.expected_pac_cost,
        visual_analysis_table.expected_typical_cost,
        visual_analysis_table.ip_cost,
        visual_analysis_table.op_cost,
        visual_analysis_table.pb_cost,
        visual_analysis_table.rx_cost,
        visual_analysis_table.assigned_cost,
        visual_analysis_table.assigned_ip_cost,
        visual_analysis_table.assigned_op_cost,
        visual_analysis_table.assigned_pb_cost,
        visual_analysis_table.assigned_rx_cost,
        visual_analysis_table.assigned_cost_unfiltered,
        visual_analysis_table.assigned_ip_cost_unfiltered,
        visual_analysis_table.assigned_op_cost_unfiltered,
        visual_analysis_table.assigned_pb_cost_unfiltered,
        visual_analysis_table.assigned_rx_cost_unfiltered,
        visual_analysis_table.pps,
        visual_analysis_table.provider_id,
        visual_analysis_table.provider_name,
        visual_analysis_table.provider_zipcode,
        visual_analysis_table.provider_type,
        visual_analysis_table.mco,
        visual_analysis_table.hh,
        visual_analysis_table.pcp,
        visual_analysis_table.vbp_attrib_provider,
        visual_analysis_table.vbp_attrib_provider_zipcode,
        visual_analysis_table.vbp_contractor,
        visual_analysis_table.year,
        visual_analysis_table.utilization,
        visual_analysis_table.ppr,
        visual_analysis_table.ppv,
        visual_analysis_table.exclusive,
        visual_analysis_table.co_occurence_count_ASTHMA,
        visual_analysis_table.co_occurence_count_ARRBLK,
        visual_analysis_table.co_occurence_count_HF,
        visual_analysis_table.co_occurence_count_COPD,
        visual_analysis_table.co_occurence_count_CAD,
        visual_analysis_table.co_occurence_count_ULCLTS,
        visual_analysis_table.co_occurence_count_BIPLR,
        visual_analysis_table.co_occurence_count_GERD,
        visual_analysis_table.co_occurence_count_HTN,
        visual_analysis_table.co_occurence_count_GLCOMA,
        visual_analysis_table.co_occurence_count_LBP,
        visual_analysis_table.co_occurence_count_CROHNS,
        visual_analysis_table.co_occurence_count_DIAB,
        visual_analysis_table.co_occurence_count_DEPRSN,
        visual_analysis_table.co_occurence_count_OSTEOA,
        visual_analysis_table.co_occurence_count_RHNTS,
        visual_analysis_table.co_occurence_count_DIVERT,
        visual_analysis_table.co_occurence_count_DEPANX,
        visual_analysis_table.co_occurence_count_PTSD,
        visual_analysis_table.co_occurence_count_SCHIZO,
        visual_analysis_table.co_occurence_count_SUDS,
        visual_analysis_table.co_occurence_count_chronic,
        visual_analysis_table.co_occurence_count_all,
        visual_analysis_table.episode_count,
        visual_analysis_table.episode_count_unfiltered,
        visual_analysis_table.qcrg_code,
        visual_analysis_table.qcrg_desc,
        visual_analysis_table.qacrg1_code,
        visual_analysis_table.qacrg1_desc,
        visual_analysis_table.qacrg2_code,
        visual_analysis_table.qacrg2_desc,
        visual_analysis_table.qacrg3_code,
        visual_analysis_table.qacrg3_desc,
        visual_analysis_table.fincrg_q,
        visual_analysis_table.q_base,
        visual_analysis_table.q_severity,
        visual_analysis_table.enrolled_num_month
 FROM epbuilder.visual_analysis_table;


