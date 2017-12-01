CREATE SCHEMA episode_builder_5_4_002;


CREATE TABLE episode_builder_5_4_002."201603-hcpc-update"
(
    hcpccode varchar(255) DEFAULT NULL,
    change varchar(255) DEFAULT NULL,
    long_desc varchar(255) DEFAULT NULL,
    short_desc varchar(255) DEFAULT NULL,
    notes varchar(255) DEFAULT NULL,
    ccs varchar(255) DEFAULT NULL,
    ccslabel varchar(255) DEFAULT NULL,
    MDC varchar(255) DEFAULT NULL,
    GROUP_ID char(8) DEFAULT NULL::"char"
);


CREATE TABLE episode_builder_5_4_002."201603-rxNorm-update"
(
    ndc varchar(255) DEFAULT NULL,
    rxcui int DEFAULT NULL::int,
    ingredient varchar(255) DEFAULT NULL,
    irxcui int DEFAULT NULL::int,
    group1 varchar(255) DEFAULT NULL,
    group2 varchar(255) DEFAULT NULL,
    group3 varchar(255) DEFAULT NULL,
    ndcExists int DEFAULT 0,
    irxcuiExists int DEFAULT 0,
    group2Exists int DEFAULT 0,
    group3Exists int DEFAULT 0,
    group1Exists int DEFAULT 0,
    group_id char(8) DEFAULT NULL::"char"
);


CREATE TABLE episode_builder_5_4_002."2016-cpt-group"
(
    ID char(8) NOT NULL DEFAULT '',
    NAME varchar(80) DEFAULT NULL,
    DESCRIPTION varchar(80) DEFAULT NULL,
    MDC_CATEGORY varchar(2) DEFAULT NULL,
    TYPE_ID varchar(4) DEFAULT NULL,
    INDEX int DEFAULT NULL::int,
    AID int DEFAULT NULL::int,
    maxindex int DEFAULT NULL::int,
    minaid int DEFAULT NULL::int
);


CREATE TABLE episode_builder_5_4_002."2016-hcpc-group"
(
    ID char(8) NOT NULL DEFAULT '',
    NAME varchar(80) DEFAULT NULL,
    DESCRIPTION varchar(80) DEFAULT NULL,
    MDC_CATEGORY varchar(2) DEFAULT NULL,
    TYPE_ID varchar(4) DEFAULT NULL,
    INDEX int DEFAULT NULL::int,
    AID int DEFAULT NULL::int,
    maxindex int DEFAULT NULL::int,
    minaid int DEFAULT NULL::int
);


CREATE TABLE episode_builder_5_4_002.betos
(
    CATEGORY varchar(3) NOT NULL,
    DESCRIPTION varchar(80) NOT NULL
);

ALTER TABLE episode_builder_5_4_002.betos ADD CONSTRAINT C_PRIMARY PRIMARY KEY (CATEGORY) DISABLED;

CREATE TABLE episode_builder_5_4_002.cc
(
    CATEGORY varchar(3) NOT NULL,
    DESCRIPTION varchar(80) DEFAULT NULL,
    IS_HCC int DEFAULT NULL::int
);


CREATE TABLE episode_builder_5_4_002.ccs
(
    CATEGORY varchar(3) NOT NULL,
    TYPE_ID varchar(4) NOT NULL,
    DESCRIPTION varchar(250) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.code
(
    VALUE varchar(10) NOT NULL DEFAULT '',
    TYPE_ID varchar(4) NOT NULL,
    MULTUM_CATEGORY varchar(3) NOT NULL,
    DESCRIPTION varchar(250) NOT NULL,
    DESCRIPTION_LONG varchar(1000) DEFAULT NULL,
    GROUP_ID char(8) NOT NULL DEFAULT 'XX0000',
    BETOS_CATEGORY varchar(3) DEFAULT NULL,
    MDC_CATEGORY varchar(2) DEFAULT NULL,
    CCS_CATEGORY varchar(3) DEFAULT NULL,
    CCS_TYPE_ID varchar(4) DEFAULT NULL,
    CC_CATEGORY varchar(3) DEFAULT NULL,
    CREATED_DATE timestamp DEFAULT NULL::timestamp,
    MODIFIED_DATE timestamp DEFAULT NULL::timestamp,
    COST float DEFAULT NULL::float,
    FREQUENCY float DEFAULT NULL::float,
    EM varchar(1) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.code_icd10
(
    VALUE varchar(10) NOT NULL DEFAULT '',
    TYPE_ID varchar(4) NOT NULL,
    MULTUM_CATEGORY varchar(3) NOT NULL,
    DESCRIPTION varchar(250) NOT NULL,
    DESCRIPTION_LONG varchar(1000) DEFAULT NULL,
    GROUP_ID char(8) NOT NULL DEFAULT 'XX0000',
    BETOS_CATEGORY varchar(3) DEFAULT NULL,
    MDC_CATEGORY varchar(2) DEFAULT NULL,
    CCS_CATEGORY varchar(3) DEFAULT NULL,
    CCS_TYPE_ID varchar(4) DEFAULT NULL,
    CC_CATEGORY varchar(3) DEFAULT NULL,
    CREATED_DATE timestamp DEFAULT NULL::timestamp,
    MODIFIED_DATE timestamp DEFAULT NULL::timestamp,
    COST float DEFAULT NULL::float,
    FREQUENCY float DEFAULT NULL::float,
    EM varchar(1) DEFAULT NULL,
    id int DEFAULT NULL::int
);


CREATE TABLE episode_builder_5_4_002.code_ndc
(
    VALUE varchar(13) NOT NULL DEFAULT '',
    TYPE_ID varchar(4) NOT NULL,
    MULTUM_CATEGORY varchar(3) NOT NULL,
    DESCRIPTION varchar(250) NOT NULL,
    DESCRIPTION_LONG varchar(1000) DEFAULT NULL,
    GROUP_ID char(8) NOT NULL DEFAULT 'XX0000',
    BETOS_CATEGORY varchar(3) DEFAULT NULL,
    MDC_CATEGORY varchar(2) DEFAULT NULL,
    CCS_CATEGORY varchar(3) DEFAULT NULL,
    CCS_TYPE_ID varchar(4) DEFAULT NULL,
    CC_CATEGORY varchar(3) DEFAULT NULL,
    CREATED_DATE timestamp DEFAULT NULL::timestamp,
    MODIFIED_DATE timestamp DEFAULT NULL::timestamp,
    COST float DEFAULT NULL::float,
    FREQUENCY float DEFAULT NULL::float,
    EM varchar(1) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.episode
(
    EPISODE_ID varchar(6) NOT NULL,
    NAME varchar(6) DEFAULT NULL,
    TYPE varchar(45) NOT NULL,
    STATUS varchar(45) DEFAULT NULL,
    DESCRIPTION varchar(37) DEFAULT NULL,
    CREATED_DATE timestamp NOT NULL,
    MODIFIED_DATE timestamp DEFAULT NULL::timestamp,
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


CREATE TABLE episode_builder_5_4_002.episode_association
(
    ID int DEFAULT NULL::int,
    ASSOCIATION varchar(45) NOT NULL,
    LEVEL int DEFAULT NULL::int,
    PRIMARY_EPISODE_ID varchar(80) NOT NULL,
    SECONDARY_EPISODE_ID varchar(80) NOT NULL,
    START_DAY varchar(8) DEFAULT NULL,
    END_DAY varchar(8) DEFAULT NULL,
    SUBSIDIARY_TO_PROC varchar(8) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.episode_lookback_trigger
(
    EPISODE_ID varchar(6) NOT NULL,
    LOOKBACK_EPISODE_ID varchar(6) NOT NULL
);


CREATE TABLE episode_builder_5_4_002.episode_to_code
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
    CORE_QUANTITY numeric(40,20) DEFAULT NULL::numeric(1,0),
    IS_POS int DEFAULT NULL::int,
    QUALIFYING_DIAGNOSIS int DEFAULT 0,
    RX_FUNCTION varchar(10) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.episode_to_code_icd10_5_3_007
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
    CORE_QUANTITY numeric(40,20) DEFAULT NULL::numeric(1,0),
    IS_POS int DEFAULT NULL::int,
    QUALIFYING_DIAGNOSIS int DEFAULT 0,
    RX_FUNCTION varchar(10) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.episode_trigger
(
    EPISODE_ID varchar(6) NOT NULL,
    FACILITY_TYPE varchar(2) NOT NULL,
    PX_CODE_POSITION varchar(10) NOT NULL,
    SERVICE_TYPE varchar(10) NOT NULL,
    LOGICAL_OPERATOR varchar(10) NOT NULL,
    DX_CODE_POSITION varchar(10) NOT NULL,
    DUAL_SERVICE_TYPE varchar(10) DEFAULT NULL,
    DUAL_SERVICE_MIN int DEFAULT NULL::int,
    DUAL_SERVICE_MAX int DEFAULT NULL::int,
    END_OF_STUDY int DEFAULT NULL::int,
    REQ_CONF_CLAIM int DEFAULT NULL::int
);


CREATE TABLE episode_builder_5_4_002.file_paths
(
    INDEX int DEFAULT NULL::int,
    WEB_ROOT varchar(100) DEFAULT NULL,
    LOCAL_ROOT varchar(100) DEFAULT NULL,
    EXPORT_EP varchar(45) DEFAULT NULL,
    EXPORT_ALL varchar(45) DEFAULT NULL,
    BACKUP_IMPORT varchar(45) DEFAULT NULL,
    KILL_PROC varchar(45) DEFAULT NULL,
    SCRIPT_ROOT varchar(45) DEFAULT NULL,
    LOG_PATH varchar(45) DEFAULT NULL,
    IMPORT_PATH varchar(45) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.function
(
    ID varchar(2) NOT NULL,
    NAME varchar(45) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.global_em_201603
(
    TYPE_ID varchar(7) DEFAULT NULL,
    VALUE varchar(12) DEFAULT NULL,
    DESCRIPTION varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."group"
(
    ID char(8) NOT NULL DEFAULT '',
    NAME varchar(80) DEFAULT NULL,
    DESCRIPTION varchar(80) DEFAULT NULL,
    MDC_CATEGORY varchar(2) DEFAULT NULL,
    TYPE_ID varchar(4) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.groups_rxcui
(
    ID varchar(8) DEFAULT NULL,
    NAME varchar(80) DEFAULT NULL,
    DESCRIPTION varchar(80) DEFAULT NULL,
    MDC_CATEGORY varchar(2) DEFAULT NULL,
    TYPE_ID varchar(4) NOT NULL DEFAULT 'RX'
);


CREATE TABLE episode_builder_5_4_002.groups_to_rxcui
(
    rxcui varchar(8) DEFAULT NULL,
    group_id varchar(8) DEFAULT NULL,
    rxc_group_id varchar(8) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.hci3_ndc_list_final
(
    ndc varchar(50) DEFAULT NULL,
    rxcui int DEFAULT NULL::int,
    ingredient varchar(255) DEFAULT NULL,
    irxcui int DEFAULT NULL::int,
    group1 varchar(255) DEFAULT NULL,
    group2 varchar(255) DEFAULT NULL,
    group3 varchar(255) DEFAULT NULL,
    "new" int DEFAULT 0,
    isInCode int DEFAULT 0
);


CREATE TABLE episode_builder_5_4_002."icd10Both-2016"
(
    id int DEFAULT NULL::int,
    value varchar(255) DEFAULT NULL,
    type_id varchar(10) DEFAULT NULL,
    description varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."icd10cm-2015"
(
    unk varchar(255) DEFAULT NULL,
    value varchar(255) DEFAULT NULL,
    unk2 int DEFAULT NULL::int,
    description varchar(255) DEFAULT NULL,
    description_long varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."icd10cm-2016"
(
    unk varchar(255) DEFAULT NULL,
    value varchar(255) DEFAULT NULL,
    unk2 int DEFAULT NULL::int,
    description varchar(255) DEFAULT NULL,
    description_long varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."icd10pcs-2015"
(
    unk varchar(255) DEFAULT NULL,
    value varchar(255) DEFAULT NULL,
    unk2 int DEFAULT NULL::int,
    description varchar(255) DEFAULT NULL,
    description_long varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."icd10pcs-2016"
(
    unk varchar(30) DEFAULT NULL,
    value varchar(30) DEFAULT NULL,
    unk2 int DEFAULT NULL::int,
    description varchar(255) DEFAULT NULL,
    description_long varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."icd9to10Both-2015"
(
    id int DEFAULT NULL::int,
    code9 varchar(25) DEFAULT NULL,
    code10 varchar(25) DEFAULT NULL,
    type_id varchar(3) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."icd9to10Both-2016"
(
    id int DEFAULT NULL::int,
    code9 varchar(25) DEFAULT NULL,
    code10 varchar(25) DEFAULT NULL,
    type_id varchar(3) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."icdcm9to10-2015"
(
    DX varchar(255) DEFAULT NULL,
    DXX varchar(255) DEFAULT NULL,
    unk int DEFAULT NULL::int
);


CREATE TABLE episode_builder_5_4_002."icdcm9to10-2016"
(
    DX varchar(255) DEFAULT NULL,
    DXX varchar(255) DEFAULT NULL,
    unk varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002."icdpcs9to10-2015"
(
    PX varchar(255) DEFAULT NULL,
    PXX varchar(255) DEFAULT NULL,
    unk int DEFAULT NULL::int
);


CREATE TABLE episode_builder_5_4_002."icdpcs9to10-2016"
(
    PX varchar(255) DEFAULT NULL,
    PXX varchar(255) DEFAULT NULL,
    unk int DEFAULT NULL::int
);


CREATE TABLE episode_builder_5_4_002.import_files
(
    ID int DEFAULT NULL::int,
    USER_ID varchar(45) NOT NULL,
    TYPE varchar(100) DEFAULT NULL,
    FILE_PATH varchar(250) DEFAULT NULL,
    MODIFIED_DATE timestamp DEFAULT NULL::timestamp,
    NAME varchar(100) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.mdc
(
    CATEGORY varchar(2) NOT NULL,
    DESCRIPTION varchar(80) NOT NULL
);


CREATE TABLE episode_builder_5_4_002.multum
(
    CATEGORY varchar(3) NOT NULL,
    DESCRIPTION varchar(80) NOT NULL
);


CREATE TABLE episode_builder_5_4_002.ndc_to_multum
(
    ID int DEFAULT NULL::int,
    NDC varchar(45) NOT NULL,
    CODE_VALUE varchar(10) NOT NULL DEFAULT ''
);


CREATE TABLE episode_builder_5_4_002.ndc_to_rxcui
(
    ndc varchar(50) DEFAULT NULL,
    rxcui int DEFAULT NULL::int,
    ndcClean varchar(11) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.note
(
    ID int DEFAULT NULL::int,
    TITLE varchar(100) DEFAULT NULL,
    BODY varchar(1096),
    EPISODE_ID varchar(6) DEFAULT NULL,
    USERS_USER_ID varchar(45) DEFAULT NULL,
    FILE_PATH varchar(250) DEFAULT NULL,
    NAME varchar(100) DEFAULT NULL,
    MODIFIED_DATE timestamp DEFAULT NULL::timestamp
);


CREATE TABLE episode_builder_5_4_002.playbook_code_order
(
    id int DEFAULT NULL::int,
    VALUE varchar(10) DEFAULT NULL,
    TYPE_ID varchar(4) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.playbook_text
(
    Episode_acronym varchar(255) DEFAULT NULL,
    Episode_description varchar(5000) DEFAULT NULL,
    Enrollment_Criteria varchar(255) DEFAULT NULL,
    Age varchar(255) DEFAULT NULL,
    Trigger_criteria varchar(5000) DEFAULT NULL,
    Episode_limit varchar(255) DEFAULT NULL,
    EPISODE_ID varchar(6) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.roles
(
    ROLE_NAME varchar(45) NOT NULL,
    DESCRIPTION varchar(256) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.test
(
    INDEX int NOT NULL DEFAULT 0,
    WEB_ROOT varchar(100) DEFAULT NULL,
    LOCAL_ROOT varchar(100) DEFAULT NULL,
    EXPORT_EP varchar(45) DEFAULT NULL,
    EXPORT_ALL varchar(45) DEFAULT NULL,
    BACKUP_IMPORT varchar(45) DEFAULT NULL,
    KILL_PROC varchar(45) DEFAULT NULL,
    SCRIPT_ROOT varchar(45) DEFAULT NULL,
    LOG_PATH varchar(45) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.transactions
(
    ID int DEFAULT NULL::int,
    USER_ID varchar(45) DEFAULT NULL,
    STATUS varchar(45) DEFAULT NULL,
    FILE_PATH varchar(250) DEFAULT NULL,
    TYPE varchar(45) DEFAULT NULL,
    START_DATE timestamp DEFAULT NULL::timestamp,
    END_DATE timestamp DEFAULT NULL::timestamp,
    PID varchar(45) DEFAULT NULL,
    EPISODE_ID varchar(100) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.type
(
    ID varchar(4) NOT NULL,
    DESCRIPTION varchar(80) NOT NULL
);


CREATE TABLE episode_builder_5_4_002.universal_risk_factors
(
    code_id varchar(255) DEFAULT NULL,
    type_id varchar(3) DEFAULT 'DX',
    group_id varchar(255) DEFAULT NULL,
    group_name varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.universal_risk_factors_10
(
    code_id varchar(255) DEFAULT NULL,
    type_id varchar(3) DEFAULT 'DX',
    group_id varchar(255) DEFAULT NULL,
    group_name varchar(255) DEFAULT NULL
);


CREATE TABLE episode_builder_5_4_002.user_roles
(
    USER_ID varchar(45) NOT NULL,
    ROLE_NAME varchar(45) NOT NULL
);


CREATE TABLE episode_builder_5_4_002.users
(
    USER_ID varchar(45) NOT NULL,
    PASSWORD varchar(256) DEFAULT NULL,
    EMAIL varchar(100) DEFAULT NULL,
    ENABLED int DEFAULT NULL::int,
    FIRST_NAME varchar(45) DEFAULT NULL,
    LAST_NAME varchar(45) DEFAULT NULL,
    NICK_NAME varchar(45) DEFAULT NULL
);



CREATE PROJECTION episode_builder_5_4_002.betos /*+createtype(L)*/ 
(
 CATEGORY,
 DESCRIPTION
)
AS
 SELECT betos.CATEGORY,
        betos.DESCRIPTION
 FROM episode_builder_5_4_002.betos
 ORDER BY betos.CATEGORY
SEGMENTED BY hash(betos.CATEGORY) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.cc /*+createtype(L)*/ 
(
 CATEGORY,
 DESCRIPTION,
 IS_HCC
)
AS
 SELECT cc.CATEGORY,
        cc.DESCRIPTION,
        cc.IS_HCC
 FROM episode_builder_5_4_002.cc
 ORDER BY cc.CATEGORY,
          cc.DESCRIPTION,
          cc.IS_HCC
SEGMENTED BY hash(cc.CATEGORY, cc.IS_HCC, cc.DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.ccs /*+createtype(L)*/ 
(
 CATEGORY,
 TYPE_ID,
 DESCRIPTION
)
AS
 SELECT ccs.CATEGORY,
        ccs.TYPE_ID,
        ccs.DESCRIPTION
 FROM episode_builder_5_4_002.ccs
 ORDER BY ccs.CATEGORY,
          ccs.TYPE_ID,
          ccs.DESCRIPTION
SEGMENTED BY hash(ccs.CATEGORY, ccs.TYPE_ID, ccs.DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.code /*+createtype(L)*/ 
(
 VALUE,
 TYPE_ID,
 MULTUM_CATEGORY,
 DESCRIPTION,
 DESCRIPTION_LONG,
 GROUP_ID,
 BETOS_CATEGORY,
 MDC_CATEGORY,
 CCS_CATEGORY,
 CCS_TYPE_ID,
 CC_CATEGORY,
 CREATED_DATE,
 MODIFIED_DATE,
 COST,
 FREQUENCY,
 EM
)
AS
 SELECT code.VALUE,
        code.TYPE_ID,
        code.MULTUM_CATEGORY,
        code.DESCRIPTION,
        code.DESCRIPTION_LONG,
        code.GROUP_ID,
        code.BETOS_CATEGORY,
        code.MDC_CATEGORY,
        code.CCS_CATEGORY,
        code.CCS_TYPE_ID,
        code.CC_CATEGORY,
        code.CREATED_DATE,
        code.MODIFIED_DATE,
        code.COST,
        code.FREQUENCY,
        code.EM
 FROM episode_builder_5_4_002.code
 ORDER BY code.VALUE,
          code.TYPE_ID,
          code.MULTUM_CATEGORY,
          code.DESCRIPTION,
          code.DESCRIPTION_LONG,
          code.GROUP_ID,
          code.BETOS_CATEGORY,
          code.MDC_CATEGORY,
          code.CCS_CATEGORY,
          code.CCS_TYPE_ID,
          code.CC_CATEGORY,
          code.CREATED_DATE,
          code.MODIFIED_DATE,
          code.COST,
          code.FREQUENCY,
          code.EM
SEGMENTED BY hash(code.TYPE_ID, code.MULTUM_CATEGORY, code.GROUP_ID, code.BETOS_CATEGORY, code.MDC_CATEGORY, code.CCS_CATEGORY, code.CCS_TYPE_ID, code.CC_CATEGORY, code.CREATED_DATE, code.MODIFIED_DATE, code.COST, code.FREQUENCY, code.EM, code.VALUE, code.DESCRIPTION, code.DESCRIPTION_LONG) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.code_icd10 /*+createtype(L)*/ 
(
 VALUE,
 TYPE_ID,
 MULTUM_CATEGORY,
 DESCRIPTION,
 DESCRIPTION_LONG,
 GROUP_ID,
 BETOS_CATEGORY,
 MDC_CATEGORY,
 CCS_CATEGORY,
 CCS_TYPE_ID,
 CC_CATEGORY,
 CREATED_DATE,
 MODIFIED_DATE,
 COST,
 FREQUENCY,
 EM,
 id
)
AS
 SELECT code_icd10.VALUE,
        code_icd10.TYPE_ID,
        code_icd10.MULTUM_CATEGORY,
        code_icd10.DESCRIPTION,
        code_icd10.DESCRIPTION_LONG,
        code_icd10.GROUP_ID,
        code_icd10.BETOS_CATEGORY,
        code_icd10.MDC_CATEGORY,
        code_icd10.CCS_CATEGORY,
        code_icd10.CCS_TYPE_ID,
        code_icd10.CC_CATEGORY,
        code_icd10.CREATED_DATE,
        code_icd10.MODIFIED_DATE,
        code_icd10.COST,
        code_icd10.FREQUENCY,
        code_icd10.EM,
        code_icd10.id
 FROM episode_builder_5_4_002.code_icd10
 ORDER BY code_icd10.VALUE,
          code_icd10.TYPE_ID,
          code_icd10.MULTUM_CATEGORY,
          code_icd10.DESCRIPTION,
          code_icd10.DESCRIPTION_LONG,
          code_icd10.GROUP_ID,
          code_icd10.BETOS_CATEGORY,
          code_icd10.MDC_CATEGORY,
          code_icd10.CCS_CATEGORY,
          code_icd10.CCS_TYPE_ID,
          code_icd10.CC_CATEGORY,
          code_icd10.CREATED_DATE,
          code_icd10.MODIFIED_DATE,
          code_icd10.COST,
          code_icd10.FREQUENCY,
          code_icd10.EM,
          code_icd10.id
SEGMENTED BY hash(code_icd10.TYPE_ID, code_icd10.MULTUM_CATEGORY, code_icd10.GROUP_ID, code_icd10.BETOS_CATEGORY, code_icd10.MDC_CATEGORY, code_icd10.CCS_CATEGORY, code_icd10.CCS_TYPE_ID, code_icd10.CC_CATEGORY, code_icd10.CREATED_DATE, code_icd10.MODIFIED_DATE, code_icd10.COST, code_icd10.FREQUENCY, code_icd10.EM, code_icd10.id, code_icd10.VALUE, code_icd10.DESCRIPTION, code_icd10.DESCRIPTION_LONG) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.code_ndc /*+createtype(L)*/ 
(
 VALUE,
 TYPE_ID,
 MULTUM_CATEGORY,
 DESCRIPTION,
 DESCRIPTION_LONG,
 GROUP_ID,
 BETOS_CATEGORY,
 MDC_CATEGORY,
 CCS_CATEGORY,
 CCS_TYPE_ID,
 CC_CATEGORY,
 CREATED_DATE,
 MODIFIED_DATE,
 COST,
 FREQUENCY,
 EM
)
AS
 SELECT code_ndc.VALUE,
        code_ndc.TYPE_ID,
        code_ndc.MULTUM_CATEGORY,
        code_ndc.DESCRIPTION,
        code_ndc.DESCRIPTION_LONG,
        code_ndc.GROUP_ID,
        code_ndc.BETOS_CATEGORY,
        code_ndc.MDC_CATEGORY,
        code_ndc.CCS_CATEGORY,
        code_ndc.CCS_TYPE_ID,
        code_ndc.CC_CATEGORY,
        code_ndc.CREATED_DATE,
        code_ndc.MODIFIED_DATE,
        code_ndc.COST,
        code_ndc.FREQUENCY,
        code_ndc.EM
 FROM episode_builder_5_4_002.code_ndc
 ORDER BY code_ndc.VALUE,
          code_ndc.TYPE_ID,
          code_ndc.MULTUM_CATEGORY,
          code_ndc.DESCRIPTION,
          code_ndc.DESCRIPTION_LONG,
          code_ndc.GROUP_ID,
          code_ndc.BETOS_CATEGORY,
          code_ndc.MDC_CATEGORY,
          code_ndc.CCS_CATEGORY,
          code_ndc.CCS_TYPE_ID,
          code_ndc.CC_CATEGORY,
          code_ndc.CREATED_DATE,
          code_ndc.MODIFIED_DATE,
          code_ndc.COST,
          code_ndc.FREQUENCY,
          code_ndc.EM
SEGMENTED BY hash(code_ndc.TYPE_ID, code_ndc.MULTUM_CATEGORY, code_ndc.GROUP_ID, code_ndc.BETOS_CATEGORY, code_ndc.MDC_CATEGORY, code_ndc.CCS_CATEGORY, code_ndc.CCS_TYPE_ID, code_ndc.CC_CATEGORY, code_ndc.CREATED_DATE, code_ndc.MODIFIED_DATE, code_ndc.COST, code_ndc.FREQUENCY, code_ndc.EM, code_ndc.VALUE, code_ndc.DESCRIPTION, code_ndc.DESCRIPTION_LONG) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.episode /*+createtype(L)*/ 
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
 SELECT episode.EPISODE_ID,
        episode.NAME,
        episode.TYPE,
        episode.STATUS,
        episode.DESCRIPTION,
        episode.CREATED_DATE,
        episode.MODIFIED_DATE,
        episode.USERS_USER_ID,
        episode.MDC_CATEGORY,
        episode.PARM_SET,
        episode.TRIGGER_TYPE,
        episode.TRIGGER_NUMBER,
        episode.SEPARATION_MIN,
        episode.SEPARATION_MAX,
        episode.BOUND_OFFSET,
        episode.BOUND_LENGTH,
        episode.CONDITION_MIN,
        episode.VERSION,
        episode.END_OF_STUDY
 FROM episode_builder_5_4_002.episode
 ORDER BY episode.EPISODE_ID,
          episode.NAME,
          episode.TYPE,
          episode.STATUS,
          episode.DESCRIPTION,
          episode.CREATED_DATE,
          episode.MODIFIED_DATE,
          episode.USERS_USER_ID,
          episode.MDC_CATEGORY,
          episode.PARM_SET,
          episode.TRIGGER_TYPE,
          episode.TRIGGER_NUMBER,
          episode.SEPARATION_MIN,
          episode.SEPARATION_MAX,
          episode.BOUND_OFFSET,
          episode.BOUND_LENGTH,
          episode.CONDITION_MIN,
          episode.VERSION,
          episode.END_OF_STUDY
SEGMENTED BY hash(episode.EPISODE_ID, episode.NAME, episode.CREATED_DATE, episode.MODIFIED_DATE, episode.MDC_CATEGORY, episode.PARM_SET, episode.TRIGGER_TYPE, episode.TRIGGER_NUMBER, episode.SEPARATION_MIN, episode.SEPARATION_MAX, episode.BOUND_OFFSET, episode.BOUND_LENGTH, episode.CONDITION_MIN, episode.END_OF_STUDY, episode.DESCRIPTION, episode.TYPE, episode.STATUS, episode.USERS_USER_ID, episode.VERSION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.episode_association /*+createtype(L)*/ 
(
 ID,
 ASSOCIATION,
 LEVEL,
 PRIMARY_EPISODE_ID,
 SECONDARY_EPISODE_ID,
 START_DAY,
 END_DAY,
 SUBSIDIARY_TO_PROC
)
AS
 SELECT episode_association.ID,
        episode_association.ASSOCIATION,
        episode_association.LEVEL,
        episode_association.PRIMARY_EPISODE_ID,
        episode_association.SECONDARY_EPISODE_ID,
        episode_association.START_DAY,
        episode_association.END_DAY,
        episode_association.SUBSIDIARY_TO_PROC
 FROM episode_builder_5_4_002.episode_association
 ORDER BY episode_association.ID,
          episode_association.ASSOCIATION,
          episode_association.LEVEL,
          episode_association.PRIMARY_EPISODE_ID,
          episode_association.SECONDARY_EPISODE_ID,
          episode_association.START_DAY,
          episode_association.END_DAY,
          episode_association.SUBSIDIARY_TO_PROC
SEGMENTED BY hash(episode_association.ID, episode_association.LEVEL, episode_association.START_DAY, episode_association.END_DAY, episode_association.SUBSIDIARY_TO_PROC, episode_association.ASSOCIATION, episode_association.PRIMARY_EPISODE_ID, episode_association.SECONDARY_EPISODE_ID) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.episode_lookback_trigger /*+createtype(L)*/ 
(
 EPISODE_ID,
 LOOKBACK_EPISODE_ID
)
AS
 SELECT episode_lookback_trigger.EPISODE_ID,
        episode_lookback_trigger.LOOKBACK_EPISODE_ID
 FROM episode_builder_5_4_002.episode_lookback_trigger
 ORDER BY episode_lookback_trigger.EPISODE_ID,
          episode_lookback_trigger.LOOKBACK_EPISODE_ID
SEGMENTED BY hash(episode_lookback_trigger.EPISODE_ID, episode_lookback_trigger.LOOKBACK_EPISODE_ID) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.episode_to_code /*+createtype(L)*/ 
(
 EPISODE_ID,
 FUNCTION_ID,
 CODE_VALUE,
 CODE_TYPE_ID,
 CODE_MULTUM_CATEGORY,
 SUBTYPE_ID,
 COMPLICATION,
 SUFFICIENT,
 CORE,
 VETTED,
 PAS,
 CORE_QUANTITY,
 IS_POS,
 QUALIFYING_DIAGNOSIS,
 RX_FUNCTION
)
AS
 SELECT episode_to_code.EPISODE_ID,
        episode_to_code.FUNCTION_ID,
        episode_to_code.CODE_VALUE,
        episode_to_code.CODE_TYPE_ID,
        episode_to_code.CODE_MULTUM_CATEGORY,
        episode_to_code.SUBTYPE_ID,
        episode_to_code.COMPLICATION,
        episode_to_code.SUFFICIENT,
        episode_to_code.CORE,
        episode_to_code.VETTED,
        episode_to_code.PAS,
        episode_to_code.CORE_QUANTITY,
        episode_to_code.IS_POS,
        episode_to_code.QUALIFYING_DIAGNOSIS,
        episode_to_code.RX_FUNCTION
 FROM episode_builder_5_4_002.episode_to_code
 ORDER BY episode_to_code.EPISODE_ID,
          episode_to_code.FUNCTION_ID,
          episode_to_code.CODE_VALUE,
          episode_to_code.CODE_TYPE_ID,
          episode_to_code.CODE_MULTUM_CATEGORY,
          episode_to_code.SUBTYPE_ID,
          episode_to_code.COMPLICATION,
          episode_to_code.SUFFICIENT,
          episode_to_code.CORE,
          episode_to_code.VETTED,
          episode_to_code.PAS,
          episode_to_code.CORE_QUANTITY,
          episode_to_code.IS_POS,
          episode_to_code.QUALIFYING_DIAGNOSIS,
          episode_to_code.RX_FUNCTION
SEGMENTED BY hash(episode_to_code.FUNCTION_ID, episode_to_code.CODE_TYPE_ID, episode_to_code.CODE_MULTUM_CATEGORY, episode_to_code.SUBTYPE_ID, episode_to_code.COMPLICATION, episode_to_code.SUFFICIENT, episode_to_code.CORE, episode_to_code.VETTED, episode_to_code.PAS, episode_to_code.IS_POS, episode_to_code.QUALIFYING_DIAGNOSIS, episode_to_code.CODE_VALUE, episode_to_code.RX_FUNCTION, episode_to_code.CORE_QUANTITY, episode_to_code.EPISODE_ID) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.episode_to_code_icd10_5_3_007 /*+createtype(L)*/ 
(
 EPISODE_ID,
 FUNCTION_ID,
 CODE_VALUE,
 CODE_TYPE_ID,
 CODE_MULTUM_CATEGORY,
 SUBTYPE_ID,
 COMPLICATION,
 SUFFICIENT,
 CORE,
 VETTED,
 PAS,
 CORE_QUANTITY,
 IS_POS,
 QUALIFYING_DIAGNOSIS,
 RX_FUNCTION
)
AS
 SELECT episode_to_code_icd10_5_3_007.EPISODE_ID,
        episode_to_code_icd10_5_3_007.FUNCTION_ID,
        episode_to_code_icd10_5_3_007.CODE_VALUE,
        episode_to_code_icd10_5_3_007.CODE_TYPE_ID,
        episode_to_code_icd10_5_3_007.CODE_MULTUM_CATEGORY,
        episode_to_code_icd10_5_3_007.SUBTYPE_ID,
        episode_to_code_icd10_5_3_007.COMPLICATION,
        episode_to_code_icd10_5_3_007.SUFFICIENT,
        episode_to_code_icd10_5_3_007.CORE,
        episode_to_code_icd10_5_3_007.VETTED,
        episode_to_code_icd10_5_3_007.PAS,
        episode_to_code_icd10_5_3_007.CORE_QUANTITY,
        episode_to_code_icd10_5_3_007.IS_POS,
        episode_to_code_icd10_5_3_007.QUALIFYING_DIAGNOSIS,
        episode_to_code_icd10_5_3_007.RX_FUNCTION
 FROM episode_builder_5_4_002.episode_to_code_icd10_5_3_007
 ORDER BY episode_to_code_icd10_5_3_007.EPISODE_ID,
          episode_to_code_icd10_5_3_007.FUNCTION_ID,
          episode_to_code_icd10_5_3_007.CODE_VALUE,
          episode_to_code_icd10_5_3_007.CODE_TYPE_ID,
          episode_to_code_icd10_5_3_007.CODE_MULTUM_CATEGORY,
          episode_to_code_icd10_5_3_007.SUBTYPE_ID,
          episode_to_code_icd10_5_3_007.COMPLICATION,
          episode_to_code_icd10_5_3_007.SUFFICIENT,
          episode_to_code_icd10_5_3_007.CORE,
          episode_to_code_icd10_5_3_007.VETTED,
          episode_to_code_icd10_5_3_007.PAS,
          episode_to_code_icd10_5_3_007.CORE_QUANTITY,
          episode_to_code_icd10_5_3_007.IS_POS,
          episode_to_code_icd10_5_3_007.QUALIFYING_DIAGNOSIS,
          episode_to_code_icd10_5_3_007.RX_FUNCTION
SEGMENTED BY hash(episode_to_code_icd10_5_3_007.FUNCTION_ID, episode_to_code_icd10_5_3_007.CODE_TYPE_ID, episode_to_code_icd10_5_3_007.CODE_MULTUM_CATEGORY, episode_to_code_icd10_5_3_007.SUBTYPE_ID, episode_to_code_icd10_5_3_007.COMPLICATION, episode_to_code_icd10_5_3_007.SUFFICIENT, episode_to_code_icd10_5_3_007.CORE, episode_to_code_icd10_5_3_007.VETTED, episode_to_code_icd10_5_3_007.PAS, episode_to_code_icd10_5_3_007.IS_POS, episode_to_code_icd10_5_3_007.QUALIFYING_DIAGNOSIS, episode_to_code_icd10_5_3_007.CODE_VALUE, episode_to_code_icd10_5_3_007.RX_FUNCTION, episode_to_code_icd10_5_3_007.CORE_QUANTITY, episode_to_code_icd10_5_3_007.EPISODE_ID) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.episode_trigger /*+createtype(L)*/ 
(
 EPISODE_ID,
 FACILITY_TYPE,
 PX_CODE_POSITION,
 SERVICE_TYPE,
 LOGICAL_OPERATOR,
 DX_CODE_POSITION,
 DUAL_SERVICE_TYPE,
 DUAL_SERVICE_MIN,
 DUAL_SERVICE_MAX,
 END_OF_STUDY,
 REQ_CONF_CLAIM
)
AS
 SELECT episode_trigger.EPISODE_ID,
        episode_trigger.FACILITY_TYPE,
        episode_trigger.PX_CODE_POSITION,
        episode_trigger.SERVICE_TYPE,
        episode_trigger.LOGICAL_OPERATOR,
        episode_trigger.DX_CODE_POSITION,
        episode_trigger.DUAL_SERVICE_TYPE,
        episode_trigger.DUAL_SERVICE_MIN,
        episode_trigger.DUAL_SERVICE_MAX,
        episode_trigger.END_OF_STUDY,
        episode_trigger.REQ_CONF_CLAIM
 FROM episode_builder_5_4_002.episode_trigger
 ORDER BY episode_trigger.EPISODE_ID,
          episode_trigger.FACILITY_TYPE,
          episode_trigger.PX_CODE_POSITION,
          episode_trigger.SERVICE_TYPE,
          episode_trigger.LOGICAL_OPERATOR,
          episode_trigger.DX_CODE_POSITION,
          episode_trigger.DUAL_SERVICE_TYPE,
          episode_trigger.DUAL_SERVICE_MIN,
          episode_trigger.DUAL_SERVICE_MAX,
          episode_trigger.END_OF_STUDY,
          episode_trigger.REQ_CONF_CLAIM
SEGMENTED BY hash(episode_trigger.EPISODE_ID, episode_trigger.FACILITY_TYPE, episode_trigger.DUAL_SERVICE_MIN, episode_trigger.DUAL_SERVICE_MAX, episode_trigger.END_OF_STUDY, episode_trigger.REQ_CONF_CLAIM, episode_trigger.PX_CODE_POSITION, episode_trigger.SERVICE_TYPE, episode_trigger.LOGICAL_OPERATOR, episode_trigger.DX_CODE_POSITION, episode_trigger.DUAL_SERVICE_TYPE) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.file_paths /*+createtype(L)*/ 
(
 INDEX,
 WEB_ROOT,
 LOCAL_ROOT,
 EXPORT_EP,
 EXPORT_ALL,
 BACKUP_IMPORT,
 KILL_PROC,
 SCRIPT_ROOT,
 LOG_PATH,
 IMPORT_PATH
)
AS
 SELECT file_paths.INDEX,
        file_paths.WEB_ROOT,
        file_paths.LOCAL_ROOT,
        file_paths.EXPORT_EP,
        file_paths.EXPORT_ALL,
        file_paths.BACKUP_IMPORT,
        file_paths.KILL_PROC,
        file_paths.SCRIPT_ROOT,
        file_paths.LOG_PATH,
        file_paths.IMPORT_PATH
 FROM episode_builder_5_4_002.file_paths
 ORDER BY file_paths.INDEX,
          file_paths.WEB_ROOT,
          file_paths.LOCAL_ROOT,
          file_paths.EXPORT_EP,
          file_paths.EXPORT_ALL,
          file_paths.BACKUP_IMPORT,
          file_paths.KILL_PROC,
          file_paths.SCRIPT_ROOT,
          file_paths.LOG_PATH,
          file_paths.IMPORT_PATH
SEGMENTED BY hash(file_paths.INDEX, file_paths.EXPORT_EP, file_paths.EXPORT_ALL, file_paths.BACKUP_IMPORT, file_paths.KILL_PROC, file_paths.SCRIPT_ROOT, file_paths.LOG_PATH, file_paths.IMPORT_PATH, file_paths.WEB_ROOT, file_paths.LOCAL_ROOT) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.function /*+createtype(L)*/ 
(
 ID,
 NAME
)
AS
 SELECT function.ID,
        function.NAME
 FROM episode_builder_5_4_002.function
 ORDER BY function.ID,
          function.NAME
SEGMENTED BY hash(function.ID, function.NAME) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.global_em_201603 /*+createtype(L)*/ 
(
 TYPE_ID,
 VALUE,
 DESCRIPTION
)
AS
 SELECT global_em_201603.TYPE_ID,
        global_em_201603.VALUE,
        global_em_201603.DESCRIPTION
 FROM episode_builder_5_4_002.global_em_201603
 ORDER BY global_em_201603.TYPE_ID,
          global_em_201603.VALUE,
          global_em_201603.DESCRIPTION
SEGMENTED BY hash(global_em_201603.TYPE_ID, global_em_201603.VALUE, global_em_201603.DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002."group" /*+createtype(L)*/ 
(
 ID,
 NAME,
 DESCRIPTION,
 MDC_CATEGORY,
 TYPE_ID
)
AS
 SELECT "group".ID,
        "group".NAME,
        "group".DESCRIPTION,
        "group".MDC_CATEGORY,
        "group".TYPE_ID
 FROM episode_builder_5_4_002."group"
 ORDER BY "group".ID,
          "group".NAME,
          "group".DESCRIPTION,
          "group".MDC_CATEGORY,
          "group".TYPE_ID
SEGMENTED BY hash("group".ID, "group".MDC_CATEGORY, "group".TYPE_ID, "group".NAME, "group".DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.groups_rxcui /*+createtype(L)*/ 
(
 ID,
 NAME,
 DESCRIPTION,
 MDC_CATEGORY,
 TYPE_ID
)
AS
 SELECT groups_rxcui.ID,
        groups_rxcui.NAME,
        groups_rxcui.DESCRIPTION,
        groups_rxcui.MDC_CATEGORY,
        groups_rxcui.TYPE_ID
 FROM episode_builder_5_4_002.groups_rxcui
 ORDER BY groups_rxcui.ID,
          groups_rxcui.NAME,
          groups_rxcui.DESCRIPTION,
          groups_rxcui.MDC_CATEGORY,
          groups_rxcui.TYPE_ID
SEGMENTED BY hash(groups_rxcui.ID, groups_rxcui.MDC_CATEGORY, groups_rxcui.TYPE_ID, groups_rxcui.NAME, groups_rxcui.DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.groups_to_rxcui /*+createtype(L)*/ 
(
 rxcui,
 group_id,
 rxc_group_id
)
AS
 SELECT groups_to_rxcui.rxcui,
        groups_to_rxcui.group_id,
        groups_to_rxcui.rxc_group_id
 FROM episode_builder_5_4_002.groups_to_rxcui
 ORDER BY groups_to_rxcui.rxcui,
          groups_to_rxcui.group_id,
          groups_to_rxcui.rxc_group_id
SEGMENTED BY hash(groups_to_rxcui.rxcui, groups_to_rxcui.group_id, groups_to_rxcui.rxc_group_id) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.hci3_ndc_list_final /*+createtype(L)*/ 
(
 ndc,
 rxcui,
 ingredient,
 irxcui,
 group1,
 group2,
 group3,
 "new",
 isInCode
)
AS
 SELECT hci3_ndc_list_final.ndc,
        hci3_ndc_list_final.rxcui,
        hci3_ndc_list_final.ingredient,
        hci3_ndc_list_final.irxcui,
        hci3_ndc_list_final.group1,
        hci3_ndc_list_final.group2,
        hci3_ndc_list_final.group3,
        hci3_ndc_list_final."new",
        hci3_ndc_list_final.isInCode
 FROM episode_builder_5_4_002.hci3_ndc_list_final
 ORDER BY hci3_ndc_list_final.ndc,
          hci3_ndc_list_final.rxcui,
          hci3_ndc_list_final.ingredient,
          hci3_ndc_list_final.irxcui,
          hci3_ndc_list_final.group1,
          hci3_ndc_list_final.group2,
          hci3_ndc_list_final.group3,
          hci3_ndc_list_final."new",
          hci3_ndc_list_final.isInCode
SEGMENTED BY hash(hci3_ndc_list_final.rxcui, hci3_ndc_list_final.irxcui, hci3_ndc_list_final."new", hci3_ndc_list_final.isInCode, hci3_ndc_list_final.ndc, hci3_ndc_list_final.ingredient, hci3_ndc_list_final.group1, hci3_ndc_list_final.group2, hci3_ndc_list_final.group3) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.import_files /*+createtype(L)*/ 
(
 ID,
 USER_ID,
 TYPE,
 FILE_PATH,
 MODIFIED_DATE,
 NAME
)
AS
 SELECT import_files.ID,
        import_files.USER_ID,
        import_files.TYPE,
        import_files.FILE_PATH,
        import_files.MODIFIED_DATE,
        import_files.NAME
 FROM episode_builder_5_4_002.import_files
 ORDER BY import_files.ID,
          import_files.USER_ID,
          import_files.TYPE,
          import_files.FILE_PATH,
          import_files.MODIFIED_DATE,
          import_files.NAME
SEGMENTED BY hash(import_files.ID, import_files.MODIFIED_DATE, import_files.USER_ID, import_files.TYPE, import_files.NAME, import_files.FILE_PATH) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.mdc /*+createtype(L)*/ 
(
 CATEGORY,
 DESCRIPTION
)
AS
 SELECT mdc.CATEGORY,
        mdc.DESCRIPTION
 FROM episode_builder_5_4_002.mdc
 ORDER BY mdc.CATEGORY,
          mdc.DESCRIPTION
SEGMENTED BY hash(mdc.CATEGORY, mdc.DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.multum /*+createtype(L)*/ 
(
 CATEGORY,
 DESCRIPTION
)
AS
 SELECT multum.CATEGORY,
        multum.DESCRIPTION
 FROM episode_builder_5_4_002.multum
 ORDER BY multum.CATEGORY,
          multum.DESCRIPTION
SEGMENTED BY hash(multum.CATEGORY, multum.DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.ndc_to_multum /*+createtype(L)*/ 
(
 ID,
 NDC,
 CODE_VALUE
)
AS
 SELECT ndc_to_multum.ID,
        ndc_to_multum.NDC,
        ndc_to_multum.CODE_VALUE
 FROM episode_builder_5_4_002.ndc_to_multum
 ORDER BY ndc_to_multum.ID,
          ndc_to_multum.NDC,
          ndc_to_multum.CODE_VALUE
SEGMENTED BY hash(ndc_to_multum.ID, ndc_to_multum.CODE_VALUE, ndc_to_multum.NDC) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.ndc_to_rxcui /*+createtype(L)*/ 
(
 ndc,
 rxcui,
 ndcClean
)
AS
 SELECT ndc_to_rxcui.ndc,
        ndc_to_rxcui.rxcui,
        ndc_to_rxcui.ndcClean
 FROM episode_builder_5_4_002.ndc_to_rxcui
 ORDER BY ndc_to_rxcui.ndc,
          ndc_to_rxcui.rxcui,
          ndc_to_rxcui.ndcClean
SEGMENTED BY hash(ndc_to_rxcui.rxcui, ndc_to_rxcui.ndcClean, ndc_to_rxcui.ndc) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.note /*+createtype(L)*/ 
(
 ID,
 TITLE,
 BODY,
 EPISODE_ID,
 USERS_USER_ID,
 FILE_PATH,
 NAME,
 MODIFIED_DATE
)
AS
 SELECT note.ID,
        note.TITLE,
        note.BODY,
        note.EPISODE_ID,
        note.USERS_USER_ID,
        note.FILE_PATH,
        note.NAME,
        note.MODIFIED_DATE
 FROM episode_builder_5_4_002.note
 ORDER BY note.ID,
          note.TITLE,
          note.BODY,
          note.EPISODE_ID,
          note.USERS_USER_ID,
          note.FILE_PATH,
          note.NAME,
          note.MODIFIED_DATE
SEGMENTED BY hash(note.ID, note.EPISODE_ID, note.MODIFIED_DATE, note.USERS_USER_ID, note.TITLE, note.NAME, note.FILE_PATH, note.BODY) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.playbook_code_order /*+createtype(L)*/ 
(
 id,
 VALUE,
 TYPE_ID
)
AS
 SELECT playbook_code_order.id,
        playbook_code_order.VALUE,
        playbook_code_order.TYPE_ID
 FROM episode_builder_5_4_002.playbook_code_order
 ORDER BY playbook_code_order.id,
          playbook_code_order.VALUE,
          playbook_code_order.TYPE_ID
SEGMENTED BY hash(playbook_code_order.id, playbook_code_order.TYPE_ID, playbook_code_order.VALUE) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.playbook_text /*+createtype(L)*/ 
(
 Episode_acronym,
 Episode_description,
 Enrollment_Criteria,
 Age,
 Trigger_criteria,
 Episode_limit,
 EPISODE_ID
)
AS
 SELECT playbook_text.Episode_acronym,
        playbook_text.Episode_description,
        playbook_text.Enrollment_Criteria,
        playbook_text.Age,
        playbook_text.Trigger_criteria,
        playbook_text.Episode_limit,
        playbook_text.EPISODE_ID
 FROM episode_builder_5_4_002.playbook_text
 ORDER BY playbook_text.Episode_acronym,
          playbook_text.Episode_description,
          playbook_text.Enrollment_Criteria,
          playbook_text.Age,
          playbook_text.Trigger_criteria,
          playbook_text.Episode_limit,
          playbook_text.EPISODE_ID
SEGMENTED BY hash(playbook_text.EPISODE_ID, playbook_text.Episode_acronym, playbook_text.Enrollment_Criteria, playbook_text.Age, playbook_text.Episode_limit, playbook_text.Episode_description, playbook_text.Trigger_criteria) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.roles /*+createtype(L)*/ 
(
 ROLE_NAME,
 DESCRIPTION
)
AS
 SELECT roles.ROLE_NAME,
        roles.DESCRIPTION
 FROM episode_builder_5_4_002.roles
 ORDER BY roles.ROLE_NAME,
          roles.DESCRIPTION
SEGMENTED BY hash(roles.ROLE_NAME, roles.DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.test /*+createtype(L)*/ 
(
 INDEX,
 WEB_ROOT,
 LOCAL_ROOT,
 EXPORT_EP,
 EXPORT_ALL,
 BACKUP_IMPORT,
 KILL_PROC,
 SCRIPT_ROOT,
 LOG_PATH
)
AS
 SELECT test.INDEX,
        test.WEB_ROOT,
        test.LOCAL_ROOT,
        test.EXPORT_EP,
        test.EXPORT_ALL,
        test.BACKUP_IMPORT,
        test.KILL_PROC,
        test.SCRIPT_ROOT,
        test.LOG_PATH
 FROM episode_builder_5_4_002.test
 ORDER BY test.INDEX,
          test.WEB_ROOT,
          test.LOCAL_ROOT,
          test.EXPORT_EP,
          test.EXPORT_ALL,
          test.BACKUP_IMPORT,
          test.KILL_PROC,
          test.SCRIPT_ROOT,
          test.LOG_PATH
SEGMENTED BY hash(test.INDEX, test.EXPORT_EP, test.EXPORT_ALL, test.BACKUP_IMPORT, test.KILL_PROC, test.SCRIPT_ROOT, test.LOG_PATH, test.WEB_ROOT, test.LOCAL_ROOT) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.transactions /*+createtype(L)*/ 
(
 ID,
 USER_ID,
 STATUS,
 FILE_PATH,
 TYPE,
 START_DATE,
 END_DATE,
 PID,
 EPISODE_ID
)
AS
 SELECT transactions.ID,
        transactions.USER_ID,
        transactions.STATUS,
        transactions.FILE_PATH,
        transactions.TYPE,
        transactions.START_DATE,
        transactions.END_DATE,
        transactions.PID,
        transactions.EPISODE_ID
 FROM episode_builder_5_4_002.transactions
 ORDER BY transactions.ID,
          transactions.USER_ID,
          transactions.STATUS,
          transactions.FILE_PATH,
          transactions.TYPE,
          transactions.START_DATE,
          transactions.END_DATE,
          transactions.PID,
          transactions.EPISODE_ID
SEGMENTED BY hash(transactions.ID, transactions.START_DATE, transactions.END_DATE, transactions.USER_ID, transactions.STATUS, transactions.TYPE, transactions.PID, transactions.EPISODE_ID, transactions.FILE_PATH) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.type /*+createtype(L)*/ 
(
 ID,
 DESCRIPTION
)
AS
 SELECT type.ID,
        type.DESCRIPTION
 FROM episode_builder_5_4_002.type
 ORDER BY type.ID,
          type.DESCRIPTION
SEGMENTED BY hash(type.ID, type.DESCRIPTION) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.universal_risk_factors /*+createtype(L)*/ 
(
 code_id,
 type_id,
 group_id,
 group_name
)
AS
 SELECT universal_risk_factors.code_id,
        universal_risk_factors.type_id,
        universal_risk_factors.group_id,
        universal_risk_factors.group_name
 FROM episode_builder_5_4_002.universal_risk_factors
 ORDER BY universal_risk_factors.code_id,
          universal_risk_factors.type_id,
          universal_risk_factors.group_id,
          universal_risk_factors.group_name
SEGMENTED BY hash(universal_risk_factors.type_id, universal_risk_factors.code_id, universal_risk_factors.group_id, universal_risk_factors.group_name) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.universal_risk_factors_10 /*+createtype(L)*/ 
(
 code_id,
 type_id,
 group_id,
 group_name
)
AS
 SELECT universal_risk_factors_10.code_id,
        universal_risk_factors_10.type_id,
        universal_risk_factors_10.group_id,
        universal_risk_factors_10.group_name
 FROM episode_builder_5_4_002.universal_risk_factors_10
 ORDER BY universal_risk_factors_10.code_id,
          universal_risk_factors_10.type_id,
          universal_risk_factors_10.group_id,
          universal_risk_factors_10.group_name
SEGMENTED BY hash(universal_risk_factors_10.type_id, universal_risk_factors_10.code_id, universal_risk_factors_10.group_id, universal_risk_factors_10.group_name) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.user_roles /*+createtype(L)*/ 
(
 USER_ID,
 ROLE_NAME
)
AS
 SELECT user_roles.USER_ID,
        user_roles.ROLE_NAME
 FROM episode_builder_5_4_002.user_roles
 ORDER BY user_roles.USER_ID,
          user_roles.ROLE_NAME
SEGMENTED BY hash(user_roles.USER_ID, user_roles.ROLE_NAME) ALL NODES KSAFE 1;

CREATE PROJECTION episode_builder_5_4_002.users /*+createtype(L)*/ 
(
 USER_ID,
 PASSWORD,
 EMAIL,
 ENABLED,
 FIRST_NAME,
 LAST_NAME,
 NICK_NAME
)
AS
 SELECT users.USER_ID,
        users.PASSWORD,
        users.EMAIL,
        users.ENABLED,
        users.FIRST_NAME,
        users.LAST_NAME,
        users.NICK_NAME
 FROM episode_builder_5_4_002.users
 ORDER BY users.USER_ID,
          users.PASSWORD,
          users.EMAIL,
          users.ENABLED,
          users.FIRST_NAME,
          users.LAST_NAME,
          users.NICK_NAME
SEGMENTED BY hash(users.ENABLED, users.USER_ID, users.FIRST_NAME, users.LAST_NAME, users.NICK_NAME, users.EMAIL, users.PASSWORD) ALL NODES KSAFE 1;



