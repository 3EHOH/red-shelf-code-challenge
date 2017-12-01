set search_path=epbuilder;

select 'Step: DeDupeAll.sql' as curstep, current_timestamp as datetime;
                                                                                                                                        
\set AUTOCOMMIT on     

/* De-dupe queries for all outputs of normalization and construction */

/*  First do episode construction outputs */

DELETE FROM assignment 
WHERE
    id NOT IN (SELECT 
        *
    FROM
        (SELECT 
            MIN(id)
        FROM
            assignment
        GROUP BY master_episode_id , master_claim_id) tmpId);


DELETE FROM episode
WHERE
    id NOT IN (SELECT 
            *
        FROM
            (SELECT 
                MIN(id)
            FROM
                episode
            GROUP BY master_episode_id , master_claim_id) tmpId);


DELETE FROM triggers 
WHERE
    id NOT IN (SELECT 
        *
    FROM
        (SELECT 
            MIN(id)
        FROM
            triggers
        GROUP BY master_episode_id , master_claim_id) tmpId);
    
    
DELETE FROM association 
WHERE
    id NOT IN (SELECT 
        *
    FROM
        (SELECT 
            MIN(id)
        FROM
            association
        GROUP BY parent_master_episode_id , child_master_episode_id , association_type , association_level) tmpId);


/* Then work through the claim tables - these are all related  */
/* Taking top down approach from claims_combined, through claim_line and claim_line_rx, finally cascading to code */

DELETE FROM claims_combined 
WHERE
    id NOT IN (SELECT 
        *
    FROM
        (SELECT 
            MIN(id)
        FROM
            claims_combined
        GROUP BY master_claim_id) tmpId);
    
    
DELETE FROM claim_line 
WHERE
    id NOT IN (SELECT 
        id
    FROM
        claims_combined);
        
        
DELETE FROM claim_line_rx 
WHERE
    id NOT IN (SELECT 
        id
    FROM
        claims_combined);


DELETE FROM code 
WHERE
    u_c_id NOT IN (SELECT 
        id
    FROM
        claims_combined);



/* Finish up other normalization outputs */


DELETE FROM member 
WHERE
    id NOT IN (SELECT 
        *
    FROM
        (SELECT 
            MIN(id)
        FROM
            member
        GROUP BY member_id , insurance_type , insurance_carrier) tmpId);


DELETE FROM enrollment 
WHERE
    id NOT IN (SELECT 
        *
    FROM
        (SELECT 
            MIN(id)
        FROM
            enrollment
        GROUP BY member_id , begin_date , end_date , insurance_product , coverage_type) tmpId);



DELETE FROM provider 
WHERE
    id NOT IN (SELECT 
        *
    FROM
        (SELECT 
            MIN(id)
        FROM
            provider
        GROUP BY provider_id , npi) tmpId);


