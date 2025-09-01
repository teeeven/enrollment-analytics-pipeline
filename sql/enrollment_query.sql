/*
Enrollment Data Query Template

This is a generalized SQL query for extracting student enrollment data.
Customize this query based on your institution's database schema.

Required columns:
- student_id: Unique identifier for each student
- student_name: Full name or displayable name for the student

Recommended additional columns:
- division: Academic division/college/school
- program: Program of study or major
- level: Academic level (Undergraduate, Graduate, Doctoral, etc.)
- status: Current enrollment status
- email: Student email for communication

Note: This template uses common table/column names. You'll need to adjust
      table names, column names, and business logic for your specific system.
*/

-- Standard enrollment query for most Student Information Systems
SELECT DISTINCT 
    s.student_id::VARCHAR as student_id,
    TRIM(CONCAT(s.first_name, ' ', s.last_name)) as student_name,
    s.email,
    d.division_name as division,
    p.program_name as program,
    CASE 
        WHEN s.academic_level = 'UG' THEN 'Undergraduate'
        WHEN s.academic_level = 'GR' THEN 'Graduate'
        WHEN s.academic_level = 'DR' THEN 'Doctoral'
        ELSE s.academic_level
    END as level,
    e.enrollment_status as status
FROM students s
    INNER JOIN enrollments e ON s.student_id = e.student_id
    LEFT JOIN programs p ON e.program_id = p.program_id
    LEFT JOIN divisions d ON p.division_id = d.division_id
WHERE 
    -- Current semester filter
    e.semester = 'Fall'
    AND e.academic_year = EXTRACT(YEAR FROM CURRENT_DATE)
    
    -- Active enrollments only
    AND e.enrollment_status IN ('Enrolled', 'Active', 'Registered')
    
    -- Exclude withdrawn/graduated students
    AND s.student_status NOT IN ('Withdrawn', 'Graduated', 'Dismissed')
    
    -- Exclude test/system accounts (adjust criteria as needed)
    AND s.student_id NOT LIKE 'TEST%'
    AND s.student_id NOT LIKE '999%'
    
ORDER BY s.student_id;

/*
Alternative query for Banner ERP systems:
SELECT DISTINCT
    spriden_id as student_id,
    spriden_last_name || ', ' || spriden_first_name as student_name,
    goremal_email_address as email,
    stvdivs_desc as division,
    stvmajr_desc as program,
    stvlevl_desc as level
FROM spriden
    JOIN sfrstcr ON spriden_pidm = sfrstcr_pidm
    JOIN stvterm ON sfrstcr_term_code = stvterm_code
    LEFT JOIN goremal ON spriden_pidm = goremal_pidm AND goremal_emal_code = 'UNIV'
    LEFT JOIN sgbstdn ON spriden_pidm = sgbstdn_pidm
    LEFT JOIN stvdivs ON sgbstdn_divs_code = stvdivs_code
    LEFT JOIN stvmajr ON sgbstdn_majr_code_1 = stvmajr_code
    LEFT JOIN stvlevl ON sgbstdn_levl_code = stvlevl_code
WHERE spriden_change_ind IS NULL
    AND sfrstcr_term_code = :current_term
    AND sfrstcr_rsta_code IN ('RW', 'RE', 'AU')
    AND stvterm_fa_proc_complete = 'Y'
ORDER BY spriden_id;
*/

/*
Alternative query for PeopleSoft systems:
SELECT DISTINCT
    ps.emplid as student_id,
    pn.name_display as student_name,
    pe.email_addr as email,
    pa.acad_org_desc as division,
    pp.descr as program,
    pl.descr as level
FROM ps_personal_data pd
    JOIN ps_names pn ON pd.emplid = pn.emplid
    JOIN ps_stdnt_enrl se ON pd.emplid = se.emplid  
    LEFT JOIN ps_email_addresses pe ON pd.emplid = pe.emplid
    LEFT JOIN ps_acad_org pa ON se.acad_org = pa.acad_org
    LEFT JOIN ps_acad_prog_tbl pp ON se.acad_prog = pp.acad_prog
    LEFT JOIN ps_acad_level_tbl pl ON se.acad_level = pl.acad_level
WHERE se.strm = :current_term
    AND se.stdnt_enrl_status = 'E'
    AND se.enrl_status_reason NOT IN ('WDRW', 'GRAD')
    AND pn.name_type = 'PRI'
    AND pe.e_addr_type = 'CAMP'
ORDER BY ps.emplid;
*/

/*
Simplified query for basic systems:
SELECT 
    student_id,
    full_name as student_name,
    'Main Campus' as division,
    major as program,
    class_level as level
FROM current_enrollments
WHERE semester = 'Fall 2024'
    AND status = 'Active'
ORDER BY student_id;
*/