create or replace view TABLEAU.TABLEAU_REST.VW_PROJECT_HIERARCHY(
	LUID,
	"Name",
	"Description",
	"Created_At",
	"Updated_At",
	"Site_LUID",
	"Owner_LUID",
	"Project_Level",
	"Environment",
	"Top_Project_Owner_LUID",
	PROJECT_LEVEL_1,
	PROJECT_LEVEL_2,
	PROJECT_LEVEL_3,
	PROJECT_LEVEL_4,
	PROJECT_LEVEL_5,
	PROJECT_LEVEL_6,
	PROJECT_LEVEL_7,
	PROJECT_LEVEL_8,
	PROJECT_LEVEL_9,
	PROJECT_LEVEL_10,
	"Full_Path",
	"Admin_Insights_Published_At"
) as

WITH RECURSIVE ProjectHierarchy AS (
    -- Base case: Root-level projects
    SELECT
        p.LUID,
        p."Name",
        p."Description",
        p."Created_At",
        p."Updated_At",
        p."Site_LUID",
        p."Content_Permissions",
        p."Parent_Project_LUID",
        p."Owner_LUID",
        p."Project_Level",
        p."Admin_Insights_Published_At",

        -- Hierarchy fields
        p."Name" AS "Full_Path",
        ARRAY_CONSTRUCT(p."Name") AS "Name_Path_Array",
        ARRAY_CONSTRUCT(p."Owner_LUID") AS "Owner_Path_Array"
    FROM TABLEAU.TABLEAU_REST.ITEMS_PROJECTS p
    WHERE p."Parent_Project_LUID" IS NULL OR "Parent_Project_LUID" = ''

    UNION ALL

    -- Recursive step: join children
    SELECT
        p.LUID,
        p."Name",
        p."Description",
        p."Created_At",
        p."Updated_At",
        p."Site_LUID",
        p."Content_Permissions",
        p."Parent_Project_LUID",
        p."Owner_LUID",
        p."Project_Level",
        p."Admin_Insights_Published_At",

        ph."Full_Path" || ' > ' || p."Name" AS "Full_Path",
        ARRAY_APPEND(ph."Name_Path_Array", p."Name") AS "Name_Path_Array",
        ARRAY_APPEND(ph."Owner_Path_Array", p."Owner_LUID") AS "Owner_Path_Array"
    FROM TABLEAU.TABLEAU_REST.ITEMS_PROJECTS p
    INNER JOIN ProjectHierarchy ph ON p."Parent_Project_LUID" = ph.LUID
    WHERE ph."Project_Level" < 9
)

SELECT
    LUID,
    "Name",
    "Description",
    "Created_At",
    "Updated_At",
    "Site_LUID",
    "Owner_LUID",
    "Project_Level",
    CASE
        WHEN COALESCE("Name_Path_Array"[0], NULL) = '- Archive' THEN 'Archive'
        WHEN COALESCE("Name_Path_Array"[0], NULL) = '- In Development' THEN 'Development'
        WHEN COALESCE("Name_Path_Array"[0], NULL) = '- UAT (User Acceptance Testing)' THEN 'UAT'
        WHEN COALESCE("Name_Path_Array"[0], NULL) = '- Certified Assets' THEN 'Certified'
        WHEN COALESCE("Name_Path_Array"[0], NULL) IN ('- Design Standards', '- Tableau Training', 'Admin Insights') THEN 'Administrative'
        WHEN COALESCE("Name_Path_Array"[0], NULL) IN ('default', 'External Assets Default Project') THEN 'Default'
        ELSE 'Production'
        END AS "Environment",
    CASE
        WHEN COALESCE("Name_Path_Array"[0], NULL) = '- Archive' AND "Project_Level" > 0 THEN REPLACE("Owner_Path_Array"[1], '"', '')
        WHEN COALESCE("Name_Path_Array"[0], NULL) = '- In Development' AND "Project_Level" > 0 THEN REPLACE("Owner_Path_Array"[1], '"', '')
        WHEN COALESCE("Name_Path_Array"[0], NULL) = '- UAT (User Acceptance Testing)' AND "Project_Level" > 0 THEN REPLACE("Owner_Path_Array"[1], '"', '')
        WHEN COALESCE("Name_Path_Array"[0], NULL) = 'Development' AND "Project_Level" > 0 THEN REPLACE("Owner_Path_Array"[1], '"', '')
        ELSE REPLACE("Owner_Path_Array"[0], '"', '')
        END AS "Top_Project_Owner_LUID",        
    -- Break out each project level into its own column (safe with COALESCE)
    REPLACE("Name_Path_Array"[0], '"', '') AS Project_Level_1,
    REPLACE("Name_Path_Array"[1], '"', '') AS Project_Level_2,
    REPLACE("Name_Path_Array"[2], '"', '') AS Project_Level_3,
    REPLACE("Name_Path_Array"[3], '"', '') AS Project_Level_4,
    REPLACE("Name_Path_Array"[4], '"', '') AS Project_Level_5,
    REPLACE("Name_Path_Array"[5], '"', '') AS Project_Level_6,
    REPLACE("Name_Path_Array"[6], '"', '') AS Project_Level_7,
    REPLACE("Name_Path_Array"[7], '"', '') AS Project_Level_8,
    REPLACE("Name_Path_Array"[8], '"', '') AS Project_Level_9,
    REPLACE("Name_Path_Array"[9], '"', '') AS Project_Level_10,
    "Full_Path",
    "Admin_Insights_Published_At"
FROM ProjectHierarchy
ORDER BY "Full_Path";
