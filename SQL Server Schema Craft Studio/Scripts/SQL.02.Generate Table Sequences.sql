-- :SETVAR OutputPath "C:\Users\...\SQL Server Schema Craft Studio\Data\Inputs\version (n)"

/*--<< CONFIGURE >>--**************************************************************************************************************************
	Set the schema names that exist in your database and assign a priority order.
	Lower number = higher priority (appears earlier in the hierarchy).
	Schemas not listed here will have NULL priority and sort last.
	Add or remove rows to match your database's naming convention.
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#UserSchemaPriority') IS NOT NULL DROP TABLE #UserSchemaPriority
	CREATE TABLE #UserSchemaPriority (schema_name VARCHAR(255), schema_priority INT)
	INSERT INTO #UserSchemaPriority (schema_name, schema_priority) VALUES
		('schema_a', 1),
		('schema_b', 2),
		('schema_c', 3),
		('schema_d', 4)
/*--<< END CONFIGURE >>--*********************************************************************************************************************/

/*--<< STEP 00-A >>--*****************************************************************************************************************************
	(i)		Extract the Schema & Database Metadata across all tables in-scope
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#AllObjectsDB') IS NOT NULL DROP TABLE #AllObjectsDB
	SELECT
		@@SERVERNAME AS server_name,
		SCHEMA_NAME(tab.schema_id) AS schema_name,
		tab.name AS table_name,
		col.name AS column_name,
		t.name AS data_type,
		CASE 
			WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar') THEN t.name + '(' + CASE WHEN col.max_length = -1 THEN 'max' ELSE CONVERT(VARCHAR(10), col.max_length) END + ')'
			WHEN t.name IN ('decimal', 'numeric') THEN t.name + '(' + CONVERT(VARCHAR(10), col.precision) + ',' + CONVERT(VARCHAR(10), col.scale) + ')'
			ELSE t.name
		END AS data_type_with_size,
		CASE WHEN col.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
		CASE WHEN idx.index_id IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_unique,
		GETDATE() AS last_accessed_timestamp,
		CASE WHEN tab.type = '1' THEN 'SYSTEM_TABLE'
			 WHEN tab.type = 'U' THEN 'USER_TABLE'
			 WHEN tab.type = 'V' THEN 'VIEW'
			 ELSE tab.type + '- is UNKNOWN'
		    END AS table_type,
		CASE WHEN pk.index_id IS NOT NULL AND ref_tab.schema_id IS NULL THEN '<Primary Key>' ELSE SCHEMA_NAME(ref_tab.schema_id) END AS referenced_schema_name,
        CASE WHEN pk.index_id IS NOT NULL AND ref_tab.name IS NULL THEN '<Primary Key>' ELSE ref_tab.name END AS referenced_table_name,
        CASE WHEN pk.index_id IS NOT NULL AND ref_col.name IS NULL THEN '<Primary Key>' ELSE ref_col.name END AS referenced_column_name
	INTO #AllObjectsDB
	FROM sys.tables tab
	INNER JOIN sys.columns col
		ON tab.object_id = col.object_id
	INNER JOIN sys.types t 
		ON col.system_type_id = t.system_type_id 
		AND col.user_type_id = t.user_type_id
	LEFT JOIN (
		SELECT object_id, index_id
		FROM sys.indexes
		WHERE is_primary_key = 1
	) idx 
		ON tab.object_id = idx.object_id
		AND col.column_id IN (
			SELECT column_id 
			FROM sys.index_columns
			WHERE object_id = idx.object_id 
			AND index_id = idx.index_id
		)
	LEFT JOIN sys.foreign_key_columns fk
		ON col.object_id = fk.parent_object_id 
		AND col.column_id = fk.parent_column_id
	LEFT JOIN sys.tables ref_tab
		ON fk.referenced_object_id = ref_tab.object_id
	LEFT JOIN sys.columns ref_col
		ON fk.referenced_object_id = ref_col.object_id 
		AND fk.referenced_column_id = ref_col.column_id
	LEFT JOIN (
		SELECT object_id, index_id
		FROM sys.indexes
		WHERE is_primary_key = 1
	) pk
		ON tab.object_id = pk.object_id
		AND col.column_id IN (
			SELECT column_id 
			FROM sys.index_columns
			WHERE object_id = pk.object_id 
				AND index_id = pk.index_id
		)
	WHERE 1 = 1
		AND SCHEMA_NAME(tab.schema_id) NOT IN ('dbo', 'cdc')
		--AND tab.name like 'case%'
		--AND col.name IN ('')
	ORDER BY schema_name, table_name, col.column_id;

/*--<< STEP 00-B >>--*****************************************************************************************************************************
	(i)		Extract the Schema in-scope and define its order
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#SchemaOrder') IS NOT NULL DROP TABLE #SchemaOrder
	SELECT
		X.schema_name,
		USP.schema_priority
	INTO #SchemaOrder
	FROM
	(	SELECT DISTINCT schema_name FROM #AllObjectsDB
		UNION
		SELECT DISTINCT referenced_schema_name FROM #AllObjectsDB WHERE referenced_schema_name IS NOT NULL AND referenced_schema_name NOT IN ('<Primary Key>')
	)X
	LEFT JOIN #UserSchemaPriority USP 
		ON USP.schema_name = X.schema_name

	/*--<< Debug >>--------------------------------------------------------------------------------------------------------------------------------
		SELECT * FROM #SchemaOrder
		SELECT * FROM #AllObjectsDB
			SELECT * FROM #AllObjectsDB WHERE table_name = 'Business_Service_Transaction_Type'
			SELECT * FROM #AllObjectsDB WHERE referenced_table_name = 'Business_Service_Transaction_Type'
	*---------------------------------------------------------------------------------------------------------------------------------<< Debug >>--*/

/*--<< STEP 01-A >>--*****************************************************************************************************************************
	(i)		Extract those tables that are Independent Tables i.e. neither a Child nor a Parent to any other table into a Temporary Table
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#UnrelatedTables') IS NOT NULL DROP TABLE #UnrelatedTables
	;WITH CTE AS
	(
		SELECT DISTINCT
			A1.schema_name,
			A1.table_name,
			A1.referenced_schema_name,
			A1.referenced_table_name
		FROM #AllObjectsDB A1
	)
		SELECT DISTINCT
			PSO.schema_priority AS parent_schema_priority,
			A2.referenced_schema_name AS parent_schema_name, 
			A2.referenced_table_name AS parent_table_name,
			CAST(NULL AS FLOAT) AS parent_table_level,
			CSO.schema_priority AS child_schema_priority,
			A2.schema_name AS child_schema_name,
			A2.table_name AS child_table_name
		INTO #UnrelatedTables
		FROM CTE X2
		JOIN #AllObjectsDB A2
			ON A2.schema_name = X2.schema_name
			AND A2.table_name = X2.table_name
			AND A2.referenced_schema_name = X2.referenced_schema_name
			AND A2.referenced_table_name = X2.referenced_table_name
		LEFT JOIN (
			SELECT DISTINCT
				schema_name,
				table_name
			FROM CTE X1
			WHERE 1 = 1
				AND X1.referenced_schema_name IS NOT NULL
				AND X1.referenced_schema_name <> '<Primary Key>'
		)X3
			ON X2.schema_name = X3.schema_name
			AND X2.table_name = X3.table_name
		LEFT JOIN #SchemaOrder CSO
			ON CSO.schema_name = X2.schema_name
		LEFT JOIN #SchemaOrder PSO
			ON PSO.schema_name = X2.referenced_schema_name
		LEFT JOIN (
			SELECT DISTINCT 
				referenced_schema_name, 
				referenced_table_name 
			FROM #AllObjectsDB
			WHERE referenced_schema_name IS NOT NULL 
			  AND referenced_schema_name <> '<Primary Key>'
		) X4
			ON X2.schema_name = X4.referenced_schema_name
			AND X2.table_name = X4.referenced_table_name
		WHERE 1 = 1
            AND X3.schema_name IS NULL
            AND X3.table_name IS NULL
            AND X4.referenced_schema_name IS NULL
            AND X4.referenced_table_name IS NULL

/*--<< STEP 01-B >>--*****************************************************************************************************************************
	(i)		Extract those tables that Self Referencing Tables into a Temporary Table
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#SelfReferencingTables') IS NOT NULL DROP TABLE #SelfReferencingTables
	SELECT DISTINCT
		PSO.schema_priority AS parent_schema_priority,
		X.referenced_schema_name AS parent_schema_name, 
		X.referenced_table_name AS parent_table_name,
		CAST(NULL AS FLOAT) AS parent_table_level,
		CSO.schema_priority AS child_schema_priority,
		X.schema_name AS child_schema_name,
		X.table_name AS child_table_name
	INTO #SelfReferencingTables
	FROM #AllObjectsDB X
	LEFT JOIN #SchemaOrder CSO
		ON CSO.schema_name = X.schema_name
	LEFT JOIN #SchemaOrder PSO
		ON PSO.schema_name = X.referenced_schema_name
	WHERE 1 = 1
		AND X.referenced_schema_name <> '<Primary Key>'
		AND X.schema_name + '.' + X.table_name = X.referenced_schema_name + '.' + X.referenced_table_name

/*--<< STEP 01-C >>--*****************************************************************************************************************************
	(i)		Extract those tables that do not have any parents into a Temporary Table
	(ii)	CTE selects all tables and their parent relationships, including schema priorities and table names and excluding self-referencing 
			tables
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#ParentMostTablesWithoutParents') IS NOT NULL DROP TABLE #ParentMostTablesWithoutParents
	;WITH CTE AS
	(
		SELECT DISTINCT 
			PSO.schema_priority AS parent_schema_priority,
			X.referenced_schema_name AS parent_schema_name, 
			X.referenced_table_name AS parent_table_name,
			CAST(NULL AS FLOAT) AS parent_table_level,
			CSO.schema_priority AS child_schema_priority,
			X.schema_name AS child_schema_name,
			X.table_name AS child_table_name
		FROM #AllObjectsDB X
		LEFT JOIN #SchemaOrder CSO
			ON CSO.schema_name = X.schema_name
		LEFT JOIN #SchemaOrder PSO
			ON PSO.schema_name = X.referenced_schema_name
		WHERE 1 = 1
			AND X.referenced_schema_name <> '<Primary Key>'
			AND X.schema_name + '.' + X.table_name <> X.referenced_schema_name + '.' + X.referenced_table_name
		--ORDER BY X.schema_name, X.table_name
	)
		SELECT X1.* 
		INTO #ParentMostTablesWithoutParents
		FROM CTE X1
		JOIN 
		(
			--Logic to select distinct parent tables that do not have any parents
			SELECT DISTINCT 
				parent_schema_name, 
				parent_table_name
			FROM CTE X2
			WHERE parent_table_name IS NULL 
			   OR NOT EXISTS (
					SELECT 1 
					FROM CTE X3
					WHERE X3.child_schema_name = X2.parent_schema_name 
					  AND X3.child_table_name = X2.parent_table_name
			)
		)X4
			ON X1.parent_schema_name = X4.parent_schema_name
			AND X1.parent_table_name = X4.parent_table_name
			AND X4.parent_schema_name + '.' + X4.parent_table_name NOT IN (SELECT parent_schema_name + '.' + parent_table_name FROM #SelfReferencingTables)
		/*--<< Debug >>--------------------------------------------------------------------------------------------------------------------------------
			# 01:	All those Parent Most tables SHOULD NOT have any Parents i.e. referenced_tables
						SELECT * FROM #AllObjectsDB 
						WHERE 1 = 1
							AND schema_name + '.' +  table_name IN (SELECT DISTINCT parent_schema_name + '.' + parent_table_name FROM #ParentMostTablesWithoutParents) 
							AND referenced_schema_name NOT IN ('<Primary Key>') AND referenced_schema_name IS NOT NULL
		
			# 02:	All those Parent Most tables SHOULD have ATLEAST 1 Child Table
						SELECT referenced_schema_name + '.' +  referenced_table_name, COUNT(*) 
						FROM #AllObjectsDB 
						WHERE 1 = 1
							AND referenced_schema_name + '.' +  referenced_table_name IN (SELECT DISTINCT parent_schema_name + '.' + parent_table_name FROM #ParentMostTablesWithoutParents) 
						GROUP BY referenced_schema_name + '.' +  referenced_table_name 
						HAVING COUNT(*) < 1
		*---------------------------------------------------------------------------------------------------------------------------------<< Debug >>--*/

/*--<< STEP 01-D >>--*****************************************************************************************************************************
	(i)		Extract those tables that do not have any children into a Temporary Table
	(ii)	CTE selects all tables and their parent relationships, including schema priorities and table names and excluding self-referencing 
			tables
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#TablesWithoutChildren') IS NOT NULL DROP TABLE #TablesWithoutChildren
	;WITH CTE AS
	(
		SELECT DISTINCT 
			PSO.schema_priority AS parent_schema_priority,
			X.referenced_schema_name AS parent_schema_name, 
			X.referenced_table_name AS parent_table_name,
			CAST(NULL AS FLOAT) AS parent_table_level,
			CSO.schema_priority AS child_schema_priority,
			X.schema_name AS child_schema_name,
			X.table_name AS child_table_name
		FROM #AllObjectsDB X
		LEFT JOIN #SchemaOrder CSO
			ON CSO.schema_name = X.schema_name
		LEFT JOIN #SchemaOrder PSO
			ON PSO.schema_name = X.referenced_schema_name
		WHERE 1 = 1
			AND X.referenced_schema_name <> '<Primary Key>'
			AND X.schema_name + '.' + X.table_name <> X.referenced_schema_name + '.' + X.referenced_table_name
		--ORDER BY X.schema_name, X.table_name
	)
		SELECT X1.* 
		INTO #TablesWithoutChildren
		FROM CTE X1
		JOIN 
		(
			--Logic to select distinct child tables that are not parent to any table (no children)
			SELECT DISTINCT 
				child_schema_name, 
				child_table_name
			FROM CTE X2
			WHERE 1 = 1
			   AND NOT EXISTS (
					SELECT 1 
					FROM CTE X3
					WHERE X3.parent_schema_name = X2.child_schema_name
					  AND X3.parent_table_name = X2.child_table_name
			)
		)X4
			ON X1.child_schema_name = X4.child_schema_name
			AND X1.child_table_name = X4.child_table_name
			AND X4.child_schema_name + '.' + X4.child_table_name NOT IN (SELECT child_schema_name + '.' + child_table_name FROM #SelfReferencingTables)
		/*--<< Debug >>--------------------------------------------------------------------------------------------------------------------------------
			# 01:	All those Child Most tables SHOULD NOT have any Child Table i.e. table_names
						SELECT * FROM #AllObjectsDB 
						WHERE 1 = 1
							AND referenced_schema_name + '.' +  referenced_table_name IN (SELECT DISTINCT child_schema_name + '.' + child_table_name FROM #TablesWithoutChildren)
		
		*---------------------------------------------------------------------------------------------------------------------------------<< Debug >>--*/

/*--<< STEP 01-E >>--*****************************************************************************************************************************
	(i)		Extract those tables that do not have any children into a Temporary Table
	(ii)	Getting the list of Most Child tables that have other tables as parents besides having the Top Most Parent Table(s) as a parent
	(iii)	Focus: child_table_object to rank it next to Parent Most Table as per the Schema Priority
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#TablesWithoutChildrenHavingRootParent') IS NOT NULL DROP TABLE #TablesWithoutChildrenHavingRootParent
	;WITH CTE AS
	(
		SELECT DISTINCT 
			child_schema_name + '.' + child_table_name AS child_object_name,
			parent_schema_name + '.' + parent_table_name AS parent_object_name
		FROM #TablesWithoutChildren
	)
		SELECT DISTINCT 
			TWC.*
		INTO #TablesWithoutChildrenHavingRootParent
		FROM #TablesWithoutChildren TWC
		WHERE TWC.child_schema_name + '.' + TWC.child_table_name NOT IN (
			SELECT DISTINCT 
				 CTE.child_object_name
			FROM CTE
			LEFT JOIN #ParentMostTablesWithoutParents RT
				ON CTE.parent_object_name = RT.parent_schema_name + '.' + RT.parent_table_name
			WHERE 1 = 1
				AND RT.parent_schema_name IS NULL
				AND RT.child_schema_name IS NULL
		)

/*--<< STEP 01-F >>--*****************************************************************************************************************************
	(i)		Extracts tables that are in the middle layers into a temporary table #AllMidLevelTables.
	(ii)	Selects all tables and their parent relationships, including schema priorities and table names and excluding self-referencing &  
			Children having Root as Parent tables.
	(iii)	Iteratively updates the levels for middle layer tables based on their parent-child relationships.
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#AllMidLevelTables') IS NOT NULL DROP TABLE #AllMidLevelTables
	SELECT DISTINCT 
		PSO.schema_priority AS parent_schema_priority,
		X.referenced_schema_name AS parent_schema_name, 
		X.referenced_table_name AS parent_table_name,
		CAST(NULL AS FLOAT) AS parent_table_level,
		CSO.schema_priority AS child_schema_priority,
		X.schema_name AS child_schema_name,
		X.table_name AS child_table_name
	INTO #AllMidLevelTables
	FROM #AllObjectsDB X
	LEFT JOIN #SchemaOrder CSO
		ON CSO.schema_name = X.schema_name
	LEFT JOIN #SchemaOrder PSO
		ON PSO.schema_name = X.referenced_schema_name
	WHERE 1 = 1
		AND X.referenced_schema_name <> '<Primary Key>'
		AND X.referenced_schema_name + '.' + X.referenced_table_name <> X.schema_name + '.' + X.table_name
		AND X.referenced_schema_name + '.' + X.referenced_table_name NOT IN (
				SELECT DISTINCT parent_schema_name + '.' + parent_table_name FROM #ParentMostTablesWithoutParents --Exclude from DB list: those parent_object_name from Parent Most Tables
				UNION 
				SELECT DISTINCT child_schema_name + '.' + child_table_name FROM #TablesWithoutChildren --Exclude from DB list: those child_object_name from Child Most Tables
			)
	ORDER BY X.schema_name, X.table_name

	WHILE (1 = 1)
	BEGIN
		UPDATE A
		SET
			parent_table_level = (SELECT MAX(ISNULL(parent_table_level, 0)) FROM #AllMidLevelTables) + 1
		FROM #AllMidLevelTables A
		JOIN
		(
			SELECT DISTINCT
				parent_schema_name,
				parent_table_name
			FROM
				#AllMidLevelTables P
			WHERE 1 = 1
				AND parent_table_level IS NULL
				AND NOT EXISTS (
					SELECT 1
					FROM #AllMidLevelTables C
					WHERE parent_table_level IS NULL
						AND P.parent_schema_name = C.child_schema_name
						AND P.parent_table_name = C.child_table_name
				)
		)X
			ON X.parent_schema_name = A.parent_schema_name
			AND X.parent_table_name = A.parent_table_name

		IF @@ROWCOUNT = 0
			BREAK;
	END;
	/*--<< Debug >>--------------------------------------------------------------------------------------------------------------------------------
		SELECT * FROM #AllMidLevelTables
			SELECT * FROM #AllMidLevelTables WHERE parent_table_level IS NULL 

		SELECT DISTINCT 
			schema_name + '.' + table_name
		FROM #AllObjectsDB 
		WHERE schema_name + '.' + table_name NOT IN (
				SELECT TargetSchema + '.' + TargetTable 
				FROM #TableHierarchy
			)
	*---------------------------------------------------------------------------------------------------------------------------------<< Debug >>--*/

/*--<< STEP 02-A >>--*****************************************************************************************************************************
	(i)		Populate the table hierarchies across all identified sections into a Temporary Table
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#TableHierarchy') IS NOT NULL DROP TABLE #TableHierarchy
	CREATE TABLE #TableHierarchy (
		ID INT IDENTITY(1,1),
		SchemaPriority INT,
		TargetSchema VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
		TargetTable VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS ,
		SectionLevel INT,
		TableLevel INT,
		Hierarchy INT
	);

	INSERT INTO #TableHierarchy (SchemaPriority, TargetSchema, TargetTable, SectionLevel, TableLevel)
	SELECT DISTINCT child_schema_priority, child_schema_name, child_table_name, -1, NULL FROM #UnrelatedTables
	UNION
	SELECT DISTINCT parent_schema_priority, parent_schema_name, parent_table_name, 1, NULL FROM #ParentMostTablesWithoutParents
	UNION
	SELECT DISTINCT child_schema_priority, child_schema_name, child_table_name, 2, NULL FROM #TablesWithoutChildrenHavingRootParent
	UNION
	SELECT DISTINCT parent_schema_priority, parent_schema_name, parent_table_name, 3, parent_table_level FROM #AllMidLevelTables WHERE parent_table_level IS NOT NULL
	UNION
	SELECT DISTINCT child_schema_priority, child_schema_name, child_table_name, 999, NULL FROM #TablesWithoutChildren
		WHERE child_schema_name + '.' + child_table_name NOT IN (SELECT DISTINCT child_schema_name + '.' + child_table_name FROM #TablesWithoutChildrenHavingRootParent)

		/*--<< Debug >>--------------------------------------------------------------------------------------------------------------------------------
			SELECT * FROM #AllObjectsDB WHERE table_name = 'CT_Submission'
			SELECT * FROM #AllObjectsDB WHERE referenced_table_name = 'CT_Submission'
			SELECT * FROM #UnrelatedTables
			SELECT * FROM #SelfReferencingTables
			SELECT * FROM #ParentMostTablesWithoutParents
			SELECT * FROM #TablesWithoutChildren
			SELECT * FROM #TablesWithoutChildrenHavingRootParent
			SELECT * FROM #AllMidLevelTables
			SELECT * FROM #TableHierarchy
		*---------------------------------------------------------------------------------------------------------------------------------<< Debug >>--*/

/*--<< STEP 02-A >>--*****************************************************************************************************************************
	(i)		Identify all the missed out tables from #TableHierarchy using the #AllObjectsDB into a Temporary Table #ListTablesMissedOut
	(ii)	Using #AllObjectsDB, identify the parent to child related tables consolidating required information for those missed
			out tables into a Temporary Table #MissedOutTables
	(iii)	Using a logic in a loop to iteratively assign an order based on these temporary tables
	(iv)	Updating the _level field in #MissedOutTables
	(v)		Insert the data from #MissedOutTables into the #TableHierarchy
******--------------------------------------------------------------------------------------------------------------------------------------******/
	IF OBJECT_ID('tempdb..#ListTablesMissedOut') IS NOT NULL DROP TABLE #ListTablesMissedOut;
	SELECT DISTINCT 
		DENSE_RANK() OVER (ORDER BY O.schema_name, O.table_name) AS ID,
		O.schema_name,
		O.table_name,
		CAST(CASE WHEN SRT.parent_schema_name IS NOT NULL THEN 1 ELSE 0 END AS BIT) AS isSelfRef,
		CAST(NULL AS FLOAT) AS _level
	INTO #ListTablesMissedOut
	FROM #AllObjectsDB O
	LEFT JOIN #TableHierarchy TH
		ON TH.TargetSchema = O.schema_name
		AND TH.TargetTable = O.table_name
	LEFT JOIN #SelfReferencingTables SRT
		ON O.schema_name = SRT.parent_schema_name
		AND O.table_name = SRT.parent_table_name
	WHERE 1 = 1
		AND TH.ID IS NULL
	--SELECT * FROM #ListTablesMissedOut

	IF OBJECT_ID('tempdb..#MissedOutTables') IS NOT NULL DROP TABLE #MissedOutTables;
	SELECT DISTINCT 
		PSO.schema_priority AS parent_schema_priority,
		O.referenced_schema_name AS parent_schema_name, 
		O.referenced_table_name AS parent_table_name,
		CAST(NULL AS FLOAT) AS parent_table_level,
		CSO.schema_priority AS child_schema_priority,
		O.schema_name AS child_schema_name,
		O.table_name AS child_table_name,
		T.isSelfRef
	INTO #MissedOutTables
	FROM #ListTablesMissedOut T
	JOIN #AllObjectsDB O
		ON T.schema_name = O.referenced_schema_name
		AND T.table_name = O.referenced_table_name
	LEFT JOIN #SchemaOrder CSO
		ON CSO.schema_name = O.schema_name
	LEFT JOIN #SchemaOrder PSO
		ON PSO.schema_name = O.referenced_schema_name
	--SELECT * FROM #MissedOutTables

	DECLARE @rowCount INT = (SELECT COUNT(*) FROM #ListTablesMissedOut)
	WHILE (@rowCount>0)
	BEGIN
		UPDATE L
		SET
			_level =
				(
					SELECT
						MAX(L2.ID) + 1 AS ID
					FROM #MissedOutTables M
					JOIN #ListTablesMissedOut L1
						ON M.child_schema_name = L1.schema_name
						AND M.child_table_name = L1.table_name
						AND L1.ID = @rowCount
					JOIN #ListTablesMissedOut L2
						ON M.parent_schema_name = L2.schema_name
						AND M.parent_table_name = L2.table_name
					WHERE 1 = 1
						AND M.parent_schema_name + '.' + M.parent_table_name IN (SELECT schema_name + '.' + table_name FROM #ListTablesMissedOut )
				)
		FROM #ListTablesMissedOut L
		WHERE ID = @rowCount

		SET @rowCount = @rowCount - 1
	END

	;WITH CTE AS
	(
		SELECT 
			*,
			DENSE_RANK() OVER(ORDER BY _level) AS Ranked
		FROM #ListTablesMissedOut
	)
		UPDATE M
		SET
			parent_table_level = CTE.Ranked
		FROM #MissedOutTables M
		JOIN CTE
			ON M.parent_schema_name = CTE.schema_name
			AND M.parent_table_name = CTE.table_name

	INSERT INTO #TableHierarchy (SchemaPriority, TargetSchema, TargetTable, SectionLevel, TableLevel)
	SELECT DISTINCT parent_schema_priority, parent_schema_name, parent_table_name, 4, parent_table_level FROM #MissedOutTables
/*--<< STEP 03-A >>--*****************************************************************************************************************************
	(i)		Use the #TableHierarchy and rank based on SectionLevel, TableLevel, SchemaPriority to derive the Hierarchy
******--------------------------------------------------------------------------------------------------------------------------------------******/
--FINAL OUTPUT::
	--SELECT *, DENSE_RANK() OVER(ORDER BY SectionLevel, TableLevel, SchemaPriority) AS Hierarchy FROM #TableHierarchy

	UPDATE TH1
	SET Hierarchy = TH2.Hierarchy
	FROM #TableHierarchy TH1
	JOIN
	(
		SELECT 
			ID,
			SchemaPriority,
			TargetSchema,
			TargetTable,
			SectionLevel,
			TableLevel,
			DENSE_RANK() OVER(ORDER BY SectionLevel, TableLevel, SchemaPriority) AS Hierarchy 
		FROM #TableHierarchy
	)TH2
		ON TH1.ID = TH2.ID

	PRINT '$(OutputPath)'
	-- :OUT $(OutputPath)\00.rawHierarchyOutput.dat
	SELECT table_hierarchy FROM
	(
		SELECT
			ID,
			TargetSchema,
			TargetTable,
			JSON_QUERY(
				(
					SELECT 
						'[' + TargetSchema + '].[' + TargetTable + ']' AS FullTableName, 
						SchemaPriority AS schema_priority, 
						TargetSchema AS schema_name, 
						TargetTable AS table_name, 
						SectionLevel AS section_level, 
						TableLevel AS table_level, 
						Hierarchy 
					FROM #TableHierarchy TH1
					WHERE TH1.ID = TH2.ID
					ORDER BY TH1.TargetTable
					FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
				)
			) AS table_hierarchy
		--INTO #HierarchySQLDB_JSON
		FROM #TableHierarchy TH2
		GROUP BY ID, TargetSchema, TargetTable
	)X