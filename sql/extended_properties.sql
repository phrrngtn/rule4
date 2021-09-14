
CREATE SCHEMA [RULE4]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [RULE4].[object_info](@obj varchar(max))
-- This is *VERY INCOMPLETE* and will fail for 'exotic' object types and may even fail for common object types!
RETURNS TABLE AS RETURN (
	SELECT ObjectSchema = OBJECT_SCHEMA_NAME(o.object_id)
			,ObjectName = OBJECT_NAME(o.object_id)
			,FullObjectName = QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) + '.' + QUOTENAME(o.name)
			,ObjectType = o.type_desc
			,IsSchema = CAST(0 AS bit)
			,level0type = 'SCHEMA'
			,level0name = OBJECT_SCHEMA_NAME(o.object_id)
			,level1type = CASE WHEN o.type_desc = 'VIEW' THEN 'VIEW'
								WHEN o.type_desc = 'USER_TABLE' THEN 'TABLE'
								WHEN o.type_desc = 'SQL_STORED_PROCEDURE' THEN 'PROCEDURE'
								WHEN o.type_desc = 'SQL_INLINE_TABLE_VALUED_FUNCTION' THEN 'FUNCTION'
								ELSE o.type_desc
							END
			,level1name = OBJECT_NAME(o.object_id)
			,level2type = NULL
			,level2name = NULL
	FROM sys.objects AS o
	WHERE o.object_id = OBJECT_ID(@obj)
	AND o.type <> 'TR' -- A trigger is a "level 3" object, beneath a table (for example). So you specify the schema, then the table, then the trigger - like a column. 3 "levels" of parameters.

	UNION ALL

	SELECT ObjectSchema = SCHEMA_NAME(SCHEMA_ID(@obj))
		,ObjectName = NULL
		,FullObjectName = QUOTENAME(SCHEMA_NAME(SCHEMA_ID(@obj)))
		,ObjectType = 'SCHEMA'
		,IsSchema = CAST(1 AS bit)
		,level0type = 'SCHEMA'
		,level0name = SCHEMA_NAME(SCHEMA_ID(@obj))
		,level1type = NULL
		,level1name = NULL
		,level2type = NULL
		,level2name = NULL
	WHERE SCHEMA_ID(@obj) IS NOT NULL


	UNION ALL

	SELECT ObjectSchema = OBJECT_SCHEMA_NAME(o.object_id)
			,ObjectName = OBJECT_NAME(o.object_id)
			,FullObjectName = QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) + '.' + QUOTENAME(o.name)
			,ObjectType =  CASE o.type_desc WHEN 'SQL_TRIGGER' THEN 'TRIGGER'
							ELSE 'COLUMN' END
			,IsSchema = CAST(0 AS bit)
			,level0type = 'SCHEMA'
			,level0name = OBJECT_SCHEMA_NAME(o.object_id)
			,level1type =  CASE o.type_desc WHEN 'SQL_TRIGGER' THEN po.type_desc
					       WHEN 'USER_TABLE' THEN 'TABLE'
						   WHEN 'SQL_STORED_PROCEDURE' THEN 'PROCEDURE'
						   WHEN 'SQL_INLINE_TABLE_VALUED_FUNCTION' THEN 'FUNCTION'
						   WHEN 'SQL_SCALAR_FUNCTION' THEN 'FUNCTION'
						   ELSE o.type_desc
						   END
			,level1name = CASE o.type_desc WHEN 'SQL_TRIGGER' THEN OBJECT_NAME(o.parent_object_id)
						  ELSE OBJECT_NAME(o.object_id) END
			,level2type = CASE o.type_desc
					WHEN 'SQL_TRIGGER' THEN 'TRIGGER'
					ELSE 'COLUMN' END
			,level2name = PARSENAME(@obj, 1)
	FROM sys.objects AS o
	LEFT OUTER JOIN sys.objects AS po
	ON (o.parent_object_id = po.object_id)
	WHERE (o.type = 'TR' AND o.object_id = object_id(@obj)) OR
	 (      PARSENAME(@obj, 1) IS NOT NULL
		AND PARSENAME(@obj, 2) IS NOT NULL
		AND PARSENAME(@obj, 3) IS NOT NULL
		AND PARSENAME(@obj, 4) IS NULL
		AND o.object_id = OBJECT_ID(QUOTENAME(PARSENAME(@obj, 3)) + '.' + QUOTENAME(PARSENAME(@obj, 2))))
)

GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [RULE4].[Split] (@sep char(1), @s varchar(512))
RETURNS table
AS
-- XXX: will break if there are more than 100 occurances of @sep (maximum level
-- of recursion)
RETURN (
    WITH Pieces(pn, start, stop) AS (
      SELECT 1, 1, CHARINDEX(@sep, @s)
      UNION ALL
      SELECT pn + 1, stop + 1, CHARINDEX(@sep, @s, stop + 1)
      FROM Pieces
      WHERE stop > 0
    )
    SELECT pn,
      SUBSTRING(@s, start, CASE WHEN stop > 0 THEN stop-start ELSE 512 END) AS s
    FROM Pieces
  )
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [RULE4].[extended_property] AS
-- This is a quite incomplete implementation of a wrapper of the sys.extended_properties table
-- It is based on 
 -- https://www.slideshare.net/caderoux/get-a-lever-and-pick-any-turtle-lifting-with-metadata

-- A more complete implementation can be found in [RULE4].[extended_property_aux] based on
--  https://www.red-gate.com/simple-talk/sql/database-delivery/reading-writing-creating-sql-server-extended-properties/
--

	SELECT 'SCHEMA' AS object_type
			,QUOTENAME(s.name) AS full_object_name
			,s.name AS object_schema
			,NULL AS [object_name]
			,NULL AS column_name
			,xp.name AS property_name
			,xp.value AS property_value
	FROM sys.extended_properties AS xp
	INNER JOIN sys.schemas AS s
		ON s.schema_id = xp.major_id
		AND xp.class_desc = 'SCHEMA'

	UNION ALL

	SELECT o.type_desc AS object_type
			,QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) + '.' + QUOTENAME(o.name) AS full_object_name
			,OBJECT_SCHEMA_NAME(o.object_id) AS object_schema
			,o.name AS [object_name]
			,NULL AS column_name
			,xp.name AS property_name
			,xp.value AS property_value
	FROM sys.extended_properties AS xp
	INNER JOIN sys.objects AS o
		ON o.object_id = xp.major_id
		AND xp.minor_id = 0
		AND xp.class_desc = 'OBJECT_OR_COLUMN'

	UNION ALL

	SELECT 'COLUMN' AS object_type
			,QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) + '.' + QUOTENAME(t.name) + '.' + QUOTENAME(c.name) AS full_object_name
			,OBJECT_SCHEMA_NAME(t.object_id) AS object_schema
			,t.name AS [object_name]
			,c.name AS column_name
			,xp.name AS property_name
			,xp.value AS property_value
	FROM sys.extended_properties AS xp
	INNER JOIN sys.objects AS t
		ON t.object_id = xp.major_id
		AND xp.minor_id <> 0
		AND xp.class_desc = 'OBJECT_OR_COLUMN'
		AND t.type_desc = 'USER_TABLE'
	INNER JOIN sys.columns AS c
		ON c.object_id = t.object_id
		AND c.column_id = xp.minor_id


GO



CREATE VIEW [RULE4].[extended_property_aux]
AS
-- thanks to the amazing Phil Factor for this: https://www.red-gate.com/simple-talk/sql/database-delivery/reading-writing-creating-sql-server-extended-properties/

SELECT --objects AND columns
        CASE WHEN ob.parent_object_id > 0
             THEN QUOTENAME(OBJECT_SCHEMA_NAME(ob.parent_object_id)) + '.'
                  + QUOTENAME(OBJECT_NAME(ob.parent_object_id)) + '.' + QUOTENAME(ob.name)
             ELSE QUOTENAME(OBJECT_SCHEMA_NAME(ob.object_id)) + '.' + QUOTENAME(ob.name)
        END + CASE WHEN ep.minor_id > 0 THEN '.' + QUOTENAME(col.name)
                   ELSE ''
              END AS path ,
        'schema' + CASE WHEN ob.parent_object_id > 0 THEN '/table'
                        ELSE ''
                   END + '/'
        + CASE WHEN ob.type IN ( 'TF', 'FN', 'IF', 'FS', 'FT' )
               THEN 'function'
               WHEN ob.type IN ( 'P', 'PC', 'RF', 'X' ) THEN 'procedure'
               WHEN ob.type IN ( 'U', 'IT' ) THEN 'table'
               WHEN ob.type = 'SQ' THEN 'queue'
               ELSE LOWER(ob.type_desc)
          END + CASE WHEN col.column_id IS NULL THEN ''
                     ELSE '/column'
                END AS thing ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.objects ob ON ep.major_id = ob.OBJECT_ID
                                     AND class = 1
        LEFT OUTER JOIN sys.columns col ON ep.major_id = col.Object_id
                                           AND class = 1
                                           AND ep.minor_id = col.column_id
UNION ALL
SELECT --indexes
        QUOTENAME(OBJECT_SCHEMA_NAME(ob.object_id)) + '.' + OBJECT_NAME(ob.object_id)
        + '.' + ix.name ,
        'schema/' + LOWER(ob.type_desc) + '/index' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.objects ob ON ep.major_id = ob.OBJECT_ID
                                     AND class = 7
        INNER JOIN sys.indexes ix ON ep.major_id = ix.Object_id
                                     AND class = 7
                                     AND ep.minor_id = ix.index_id
UNION ALL
SELECT --Parameters
        OBJECT_SCHEMA_NAME(ob.object_id) + '.' + OBJECT_NAME(ob.object_id)
        + '.' + par.name ,
        'schema/' + LOWER(ob.type_desc) + '/parameter' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.objects ob ON ep.major_id = ob.OBJECT_ID
                                     AND class = 2
        INNER JOIN sys.parameters par ON ep.major_id = par.Object_id
                                         AND class = 2
                                         AND ep.minor_id = par.parameter_id
UNION ALL
SELECT --schemas
        sch.name ,
        'schema' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.schemas sch ON class = 3
                                      AND ep.major_id = SCHEMA_ID
UNION ALL --Database
SELECT  DB_NAME() ,
        '' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
WHERE   class = 0
UNION ALL--XML Schema Collections
SELECT  SCHEMA_NAME(SCHEMA_ID) + '.' + xc.name ,
        'schema/xml_Schema_collection' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.xml_schema_collections xc ON class = 10
                                                    AND ep.major_id = xml_collection_id
UNION ALL
SELECT --Database Files
        df.name ,
        'database_file' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.database_files df ON class = 22
                                            AND ep.major_id = file_id
UNION ALL
SELECT --Data Spaces
        ds.name ,
        'dataspace' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.data_spaces ds ON class = 20
                                         AND ep.major_id = data_space_id
UNION ALL
SELECT --USER
        dp.name ,
        'database_principal' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.database_principals dp ON class = 4
                                                 AND ep.major_id = dp.principal_id
UNION ALL
SELECT --PARTITION FUNCTION
        pf.name ,
        'partition_function' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.partition_functions pf ON class = 21
                                                 AND ep.major_id = pf.function_id
UNION ALL
SELECT --REMOTE SERVICE BINDING
        rsb.name ,
        'remote service binding' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.remote_service_bindings rsb ON class = 18
                                                      AND ep.major_id = rsb.remote_service_binding_id
UNION ALL
SELECT --Route
        rt.name ,
        'route' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.routes rt ON class = 19
                                    AND ep.major_id = rt.route_id
UNION ALL
SELECT --Service
        sv.name COLLATE DATABASE_DEFAULT ,
        'service' ,
        ep.name ,
        value
FROM    sys.extended_properties ep
        INNER JOIN sys.services sv ON class = 17
                                      AND ep.major_id = sv.service_id
UNION ALL
SELECT -- 'CONTRACT'
        svc.name ,
        'service_contract' ,
        ep.name ,
        value
FROM    sys.service_contracts svc
        INNER JOIN sys.extended_properties ep ON class = 16
                                                 AND ep.major_id = svc.service_contract_id
UNION ALL
SELECT -- 'MESSAGE TYPE'
        smt.name ,
        'message_type' ,
        ep.name ,
        value
FROM    sys.service_message_types smt
        INNER JOIN sys.extended_properties ep ON class = 15
                                                 AND ep.major_id = smt.message_type_id
UNION ALL
SELECT -- 'assembly'
        asy.name ,
        'assembly' ,
        ep.name ,
        value
FROM    sys.assemblies asy
        INNER JOIN sys.extended_properties ep ON class = 5
                                                 AND ep.major_id = asy.assembly_id
UNION ALL
SELECT -- 'PLAN GUIDE'
        pg.name ,
        'plan_guide' ,
        ep.name ,
        value
FROM    sys.plan_guides pg
        INNER JOIN sys.extended_properties ep ON class = 27
                                                 AND ep.major_id = pg.plan_guide_id
GO




CREATE PROCEDURE [RULE4].[add_extended_property]
	@obj AS varchar(max)
	,@name AS sysname
	,@value AS sql_variant
AS
BEGIN
	DECLARE @level0type AS varchar(128)
	DECLARE @level0name AS sysname
	DECLARE @level1type AS varchar(128)
	DECLARE @level1name AS sysname
	DECLARE @level2type AS varchar(128)
	DECLARE @level2name AS sysname

	SELECT @level0type = level0type
		,@level0name = level0name
		,@level1type = level1type
		,@level1name = level1name
		,@level2type = level2type
		,@level2name = level2name
	FROM RULE4.object_info(@obj)

	EXEC sys.sp_addextendedproperty
		@name = @name
		,@value = @value
		,@level0type = @level0type
		,@level0name = @level0name
		,@level1type = @level1type
		,@level1name = @level1name
		,@level2type = @level2type
		,@level2name = @level2name
END

GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [RULE4].[drop_extended_property]
	@obj AS varchar(max)
	,@name AS sysname
AS
BEGIN
	DECLARE @level0type AS varchar(128)
	DECLARE @level0name AS sysname
	DECLARE @level1type AS varchar(128)
	DECLARE @level1name AS sysname
	DECLARE @level2type AS varchar(128)
	DECLARE @level2name AS sysname

	SELECT @level0type = level0type
		,@level0name = level0name
		,@level1type = level1type
		,@level1name = level1name
		,@level2type = level2type
		,@level2name = level2name
	FROM RULE4.object_info(@obj)

	EXEC sys.sp_dropextendedproperty
		@name = @name
		,@level0type = @level0type
		,@level0name = @level0name
		,@level1type = @level1type
		,@level1name = @level1name
		,@level2type = @level2type
		,@level2name = @level2name
END
GO




CREATE PROCEDURE [RULE4].[update_extended_property]
	@obj AS VARCHAR(MAX)
	,@name AS sysname
	,@value AS SQL_VARIANT
AS
BEGIN
	DECLARE @level0type AS VARCHAR(128)
	DECLARE @level0name AS sysname
	DECLARE @level1type AS VARCHAR(128)
	DECLARE @level1name AS sysname
	DECLARE @level2type AS VARCHAR(128)
	DECLARE @level2name AS sysname

	SELECT @level0type = level0type
		,@level0name = level0name
		,@level1type = level1type
		,@level1name = level1name
		,@level2type = level2type
		,@level2name = level2name
	FROM RULE4.object_info(@obj)

	EXEC sys.sp_updateextendedproperty
		@name = @name
		,@value = @value
		,@level0type = @level0type
		,@level0name = @level0name
		,@level1type = @level1type
		,@level1name = @level1name
		,@level2type = @level2type
		,@level2name = @level2name
END

GO




-- Create extended properties view instead of insert trigger
CREATE TRIGGER [RULE4].[extended_property_delete]
ON [RULE4].[extended_property]
INSTEAD OF DELETE
AS
BEGIN

-- object_type, full_object_name, object_schema, object_name, sub_name, property_name, property_value
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @object_schema sysname,
		    @object_name sysname,
		    @column_name sysname,
		    @property_name sysname,
		    @property_value sql_variant,
		    @full_name sysname;

	DECLARE deleted_rows CURSOR
	FORWARD_ONLY READ_ONLY
	FOR
    SELECT object_schema,
		   [object_name],
		   column_name,
		   property_name,
		   property_value
	  FROM deleted;

	OPEN deleted_rows;

	FETCH NEXT FROM deleted_rows
	INTO @object_schema,
		 @object_name,
		 @column_name,
		 @property_name,
		 @property_value;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @object_schema IS NULL AND @object_name IS NULL AND @column_name IS NULL
			EXECUTE [RULE4].[drop_extended_property]  NULL, @property_name;	-- Database
		ELSE IF @object_schema IS NULL AND @column_name IS NULL
			EXECUTE [RULE4].[drop_extended_property]   @object_name, @property_name;	-- Schema
		ELSE IF @column_name IS NULL
		BEGIN
			SET @full_name = COALESCE(N'[' + @object_schema + N']', '') + COALESCE(N'.[' + @object_name + ']', '');
			EXECUTE  [RULE4].[drop_extended_property] @full_name, @property_name, @property_value;	-- Table/Proc/Function
		END ELSE
		BEGIN
			SET @full_name = COALESCE(N'[' + @object_schema + N']', '') + COALESCE(N'.[' + @object_name + ']', '') +
				COALESCE(N'.[' + @column_name + N']', '');
			EXECUTE  [RULE4].[drop_extended_property] @full_name, @property_name; -- Column
		END;

		FETCH NEXT FROM deleted_rows
		into @object_schema,
			@object_name,
			@column_name,
			@property_name,
			@property_value;
	END;
	CLOSE deleted_rows;
	DEALLOCATE deleted_rows;

END;



GO




-- Create extended properties view instead of insert trigger
CREATE TRIGGER [RULE4].[extended_property_insert]
ON [RULE4].[extended_property]
INSTEAD OF INSERT
AS
BEGIN

-- object_type, full_object_name, object_schema, object_name, sub_name, property_name, property_value
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @object_schema sysname,
		    @object_name sysname,
		    @column_name sysname,
		    @property_name sysname,
		    @property_value sql_variant,
		    @full_name sysname;

	DECLARE Inserted_Rows CURSOR
	FORWARD_ONLY READ_ONLY
	FOR
    SELECT object_schema,
		   [object_name],
		   column_name,
		   property_name,
		   property_value
	  FROM inserted;

	OPEN Inserted_Rows;

	FETCH NEXT FROM Inserted_Rows
	INTO @object_schema,
		 @object_name,
		 @column_name,
		 @property_name,
		 @property_value;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @object_schema IS NULL AND @object_name IS NULL AND @column_name IS NULL
			EXECUTE [RULE4].[add_extended_property]  NULL, @property_name, @property_value;	-- Database
		ELSE IF @object_schema IS NULL AND @column_name IS NULL
			EXECUTE [RULE4].[add_extended_property]  @object_name, @property_name, @property_value;	-- Schema
		ELSE IF @column_name IS NULL
		BEGIN
			SET @full_name = COALESCE(N'[' + @object_schema + N']', '') + COALESCE(N'.[' + @object_name + ']', '');
			EXECUTE  [RULE4].[add_extended_property] @full_name, @property_name, @property_value;	-- Table/Proc/Function
		END ELSE
		BEGIN
			SET @full_name = COALESCE(N'[' + @object_schema + N']', '') + COALESCE(N'.[' + @object_name + ']', '') +
				COALESCE(N'.[' + @column_name + N']', '');
			EXECUTE  [RULE4].[add_extended_property] @full_name, @property_name, @property_value; -- Column
		END;

		FETCH NEXT FROM Inserted_Rows
		into @object_schema,
			@object_name,
			@column_name,
			@property_name,
			@property_value;
	END;
	CLOSE Inserted_Rows;
	DEALLOCATE Inserted_Rows;

END;


GO


-- Create extended properties view instead of insert trigger
CREATE TRIGGER [RULE4].[extended_property_update]
ON [RULE4].[extended_property]
INSTEAD OF UPDATE
AS
BEGIN

-- object_type, full_object_name, object_schema, object_name, sub_name, property_name, property_value
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @object_schema sysname,
		    @object_name sysname,
		    @column_name sysname,
		    @property_name sysname,
		    @property_value sql_variant,
		    @full_name sysname;

	DECLARE updated_rows CURSOR
	FORWARD_ONLY READ_ONLY
	FOR
    SELECT object_schema,
		   [object_name],
		   column_name,
		   property_name,
		   property_value
	  FROM inserted;

	OPEN updated_rows;

	FETCH NEXT FROM updated_rows
	INTO @object_schema,
		 @object_name,
		 @column_name,
		 @property_name,
		 @property_value;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @object_schema IS NULL AND @object_name IS NULL AND @column_name IS NULL
			EXECUTE [RULE4].[update_extended_property]  NULL, @property_name, @property_value;	-- Database
		ELSE IF @object_schema IS NULL AND @column_name IS NULL
			EXECUTE [RULE4].[update_extended_property]  @object_name, @property_name, @property_value;	-- Schema
		ELSE IF @column_name IS NULL
		BEGIN
			SET @full_name = COALESCE(N'[' + @object_schema + N']', '') + COALESCE(N'.[' + @object_name + ']', '');
			EXECUTE [RULE4].[update_extended_property] @full_name, @property_name, @property_value;	-- Table/Proc/Function
		END ELSE
		BEGIN
			SET @full_name = COALESCE(N'[' + @object_schema + N']', '') + COALESCE(N'.[' + @object_name + ']', '') +
				COALESCE(N'.[' + @column_name + N']', '');
			EXECUTE [RULE4].[update_extended_property] @full_name, @property_name, @property_value; -- Column
		END;

		FETCH NEXT FROM updated_rows
		into @object_schema,
			@object_name,
			@column_name,
			@property_name,
			@property_value;
	END;
	CLOSE updated_rows;
	DEALLOCATE updated_rows;

END;


GO


CREATE USER [break_ownership_user] WITHOUT LOGIN WITH DEFAULT_SCHEMA=[dbo];
GO

-- if we change the ownership on the VIEW then ownership chaining is broken and we
-- get emergent row-level access control on the data!
ALTER AUTHORIZATION ON OBJECT::RULE4.extended_property TO [break_ownership_user];
