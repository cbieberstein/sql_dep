import pandas as pd
from sqlalchemy import create_engine


def connect_sqlserver_windowsauth(hostname, dbname, port="1433"):
    """
    connect to MS SQL SERVER using windows authentication
    """
    # Use Windows credentials
    engine = create_engine(f"mssql+pymssql://{hostname}:{port}/{dbname}")
    return engine


def fetch_all_dbs(engine):
    sql = """
    select
        database_id,
        name,
        create_date,
        compatibility_level,
        recovery_model_desc
    from
        sys.databases
    where
        name not in ('master', 'model', 'msdb', 'tempdb')
    """
    with engine.connect() as connection:
        result = pd.read_sql(sql,connection)
    return result


def fetch_db_dependencies(dbname, engine):
    sql = f"""
    SELECT
            '{dbname}' as [DATABASE]
            ,schema_name(sysobj.schema_id)		as referencing_schema_name
            ,object_name(deps.referencing_id)	as referencing_object_name
            ,sysobj.type_desc					as referencing_type_desc
            ,deps.referenced_server_name
            ,isnull(deps.referenced_database_name, '{dbname}') as referenced_db_name
            ,isnull(deps.referenced_schema_name,'dbo') as referenced_schema_name
            ,deps.referenced_entity_name
    FROM
        [{dbname}].sys.sql_expression_dependencies deps
    left outer join
        [{dbname}].sys.objects sysobj on (deps.referencing_id = sysobj.object_id)
    order by
        1,2,3
    """
    with engine.connect() as connection:
        result = pd.read_sql(sql,connection)
    return result


def fetch_all_db_dependencies(engine):
    databases = fetch_all_dbs(engine)
    result = pd.DataFrame()
    for db in list(databases['name'].sort_values()):
        engine = connect_sqlserver_windowsauth('bidatacentre',db)
        temp = fetch_db_dependencies(db, engine)
        result = pd.concat([result,temp])
    return result


def get_parents(deps_df,db, schema, object_name):
    # Filter the deps dataframe from fetch_all_db_dependencies to parents for the given object.
    parents = deps.loc[(deps['DATABASE'] == db) &
                       (deps['referencing_schema_name'] == schema) &
                       (deps['referencing_object_name'] == object_name)]
    # Trim result to these 4 columns
    parents = parents[['referenced_server_name',
                       'referenced_db_name',
                       'referenced_schema_name',
                       'referenced_entity_name']]
    return parents


def get_children(deps_df, db, schema, object_name):
    children = deps_df.loc[(deps_df['referenced_db_name'] == db) &
                           (deps_df['referenced_schema_name'] == schema) &
                           (deps_df['referenced_entity_name'] == object_name)]
    # Trim result to these 4 columns
    children = children[['DATABASE',
                         'referencing_schema_name',
                         'referencing_object_name',
                         'referencing_type_desc']]
    return children


def fetch_table_details(engine):
    sql = """
    select schema_name(tab.schema_id) as schema_name,
           tab.name as table_name,
           col.name as column_name,
           /* t.name as data_type, */
           t.name +
           case when t.is_user_defined = 0 then
                     isnull('(' +
                     case when t.name in ('binary', 'char', 'nchar',
                               'varchar', 'nvarchar', 'varbinary') then
                               case col.max_length
                                    when -1 then 'MAX'
                                    else
                                         case when t.name in ('nchar',
                                                   'nvarchar') then
                                                   cast(col.max_length/2
                                                   as varchar(4))
                                              else cast(col.max_length
                                                   as varchar(4))
                                         end
                               end
                          when t.name in ('datetime2', 'datetimeoffset',
                               'time') then
                               cast(col.scale as varchar(4))
                          when t.name in ('decimal', 'numeric') then
                                cast(col.precision as varchar(4)) + ', ' +
                                cast(col.scale as varchar(4))
                     end + ')', '')
                else ':' +
                     (select c_t.name +
                             isnull('(' +
                             case when c_t.name in ('binary', 'char',
                                       'nchar', 'varchar', 'nvarchar',
                                       'varbinary') then
                                        case c.max_length
                                             when -1 then 'MAX'
                                             else
                                                  case when t.name in
                                                            ('nchar',
                                                            'nvarchar') then
                                                            cast(c.max_length/2
                                                            as varchar(4))
                                                       else cast(c.max_length
                                                            as varchar(4))
                                                  end
                                        end
                                  when c_t.name in ('datetime2',
                                       'datetimeoffset', 'time') then
                                       cast(c.scale as varchar(4))
                                  when c_t.name in ('decimal', 'numeric') then
                                       cast(c.precision as varchar(4)) + ', '
                                       + cast(c.scale as varchar(4))
                             end + ')', '')
                        from sys.columns as c
                             inner join sys.types as c_t
                                 on c.system_type_id = c_t.user_type_id
                       where c.object_id = col.object_id
                         and c.column_id = col.column_id
                         and c.user_type_id = col.user_type_id
                     )
            end as data_type_ext,
            case when col.is_nullable = 0 then 'N'
                 else 'Y' end as nullable,
            case when def.definition is not null then def.definition
                 else '' end as default_value,
            case when pk.column_id is not null then 'PK'
                 else '' end as primary_key,
            case when fk.parent_column_id is not null then 'FK'
                 else '' end as foreign_key,
            case when uk.column_id is not null then 'UK'
                 else '' end as unique_key,
            case when ch.check_const is not null then ch.check_const
                 else '' end as check_constraint,
            cc.definition as computed_column_definition,
            ep.value as comments
       from sys.tables as tab
            left join sys.columns as col
                on tab.object_id = col.object_id
            left join sys.types as t
                on col.user_type_id = t.user_type_id
            left join sys.default_constraints as def
                on def.object_id = col.default_object_id
            left join (
                      select index_columns.object_id,
                             index_columns.column_id
                        from sys.index_columns
                             inner join sys.indexes
                                 on index_columns.object_id = indexes.object_id
                                and index_columns.index_id = indexes.index_id
                       where indexes.is_primary_key = 1
                      ) as pk
                on col.object_id = pk.object_id
               and col.column_id = pk.column_id
            left join (
                      select fc.parent_column_id,
                             fc.parent_object_id
                        from sys.foreign_keys as f
                             inner join sys.foreign_key_columns as fc
                                 on f.object_id = fc.constraint_object_id
                       group by fc.parent_column_id, fc.parent_object_id
                      ) as fk
                on fk.parent_object_id = col.object_id
               and fk.parent_column_id = col.column_id
            left join (
                      select c.parent_column_id,
                             c.parent_object_id,
                             'Check' check_const
                        from sys.check_constraints as c
                       group by c.parent_column_id,
                             c.parent_object_id
                      ) as ch
                on col.column_id = ch.parent_column_id
               and col.object_id = ch.parent_object_id
            left join (
                      select index_columns.object_id,
                             index_columns.column_id
                        from sys.index_columns
                             inner join sys.indexes
                                 on indexes.index_id = index_columns.index_id
                                and indexes.object_id = index_columns.object_id
                        where indexes.is_unique_constraint = 1
                        group by index_columns.object_id,
                              index_columns.column_id
                      ) as uk
                on col.column_id = uk.column_id
               and col.object_id = uk.object_id
            left join sys.extended_properties as ep
                on tab.object_id = ep.major_id
               and col.column_id = ep.minor_id
               and ep.name = 'MS_Description'
               and ep.class_desc = 'OBJECT_OR_COLUMN'
            left join sys.computed_columns as cc
                on tab.object_id = cc.object_id
               and col.column_id = cc.column_id
      order by schema_name,
            table_name,
            column_name;
    """
    with engine.connect() as connection:
        result = pd.read_sql(sql, connection)
    return result


def fetch_table_dates_and_rowcount(engine):
    sql = """
    select schema_name(tab.schema_id) as schema_name,
           tab.name as table_name,
           tab.create_date as created,
           tab.modify_date as last_modified,
           p.rows as num_rows,
           ep.value as comments
      from sys.tables tab
           inner join (select distinct
                              p.object_id,
                              sum(p.rows) rows
                         from sys.tables t
                              inner join sys.partitions p
                                  on p.object_id = t.object_id
                        group by p.object_id,
                              p.index_id) p
                on p.object_id = tab.object_id
            left join sys.extended_properties ep
                on tab.object_id = ep.major_id
               and ep.name = 'MS_Description'
               and ep.minor_id = 0
               and ep.class_desc = 'OBJECT_OR_COLUMN'
      order by schema_name,
            table_name
    """
    with engine.connect() as connection:
        result = pd.read_sql(sql, connection)
    return result



def fetch_all_rows(engine, schema, table):
    sql=f"select * from {schema}.{table}"""
    with engine.connect() as connection:
        result = pd.read_sql(sql, connection)
    return result


def fetch_sample_rows(engine, schema, table, sample_pct):
    sql=f"select * from {schema}.{table} TABLESAMPLE({sample_pct} PERCENT)"""
    with engine.connect() as connection:
        result = pd.read_sql(sql, connection)
    return result


def fetch_view_column_details(engine):
    get_view_column_details_sql = """
    select schema_name(v.schema_id) as schema_name,
           v.name as view_name, 
           col.name as column_name,
           t.name as data_type,
           t.name + 
           case when t.is_user_defined = 0 then 
                     isnull('(' + 
                     case when t.name in ('binary', 'char', 'nchar',
                               'varchar', 'nvarchar', 'varbinary') then
                               case col.max_length 
                                    when -1 then 'MAX' 
                                    else 
                                         case 
                                             when t.name in ('nchar', 
                                                  'nvarchar') then
                                                  cast(col.max_length/2 
                                                  as varchar(4))
                                             else cast(col.max_length 
                                                  as varchar(4))
                                         end
                               end
                          when t.name in ('datetime2', 
                               'datetimeoffset', 'time') then 
                                cast(col.scale as varchar(4))
                          when t.name in ('decimal', 'numeric') then 
                               cast(col.precision as varchar(4)) + ', ' +
                               cast(col.scale as varchar(4))
                     end + ')', '')        
                else ':' +
                     (select c_t.name + 
                             isnull('(' + 
                             case when c_t.name in ('binary', 'char',
                                       'nchar', 'varchar', 'nvarchar',
                                       'varbinary') then
                                       case c.max_length
                                            when -1 then 'MAX'
                                            else case when t.name in
                                                           ('nchar',
                                                            'nvarchar')
                                                      then cast(c.max_length/2
                                                           as varchar(4))
                                                      else cast(c.max_length
                                                           as varchar(4))
                                                 end
                                       end
                                  when c_t.name in ('datetime2', 
                                       'datetimeoffset', 'time') then
                                       cast(c.scale as varchar(4))
                                  when c_t.name in ('decimal', 'numeric') then
                                       cast(c.precision as varchar(4)) +
                                       ', ' + cast(c.scale as varchar(4))
                             end + ')', '')
                        from sys.columns as c
                             inner join sys.types as c_t 
                                 on c.system_type_id = c_t.user_type_id
                       where c.object_id = col.object_id
                         and c.column_id = col.column_id
                         and c.user_type_id = col.user_type_id
                     ) 
           end as data_type_ext,
           case when col.is_nullable = 0 then 'N' else 'Y' end as nullable,
           ep.value as comments
      from sys.views as v
           join sys.columns as col
               on v.object_id = col.object_id
           left join sys.types as t
               on col.user_type_id = t.user_type_id
           left join sys.extended_properties as ep 
               on v.object_id = ep.major_id
              and col.column_id = ep.minor_id
              and ep.name = 'MS_Description'        
              and ep.class_desc = 'OBJECT_OR_COLUMN'
     order by schema_name,
           view_name,
           column_name;
    """
    with engine.connect() as connection:
        result = pd.read_sql(get_view_column_details_sql, connection)
    return result


def fetch_view_definitions(engine):
    # REPLACED THIS SO WE HAVE CREATE/MODIFY DATE
    # get_view_definition_sql="select name, definition FROM sys.objects o JOIN sys.sql_modules m on m.object_id = o.object_id AND o.type = 'V'"
    get_view_definition_sql = """
    select schema_name(v.schema_id) as schema_name,
           v.name as view_name,
           v.create_date as created,
           v.modify_date as last_modified,
           m.definition,
           ep.value as comments
      from sys.views v
           left join sys.extended_properties ep 
               on v.object_id = ep.major_id
              and ep.name = 'MS_Description'
              and ep.minor_id = 0
              and ep.class_desc = 'OBJECT_OR_COLUMN'
           inner join sys.sql_modules m 
               on m.object_id = v.object_id
     order by schema_name,
              view_name
    """
    with engine.connect() as connection:
        result = pd.read_sql(get_view_definition_sql, connection)
    return result

