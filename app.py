from queries import *
import sqlparse
import re
import os


def main():
    outdir = "./output"

    # Get all database names on this server
    engine = connect_sqlserver_windowsauth('bidatacentre', 'master')
    db_df = fetch_all_dbs(engine)
    for db in list(db_df['name']):
        print(f"Prepping directories for DB: {db}")
        os.makedirs(f"{outdir}/{db}", exist_ok=True)

    # Loop through all the DBs (fetch_all_dbs excludes:  master, model, msdb, tempdb)
    for db in list(db_df['name']):
        print(f"Processing DB: {db}")
        engine = connect_sqlserver_windowsauth('bidatacentre',db)
        print(f"Retrieving Table details for DB: {db}")
        # Table Details
        tcd = fetch_table_details(engine)
        tab_dates = fetch_table_dates_and_rowcount(engine)

        # View Details
        print(f"Retrieving View details for DB: {db}")
        vcd = fetch_view_column_details(engine)
        view_def = fetch_view_definitions(engine)

        # Get all DB Dependencies
        print(f"Retrieving Dependency details for DB: {db}")
        deps_df = fetch_all_db_dependencies(engine)

        # Generate MD file for each schema.table in this database
        print(f"Generating Markdown files for DB: {db}")
        for row in tab_dates.itertuples():
            output_table_md_file(tcd, tab_dates, deps_df, db, row.schema_name, row.table_name, outdir)

        # Generate MD file for each schema.view in this database
        for row in view_def.itertuples():
            output_view_md_file(vcd, view_def, deps_df, db, row.schema_name, row.view_name, outdir)

        # Add generate data profile md / html
        # Add generate data expectations


def markdown_table(in_df):
    header = ""
    line2 = ""
    data = ""
    for col in in_df.columns:
        header = header + f"| {col} "
        line2 = line2 + f"| --- "
    header = header + "|\n"
    line2 = line2 + "|\n"
    for row in in_df.itertuples():
        for col in range(1,len(row)):
            data = data + f"| {row[col]} "
        data = data + "|\n"
    table_string = header + line2 + data + "\n"
    return table_string


def output_view_md_file(view_cols_df, view_def_df, deps_df, db, schema_name, view_name, outputdir):
    #print(f"IN: output_view_md_file(view_cols_df, view_def_df, deps_df, {db}, {schema_name},  {view_name}, {outputdir}) \n")

    # Filter to the specific db.schema.table
    filtered_vcd = view_cols_df.loc[ (view_cols_df['view_name'] == view_name) & \
                                    (view_cols_df['schema_name'] == schema_name)].copy(deep=True)

    filtered_view_def = view_def_df.loc[(view_def_df['view_name'] == view_name) & \
                                  (view_def_df['schema_name'] == schema_name)].copy(deep=True)

    parents = deps_df.loc[(deps_df['referencing_object_name'] == view_name) & \
                          (deps_df['referencing_schema_name'] == schema_name)].copy(deep=True)

    children = deps_df.loc[(deps_df['referenced_entity_name'] == view_name) & \
                           (deps_df['referenced_schema_name'] == schema_name)].copy(deep=True)


    # Create columns to use for obsidian links to parents / children
    parents['Link'] = '[Link](' + \
                       parents['referenced_db_name'] + '.' + \
                       parents['referenced_schema_name'] + '.' + \
                       parents['referenced_entity_name'] + ')'

    children['Link'] = '[Link](' + \
                       children['DATABASE'] + '.' + \
                       children['referencing_schema_name'] + '.' + \
                       children['referencing_object_name'] + ')'


    # Now create the file with a fully qualified name for the view
    with open(f"{outputdir}/{db}/{db}.{schema_name}.{view_name.replace('/', '')}.md", 'w') as ofile:
        # Write tags:
        ofile.write(f"---\ntags: [view, {schema_name}, {view_name}]\n---\n\n")
        ofile.write(f"## VIEW: {db}.{schema_name}.{view_name}\n\n")
        ofile.write("### DETAILS:\n\n")
        ofile.write(markdown_table(filtered_view_def[['created', 'last_modified', 'comments']]))
        ofile.write("### USER NOTES:\n\n")
        ofile.write(f"![[notes/{db}.{schema_name}.{view_name}.notes.md]]\n\n")
        ofile.write("### COLUMNS:\n\n")
        ofile.write(markdown_table(filtered_vcd.drop(columns=['schema_name',
                                                              'view_name',
                                                              'data_type'])))
        if len(parents) > 0:
            ofile.write("### PARENTS:\n")
            # Drop columns describing this view and create markdown table of children
            ofile.write( \
                markdown_table( \
                    parents.drop(
                        columns=['DATABASE',
                                 'referencing_schema_name',
                                 'referencing_object_name',
                                 'referencing_type_desc'
                                 ]
                    )
                )
            )

        if len(children) > 0:
            ofile.write("### CHILDREN:\n\n")
            ofile.write( \
                markdown_table( \
                    children.drop(
                        columns=['referenced_server_name',
                                 'referenced_db_name',
                                 'referenced_schema_name',
                                 'referenced_entity_name'
                                 ]
                    )
                )
            )

        ofile.write("### DEFINITION:\n\n")
        if len(filtered_view_def['definition']) > 0:
            actual = list(filtered_view_def['definition'])[0]
            cleaned = actual.replace("\r", "")
            cleaned = re.sub(r"^\s+$", "", cleaned, re.MULTILINE)
            cleaned = re.sub(r"^\s+", "", cleaned, re.MULTILINE)
            cleaned = sqlparse.format(cleaned.replace("\t", " "),
                                      reindent=True,
                                      keyword_case='upper')
        # Show SQL code for view definition in markdown ``` code block ```
        ofile.write(f"```\n{cleaned}\n```\n\n")
        ofile.write("\n\n")


def output_table_md_file(tab_cols_df, tab_dates_df, deps_df, db, schema_name, table_name, outputdir):
    #print(f"IN: output_table_md_file( tab_cols_df, tab_dates_df, tab_deps_df, {db}, {schema_name},  {table_name}) \n")

    # Filter to the specific db.schema.table
    filtered_tcd = tab_cols_df.loc[ (tab_cols_df['table_name'] == table_name) & \
                                (tab_cols_df['schema_name'] == schema_name) ].copy(deep=True)

    filtered_td = tab_dates_df.loc[ (tab_dates_df['table_name'] == table_name) & \
                                    (tab_dates_df['schema_name'] == schema_name) ].copy(deep=True)

    parents = deps_df.loc[ (deps_df['referencing_object_name'] == table_name) &\
                           (deps_df['referencing_schema_name'] == schema_name)  ].copy(deep=True)

    children = deps_df.loc[ (deps_df['referenced_entity_name'] == table_name) & \
                            (deps_df['referenced_schema_name'] == schema_name) ].copy(deep=True)

    # Create columns to use for obsidian links to parents / children
    parents['Link'] = '[Link](' + \
                       parents['referenced_db_name'] + '.' + \
                       parents['referenced_schema_name'] + '.' + \
                       parents['referenced_entity_name'] + ')'

    children['Link'] = '[Link](' + \
                       children['DATABASE'] + '.' + \
                       children['referencing_schema_name'] + '.' + \
                       children['referencing_object_name'] + ')'


    # Now create the file with a fully qualified name for the view
    with open(f"{outputdir}/{db}/{db}.{schema_name}.{table_name.replace('/', '')}.md", 'w') as ofile:
        # Write tags:
        ofile.write(f"---\ntags: [table, {schema_name}, {table_name}]\n---\n\n")
        ofile.write(f"## TABLE: {db}.{schema_name}.{table_name}\n\n")
        ofile.write("### DETAILS:\n\n")
        ofile.write(markdown_table( filtered_td ))
        ofile.write("### USER NOTES:\n\n")
        ofile.write(f"![[../notes/{db}.{schema_name}.{table_name}.notes.md]]\n\n")
        ofile.write("### COLUMNS:\n\n")
        ofile.write(markdown_table( filtered_tcd.drop(columns=['schema_name', 'table_name']) ))
        if len(parents) > 0:
            ofile.write("### PARENTS:\n")
            # Drop columns describing this view and create markdown table of children
            ofile.write( \
                markdown_table( \
                    parents.drop(
                        columns=['DATABASE',
                                 'referencing_schema_name',
                                 'referencing_object_name',
                                 'referencing_type_desc'
                                 ]
                    )
                )
            )
        if len(children) > 0:
            ofile.write("### CHILDREN:\n")
            # Drop columns describing this view and create markdown table of children
            ofile.write( \
                markdown_table( \
                    children.drop(
                        columns=['referenced_server_name',
                                 'referenced_db_name',
                                 'referenced_schema_name',
                                 'referenced_entity_name',
                                 ]
                    )
                )
            )


# ----------------------------------------------------------------------------
if __name__ == "__main__":
    main()

