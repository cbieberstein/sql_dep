from queries import *
import sqlparse
import re

def main():
    outdir = "./output/"

    # Get all database names on this server
    engine = connect_sqlserver_windowsauth('bidatacentre', 'master')
    db_df = fetch_all_dbs(engine)

    # Loop through all the DBs (fetch_all_dbs excludes:  master, model, msdb, tempdb)
    for db in list(db_df['name']):
        engine = connect_sqlserver_windowsauth('bidatacentre',db)

        # Table Details
        tcd = fetch_table_details(engine)
        tab_dates = fetch_table_dates_and_rowcount(engine)

        # View Details
        vcd = fetch_view_column_details(engine)
        view_def = fetch_view_definitions(engine)

        # Get all DB Dependencies
        deps_df = fetch_all_db_dependencies(engine)

        # Iterate through all tables in this database, generating a .md file for each
        for schema_name in list(tcd['schema_name'].unique()):
            for table_name in list(tcd['table_name'].unique()):
                tcd_filter = (tcd['table_name'] == table_name) &\
                             (tcd['schema_name'] == schema_name)
                tab_dates_filter = (tab_dates['table_name'] == table_name) &\
                                   (tab_dates['schema_name'] == schema_name)
                # Parents don't apply to tables
                #deps_parent_filter = (deps_df['referencing_object_name'] == table_name) &\
                #                     (deps_df['referencing_schema_name'] == schema_name)
                deps_child_filter = (deps_df['referenced_entity_name'] == table_name) &\
                                    (deps_df['referenced_schema_name'] == schema_name)

            # Now create the file with a fully qualified name for the view
            with open(f"{outdir}{db}.{schema_name}.{table_name.replace("/),"")}.md", 'w') as ofile:
                ofile.write(f"## TABLE: {db}.{schema_name}.{table_name}\n")
                ofile.write("### DETAILS:\n")
                ofile.write(markdown_table(tab_dates.loc[tab_dates_filter]))
                ofile.write("### USER NOTES:\n\n")
                ofile.write(f"![[include/{db}/{schema_name}/{table_name}_user_notes.md]]\n\n")
                ofile.write("### COLUMNS:\n")
                ofile.write(markdown_table(tcd.loc[tcd_filter]))
                # Parents don't apply to tables
                # ofile.write("### PARENTS:\n")
                # if len(deps_df.loc[deps_parent_filter]) > 0:
                #     ofile.write(markdown_table(deps_df.loc[deps_parent_filter]))
                # else:
                #     ofile.write("No Parents\n")
                ofile.write("### CHILDREN:\n")
                if len(deps_df.loc[deps_child_filter]) > 0:
                    ofile.write(markdown_table(deps_df.loc[deps_child_filter]))
                else:
                    ofile.write("No Children\n")

        # Iterate through all views in this database, generating a .md file for each
        for schema_name in list(vcd['schema_name'].unique()):
            for view_name in list(vcd['view_name'].unique()):
                # Define filters for this view_name in our reference tables
                vcd_filter = (vcd['view_name'] == view_name) &\
                             (vcd['schema_name'] == schema_name)
                view_def_filter = (view_def['view_name'] == view_name) &\
                                  (view_def['schema_name'] == schema_name)
                deps_parent_filter = (deps_df['referencing_object_name'] == view_name) &\
                                     (deps_df['referencing_schema_name'] == schema_name) & \
                                     (deps_df['DATABASE'].str.lower() == db.lower() )
                deps_child_filter = (deps_df['referenced_entity_name'] == view_name) &\
                                    (deps_df['referenced_schema_name'] == schema_name) & \
                                    (deps_df['referenced_db_name'].str.lower() == db.lower() )

                # Now create the file with a fully qualified name for the view
                with open(f"{outdir}{db}.{schema_name}.{view_name.replace("/),"")}.md", 'w') as ofile:
                    ofile.write(f"## VIEW: {db}.{schema_name}.{view_name}\n")
                    ofile.write("### DETAILS:\n")
                    ofile.write(markdown_table(
                            view_def.loc[view_def_filter][['created','last_modified','comments']]
                        ))
                    ofile.write("### USER NOTES:\n\n")
                    ofile.write(f"![[include/{db}/{schema_name}/{view_name}_user_notes.md]]\n\n")
                    ofile.write("### COLUMNS:\n")
                    ofile.write(markdown_table(vcd.loc[vcd_filter]))
                    ofile.write("### PARENTS:\n")
                    if len(deps_df.loc[deps_parent_filter]) > 0:
                        ofile.write(markdown_table(deps_df.loc[deps_parent_filter]))
                    else:
                        ofile.write("No Parents\n")
                    ofile.write("### CHILDREN:\n")
                    if len(deps_df.loc[deps_child_filter]) > 0:
                        ofile.write(markdown_table(deps_df.loc[deps_child_filter]))
                    else:
                        ofile.write("No Children\n")
                    ofile.write("### DEFINITION:\n")
                    if len(view_def.loc[view_def_filter]['definition']) > 0:
                        actual = list(view_def.loc[view_def_filter]['definition'])[0]
                        cleaned = actual.replace("\r", "")
                        cleaned = re.sub(r"^\s+$", "", cleaned, re.MULTILINE)
                        cleaned = re.sub(r"^\s+", "", cleaned, re.MULTILINE)
                        cleaned = sqlparse.format(cleaned.replace("\t"," "),
                            reindent=True,
                            keyword_case='upper')
                    # Show SQL code for view definition in markdown ``` code block ```
                    ofile.write(f"#### CLEANED SQL:\n```\n{cleaned}\n```\n\n")
                    # Do not write the actual SQL its just too ugly
                    #ofile.write(f"#### ACTUAL SQL:\n```\n{actual}\n```\n\n")
                    ofile.write("\n\n")
                # Done with View file output



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

# ----------------------------------------------------------------------------
if __name__ == "__main__":
    main()

