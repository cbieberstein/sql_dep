from queries import *
import sqlparse

def main():
    outdir = "./output/"
    db = 'BI'
    engine = connect_sqlserver_windowsauth('bidatacentre',db)

    # Table Details
    #tab_det_df = fetch_table_details(engine)
    #tab_dates = fetch_table_dates_and_rowcount(engine)

    # View Details
    vcd = fetch_view_column_details(engine)
    view_def = fetch_view_definitions(engine)

    # Get all DB Dependencies
    deps_df = fetch_all_db_dependencies(engine)

    # Iterate through all views in this database, generating a .md file for each
    for view_name in list(vcd['view_name'].unique()):
        # Define filters for this view_name in our reference tables
        vcd_filter = vcd['view_name'] == view_name
        view_def_filter = view_def['view_name'] == view_name
        deps_parent_filter = deps_df['referencing_object_name'] == view_name
        deps_child_filter = deps_df['referenced_entity_name'] == view_name

        # All columns are in the same schema so just take the first schema
        view_schema = list(vcd.loc[vcd_filter]['schema_name'])[0]

        # Now create the file with a fully qualified name for the view
        with open(f"{outdir}{db}.{view_schema}.{view_name}.md", 'w') as ofile:
            ofile.write(f"# VIEW: {db}.{view_schema}.{view_name}\n")
            ofile.write("### DETAILS:\n")
            ofile.write(markdown_table(
                    view_def.loc[view_def_filter][['created','last_modified','comments']]
                ))
            ofile.write("### COLUMNS:\n")
            ofile.write(markdown_table(vcd.loc[vcd_filter]))
            ofile.write("### PARENTS:\n")
            if len(deps_df.loc[deps_parent_filter]) > 0:
                ofile.write(markdown_table(deps_df.loc[deps_parent_filter]))
            else:
                ofile.write("No Parents.... this is a view, somethings wrong here :) ")
            ofile.write("### CHILDREN:\n")
            if len(deps_df.loc[deps_child_filter]) > 0:
                ofile.write(markdown_table(deps_df.loc[deps_child_filter]))
            else:
                ofile.write("No Children\n")
            ofile.write("### DEFINITION:\n")
            actual = list(view_def.loc[view_def_filter]['definition'])[0]
            cleaned = sqlparse.format(
                actual.replace("[","").replace("]","").replace("\t"," "),
                reindent=True,
                keyword_case='upper')
            ofile.write(f"####ACTUAL:\n{actual}\n\n")
            ofile.write(f"####CLEANED:\n{cleaned}\n\n")
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

