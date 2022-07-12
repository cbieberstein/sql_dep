import pandas as pd
from sqlalchemy import create_engine
from queries import *
from pathlib import Path


def main():
    # Move to config file
    DataCatalog_Path = Path("C:/obsidian/datacatalog")
    Include_Path = DataCatalog_Path / "include"

    engine = connect_sqlserver_windowsauth('bidatacentre', 'master')
    db_df = fetch_all_dbs(engine)
    for db in list(db_df['name']):
        engine = connect_sqlserver_windowsauth('bidatacentre',db)
        tab_det_df = fetch_table_details(engine)
        tab_dates = fetch_table_dates_and_rowcount(engine)
        for db_schema in list(tab_det_df['schema_name'].unique()):
            # Include Path will hold user notes
            (Include_Path / db / db_schema).mkdir(parents=True, exist_ok=True)
            # Datacatalog Path will hold all programmatically generated files
            (DataCatalog_Path / db / db_schema).mkdir(parents=True, exist_ok=True)
            for table in list((tab_det_df.loc[tab_det_df['schema_name'] == db_schema])['table_name'].unique()):
                # Just the column details for this table
                tab_df = tab_det_df.loc[(tab_det_df['schema_name'] == db_schema) & (tab_det_df['table_name'] == table)]
                tab_df = tab_df[['column_name',
                                 'data_type_ext',
                                 'nullable',
                                 'default_value',
                                 'primary_key',
                                 'foreign_key',
                                 'unique_key',
                                 'comments']]
                tab_df.rename(columns={'column_name': 'Column',
                     'data_type_ext': 'DataType',
                     'nullable': 'Nullable',
                     'default_value': 'Default',
                     'primary_key': 'IS_PK',
                     'foreign_key': 'IS_FK',
                     'unique_key': 'Unique',
                     'comments': 'Comments'}, inplace=True)
                tabfile = table + ".md"
                row = tab_dates.loc[(tab_dates['schema_name'] == db_schema) & (tab_dates['table_name'] == table)]
                with open ((DataCatalog_Path / db / db_schema / tabfile ), mode='w') as file:
                    file.write(f"DB: **{db}**\nSCHEMA: **{db_schema}**\nOBJECT: **{table}** \n")
                    file.write(f"Created: **{row.iloc[0]['created']}**\nModified: **{row.iloc[0]['last_modified']}**\n")
                    file.write(f"Rows: **{row.iloc[0]['num_rows']}**\nComments: **{row.iloc[0]['comments']}**\n\n")
                    file.write("##### DEPENDENCIES :\n\n")
                    file.write("""```mermaid
graph LR
A --> B
B --> C
```
""")
                    file.write("##### COLUMN DEFINITIONS:\n\n")
                    file.write(markdown_table(tab_df))
                    file.write("\n\n")

                    file.write("##### USER NOTES:\n\n")
                    file.write(f"![[include/{db}/{db_schema}/{table}_user_notes.md]]\n\n")
    # End Main



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
