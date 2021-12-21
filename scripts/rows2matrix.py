#!/usr/bin/python

# Created by: Anderson Brito
#
# row2matrix.py -> It converts stacked rows of values in two columns into a matrix
#
#
# Release date: 2021-08-22
# Last update: 2021-09-22

import pandas as pd
import argparse
import time
import itertools

pd.set_option('max_columns', 100)
# print(pd.show_versions())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate matrix of occurrences at the intersection of two or more columns",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="TSV file with data to be aggregated as two-dimensional matrix")
    parser.add_argument("--xvar", required=True, type=str, help="Data that goes in the X axis of the matrix")
    parser.add_argument("--xtype", required=False, type=str, help="Is the x variable a time variable (date)? If so, enter 'time'")
    parser.add_argument("--target", required=False, type=str, help="Target column, when variable is already aggregated")
    parser.add_argument("--sum-target",required=False, nargs=1, type=str, default='no',
                        choices=['no', 'yes'], help="Should values in target column be summed up?")
    parser.add_argument("--format",required=False, nargs=1, type=str, default='float',
                        choices=['float', 'integer'], help="What is the format of the data points (float/integer)?")
    parser.add_argument("--yvar", nargs="+", required=True, type=str, help="One or more columns to be used as index")
    parser.add_argument("--unique-id", required=True, type=str, help="Column including the unique ids to be displayed in the Y axis")
    parser.add_argument("--extra-columns", nargs="+", required=False, type=str, help="Extra columns to export")
    parser.add_argument("--filter", nargs="+", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--start-date", required=False, type=str,  help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=False, type=str,  help="End date in YYYY-MM-DD format")
    parser.add_argument("--output", required=True, help="TSV matrix")
    args = parser.parse_args()

    input = args.input
    x_var = args.xvar
    x_type = args.xtype
    target_variable = args.target
    sum_target = args.sum_target[0]
    data_format = args.format[0]
    y_var = args.yvar
    y_unique_id = args.unique_id
    extra_cols = args.extra_columns
    filters = args.filter
    start_date = args.start_date
    end_date = args.end_date
    output = args.output

    # path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/sgtf_omicron/analyses/'
    # input = path + 'results/combined_testdata_geo.tsv'
    # x_var = 'date_testing'
    # x_type = 'time'
    # y_var = ['ADM2_PCODE', 'S_detection']
    # y_unique_id ='ADM2_PCODE'
    # target_variable = ''
    # sum_target = ''
    # data_format = 'integer'
    # extra_cols = ['ADM1_PT', 'ADM2_PT']
    # filters = ['~test_result:Negative']
    # start_date = '2021-12-01' # start date above this limit
    # end_date = '2021-12-18' # end date below this limit
    # output = path + 'matrix.tsv'


    def load_table(file):
        df = ''
        if str(file).split('.')[-1] == 'tsv':
            separator = '\t'
            df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
        elif str(file).split('.')[-1] == 'csv':
            separator = ','
            df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
        elif str(file).split('.')[-1] in ['xls', 'xlsx']:
            df = pd.read_excel(file, index_col=None, header=0, sheet_name=0, dtype='str')
            df.fillna('', inplace=True)
        else:
            print('Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX')
            exit()
        return df


    df = load_table(input)
    df.fillna('', inplace=True)

    for idx in y_var:
        df = df[~df[idx].isin([''])]

    dfF = pd.DataFrame()
    if filters not in ['', None]:
        print('\nFiltering rows based on provided values...')
        # for filter_value in sorted([f.strip() for f in filters.split(',')]):
        for filter_value in sorted(filters):
            col = filter_value.split(':')[0]
            val = filter_value.split(':')[1]

            # inclusion of specific rows
            if not filter_value.startswith('~'):
                print('\t- Including only rows with \'' + col + '\' = \'' + val + '\'')
                if dfF.empty:
                    df_filtered = df[df[col].isin([val])]
                    dfF = dfF.append(df_filtered)
                else:
                    dfF = dfF[dfF[col].isin([val])]
                print(dfF)
        # for filter_value in sorted([f.strip() for f in filters.split(',')]):
        for filter_value in sorted(filters):
            col = filter_value.split(':')[0]
            val = filter_value.split(':')[1]
            # exclusion of specific rows
            if filter_value.startswith('~'):
                col = col[1:]
                print('\t- Excluding all rows with \'' + col + '\' = \'' + val + '\'')
                if dfF.empty:
                    df_filtered = df[~df[col].isin([val])]
                    dfF = dfF.append(df_filtered)
                else:
                    dfF = dfF[~dfF[col].isin([val])]
    df = dfF
    # print(df.head)

    # filter by time
    if x_type == 'time':
        today = time.strftime('%Y-%m-%d', time.gmtime())
        df[x_var] = pd.to_datetime(df[x_var])  # converting to datetime format
        if start_date in [None, '']:
            start_date = df[x_var].min()
        if end_date in [None, '']:
            end_date = today
        mask = (df[x_var] >= start_date) & (df[x_var] <= end_date)  # mask any lines with dates outside the start/end dates
        df = df.loc[mask]  # apply mask
        df[x_var] = df[x_var].dt.strftime('%Y-%m-%d')

    if x_type == 'time':
        time_range = [day.strftime('%Y-%m-%d') for day in list(pd.date_range(pd.to_datetime(start_date), pd.to_datetime(end_date), freq='d'))]
        data_cols = time_range
    else:
        data_cols = sorted(df[x_var].unique().tolist())


    list_ids = []
    for col_index in y_var:
        ids = []
        for idx, row in df.iterrows():
            id = df.loc[idx, col_index]
            if id not in ids:
                ids.append(id)
        list_ids.append(ids)

    print('\nA total of ' + str(len(df.index) + 1) + ' rows were included after filtering (by values and time period).')

    # print(data_cols)
    # print(rows)

    # set new indices
    df.insert(0, 'unique_id1', '')
    df['unique_id1'] = df[y_var].astype(str).sum(axis=1)
    df.insert(1, 'unique_id2', '')
    df['unique_id2'] = df[y_unique_id]#.astype(str).sum(axis=1)

    # # index = pd.MultiIndex.from_tuples(rows, names=y_var)
    df2 = pd.DataFrame(columns=data_cols)
    # df2['unique_id1'] = df2[y_var].astype(str).sum(axis=1)

    # indexing
    df2.insert(0, 'unique_id1', '')
    for y_col in y_var:
        df2.insert(0, y_col, '')
    rows = list(itertools.product(*list_ids))
    for idx, id_names in enumerate(rows):
        unique_id1 = ''.join(id_names)
        pos = y_var.index(y_unique_id)
        unique_id2 = id_names[pos]
        # print(unique_id1, unique_id2)
        df2.loc[idx, 'unique_id1'] = unique_id1
        df2.loc[idx, 'unique_id2'] = unique_id2
        for num, col_name in enumerate(y_var):
            value = id_names[num]
            df2.loc[idx, col_name] = value


    df2 = df2.fillna(0) # with 0s rather than NaN
    df2.set_index('unique_id1', inplace=True)

    if extra_cols not in [None, '']:
        for column in extra_cols:
            if column in df.columns.to_list():
                df2.insert(0, column, '')

    if target_variable in ['', None]:
        y_var = list(set(y_var))
        df1 = df.groupby([x_var] + ['unique_id1']).size().to_frame(name='count').reset_index() # group and count occorrences
    else:
        if sum_target == 'yes':
            if data_format == 'float':
                df[target_variable] = df[target_variable].astype(float)
            else:
                df[target_variable] = df[target_variable].astype(int)

            df1 = df.groupby([x_var] + y_var, sort=False)[target_variable].sum().reset_index(name='count')

            if data_format == 'float':
                df1['count'] = df1['count'].round(2)
        else:
            df1 = df.rename(columns={target_variable: 'count'})

    # print(df)
    # if len(y_var) > 0:
    # df[y_unique_id] = df[y_unique_id].astype(str)
    # df1[y_var] = df1[y_var].astype(str)

    df.set_index('unique_id1', inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    #
    # print(df.head)
    # print(df2.head)
    # print(df2)

    # fill extra columns with their original content
    if extra_cols not in [None, '']:
        for column in extra_cols:
            if column in df.columns.to_list():
                for idx, row in df2.iterrows():
                    unique_id2 = df2.loc[idx, 'unique_id2']
                    value = df.loc[df['unique_id2'] == unique_id2, column][0]
                    # print(idx, column, value)
                    df2.at[idx, column] = value

    # populate output dataframe
    for idx, row in df1.iterrows():
        x = df1.loc[idx, x_var]
        y = df1.loc[idx, 'unique_id1']
        count = df1.loc[idx, 'count']
        if count < 0:
            count = 0
        df2.at[y, x] = count

    # save
    df2 = df2.reset_index()
    df2 = df2.drop(columns=['unique_id1', 'unique_id2'])
    df2 = df2[y_var + extra_cols + data_cols]
    df2 = df2.sort_values(by=y_unique_id)
    df2.to_csv(output, sep='\t', index=False)
    print('\nConversion successfully completed.\n')
