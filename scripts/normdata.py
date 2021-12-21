import pandas as pd
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Normalize data matrix, using another matrix or constant values",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input1", required=True, help="Main matrix, used as the numerator")
    parser.add_argument("--input2", required=True, type=str,  help="Secondary matrix, with values used as denominators")
    parser.add_argument("--index1", nargs="+", required=True, type=str, help="Columns with unique identifiers in the numerator file")
    parser.add_argument("--index2", nargs="+", required=True, type=str, help="Columns with unique identifiers in the denominator file, at least one match index1")
    parser.add_argument("--norm-var", required=False, type=str,  help="Single column to be used for normalization of all columns (e.g. population)")
    parser.add_argument("--rate", required=False, type=int,  help="Rate factor for normalization (e.g. 100000 habitants)")
    parser.add_argument("--filter", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--output", required=True, help="TSV matrix with normalized values")
    args = parser.parse_args()

    input1 = args.input1
    input2 = args.input2
    unique_id1 = args.index1
    unique_id2 = args.index2
    norm_variable = args.norm_var
    rate_factor = args.rate
    filters = args.filter
    output = args.output


    # path = '~/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/sgtf_omicron/data_test/'
    # input1 = path + 'mock_matrix_sgtf_cities.tsv'
    # input2 = path + 'mock_matrix_cities_denominator.tsv'#'matrix_brazil-states_cases_months.tsv'#
    # unique_id1 = ['ADM2_PCODE', 'S_detection']
    # unique_id2 = ['ADM2_PCODE']
    # norm_variable = ''
    # rate_factor = ''
    # filters = None#'~S_detection:Detected'
    # output = path + 'mock_matrix_percentages_cities.tsv'


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

    # open dataframe
    df = load_table(input1)
    df.fillna('', inplace=True)


    for idx in unique_id1:
        df = df[~df[idx].isin([''])]

    dfF = pd.DataFrame()
    if filters not in ['', None]:
        print('\nFiltering rows based on provided values...')
        for filter_value in sorted([f.strip() for f in filters.split(',')]):
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
                # print(dfF)
        for filter_value in sorted([f.strip() for f in filters.split(',')]):
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

    df2 = load_table(input2)
    df2.fillna('', inplace=True)

    # print(df2.head)
    # print(df2.columns.tolist())

    # get columns
    date_columns = []
    for column in df.columns.to_list():
        if column[-1].isdecimal():
            if norm_variable in ['', None]:
                if column in df2.columns.tolist():
                    date_columns.append(column)
            else:
                date_columns.append(column)


    # set new indices
    df.insert(0, 'unique_id1', '')
    df['unique_id1'] = df[unique_id1].astype(str).sum(axis=1)
    df.insert(1, 'unique_id2', '')
    df['unique_id2'] = df[unique_id2].astype(str).sum(axis=1)

    df2.insert(0, 'unique_id2', '')
    df2['unique_id2'] = df2[unique_id2].astype(str).sum(axis=1)

    # create empty dataframes
    nondate_columns = [column for column in df.columns.to_list() if column not in date_columns]
    # print(date_columns)
    # print(nondate_columns)

    df3 = df.filter(nondate_columns, axis=1)

    # set new index
    # df.set_index(unique_id1, inplace=True)
    df2.set_index('unique_id2', inplace=True)
    df3.set_index('unique_id1', inplace=True)

    # print(df)
    # print(df2)
    # print(df3)

    # perform normalization
    for idx, row in df.iterrows():
        # print('\n' + str(idx))
        id1 = str(df.loc[idx, 'unique_id1'])
        id2 = str(df.loc[idx, 'unique_id2'])
        for time_col in date_columns:
            # print(time_col, df.loc[idx, time_col])
            numerator = float(df.loc[idx, time_col])
            # numerator = df.loc[(df[unique_id1] == id1), time_col]
            # print(id1, numerator)
            if norm_variable in ['', None]:
                denominator = float(df2.loc[id2, time_col])
                # denominator = int(df2.loc[(df2[unique_id2] == id2), time_col])
            else:
                denominator = float(df2.loc[id2, norm_variable])
                # denominator = int(df2.loc[(df2[unique_id2] == id2), norm_variable])

            if denominator == 0: # prevent division by zero
                normalized = 0
            else:
                if norm_variable in ['', None]:
                    normalized = '%.3f' % (numerator / denominator)
                else:
                    if rate_factor in ['', None]:
                        rate_factor = 1
                        print('\nNo rate factor provided. Using "1" instead.')
                    normalized = '%.3f' % ((numerator * rate_factor) / denominator)

            # print(numerator, denominator)
            # print(normalized)
            df3.at[id1, time_col] = normalized
            # print(df3.loc[(df3[unique_id1] == id1), time_col])
            # df3.loc[(df3[unique_id1] == id1), time_col] = normalized

    df3 = df3.reset_index()
    df3 = df3.drop(columns=['unique_id1', 'unique_id2'])

    # output converted dataframes
    df3.to_csv(output, sep='\t', index=False)
    print('\nNormalization successfully completed.\n')
