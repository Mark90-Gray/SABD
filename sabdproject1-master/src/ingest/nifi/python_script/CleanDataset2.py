import pandas as pd  # sudo -H pip3 install pandas
import sys


def fix_data(val):
    if str(val) == "":
        return ""
    else:
        return str(val).replace(",", " ")


def main():
    csv_file = sys.stdin
    df = pd.read_csv(csv_file)

    for i in range(0, 2):
        column_name = df.columns[i]
        df[column_name] = df[column_name].apply(lambda val: fix_data(val))
    #final_df = df[:, df.columns.isin['Lat', 'Lon']]
    df.to_csv(sys.stdout, index=None)


if __name__ == '__main__':
    main()
