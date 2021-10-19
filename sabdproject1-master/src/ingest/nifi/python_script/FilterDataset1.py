import pandas as pd  # sudo -H pip3 install pandas
import sys

def main():

    csv_file = sys.stdin
    df = pd.read_csv(csv_file)
    final_df=df[['data','dimessi_guariti','tamponi']]
    final_df.to_csv(sys.stdout,index=None)
if __name__ == '__main__':
    main()