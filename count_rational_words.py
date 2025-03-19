import json
import pandas as pd
import re
import argparse

def load_rational_dictionary(dict_path):
    df = pd.read_csv(dict_path)
    df['regex'] = df['regex'].astype(str)
    df = df[['Words', 'regex']].rename(columns={'Words':'word'}).dropna()
    return df

def count_rational_words(tokenized_file, rational_df, start_idx, end_idx):
    records = []

    with open(tokenized_file, 'r') as f:
        for i, line in enumerate(f):
            if i < start_idx:
                continue
            if i >= end_idx:
                break

            opinion = json.loads(line)
            doc_id = opinion['document_id']
            opinion_type = opinion['opinion_type']
            tokens = opinion['tokens']
            text = ' '.join(tokens).lower()

            for _, row in rational_df.iterrows():
                word, regex = row['word'], row['regex']
                matches = re.findall(regex, text)
                count = len(matches)

                if count > 0:
                    records.append({
                        'document_id': doc_id,
                        'opinion_type': opinion_type,
                        'word': word,
                        'regex': regex,
                        'count': count
                    })

    return pd.DataFrame(records)

def main(tokenized_file, rational_dict_file, output_file, start_idx, end_idx):
    rational_df = load_rational_dictionary(rational_dict_file)
    df_detail = count_rational_words(tokenized_file, rational_df, start_idx, end_idx)

    if df_detail.empty:
        print("No records found in this range.")
    else:
        df_detail.to_csv(output_file, index=False)
        print(f"Saved rational word counts to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tokenized', required=True, help='Path to tokenized opinions JSONL file')
    parser.add_argument('--dictionary', required=True, help='Path to rational dictionary CSV')
    parser.add_argument('--output', required=True, help='Path to save rational word counts CSV')
    parser.add_argument('--start', type=int, required=True, help='Start index')
    parser.add_argument('--end', type=int, required=True, help='End index')

    args = parser.parse_args()
    rational_df = load_rational_dictionary(args.dictionary)
    df_detail = count_rational_words(args.tokenized, rational_df, args.start, args.end)
    if df_detail.empty:
        print("No records found in this range.")
    else:
        df_detail.to_csv(args.output, index=False)
        print(f"Saved rational word counts to {args.output}")
