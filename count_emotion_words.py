import json
import pandas as pd
import re
import argparse

def load_emotion_dictionary(emotion_dict_path):
    df = pd.read_csv(emotion_dict_path)
    df['regex'] = df['regex'].astype(str)
    df = df[['Words', 'regex']].rename(columns={'Words':'word'}).dropna()
    return df

def count_emotion_words(tokenized_file, emotion_df, start_idx, end_idx):
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

            for _, row in emotion_df.iterrows():
                word, regex = row['word'], row['regex']
                matches = re.findall(regex, text)
                count = len(matches)
                if count > 0:  # Only save counts > 0
                    records.append({
                        'document_id': doc_id,
                        'opinion_type': opinion_type,
                        'word': word,
                        'regex': regex,
                        'count': count
                    })

    return pd.DataFrame(records)

def main(tokenized_file, emotion_dict_file, output_file, start_idx, end_idx):
    emotion_df = load_emotion_dictionary(emotion_dict_file)
    df_detail = count_emotion_words(tokenized_file, emotion_df, start_idx, end_idx)

    if df_detail.empty:
        print("No records found in this range.")
        return

    # Save detailed counts
    df_detail.to_csv(output_file, index=False)
    print(f"Emotion word counts saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tokenized', type=str, required=True, help='Path to tokenized opinions JSONL file')
    parser.add_argument('--dictionary', type=str, required=True, help='Path to emotion dictionary CSV file')
    parser.add_argument('--output', type=str, required=True, help='Path to save emotion word count CSV')
    parser.add_argument('--start', type=int, required=True, help='Start index for processing')
    parser.add_argument('--end', type=int, required=True, help='End index for processing')

    args = parser.parse_args()
    main(args.tokenized, args.dictionary, args.output, args.start, args.end)
