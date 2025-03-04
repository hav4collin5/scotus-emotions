import json
import pandas as pd
import re
import argparse
from collections import defaultdict


def load_emotion_dictionary(emotion_dict_path):
    """Load emotion dictionary from CSV file."""
    df = pd.read_csv(emotion_dict_path)
    df['regex'] = df['regex'].astype(str)  # Ensure regex column is string
    emotion_words = df['regex'].tolist()
    return emotion_words


def count_emotion_words(tokenized_file, emotion_words, start_idx, end_idx):
    """Count occurrences of emotion words in a chunk of tokenized opinions."""
    results = []
    
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
            
            text = ' '.join(tokens).lower()  # Convert to lowercase for matching
            word_counts = defaultdict(int)
            
            for regex in emotion_words:
                if isinstance(regex, str) and regex.strip():  # Ensure regex is a valid string
                    matches = re.findall(regex, text)
                    word_counts[regex] = len(matches)
            
            results.append({
                'document_id': doc_id,
                'opinion_type': opinion_type,
                **word_counts
            })
    
    return pd.DataFrame(results)


def main(tokenized_file, emotion_dict_file, output_file, start_idx, end_idx):
    emotion_words = load_emotion_dictionary(emotion_dict_file)
    df_counts = count_emotion_words(tokenized_file, emotion_words, start_idx, end_idx)
    df_counts.to_csv(output_file, index=False)
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
