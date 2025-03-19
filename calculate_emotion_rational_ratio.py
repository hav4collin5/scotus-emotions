import pandas as pd
import argparse

def load_counts(file_path):
    """Load the aggregated counts."""
    return pd.read_csv(file_path)

def calculate_ratios(emotion_df, rational_df, start, end):
    """Calculate emotion-to-rational word ratios by document ID and opinion type."""
    # Filter document_id based on specified range
    doc_ids = sorted(emotion_df['document_id'].unique())[start:end]

    emotion_filtered = emotion_df[emotion_df['document_id'].isin(doc_ids)]
    rational_filtered = rational_df[rational_df['document_id'].isin(doc_ids)]

    # Sum counts grouped by document_id and opinion_type
    emotion_sums = emotion_filtered.groupby(['document_id', 'opinion_type'])['count'].sum().reset_index(name='emotion_count')
    rational_sums = rational_filtered.groupby(['document_id', 'opinion_type'])['count'].sum().reset_index(name='rational_count')

    # Merge summed counts
    merged = pd.merge(emotion_sums, rational_sums, how='inner', on=['document_id', 'opinion_type'])

    # Calculate the ratio (handle division by zero)
    merged['ratio'] = merged.apply(
        lambda row: row['emotion_count'] / row['rational_count'] if row['rational_count'] > 0 else None,
        axis=1
    )

    return merged[['document_id', 'opinion_type', 'ratio']]

def main(emotion_file, rational_file, output_file, start, end):
    emotion_df = load_counts(emotion_file)
    rational_df = load_counts(rational_file)

    ratios_df = calculate_ratios(emotion_df, rational_df, start, end)

    # Save output
    ratios_df.to_csv(output_file, index=False)
    print(f"Saved ratios to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--emotion', required=True, help='Aggregated emotion counts CSV file')
    parser.add_argument('--rational', required=True, help='Aggregated rational counts CSV file')
    parser.add_argument('--output', required=True, help='Output CSV file for ratios')
    parser.add_argument('--start', type=int, required=True, help='Start index for document IDs')
    parser.add_argument('--end', type=int, required=True, help='End index for document IDs')

    args = parser.parse_args()
    main(args.emotion, args.rational, args.output, args.start, args.end)
