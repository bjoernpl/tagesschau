import datasets
import re
import argparse

def clean(example):
    example["short_text"] = example["short_text"].strip()
    # remove right trailing "\nmehr"
    example["short_text"] = re.sub(r"\nmehr$", "", example["short_text"])
    example["headline"] = example["headline"].strip()
    example["short_headline"] = example["short_headline"].strip()
    example["article"] = example["article"].strip()
    return example

def clean_csv(path):
    dataset = datasets.load_dataset("csv", data_files=path, delimiter="\t")
    dataset = dataset.drop_duplicates(subset=['article'], ignore_index=True)
    dataset = dataset.map(clean)
    dataset = dataset.sort("date", reverse=True)
    return dataset

def main(args):
    ds = clean_csv(args.path)
    if args.upload_path:
        ds.push_to_hub(args.upload_path)
    else:
        ds.save_to_disk(args.save_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean the scraped data and save it to disk or upload it to the HuggingFace Hub."
    )
    parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="The path to the csv file.",
    )
    parser.add_argument(
        "--save_path",
        type=str,
        default="./cleaned_data",
        help="The path to the directory where the cleaned data should be saved.",
    )
    parser.add_argument(
        "--upload_path",
        type=str,
        default=None,
        help="The path to the HuggingFace Hub where the cleaned data should be uploaded.",
    )
    args = parser.parse_args()
    main(args)
