import databento as db

INPUT_PATH = "CLX5_mbo (2).dbn"
OUTPUT_PATH = "CLX5_mbo.csv"

def main():
    print("Loading DBN file:", INPUT_PATH)
    store = db.DBNStore.from_file(INPUT_PATH)

    store.to_csv(OUTPUT_PATH)

    print("CSV written to:", OUTPUT_PATH)
    print("Done!")

if __name__ == "__main__":
    main()
