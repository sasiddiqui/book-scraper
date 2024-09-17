import json

def convert(fn):
    data = json.load(open(fn))
    new = []
    skipped = 0

    ess_attrs = {
        "URL" : "url",
        "Title" : "title",
        "Price" : "price",
    }

    opt_attrs = {
        "In Stock" : "instock",
        "Image" : "image",
        "Editor" : "editor",
        "pages" : "pages",
        "Binding" : "binding",
        "Year Published" : "year",
    }

    for book in data:

        new_book = {}

        if any(k not in book for k in ess_attrs.keys()):
            print(f"Skipping book {book['Title']} because it is missing essential attributes")
            skipped += 1
            continue

        for k, v in ess_attrs.items():
            new_book[v] = book.get(k, "")

        for k, v in opt_attrs.items():
            if k in book:
                new_book[v] = book.get(k)

        new.append(new_book)

    print(f"Skipped {skipped} books")
    return new


if __name__ == "__main__":
    json.dump(convert("jsons/zakariyyabooks.json"), open("jsons/zakariyyabooksconverted.json", "w"), indent=4)