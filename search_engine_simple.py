import pathlib
import os
import json
from whoosh.qparser.syntax import OrGroup
from whoosh.query import Query
from whoosh.qparser import QueryParser
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID

schema = Schema(
    name=ID(stored=True),
    content=TEXT,
)

POEMS_DIR = './datasets/Poems'
QUERIES_DIR = './datasets/Queries'
INDEX_DIR = './indices/simple'


def make_indices():
    if not os.path.exists(INDEX_DIR):
        os.makedirs(INDEX_DIR)

    file_indexer = create_in(INDEX_DIR, schema)
    index_writer = file_indexer.writer()

    poem_files_paths = set(pathlib.Path(POEMS_DIR).glob("*.persian_poem"))
    for poem_file_path in poem_files_paths:
        with open(poem_file_path, 'r', encoding='utf8') as poem_file:
            poem_text = poem_file.read()
            # Add more poem preprocessing here...
            poem_filename = poem_file_path.name
            index_writer.add_document(
                name=poem_filename,
                content=poem_text
            )
    index_writer.commit()


def search(query: Query) -> list:
    indexer = open_dir(INDEX_DIR)
    with indexer.searcher() as searcher:
        results = list(
            map(
                lambda result_item: result_item.get('name'),
                list(
                    map(
                        lambda search_result: search_result.fields(),
                        list(searcher.search(query)),
                    )
                )
            )
        )

        return results


def evaluate_result(actual: list, expected: list) -> float:
    score = 0
    for hit in actual:
        if hit in expected:
            score += 1
    return score / len(expected) if len(expected) > 0 else 0.0


def evaluate_results(actual_results: dict, expected_results: dict) -> dict:
    scores = dict().fromkeys(actual_results.keys())
    for (actual_result_query, actual_result_hits) in actual_results.items():
        expected_result_hits = expected_results[actual_result_query]
        score = evaluate_result(actual_result_hits, expected_result_hits)
        scores[actual_result_query] = score
    return scores


def read_expected_results() -> dict:
    results = dict()
    with open(
        './RelevanceAssessment.txt',
        'r',
        encoding='utf8'
    ) as results_file:
        lines = results_file.readlines()
        for i in range(0, len(lines), 3):
            query_line = lines[i].strip()
            results_line = lines[i+1]
            results_set = [item.strip() for item in results_line.split()]
            results.update([(query_line, results_set)])
        return results


def main():
    query_file_paths = set(pathlib.Path(QUERIES_DIR).glob('*.persian_query'))
    query_results = dict().fromkeys(
        map(
            lambda file_path: file_path.name,
            query_file_paths
        )
    )
    for query_file_path in query_file_paths:
        with open(query_file_path, 'r', encoding='utf8') as query_file:
            query_text = query_file.read()
            # Maybe add more query preprocessing stuffs...
            query = QueryParser(
                "content",
                schema,
                group=OrGroup
            ).parse(query_text)
            search_results = search(query)
            query_filename = query_file_path.name
            query_results[query_filename] = search_results

    expected_results = read_expected_results()
    scores = evaluate_results(query_results, expected_results)
    print('Scores by query:', json.dump(scores, indent=4))
    print(f'Mean: {sum(scores.values())/len(scores)}')


main()
