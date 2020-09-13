import json
import sqlite3
from collections import defaultdict, Counter
from random import choice
from flask import Flask, request, jsonify, make_response, redirect


def get_relation(block):
    prefix = '# relation = '
    for line in block.splitlines():
        if line.startswith(prefix):
            return line.strip()[len(prefix):]


def populate_headers_basic(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Credentials'] = 'true'


def populate_headers_json(resp):
    populate_headers_basic(resp)
    resp.headers['Content-Type'] = 'application/json'


def populate_headers_text(resp):
    populate_headers_basic(resp)
    resp.headers['Content-Type'] = 'text/plain'


def get_processed():
    with open('data/processed.json', 'r', encoding='utf-8') as inp:
        return {k: set(v) for k, v in json.load(inp).items()}


def dump_processed(processed_dict):
    with open('data/processed.json', 'w', encoding='utf-8') as out:
        json.dump({k: sorted(v) for k, v in processed_dict.items()}, out, indent=2)


def get_discarded():
    with open('data/discarded.json', 'r', encoding='utf-8') as inp:
        return set(json.load(inp))


def dump_discarded(discarded_set):
    with open('data/discarded.json', 'w', encoding='utf-8') as out:
        json.dump(sorted(discarded_set), out, indent=2)


app = Flask(__name__)

dbconn = sqlite3.connect('data/tacred_align.sqlite')
cursor = dbconn.cursor()

record_dict = {
    record_id : {
        'en': en,
        'ru_original': ru,
        'ru_modified': ru_mod,
        'ko_original': ko,
        'ko_modified': ko_mod
    }
    for record_id, en, ru, ru_mod, ko, ko_mod 
    in cursor.execute("SELECT * FROM align")
}
record_ids = [el for el in record_dict]

relation_dict = {
    record_id : get_relation(blocks['en']) 
    for record_id, blocks in record_dict.items()
}

with open('requirements.json', 'r', encoding='utf-8') as inp:
    requirements = json.load(inp)


def needed(record_id, language, processed, discarded):
    # Try discarding by id
    if record_id in processed['both'] or \
        record_id in processed[language] or \
        record_id in discarded:
        return False
    # Try discarding by relation
    relation = relation_dict[record_id]
    satisfied = get_satisfied(processed)
    if requirements[relation] == satisfied["both"][relation] or \
        requirements[relation] == satisfied[language][relation]:
        return False
    return True


def get_satisfied(processed):
    result = {
        'ko': { k: 0 for k in requirements },
        'ru': { k: 0 for k in requirements },
        'both': { k: 0 for k in requirements }
    }
    for key in ['ru', 'ko', 'both']:
        for record_id in processed[key]:
            relation = relation_dict[record_id]
            result[key][relation] += 1
    return result


@app.route('/stats', methods=['GET'])
def stats_handler():
    processed = get_processed()
    satisfied = get_satisfied(processed)
    relations = sorted(requirements, key=lambda rel: requirements[rel], reverse=True)
    buffer = []
    buffer.append('total')
    buffer.append(f'\tru: {sum(satisfied["ru"].values()) + sum(satisfied["both"].values())}')
    buffer.append(f'\tko: {sum(satisfied["ko"].values()) + sum(satisfied["both"].values())}')
    buffer.append(f'out of {sum(requirements.values())}\n')
    for rel in relations:
        buffer.append(rel)
        buffer.append(f'\tru: {satisfied["ru"][rel] + satisfied["both"][rel]}')
        buffer.append(f'\tko: {satisfied["ko"][rel] + satisfied["both"][rel]}')
        buffer.append(f'out of {requirements[rel]}\n')
    resp = make_response('\n'.join(buffer), 200)
    populate_headers_text(resp)
    return resp


@app.route('/<language>/byid/<record_id>', methods=['GET'])
def byid_handler(language, record_id):
    if language not in ['ru', 'ko']:
        response = make_response(f'Wrong language: {language}', 400)
        response.headers['Content-Type'] = 'text/plain'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    elif record_id not in record_dict:
        response = make_response(f'Wrong id: {record_id}', 400)
        response.headers['Content-Type'] = 'text/plain'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response

    # TODO: notify if a discarded record is requested
    processed = get_processed()
    done_in_other_lang = False
    other_lang = 'ru' if language == 'ko' else 'ko'
    if record_id in processed[other_lang]:
        done_in_other_lang = True
    response = make_response(jsonify({
        'id': record_id,
        'done_in_other_lang': done_in_other_lang,
        'source': record_dict[record_id]['en'],
        'target': record_dict[record_id]['ru_modified' if language == 'ru' else 'ko_modified']
    }), 200)
    populate_headers_json(response)
    return response


@app.route('/<language>/nextsentence', methods=['GET'])
def nextsentence_handler(language):
    if language not in ['ru', 'ko']:
        response = make_response(f'Wrong language: {language}', 400)
        response.headers['Content-Type'] = 'text/plain'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    discarded = get_discarded()
    processed = get_processed()
    if requirements == get_satisfied(processed):
        response = make_response(jsonify({'done': True}), 200)
        populate_headers_json(response)
        return response

    done_in_other_lang = False
    # First try to suggest sentences already aligned by the other party.
    other_lang = 'ru' if language == 'ko' else 'ko'
    if processed[other_lang]:
        next_id = processed[other_lang].pop()
        processed[other_lang].add(next_id)  # Delete only when the sentence is returned.
        done_in_other_lang = True
    # If this fails, suggest a random sentence.
    else:
        while True:
            next_id = choice(record_ids)
            if needed(next_id, language, processed, discarded):
                break
    
    response = make_response(jsonify({
        'id': next_id,
        'done_in_other_lang': done_in_other_lang,  # So that people do not discard lightly.
        'source': record_dict[next_id]['en'],
        'target': record_dict[next_id]['ru_modified' if language == 'ru' else 'ko_modified']
    }), 200)
    populate_headers_json(response)
    return response


@app.route('/<language>/discardsentence', methods=['POST'])
def discard_sentence_handler(language):
    if language not in ['ru', 'ko']:
        response = make_response(f'Wrong language: {language}', 400)
        populate_headers_text(response)
        return response
    data = json.loads(request.data)
    discarded = get_discarded()
    processed = get_processed()
    discarded.add(data['id'])
    for k in processed:
        processed[k].discard(data['id'])
    dump_discarded(discarded)
    dump_processed(processed)
    response = make_response('Update successful', 200)
    populate_headers_text(response)
    return response


@app.route('/<language>/updatesentence', methods=['POST'])
def update_sentence_handler(language):
    if language not in ['ru', 'ko']:
        response = make_response(f'Wrong language: {language}', 400)
        populate_headers_text(response)
        return response

    data = json.loads(request.data)
    processed = get_processed()
    record_id = data['id']
    target_block = data['conllu']

    # Update in the dict
    record_dict[record_id]['ru_modified' if language == 'ru' else 'ko_modified'] = target_block
    
    # Update in the db
    cursor.execute(
        f"""
        UPDATE align SET `{"ru_modified" if language == "ru" else "ko_modified"}` = ?
        WHERE `id` = ?""",
        (target_block, record_id))
    dbconn.commit()
    
    # Update stats
    other_lang = 'ru' if language == 'ko' else 'ko'
    if record_id in processed[other_lang]:
        processed['both'].add(record_id)
        processed[other_lang].remove(record_id)
    else:
        processed[language].add(record_id)
    dump_processed(processed)
    
    response = make_response('Update successful', 200)
    populate_headers_text(response)
    return response