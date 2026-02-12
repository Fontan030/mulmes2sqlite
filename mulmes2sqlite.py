import argparse
import os
import time

from vkhtml_parser import VKhtmlParser
from tgjson_parser import TGjsonParser
from db_handler import DBHandler

def scan_input_path(parser):
    data_entries_list = parser.create_data_entries()
    return data_entries_list

def ask_user_before_parsing(data_entries_list):
    chats_total = sum(d['chat_count'] for d in data_entries_list)
    print(f'Found {chats_total} chats:')
    for i, data_entry in enumerate(data_entries_list):
        print(f"[{i+1}] {data_entry['name']}")
    print("Type 'a' to import all chats (default), 's' to select the desired chats, 'q' to quit")
    while True:
        user_answer = input('> ')
        if not user_answer or user_answer == 'a':
            parse_chats(data_entries_list, data_parser)
            break
        elif user_answer == 's':
            select_chats(data_entries_list)
            break
        elif user_answer == 'q':
            quit()
        else:
            print(f'Unknown command: {user_answer}')

def parse_chats(data_entries_list, data_parser):
    data_src = selected_parser[:2]
    start_time = time.time()
    for i, data_entry in enumerate(data_entries_list):
        data_name = data_entry['name']
        print(f'[{i+1}/{len(data_entries_list)}] Processing {data_name}')
        parser_output = data_parser.process_data_entry(data_entry)
        for chat_obj in parser_output:
            dbhandler.insert_chat_to_db(chat_obj, data_src)

    elapsed_time = round(time.time() - start_time, 3)
    print(f'Inserted {dbhandler.msg_counter} messages in {elapsed_time}s')
    
    print('Processing user IDs and chat IDs...')
    dbhandler.insert_users_to_db(data_parser.usernames_dict, data_src)
    dbhandler.update_ids_in_db()
    elapsed_time = round(time.time() - start_time, 3)
    print(f'Done! Total time spent: {elapsed_time}s')

def select_chats(data_entries_list):
    new_data_entries_list = []
    print('Enter the chats to import (example: 1,3,5)')
    user_input = input('> ')
    if ',' in user_input:
        selected_indexes = list( map(int, user_input.split(',')) )
    elif user_input.isnumeric():
        selected_indexes = [ int(user_input) ]
    else:
        print('Error parsing user input')
        quit()

    for i in selected_indexes:
        new_data_entries_list.append(data_entries_list[i-1])
    parse_chats(new_data_entries_list, data_parser)

def fmt_size(num, suffix='B'):
    for unit in ('', 'Ki', 'Mi', 'Gi'):
        if abs(num) < 1024.0:
            return f'{num:3.1f} {unit}{suffix}'
        num /= 1024.0
    return f'{num:.1f} Ti{suffix}'

def main():
    global data_parser, selected_parser, dbhandler
    argparser = argparse.ArgumentParser(
        prog='mulmes2sqlite',
        description='Merge chats from multiple messengers into a single SQLite database')
    argparser.add_argument('-p', '--parser', help='selected data parser (vkhtml, tgjson)')
    argparser.add_argument('--bs4-backend', default='html.parser', help='BeautifulSoup4 backend')
    argparser.add_argument('-j', help='CPU count (if multiprocessing is available)')
    argparser.add_argument('-i', '--input', help='input directory')
    argparser.add_argument('db_file', help="SQLite database file (will be created if it doesn't exist)")
    args = argparser.parse_args()

    selected_parser = args.parser
    input_path = args.input
    db_file = args.db_file
    dbhandler = DBHandler(db_file)

    if not selected_parser:
        print('No parser selected')
    elif selected_parser == 'vkhtml':
        bs4_backend = args.bs4_backend
        proc_count = int(args.j) if args.j else os.cpu_count() // 2
        data_parser = VKhtmlParser(input_path, bs4_backend, proc_count)
    elif selected_parser == 'tgjson':
        data_parser = TGjsonParser(input_path)
    else:
        print(f'Error: unknown parser {selected_parser}')

    if 'data_parser' in globals():
        data_entries_list = scan_input_path(data_parser)
        if data_entries_list:
            ask_user_before_parsing(data_entries_list)

if __name__ == "__main__":
    main()