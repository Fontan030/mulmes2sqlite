import argparse
import os
import sys
import time

from vkhtml_parser import VKhtmlParser
from tgjson_parser import TGjsonParser
from db_handler import DBHandler

ui_txt = {
    'prog':    "mulmes2sqlite",
    'desc':    "Merge chats from multiple messengers into a single SQLite database",
    '-p':      "selected data parser (vkhtml, tgjson)",
    'bs4b':    "BeautifulSoup4 backend (html.parser or lxml)",
    '-j':      "CPU count (if multiprocessing is available)",
    '-i':      "input directory or ZIP file",
    'db_help': "SQLite database file (will be created if it doesn't exist)",
    'no_pars': "No parser selected",
    'no_inp':  "No valid input found",
    'commands_list':
"Type 'a' to import all chats (default), 's' to select the desired chats,\n'q' to quit",
    'select_chats': "Enter the chats to import (example: 1,3,5)",
    }

class Mulmes2sqliteCLI:
    def __init__(self):
        argparser = argparse.ArgumentParser(prog=ui_txt['prog'], description=ui_txt['desc'])
        argparser.add_argument('-p', '--parser', help = ui_txt['-p'] )
        argparser.add_argument('--bs4-backend', default='html.parser', help = ui_txt['bs4b'] )
        argparser.add_argument('-j', help = ui_txt['-j'] )
        argparser.add_argument('-i', '--input', help = ui_txt['-i'] )
        argparser.add_argument('db_file', help = ui_txt['db_help'] )
        args = argparser.parse_args()

        self.selected_parser = args.parser
        self.db_file = args.db_file
        self.data_parser = None
        input_path = args.input

        if not self.selected_parser:
            print(ui_txt['no_pars'])
        elif self.selected_parser == 'vkhtml':
            bs4_backend = args.bs4_backend
            proc_count = int(args.j) if args.j else os.cpu_count() // 2
            self.data_parser = VKhtmlParser(input_path, bs4_backend, proc_count)
        elif self.selected_parser == 'tgjson':
            self.data_parser = TGjsonParser(input_path)
        else:
            print(f'Error: unknown parser {self.selected_parser}')

        if self.data_parser:
            self.scan_input_path()

    def scan_input_path(self):
        data_entries_list = self.data_parser.create_data_entries()
        if data_entries_list:
            self.ask_user_before_parsing(data_entries_list)
        else:
            print(ui_txt['no_inp'])

    def ask_user_before_parsing(self, data_entries_list: list):
        chats_total = sum(d['chat_count'] for d in data_entries_list)
        print(f'Found {chats_total} chats:')
        for i, data_entry in enumerate(data_entries_list):
            print(f"[{i+1}] {data_entry['name']}")
        print(ui_txt['commands_list'])
        while True:
            user_answer = input('> ')
            if not user_answer or user_answer == 'a':
                self.parse_chats(data_entries_list)
                break
            if user_answer == 's':
                self.select_chats(data_entries_list)
                break
            if user_answer == 'q':
                sys.exit()
            else:
                print(f'Unknown command: {user_answer}')

    def parse_chats(self, data_entries_list: list):
        dbhandler = DBHandler(self.db_file)
        data_src = self.selected_parser[:2]
        start_time = time.time()
        for i, data_entry in enumerate(data_entries_list):
            data_name = data_entry['name']
            print(f'[{i+1}/{len(data_entries_list)}] Processing {data_name}')
            parser_output = self.data_parser.process_data_entry(data_entry)
            for chat_obj in parser_output:
                dbhandler.insert_chat_to_db(chat_obj, data_src)

        elapsed_time = round(time.time() - start_time, 3)
        print(f'Inserted {dbhandler.msg_counter} messages in {elapsed_time}s')

        print('Processing user IDs and chat IDs...')
        dbhandler.insert_users_to_db(self.data_parser.usernames_dict, data_src)
        dbhandler.update_ids_in_db()
        elapsed_time = round(time.time() - start_time, 3)
        print(f'Done! Total time spent: {elapsed_time}s')

    def select_chats(self, data_entries_list: list):
        new_data_entries_list = []
        print(ui_txt['select_chats'])
        user_input = input('> ')
        if ',' in user_input:
            selected_indexes = list( map(int, user_input.split(',')) )
        elif user_input.isnumeric():
            selected_indexes = [ int(user_input) ]
        else:
            print('Error parsing user input')
            sys.exit()

        for i in selected_indexes:
            new_data_entries_list.append(data_entries_list[i-1])
        self.parse_chats(new_data_entries_list)  

if __name__ == "__main__":
    cli_interface = Mulmes2sqliteCLI()
