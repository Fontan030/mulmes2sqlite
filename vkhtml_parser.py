import datetime
import glob
import json
import logging
import os
from base64 import b64decode
from bs4 import BeautifulSoup
from tqdm import tqdm

try:
    import multiprocessing
    mp_enabled = True
except:
    mp_enabled = False

class VKhtmlParser:
    def __init__(self, bs4_backend, proc_count):
        self.default_filename = 'messages0.html'
        self.vk_encoding = 'cp1251'
        self.bs4_backend = bs4_backend
        self.proc_count = proc_count if mp_enabled else 1
        print(f'VKhtmlParser backend: {bs4_backend}, process count: {self.proc_count}')
        self.own_user_id = 0
        self.usernames_dict = dict()
        self.months_dict = {
            'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04',
            'мая': '05', 'июн': '06', 'июл': '07', 'авг': '08',
            'сен': '09', 'окт': '10', 'ноя': '11', 'дек': '12' }
        self.attachment_types = {
            'photo': 'Фотография',
            'video': 'Видеозапись',
            'audio': 'Аудиозапись',
            'sticker': 'Стикер',
            'file': 'Файл',
            #'voice_message': 'Голосовое сообщение',
            'wall_post': 'Запись на стене',
            'wall_comment': 'Комментарий на стене',
            'link': 'Ссылка',
            'article': 'Статья',
            'map': 'Карта',
            'poll': 'Опрос',
            'gift': 'Подарок',
            'story': 'История',
            'playlist': 'Плейлист',
            'photo_album': 'Альбом фотографий',
            'phone_call': 'Звонок',
            'market_item': 'Товар',
            'money_transfer': 'Денежный перевод',
            'money_request': 'Запрос на денежный перевод',
            'deleted_msg': 'Сообщение удалено'}
        self.attachment_types_inv = {v: k for k, v in self.attachment_types.items()}
        self.read_bytes_count = 0

    def read_htm_file(self, filepath):
        try:
            with open(filepath, 'r', encoding=self.vk_encoding) as f:
                return f.read()
        except Exception as e:
            logging.error(f'Error reading file: {e}')

    def create_data_entry(self, filename):
        dir_path = os.path.dirname(filename)
        html_content = self.read_htm_file(filename)
        soup = BeautifulSoup(html_content, self.bs4_backend)
        ui_crumb_div = soup.find('div', class_='ui_crumb')
        if ui_crumb_div:
            chat_name = ui_crumb_div.text
        if not self.own_user_id:
            self.own_user_id = self.parse_own_id(soup)
            self.own_username = self.parse_own_username()
            print(f'Your ID and username: {self.own_user_id}, {self.own_username}')
        data_entry = {'chat_count': 1, 'name': chat_name, 'path': dir_path}
        return data_entry

    def process_data_entry(self, data_entry):
        chat_users = dict()
        data_path = data_entry['path']
        chat_id = os.path.basename(data_path)
        html_list = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith('.html')]
        msg_list = []
        if self.proc_count != 1:
            with multiprocessing.Pool(self.proc_count) as pool:
                p = pool.imap(self.process_single_html, html_list, chunksize=8)
                for message_chunk, users_subset, r_b_count in tqdm(p, total=len(html_list)):
                    msg_list.extend(message_chunk)
                    chat_users.update(users_subset)
                    self.read_bytes_count += r_b_count
        else:
            for file in tqdm(html_list):
                message_chunk, users_subset = self.process_single_html(file)
                msg_list.extend(message_chunk)
                chat_users.update(users_subset)
        self.usernames_dict.update(chat_users)
        chat_obj = {
            'id': chat_id,
            'name': data_entry['name'],
            'msg_list': msg_list}
        return [ chat_obj ]

    def process_single_html(self, html_path):
        chat_id = os.path.basename(os.path.dirname(html_path))
        msg_list = []
        users_subset = dict()
        html_content = self.read_htm_file(html_path)
        soup = BeautifulSoup(html_content, self.bs4_backend)
        r_b_count = os.path.getsize(html_path)

        for msg_div in soup.find_all('div', class_='message'):
            is_service_msg = 0
            msg_id = msg_div.get('data-id')
            header = msg_div.find('div', class_='message__header')
            if header:
                user_id, username = self.parse_user(header)
                users_subset[user_id] = username
                date_str = header.text.split(', ')[1]
                date, edited = self.parse_date(date_str)
                if edited:
                    edited_span = header.find('span', class_='message-edited')
                    edited = self.parse_date(edited_span.get('title'))[0]
            kludges_div = msg_div.find('div', class_='kludges')
            if kludges_div:
                attachments_raw = kludges_div.find_all('div', class_='attachment')
                attachments, fwd_messages = self.parse_attachments(attachments_raw)
                service_msg_div = kludges_div.find('a', class_='im_srv_lnk')
                if service_msg_div:
                    is_service_msg = 1
                    msg_text = kludges_div.text
                kludges_div.decompose()
            else:
                attachments, fwd_messages = None, None
            msg_body = msg_div.select('div > div')[1]
            if msg_body and is_service_msg == 0:
                msg_text = msg_body.get_text(separator='\n', strip=True)
            processed_msg = {
                'msg_id_orig': msg_id,
                'chat_id_orig': chat_id,
                'from_id_orig': user_id,
                'date': date,
                'text': msg_text,
                'attachments': attachments,
                'fwd_messages': fwd_messages,
                'is_service_msg': is_service_msg,
                'edited': edited,
                'has_formatting': 0,
                'data_src': 1}
            msg_list.append(processed_msg)

        return msg_list, users_subset, r_b_count

    def parse_user(self, header_div):
        user_link = header_div.find('a')
        if not user_link:
            user_id, username = self.own_user_id, self.own_username
        else:
            username = user_link.text
            user_id_str = user_link.get('href').split('/')[-1]
            id_digits = ''.join(filter(str.isdigit, user_id_str))
            if user_id_str[:2] == 'id':
                user_id = int(id_digits)
            else: #if UID prefix is 'club' or 'public'
                user_id = int(id_digits) * -1

        return user_id, username

    def parse_date(self, date_str):
        edited = 0
        try:
            parts = date_str.strip().split(' в ')
            if len(parts) != 2:
                return 0, 0
            date_part, time_part = parts
            day, month_name, year = date_part.split()
            month = self.months_dict.get(month_name.lower())
            if not month:
                return 0, 0
            if 'ред.' in time_part:
                edited = 1
                time_part = time_part.split()[0]
            hour, min, sec = time_part.split(':')
            datetime_args = list(map(int, (year, month, day, hour, min, sec)))
            unix_timestamp = int(datetime.datetime(*datetime_args).timestamp())
            return unix_timestamp, edited
        except Exception as e:
            logging.error(f'Error parsing date {date_str}: {e}')
            return 0, 0

    def parse_attachments(self, attachments_raw):
        attachments_list = []
        fwd_messages = None
        for a in attachments_raw:
            att = dict()
            bs4_desc = a.find('div', class_='attachment__description').get_text()
            if 'прикреп' in bs4_desc:
                fwd_msg_count = int( bs4_desc.split()[0] )
                fwd_messages = [ {'count': fwd_msg_count} ]
                # fwd_messages would be a list if it was possible to parse them
                continue
            att['type'] = self.attachment_types_inv.get(bs4_desc)
            bs4_link = a.find('a', class_='attachment__link')
            if bs4_link:
                att['url'] = bs4_link.get('href')
            if att['type'] == 'file':
                if '.ogg' in att.get('url'): # distinguish files and voice messages
                    att['type'] = 'voice_message'
            elif not att['type']:
                att['type'] = 'unknown'
                att['misc'] = bs4_desc
            attachments_list.append(att)
        if attachments_list:
            attachments_json = json.dumps(attachments_list, ensure_ascii=False)
        else:
            attachments_json = None
        return attachments_json, fwd_messages

    def parse_own_id(self, soup):
        try:
            page_meta = soup.find('meta', {'name': 'jd'})
            base64_json = page_meta.get('content')
            while len(base64_json) % 4 != 0:
                base64_json += '=' # padding for b64decode
            decoded_json = json.loads( b64decode(base64_json) )
            own_user_id = decoded_json['user_id']
        except:
            own_user_id = 1
        return own_user_id

    def parse_own_username(self):
        own_name_placeholder = 'Вы'
        try:
            files_to_scan = glob.glob(f'{self.working_dir}/**/page-info.html', recursive=True)
            if files_to_scan:
                html_content = self.read_htm_file(files_to_scan[0])
                soup = BeautifulSoup(html_content, self.bs4_backend)
                search_str = 'Полное имя'
                full_name_div = soup.find('div', class_='item__tertiary', string=search_str)
                parent_div = full_name_div.find_parent('div')
                username_div = parent_div.select('div > div')[1]
                own_username_raw = username_div.get_text(strip=True)
            # remove double space '  '
            own_username = ' '.join(own_username_raw.split())
        except:
            own_username = own_name_placeholder
        return own_username