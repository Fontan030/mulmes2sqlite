import datetime
import json
import os
from base64 import b64decode
from bs4 import BeautifulSoup
from tqdm import tqdm

from input_handler import InputHandler

try:
    import multiprocessing
    MP_ENABLED = True
except:
    MP_ENABLED = False

class VKhtmlParser:
    def __init__(self, input_path: str, bs4_backend: str, proc_count: int):
        vk_encoding, target_ext = 'cp1251', '.html'
        self.inp = InputHandler(input_path, vk_encoding, target_ext)
        self.bs4_backend = bs4_backend
        self.proc_count = proc_count if MP_ENABLED else 1
        print(f'VKhtmlParser backend: {bs4_backend}, process count: {self.proc_count}')
        self.own_user_id, self.own_username = 0, ''
        self.usernames_dict: dict[int, str] = {}
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
        self.srv_actions_dict = {
            'создал': 'create_group',
            'пригласил': 'invite_members',
            'по ссылке': 'join_group_by_link', # присоединилась / присоединился
            'исключил': 'remove_members',
            'из чата': 'leave_chat',    # вышла / вышел
            'вернул': 'return_to_chat', # вернулась / вернулся
            'закрепил': 'pin_message',
            'открепил': 'unpin_message',
            'обновил': 'edit_group_photo',  # фотографию чата
            'удалил': 'delete_group_photo', # фотографию чата
            'название': 'edit_group_title', # изменил(а) название чата
            'оформление': 'edit_chat_theme', # изменил(а) оформление чата
            'скриншот': 'take_screenshot' # сделал(а) скриншот чата
            }

    def create_data_entries(self) -> list:
        data_entries_list = []
        target_filename = 'messages0.html'
        full_file_list = self.inp.get_file_list()
        files_to_scan = [f for f in full_file_list if f.endswith(target_filename)]
        for filename in files_to_scan:
            try:
                dir_path = os.path.dirname(filename)
                files_in_same_dir = []
                for f in full_file_list:
                    if os.path.dirname(f) == dir_path:
                        files_in_same_dir.append(f)
                raw_html = self.inp.get_file(filename)
                soup = BeautifulSoup(raw_html, self.bs4_backend)
                ui_crumb_div = soup.find('div', class_='ui_crumb')
                if ui_crumb_div:
                    chat_name = ui_crumb_div.text
                if not self.own_user_id:
                    self.parse_own_id(soup)
                    self.parse_own_username(full_file_list)
                    print(f'Your ID and username: {self.own_user_id}, {self.own_username}')
                data_entry = {
                    'chat_count': 1,
                    'name': chat_name,
                    'path': dir_path,
                    'files': files_in_same_dir}
                data_entries_list.append(data_entry)
            except Exception as e:
                print(f'Skipping file {filename}: {e}')
        return data_entries_list

    def process_data_entry(self, data_entry: dict) -> list:
        chat_users = {}
        data_path = data_entry['path']
        chat_id = int( os.path.basename(data_path) )
        peer_type = self.get_peer_type(chat_id)
        html_list = data_entry['files']
        msg_list = []
        if self.proc_count != 1:
            with multiprocessing.Pool(self.proc_count) as pool:
                p = pool.imap(self.process_single_html, html_list, chunksize=8)
                for message_chunk, users_subset in tqdm(p, total=len(html_list)):
                    msg_list.extend(message_chunk)
                    chat_users.update(users_subset)
        else:
            for file in tqdm(html_list):
                message_chunk, users_subset = self.process_single_html(file)
                msg_list.extend(message_chunk)
                chat_users.update(users_subset)
        self.usernames_dict.update(chat_users)
        chat_obj = {
            'id': chat_id,
            'peer_type': peer_type,
            'name': data_entry['name'],
            'msg_list': msg_list}
        return [ chat_obj ]

    def process_single_html(self, html_path: str):
        chat_id = os.path.basename(os.path.dirname(html_path))
        msg_list = []
        users_subset = {}
        raw_html = self.inp.get_file(html_path)
        soup = BeautifulSoup(raw_html, self.bs4_backend)

        for msg_div in soup.find_all('div', class_='message'):
            is_service_msg, service_msg_data = 0, None
            msg_id = msg_div.get('data-id')
            header = msg_div.find('div', class_='message__header')
            if header:
                user_id, username = self.parse_msg_header(header)
                users_subset[user_id] = username
                date_str = header.text.split(', ')[1]
                date, edited = self.parse_date(date_str)
                if edited:
                    edited_span = header.find('span', class_='message-edited')
                    edited = self.parse_date(edited_span.get('title'))[0]
            else:
                continue
            kludges_div = msg_div.find('div', class_='kludges')
            if kludges_div:
                attachments_raw = kludges_div.find_all('div', class_='attachment')
                attachments, fwd_messages = self.parse_attachments(attachments_raw)
                if attachments_raw:
                    for att_div in attachments_raw:
                        att_div.decompose()
                service_msg_div = kludges_div.find('a', class_='im_srv_lnk')
                if service_msg_div:
                    is_service_msg = 1
                    msg_text, service_msg_data = self.parse_service_msg(kludges_div)
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
                'service_msg_data': service_msg_data,
                'edited': edited,
                'has_formatting': 0,
                'data_src': 1}
            msg_list.append(processed_msg)

        return msg_list, users_subset

    def parse_msg_header(self, header_div):
        user_link = header_div.find('a')
        if not user_link:
            user_id, username = self.own_user_id, self.own_username
        else:
            username = user_link.text
            vk_url = user_link.get('href')
            user_id = self.extract_uid_from_url(vk_url)
        return user_id, username

    def extract_uid_from_url(self, vk_url: str) -> int:
        user_id_str = vk_url.split('/')[-1]
        id_digits = ''.join(filter(str.isdigit, user_id_str))
        if user_id_str[:2] == 'id':
            user_id = int(id_digits)
        else: #if UID prefix is 'club' or 'public'
            user_id = int(id_digits) * -1
        return user_id

    def parse_date(self, date_str: str):
        edited = 0
        try:
            date_part, time_part = date_str.strip().split(' в ')
            day, month_name, year = date_part.split()
            month = self.months_dict.get(month_name.lower())
            if not month:
                return 0, 0
            if 'ред.' in time_part:
                edited = 1
                time_part = time_part.split()[0]
            hour, minute, sec = time_part.split(':')
            datetime_args = list(map(int, (year, month, day, hour, minute, sec)))
            unix_timestamp = int(datetime.datetime(*datetime_args).timestamp())
            return unix_timestamp, edited
        except Exception as e:
            print(f'Error parsing date {date_str}: {e}')
            return 0, 0

    def parse_attachments(self, attachments_raw):
        attachments_list = []
        fwd_messages = None
        for a in attachments_raw:
            att = {}
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
            self.own_user_id = decoded_json['user_id']
        except:
            self.own_user_id = 1

    def parse_own_username(self, full_file_list: list):
        own_name_placeholder = 'Вы'
        pg_info_filename = 'page-info.html'
        try:
            files_to_scan = [f for f in full_file_list if f.endswith(pg_info_filename)]
            if files_to_scan:
                raw_html = self.inp.get_file(files_to_scan[0])
                soup = BeautifulSoup(raw_html, self.bs4_backend)
                search_str = 'Полное имя'
                full_name_div = soup.find('div', class_='item__tertiary', string=search_str)
                parent_div = full_name_div.find_parent('div')
                username_div = parent_div.select('div > div')[1]
                own_username_raw = username_div.get_text(strip=True)
            # remove double space '  '
            self.own_username = ' '.join(own_username_raw.split())
        except:
            self.own_username = own_name_placeholder

    def get_peer_type(self, chat_id: int) -> str:
        if chat_id >= 0:
            peer_type = 'user' if chat_id < 2000000000 else 'group_chat'
            if chat_id == 100:
                peer_type = 'service'
        else:
            peer_type = 'bot'
        return peer_type

    def parse_service_msg(self, kludges_div):
        action_text, service_msg_data = '', None
        links_list = kludges_div.select('div > a')
        action_text_raw = links_list[0].next_element.next_element
        for k, v in self.srv_actions_dict.items():
            if k in action_text_raw:
                action_text = v
                break
        if not action_text:
            action_text, service_msg_data = 'unknown', action_text_raw
        if len(links_list) > 1:
            try:
                username = links_list[1].text
                vk_url = links_list[1].get('href')
                user_id = self.extract_uid_from_url(vk_url)
                service_msg_data = {'user_id': user_id, 'username': username}
            except:
                pass
        bold_txt_list = kludges_div.select('div > b')
        if bold_txt_list:
            chat_title = bold_txt_list[-1].text
            service_msg_data = {'title': chat_title}
        return action_text, service_msg_data
