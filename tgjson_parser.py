import json

from input_handler import InputHandler

class TGjsonParser:
    def __init__(self, input_path: str):
        tg_encoding, target_ext = 'utf-8', '.json'
        self.inp = InputHandler(input_path, tg_encoding, target_ext)
        self.usernames_dict: dict[int, str] = {}
        self.attachment_attrs = [ 'file_name', 'file_size', 'width', 'height', 'duration_seconds' ]
        self.not_included_strs = [
            "(File not included. Change data exporting settings to download.)",
            "(File exceeds maximum size. Change data exporting settings to download.)",
            "(File unavailable, please try again later)"
            ]
        self.peer_types = {
            'verification_codes': 'service',
            'personal_chat': 'user',
            'bot_chat': 'bot',
            'private_group': 'group_chat',
            'private_supergroup': 'group_chat',
            'public_supergroup': 'group_chat',
            'private_channel': 'channel',
            'public_channel': 'channel'
            }
        self.plain_txt_types = ('plain', 'hashtag', 'custom_emoji','bot_command', 'phone')
        # for converting Telegram text formatting into HTML tags
        self.formatted_txt_types = {
            'bold': 'b',
            'italic': 'i',
            'spoiler': 'details',
            'strikethrough': 's',
            'blockquote': 'blockquote',
            'code': 'tt',
            'pre': 'tt'
            }

    def create_data_entries(self) -> list:
        data_entries_list = []
        target_filename = 'result.json'
        full_file_list = self.inp.get_file_list()
        files_to_scan = [f for f in full_file_list if f.endswith(target_filename)]
        for filename in files_to_scan:
            try:
                raw_json = self.inp.get_file(filename)
                json_data = json.loads(raw_json)
                if 'chats' in json_data:
                    chat_count = len(json_data['chats']['list'])
                    name_str = f'Full data export ({chat_count} chats)'
                elif 'messages' in json_data:
                    chat_count, name_str = 1, json_data['name']
                else:
                    break
                data_entry = {
                    'chat_count': chat_count,
                    'name': name_str,
                    'path': filename}
                data_entries_list.append(data_entry)
            except Exception as e:
                print(f'Skipping file {filename}: {e}')
        return data_entries_list

    def process_data_entry(self, data_entry: dict) -> list:
        output_chat_list = []
        raw_json = self.inp.get_file( data_entry['path'] )
        json_data = json.loads(raw_json)
        if data_entry['chat_count'] == 1:
            chat_obj = self.process_single_chat(json_data)
            output_chat_list.append(chat_obj)
        else:
            for chat in json_data['chats']['list']:
                chat_obj = self.process_single_chat(chat)
                output_chat_list.append(chat_obj)
        return output_chat_list

    def process_single_chat(self, json_chat: dict) -> dict:
        msg_list = []
        chat_id = json_chat['id']
        tg_chat_type = json_chat['type']
        if tg_chat_type in self.peer_types:
            peer_type = self.peer_types[tg_chat_type]
        else:
            peer_type = tg_chat_type
        chat_name = json_chat['name'] if json_chat.get('name') else 'DELETED'

        for msg in json_chat['messages']:
            processed_msg = self.process_single_message(msg, chat_id)
            if processed_msg:
                msg_list.append(processed_msg)
        chat_obj = {
            'id': chat_id,
            'peer_type': peer_type,
            'name': chat_name,
            'msg_list': msg_list}
        return chat_obj

    def process_single_message(self, msg: dict, chat_id: int) -> dict:
        if msg['type'] == 'message':
            is_service_msg, service_msg_data = 0, None
            user_id = self.parse_user(msg['from_id'], msg['from'])
            msg_text, has_formatting = self.parse_msg_text(msg)
            reply_to_id = msg.get('reply_to_message_id')
        elif msg['type'] == 'service':
            is_service_msg, has_formatting = 1, 0
            user_id = self.parse_user(msg['actor_id'], msg['actor'])
            msg_text, service_msg_data = self.parse_service_msg(msg)
            reply_to_id = msg.get('message_id')
        else:
            return {}
        date, edited = self.parse_date(msg)
        attachments = self.parse_attachments(msg)
        fwd_from_id = self.parse_fwd_from_id(msg)
        processed_msg = {
            'msg_id_orig': msg['id'],
            'chat_id_orig': chat_id,
            'from_id_orig': user_id,
            'date': date,
            'is_service_msg': is_service_msg,
            'service_msg_data': service_msg_data,
            'edited': edited,
            'has_formatting': has_formatting,
            'reply_to_id_orig': reply_to_id,
            'data_src': 2}
        if not fwd_from_id:
            processed_msg['text'] = msg_text
            processed_msg['attachments'] = attachments
        else:
            processed_msg['text'] = ''
            processed_msg['fwd_messages'] = [ {
                'from_id_orig': fwd_from_id,
                'text': msg_text,
                'attachments': attachments} ]
        return processed_msg

    def parse_user(self, user_id_str: str, username: str) -> int:
        if not username:
            username = 'DELETED'
        id_digits = ''.join(filter(str.isdigit, user_id_str))
        if user_id_str[:4] == 'user':
            user_id = int(id_digits)
        elif user_id_str[:4] == 'chan': # 'channel'
            user_id = int(id_digits) * -1
        else:
            print(f'Error: unknown user prefix {user_id_str}')
            return 0
        self.usernames_dict[user_id] = username
        return user_id

    def parse_date(self, msg: dict):
        date = msg.get('date_unixtime')
        edited = msg['edited_unixtime'] if msg.get('edited_unixtime') else 0
        return date, edited

    def parse_msg_text(self, msg: dict):
        msg_text, has_formatting = '', 0
        for e in msg.get('text_entities'):
            txt, etype = e['text'], e['type']
            if etype in self.plain_txt_types:
                msg_text += txt
            else:
                has_formatting = 1
                if etype in self.formatted_txt_types:
                    html_tag = self.formatted_txt_types[etype]
                    msg_text += f'<{html_tag}>{txt}</{html_tag}>'
                elif etype == 'link':
                    msg_text += f'<a href="{txt}">{txt}</a>'
                elif etype == 'text_link':
                    href = e['href']
                    msg_text += f'<a href="{href}">{txt}</a>'
                elif etype == 'mention':
                    href = f'https://t.me/{txt[1:]}'
                    msg_text += f'<a href="{href}">{txt}</a>'
                else:
                    msg_text += txt
                    print(f'Unknown formatting type {etype}')
        if not msg_text and msg.get('sticker_emoji'):
            msg_text = msg['sticker_emoji']
        return msg_text, has_formatting

    def parse_attachments(self, msg: dict):
        attachments = {}
        if msg.get('media_type'):
            attachments['type'] = msg.get('media_type')
            attachments['local_path'] = msg.get('file')
        elif msg.get('photo'):
            attachments['type'] = 'photo'
            attachments['local_path'] = msg.get('photo')
            attachments['file_size'] = msg.get('photo_file_size')
        elif msg.get('file'):
            attachments['type'] = 'file'
            attachments['local_path'] = msg.get('file')
        elif msg.get('poll'):
            attachments['type'] = 'poll'
            attachments['data'] = msg.get('poll')

        if attachments.get('local_path') in self.not_included_strs:
            attachments['local_path'] = 'not_included'
        if attachments.get('type'):
            if '_file' in attachments['type']:
                attachments['type'] = attachments['type'].replace('_file', '')
                # replace 'audio_file' with 'audio' and 'video_file' with 'video'
                # to keep unified type names in tgjson and vkhtml parsers

        for attr in self.attachment_attrs:
            if msg.get(attr):
                attachments[attr] = msg[attr]

        if attachments:
            return [ attachments ]
        else:
            return None

    def parse_fwd_from_id(self, msg: dict) -> int:
        if msg.get('forwarded_from_id'):
            fwd_from_id = self.parse_user(msg['forwarded_from_id'], msg['forwarded_from'])
        else:
            fwd_from_id = 0
        return fwd_from_id

    def parse_service_msg(self, msg: dict):
        msg_data = {}
        action_text = msg.get('action')
        if msg.get('title'):
            msg_data['title'] = msg['title']
        if msg.get('members'):
            members_list = [ {'username': u} for u in msg['members'] ]
            if len(members_list) == 1 and not msg_data.get('title'):
                msg_data = members_list[0]
            else:
                msg_data['members'] = members_list # for create_group action
            if action_text == 'remove_members':
                if members_list[0]['username'] == msg.get('actor'):
                    action_text, msg_data = 'leave_chat', {}

        service_msg_data = msg_data if msg_data else None
        return action_text, service_msg_data
