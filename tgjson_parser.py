import json
import os
import logging

class TGjsonParser:
    def __init__(self):
        self.usernames_dict = dict()
        self.default_filename = 'result.json'
        self.plain_txt_types = ('plain', 'hashtag', 'custom_emoji','bot_command', 'phone')
        self.attachment_attrs = [ 'file_name', 'file_size', 'width', 'height', 'duration_seconds' ]
        self.not_included_str = '(File not included. Change data exporting settings to download.)'
    
    def read_json_file(self, filepath):
        try:
            with open(filepath, 'r') as f:
                return json.loads(f.read())
        except Exception as e:
            logging.error(f'Error reading file: {e}')
    
    def create_data_entry(self, filename):
        dir_path = os.path.dirname(filename)
        json_data = self.read_json_file(filename)
        if 'chats' in json_data:
            chat_count = len(json_data['chats']['list'])
            name_str = f'Full data export ({chat_count} chats)'
            data_entry = {'chat_count': chat_count, 'name': name_str, 'path': filename}
        elif 'messages' in json_data:
            data_entry = {'chat_count': 1, 'name': json_data['name'], 'path': filename}
        return data_entry

    def process_data_entry(self, data_entry):
        output_chat_list = []
        json_data = self.read_json_file(data_entry['path'])
        if data_entry['chat_count'] == 1:
            chat_obj = self.process_single_chat(json_data)
            output_chat_list.append(chat_obj)
        else:
            for chat in json_data['chats']['list']:
                chat_obj = self.process_single_chat(chat)
                output_chat_list.append(chat_obj)
        return output_chat_list

    def process_single_chat(self, json_chat):
        msg_list = []
        chat_id = json_chat['id']
        chat_name = json_chat['name'] if json_chat.get('name') else 'DELETED'
        
        for msg in json_chat['messages']:
            if msg['type'] == 'message':
                is_service_msg = 0
                user_id = self.parse_user(msg['from_id'], msg['from'])
            elif msg['type'] == 'service':
                is_service_msg = 1
                user_id = self.parse_user(msg['actor_id'], msg['actor'])
            date, edited = self.parse_date(msg)
            msg_text, has_formatting = self.parse_msg_text(msg)
            attachments = self.parse_attachments(msg)
            fwd_from_id = self.parse_fwd_from_id(msg)
            processed_msg = {
                'msg_id_orig': msg['id'],
                'chat_id_orig': chat_id,
                'from_id_orig': user_id,
                'date': date,
                'is_service_msg': is_service_msg,
                'edited': edited,
                'has_formatting': has_formatting,
                'data_src': 2}
            if not fwd_from_id:
                processed_msg['text'] = msg_text
                processed_msg['attachments'] = attachments
            else:
                processed_msg['text'] = ''
                fwd_messages = [ {
                    'from_id_orig': fwd_from_id,
                    'text': msg_text,
                    'attachments': attachments} ]
                fwd_messages_json = json.dumps(fwd_messages, ensure_ascii=False)
                processed_msg['fwd_messages'] = fwd_messages_json
            msg_list.append(processed_msg)
        chat_obj = {
            'id': chat_id,
            'name': chat_name,
            'msg_list': msg_list}
        return chat_obj

    def parse_user(self, user_id_str, username):
        id_digits = ''.join(filter(str.isdigit, user_id_str))
        if user_id_str[:4] == 'user':
            user_id = int(id_digits)
        elif user_id_str[:4] == 'chan': # 'channel'
            user_id = int(id_digits) * -1
        else:
            print(f'Error: unknown user prefix {user_id_str}')
            return 0, ''
        self.usernames_dict[user_id] = username
        return user_id

    def parse_date(self, msg):
        edited = msg['edited_unixtime'] if msg.get('edited_unixtime') else 0
        date = msg.get('date_unixtime')
        return date, edited

    def parse_msg_text(self, msg):
        msg_text, has_formatting = '', 0
        for e in msg.get('text_entities'):
            txt, etype = e['text'], e['type']
            if etype in self.plain_txt_types:
                msg_text += txt
            else:
                has_formatting = 1
                if etype == 'bold':
                    msg_text += f'<b>{txt}</b>'
                elif etype == 'italic':
                    msg_text += f'<i>{txt}</i>'
                elif etype == 'spoiler':
                    msg_text += f'<details>{txt}</details>'
                elif etype == 'strikethrough':
                    msg_text += f'<s>{txt}</s>'
                elif etype == 'blockquote':
                    msg_text += f'<blockquote>{txt}</blockquote>'
                elif etype == 'link':
                    msg_text += f'<a href="{txt}">{txt}</a>'
                elif etype == 'text_link':
                    href = e['href']
                    msg_text += f'<a href="{href}">{txt}</a>'
                elif etype == 'mention':
                    href = f'https://t.me/{txt[1:]}'
                    msg_text += f'<a href="{href}">{txt}</a>'
                elif etype in ('code', 'pre'):
                    msg_text += f'<tt>{txt}</tt>'
                else:
                    msg_text += txt
                    print(f'Unknown formatting type {etype}')
        if not msg_text and msg.get('sticker_emoji'):
            msg_text = msg['sticker_emoji']
        elif msg.get('type') == 'service':
            msg_text = msg.get('action') # temporary workaround
        return msg_text, has_formatting

    def parse_attachments(self, msg):
        attachments = dict()
        if msg.get('media_type'):
            attachments['type'] = msg['media_type']
            attachments['local_path'] = msg['file']
        elif msg.get('photo'):
            attachments['type'] = 'photo'
            attachments['local_path'] = msg['photo']
            attachments['file_size'] = msg['photo_file_size']
        elif msg.get('file'):
            attachments['type'] = 'file'
            attachments['local_path'] = msg['file']
        elif msg.get('poll'):
            attachments['type'] = 'poll'
            attachments['data'] = msg['poll']

        if attachments.get('local_path') == self.not_included_str:
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
            return attachments
        else:
            return None

    def parse_fwd_from_id(self, msg):
        if msg.get('forwarded_from_id'):
            fwd_from_id = self.parse_user(msg['forwarded_from_id'], msg['forwarded_from'])
            return fwd_from_id
        else:
            return None