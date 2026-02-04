from sqlite_utils import Database

class DBHandler:
    def __init__(self, db_path):
        self.src_dict = {
            'vk': 1,
            'tg': 2,
            'wa': 3 }
        self.db = Database(db_path)
        self.msg_counter = 0
        if self.db['messages'].exists() == False:
            self.create_db()

    def create_db(self):
        self.db['chats'].create({
            'chat_id': int, # generated
            'chat_name': str,
            'last_msg_date': int,
            'msg_count': int,
            'chat_id_orig': int,
            'data_src': int
        }, pk='chat_id')
        self.db['messages'].create({
            'msg_id': int, # generated
            'chat_id': int, # generated
            'from_id': int, # generated
            'date': int,
            'text': str,
            'attachments': str,
            'fwd_messages': str,
            'is_service_msg': int,
            'edited': int,
            'has_formatting': int,
            'msg_id_orig': int,
            'chat_id_orig': int,
            'from_id_orig': int,
            'data_src': int
        }, pk='msg_id')
        self.db['usernames'].create({
            'user_id': int,
            'name': str,
            'orig_id': int,
            'data_src': int
        }, pk='user_id')
        self.db.create_view('messages_view', '''
            SELECT chats.chat_name,
            datetime(messages.date, "unixepoch", "localtime") AS "date",
            usernames.name AS "from",
            text,
            attachments,
            fwd_messages,
            messages.data_src
            FROM messages
            JOIN chats ON messages.chat_id = chats.chat_id
            JOIN usernames ON messages.from_id = usernames.user_id
            ORDER BY messages.date;
        ''')

    def insert_chat_to_db(self, chat_obj, data_src):
        msg_list = chat_obj['msg_list']
        self.db['messages'].insert_all({k: v for k, v in msg.items() } for msg in msg_list)
        last_msg = max(msg_list, key=lambda x:x['date'])
        self.db['chats'].insert({
            'chat_id_orig': chat_obj['id'],
            'chat_name': chat_obj['name'],
            'last_msg_date': last_msg['date'],
            'msg_count': len(msg_list),
            'data_src': self.src_dict[data_src]})
        self.msg_counter += len(msg_list)

    def insert_users_to_db(self, usernames_dict, data_src):
        new_users = []
        known_orig_ids = []
        for row in self.db['usernames'].rows:
            if row['data_src'] == self.src_dict[data_src]:
                known_orig_ids.append(row['orig_id'])
        for orig_id, username in usernames_dict.items():
            if orig_id not in known_orig_ids:
                new_users.append({
                    'name': username,
                    'orig_id': orig_id,
                    'data_src': self.src_dict[data_src]} )
        self.db['usernames'].insert_all({k: v for k, v in user.items() } for user in new_users)

    def update_ids_in_db(self):
        update_from_id_query = '''
        UPDATE messages
        SET from_id = (SELECT user_id FROM usernames WHERE orig_id = messages.from_id_orig)
        WHERE from_id IS NULL;
        '''
        update_chat_id_query = '''
        UPDATE messages
        SET chat_id = (SELECT chat_id FROM chats WHERE chat_id_orig = messages.chat_id_orig)
        WHERE chat_id IS NULL;
        '''
        with self.db.conn:
            self.db.execute(update_from_id_query)
            self.db.execute(update_chat_id_query)
